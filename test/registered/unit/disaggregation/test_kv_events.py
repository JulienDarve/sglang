"""Unit tests for srt/disaggregation/kv_events KV-event publisher rank selection.

Covers the data-parallel rank used to offset each scheduler's KV-event
publisher port, across pure DP, DP-attention, and single-replica modes. The
port offset must make every independent KV cache publish on a distinct port so
the router can subscribe per replica (the `dp_size` it reads from
`/server_info`).
"""

import atexit
import importlib
import json
import threading
import time
import unittest
from unittest.mock import patch

import grpc
import msgspec
import zmq

from sglang.srt.disaggregation.kv_events import (
    AllBlocksCleared,
    BlockRemoved,
    BlockStored,
    EventBatch,
    EventPublisherFactory,
    KVEventBatch,
    NullEventPublisher,
    StorageMedium,
    ZmqEventPublisher,
    kv_event_namespace_seed,
    namespaced_block_hash,
    resolve_load_pub_range,
)
from sglang.srt.runtime_context import describe_kv_events_publisher
from sglang.srt.server_args import ServerArgs
from sglang.srt.utils.network import get_free_port
from sglang.test.ci.ci_register import register_cpu_ci
from sglang.test.kv_event_stream import KvEventStream
from sglang.test.rust_server_args import standalone_server_args
from sglang.test.test_utils import CustomTestCase, is_rust_server_built

register_cpu_ci(est_time=16, suite="base-a-test-cpu")


class TestLocalKvEventSource(CustomTestCase):
    def _publisher(self, attn_dp_rank=0, **config):
        publisher = EventPublisherFactory.create(
            json.dumps({"publisher": "zmq", **config}), attn_dp_rank=attn_dp_rank
        )
        atexit.unregister(publisher.shutdown)
        self.addCleanup(publisher.shutdown)
        return publisher

    def test_advertised_source_delivers_events(self):
        for host, bind, topic in (("*", None, "kv"), ("127.0.0.1", True, "")):
            with self.subTest(host=host):
                with zmq.Context.instance().socket(zmq.PUB) as probe:
                    port = probe.bind_to_random_port("tcp://127.0.0.1")
                publisher = self._publisher(
                    attn_dp_rank=4,
                    endpoint=f"tcp://{host}:{port - 4}",
                    bind=bind,
                    topic=topic,
                )
                source = publisher.describe_local_source(64)
                self.assertEqual(
                    source,
                    dict(
                        dp_rank=4,
                        endpoint=f"tcp://127.0.0.1:{port}",
                        topic=topic,
                        block_size=64,
                    ),
                )
                self._assert_event_received(publisher, source)

    def _assert_event_received(self, publisher, source):
        with zmq.Context() as context, context.socket(zmq.SUB) as subscriber:
            subscriber.setsockopt_string(zmq.SUBSCRIBE, source["topic"])
            subscriber.connect(source["endpoint"])
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                publisher.publish(KVEventBatch(ts=1.0, events=[AllBlocksCleared()]))
                if subscriber.poll(100):
                    topic, _, payload = subscriber.recv_multipart()
                    self.assertEqual(topic, source["topic"].encode())
                    batch = msgspec.msgpack.decode(payload, type=KVEventBatch)
                    self.assertEqual(batch.attn_dp_rank, source["dp_rank"])
                    self.assertEqual(batch.events, [AllBlocksCleared()])
                    break
            else:
                self.fail("No event received from the advertised local source")


class TestNullPublisherConfig(CustomTestCase):
    def test_config_without_a_real_publisher_builds_the_noop(self):
        """A config that names the "null" publisher, or omits the key (the schema
        default), builds the no-op publisher instead of failing at startup."""
        for config in ({"publisher": "null"}, {"endpoint": "tcp://*:5557"}):
            with self.subTest(config=config):
                publisher = EventPublisherFactory.create(json.dumps(config))
                self.assertIsInstance(publisher, NullEventPublisher)


class TestOffsetEndpointPort(CustomTestCase):
    def test_ipc_endpoint_gets_a_dp_suffix(self):
        self.assertEqual(
            ZmqEventPublisher.offset_endpoint_port("ipc:///tmp/kv_events", 2),
            "ipc:///tmp/kv_events_dp2",
        )


class TestShutdown(CustomTestCase):
    def test_full_queue_does_not_skip_socket_cleanup(self):
        with patch.object(threading.Thread, "start"):
            publisher = ZmqEventPublisher(
                attn_dp_rank=0, endpoint="inproc://kv-shutdown-test", max_queue_size=1
            )
        atexit.unregister(publisher.shutdown)
        publisher.publish(KVEventBatch(ts=1.0, events=[]))
        publisher.shutdown()
        self.assertTrue(publisher._pub.closed)


class TestResolveLoadPubRange(CustomTestCase):
    """The single source of truth both the bind and /server_info route through."""

    def test_reason_is_set_only_for_actionable_declines(self):
        # Off by default is unremarkable (no reason); an opt-in the operator
        # asked for that can't resolve is worth surfacing.
        _, quiet = resolve_load_pub_range(
            kv_endpoint="tcp://10.0.0.5:5557", replay_endpoint=None, dp_size=1
        )
        self.assertIsNone(quiet)
        _, auto_loud = resolve_load_pub_range(
            kv_endpoint="tcp://10.0.0.5:5557",  # connect-style: can't derive
            replay_endpoint=None,
            dp_size=1,
            load_publish_endpoint="auto",
        )
        self.assertIsNotNone(auto_loud)
        # A missing config surfaces at startup — no message may render a bare
        # "None". Both the likely mistakes (auto and an explicit address
        # without --kv-events-config) go through this.
        for endpoint in ("auto", "tcp://*:7000"):
            with self.subTest(endpoint=endpoint):
                _, no_cfg = resolve_load_pub_range(
                    kv_endpoint=None,
                    replay_endpoint=None,
                    dp_size=1,
                    load_publish_endpoint=endpoint,
                )
                self.assertIsNotNone(no_cfg)
                self.assertNotIn("None", no_cfg)
                self.assertIn("--kv-events-config", no_cfg)


class TestBlockStoredWireFormat(CustomTestCase):
    def _event(self, **extra):
        return BlockStored(
            block_hashes=[123],
            parent_block_hash=None,
            token_ids=[1, 2],
            block_size=2,
            lora_id=None,
            medium=StorageMedium.GPU,
            **extra,
        )

    def test_one_decoder_reads_a_mixed_batch(self):
        batch = KVEventBatch(
            ts=1.0,
            events=[
                self._event(),
                self._event(cache_salt="tenant-a"),
                self._event(session_id="session-a"),
                BlockRemoved(block_hashes=[123], medium=StorageMedium.GPU),
                AllBlocksCleared(),
            ],
        )
        round_tripped = msgspec.msgpack.decode(
            msgspec.msgpack.encode(batch), type=KVEventBatch
        )
        stored = round_tripped.events[:3]
        self.assertEqual([e.cache_salt for e in stored], [None, "tenant-a", None])
        self.assertEqual([e.session_id for e in stored], [None, None, "session-a"])
        self.assertIsInstance(round_tripped.events[3], BlockRemoved)
        self.assertIsInstance(round_tripped.events[4], AllBlocksCleared)


class TestRustServerDecodedFields(CustomTestCase):
    def test_event_fields_match_the_rust_server_decoder(self):
        """The Rust server's KvEvents relay decodes exactly these fields
        (rust/sglang-server/src/api_server/grpc/kv_events/wire.rs) and drops any
        other; a new one belongs there and in proto/sglang/kv/v1 too."""
        self.assertEqual(EventBatch.__struct_fields__, ("ts", "events", "attn_dp_rank"))
        self.assertEqual(
            {
                event.__struct_config__.tag: event.__struct_fields__
                for event in (BlockStored, BlockRemoved, AllBlocksCleared)
            },
            {
                "BlockStored": (
                    "block_hashes",
                    "parent_block_hash",
                    "token_ids",
                    "block_size",
                    "lora_id",
                    "medium",
                    "cache_salt",
                    "session_id",
                    "lora_name",
                    "mm_routing_spans",
                ),
                "BlockRemoved": ("block_hashes", "medium"),
                "AllBlocksCleared": (),
            },
        )


class TestNamespacedBlockHash(CustomTestCase):
    def test_values_are_pinned(self):
        # Rust has its own copy of this function, and its test uses the same
        # values. Thus the two copies must give the same hashes, and the hashes
        # must not change.
        seed = kv_event_namespace_seed(extra_key="lora-a", cache_salt=None)
        self.assertEqual(
            namespaced_block_hash(123, namespace_seed=seed), 9220659119954863560
        )
        self.assertEqual(
            namespaced_block_hash(-5, namespace_seed=seed), -1810974732006142875
        )


class TestReplay(CustomTestCase):
    def test_descriptor_advertises_replay_port(self):
        def descriptor(**cfg):
            cfg = msgspec.json.encode({"publisher": "zmq", **cfg}).decode()
            args = ServerArgs(model_path="dummy", page_size=16, kv_events_config=cfg)
            return describe_kv_events_publisher(args)

        self.assertNotIn("replay_endpoint_port_base", descriptor())
        self.assertEqual(
            descriptor(replay_endpoint="tcp://*:6000")["replay_endpoint_port_base"],
            6000,
        )

    def test_router_serves_buffered_batches_then_end_seq(self):
        # The router's DEALER client relies on this exact framing.
        replay = f"tcp://127.0.0.1:{get_free_port()}"
        publisher = ZmqEventPublisher(
            attn_dp_rank=0,
            endpoint="inproc://kv-replay-test",
            replay_endpoint=replay,
            topic="kv@prefill@model",
        )
        dealer = zmq.Context.instance().socket(zmq.DEALER)
        dealer.setsockopt(zmq.RCVTIMEO, 5000)
        dealer.connect(replay)
        try:
            for _ in range(3):
                publisher.publish(KVEventBatch(ts=1.0, events=[AllBlocksCleared()]))
            publisher._event_queue.join()
            dealer.send_multipart([b"", (1).to_bytes(8, "big")])
            frames = [dealer.recv_multipart() for _ in range(3)]
        finally:
            dealer.close(linger=0)
            publisher.shutdown()
        self.assertEqual([len(f) for f in frames], [4, 4, 4])
        seqs = [int.from_bytes(f[2], "big", signed=True) for f in frames]
        self.assertEqual(seqs, [1, 2, -1])
        self.assertEqual([f[0] for f in frames], [b""] * 3)
        self.assertEqual([f[1] for f in frames], [b"kv@prefill@model"] * 2 + [b""])
        for frame in frames[:2]:
            batch = msgspec.msgpack.decode(frame[3], type=KVEventBatch)
            self.assertEqual(batch.events, [AllBlocksCleared()])
            self.assertEqual(batch.attn_dp_rank, 0)
        self.assertEqual(frames[2][3], b"")

    def test_full_replay_reaches_a_reader_that_reads_after_it_is_sent(self):
        """A replay of the whole buffer keeps every batch and the end marker
        although the reader takes nothing until the burst is sent, also after
        earlier replays leave ZMQ's read acknowledgements stale."""
        steps = 2500
        replay = f"tcp://127.0.0.1:{get_free_port()}"
        publisher = ZmqEventPublisher(
            attn_dp_rank=0,
            endpoint="inproc://kv-full-replay-test",
            replay_endpoint=replay,
            buffer_steps=steps,
        )
        atexit.unregister(publisher.shutdown)
        self.addCleanup(publisher.shutdown)
        dealer = zmq.Context.instance().socket(zmq.DEALER)
        self.addCleanup(dealer.close, linger=0)
        dealer.connect(replay)
        # About 3 KB each, more than the socket buffers hold in all.
        stored = BlockStored(
            block_hashes=[1],
            parent_block_hash=None,
            token_ids=list(range(1000)),
            block_size=1000,
            lora_id=None,
        )
        batch = KVEventBatch(ts=1.0, events=[stored])
        for _ in range(steps):
            publisher.publish(batch)
        publisher._event_queue.join()

        for first in range(4):
            dealer.send_multipart([b"", (0).to_bytes(8, "big")])
            self.assertTrue(dealer.poll(5000), "the replay did not start")
            # The publisher thread sends the whole replay before this batch.
            publisher.publish(batch)
            publisher._event_queue.join()
            numbers = []
            while True:
                self.assertTrue(
                    dealer.poll(1000), f"replay stalled after {numbers[-1:]}"
                )
                seq = int.from_bytes(dealer.recv_multipart()[2], "big", signed=True)
                if seq == -1:
                    break
                numbers.append(seq)
            self.assertEqual(numbers, list(range(first, first + steps)))


def _rust_server(ext, grpc_port: int, kv_event_relay):
    return ext.Server(
        standalone_server_args(
            ext,
            port=get_free_port(),
            grpc_port=grpc_port,
            deployment_info=json.dumps({"kv_event_relay": kv_event_relay}),
        )
    )


@unittest.skipUnless(is_rust_server_built(), "embedded rust server extension not built")
class TestRustServerKvEventStream(CustomTestCase):
    """The Rust server's `sglang.kv.v1.KvEvents` stream of the real publisher."""

    def setUp(self):
        self.ext = importlib.import_module("sglang.srt.rust_extensions._server")
        self.grpc_port = get_free_port()
        self.stream = KvEventStream(self.grpc_port)
        self.addCleanup(self.stream.close)

    def _subscribe(self, resume=None):
        responses = self.stream.subscribe(data_parallel_rank=2, resume=resume)
        self.addCleanup(responses.cancel)
        return responses

    def test_stream_carries_the_zmq_sequence_numbers(self):
        # Rank 2 publishes on the configured port + 2.
        endpoint = f"tcp://*:{get_free_port() - 2}"
        publisher = EventPublisherFactory.create(
            json.dumps({"publisher": "zmq", "endpoint": endpoint}),
            attn_dp_rank=2,
            local_relay=True,
        )
        atexit.unregister(publisher.shutdown)
        self.addCleanup(publisher.shutdown)
        server = _rust_server(
            self.ext, self.grpc_port, publisher.describe_local_relay(16)
        )
        self.addCleanup(server.shutdown)
        published = 0

        def publish():
            nonlocal published
            publisher.publish(KVEventBatch(ts=2.5, events=[AllBlocksCleared()]))
            published += 1

        with zmq.Context() as context, context.socket(zmq.SUB) as subscriber:
            subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
            subscriber.setsockopt(zmq.RCVTIMEO, 5000)
            subscriber.connect(publisher.describe_local_source(16)["endpoint"])
            # A SUB socket misses what is published before it joins.
            for _ in range(100):
                publish()
                if subscriber.poll(50):
                    break
            live = self._subscribe()
            self.assertEqual(next(live).start.data_parallel_rank, 2)
            for _ in range(5):
                publish()
            last = published - 1
            zmq_numbers = [-1]
            while zmq_numbers[-1] < last:
                zmq_numbers.append(
                    int.from_bytes(subscriber.recv_multipart()[1], "big")
                )

        grpc_batches = [next(live).batch]
        while grpc_batches[-1].sequence_number < last:
            grpc_batches.append(next(live).batch)
        grpc_numbers = [batch.sequence_number for batch in grpc_batches]
        # Each number once and in order, the last five as on ZMQ.
        self.assertEqual(grpc_numbers, list(range(grpc_numbers[0], last + 1)))
        self.assertEqual(grpc_numbers[-5:], zmq_numbers[-5:])
        self.assertEqual(
            {(batch.ts, batch.data_parallel_rank) for batch in grpc_batches}, {(2.5, 2)}
        )

        # From the publisher's replay buffer.
        resumed = self._subscribe(resume=0)
        next(resumed)
        self.assertEqual(
            [next(resumed).batch.sequence_number for _ in range(published)],
            list(range(published)),
        )

    def test_kv_events_off_advertise_and_serve_no_stream(self):
        for config in (None, json.dumps({"publisher": "null"})):
            publisher = EventPublisherFactory.create(config, local_relay=True)
            self.assertIsNone(publisher.describe_local_relay(16))
        server = _rust_server(self.ext, self.grpc_port, None)
        self.addCleanup(server.shutdown)
        with self.assertRaises(grpc.RpcError) as error:
            next(self._subscribe())
        self.assertEqual(error.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)


if __name__ == "__main__":
    unittest.main()
