"""Unit tests for srt/disaggregation/kv_events KV-event publisher rank selection.

Covers the data-parallel rank used to offset each scheduler's KV-event
publisher port, across pure DP, DP-attention, and single-replica modes. The
port offset must make every independent KV cache publish on a distinct port so
the router can subscribe per replica (the `dp_size` it reads from
`/server_info`).
"""

import atexit
import json
import os
import threading
import time
import unittest
from typing import get_args, get_type_hints
from unittest.mock import patch

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
from sglang.test.test_utils import CustomTestCase

register_cpu_ci(est_time=12, suite="base-a-test-cpu")

RUST_KV_EVENT_BATCH_FIXTURE = os.path.join(
    os.path.dirname(__file__),
    *[os.pardir] * 4,
    "rust/sglang-server/testdata/kv_event_batch.hex",
)


class TestLocalKvEventSource(CustomTestCase):
    def _publisher(self, attn_dp_rank=0, local_relay=False, **config):
        publisher = EventPublisherFactory.create(
            json.dumps({"publisher": "zmq", **config}),
            attn_dp_rank=attn_dp_rank,
            local_relay=local_relay,
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

    def test_private_sockets_carry_the_stream_for_the_rust_server(self):
        """The private sockets the Rust server relays carry the published batches
        and a replay; only a publishing zmq publisher binds them."""
        for config in (None, json.dumps({"publisher": "null"})):
            off = EventPublisherFactory.create(config, local_relay=True)
            self.assertIsNone(off.describe_local_relay(16))

        publisher = self._publisher(local_relay=True, endpoint="inproc://kv-relay-test")
        relay = publisher.describe_local_relay(16)
        self.assertEqual((relay["dp_rank"], relay["block_size"]), (0, 16))
        self._assert_event_received(publisher, {**relay, "topic": ""})

        # No replay endpoint is configured: the private one still answers.
        dealer = zmq.Context.instance().socket(zmq.DEALER)
        self.addCleanup(dealer.close, linger=0)
        dealer.setsockopt(zmq.RCVTIMEO, 5000)
        dealer.connect(relay["replay_endpoint"])
        dealer.send_multipart([b"", (0).to_bytes(8, "big")])
        self.assertEqual(dealer.recv_multipart()[2], (0).to_bytes(8, "big"))


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
                attn_dp_rank=0,
                endpoint="inproc://kv-shutdown-test",
                max_queue_size=1,
                local_relay=True,
            )
        atexit.unregister(publisher.shutdown)
        publisher.publish(KVEventBatch(ts=1.0, events=[]))
        publisher.shutdown()
        self.assertTrue(publisher._pub.closed)
        self.assertFalse(os.path.exists(publisher._relay_dir))


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
        """A new event type fails the Rust server's decoder (kv_events/wire.rs), a new
        field is dropped: add both there, in proto/sglang/kv/v1 and in the fixture."""
        self.assertEqual(EventBatch.__struct_fields__, ("ts", "events", "attn_dp_rank"))
        (events,) = get_args(get_type_hints(KVEventBatch)["events"])
        self.assertEqual(
            {
                event.__struct_config__.tag: event.__struct_fields__
                for event in get_args(events)
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
        batch = KVEventBatch(
            ts=1.5,
            attn_dp_rank=3,
            events=[
                BlockStored(
                    block_hashes=[-7, 2**62],
                    parent_block_hash=-1,
                    token_ids=[1, 2, 2**40, 3],
                    block_size=2,
                    lora_id=5,
                    medium=StorageMedium.CPU,
                    cache_salt="salt",
                    session_id="session",
                    lora_name="adapter",
                    mm_routing_spans=[[(0, 1, "image")], []],
                ),
                # An EAGLE page: (token, next token) pairs.
                BlockStored(
                    block_hashes=[11],
                    parent_block_hash=None,
                    token_ids=[(1, 2), (2, 3)],
                    block_size=2,
                    lora_id=None,
                ),
                BlockRemoved(block_hashes=[-7], medium=StorageMedium.GPU),
                AllBlocksCleared(),
            ],
        )
        with open(RUST_KV_EVENT_BATCH_FIXTURE) as fixture:
            self.assertEqual(
                msgspec.msgpack.encode(batch).hex(), fixture.read().strip()
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


if __name__ == "__main__":
    unittest.main()
