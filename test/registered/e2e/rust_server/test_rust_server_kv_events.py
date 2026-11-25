"""The Rust server's `sglang.kv.v1.KvEvents` stream on a real engine carries the
scheduler's batches with the sequence numbers of its ZMQ endpoint, and its
snapshot holds the blocks that the stream stored."""

import json
import time
import unittest
from typing import Optional

import grpc
import msgspec
import requests
import zmq
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from sglang.srt.disaggregation.kv_events import AllBlocksCleared, KVEventBatch
from sglang.srt.utils.network import get_free_port
from sglang.test.ci.ci_register import register_cuda_ci
from sglang.test.test_utils import (
    DEFAULT_SMALL_MODEL_NAME_FOR_TEST,
    DEFAULT_TIMEOUT_FOR_SERVER_LAUNCH,
    DEFAULT_URL_FOR_TEST,
    CustomTestCase,
    is_rust_server_built,
    popen_launch_server,
    terminate_and_kill_process_tree,
)

register_cuda_ci(est_time=90, stage="base-b", runner_config="1-gpu-small")

_FIELD = descriptor_pb2.FieldDescriptorProto

# The fields this test reads (proto/sglang/kv/v1/kv_events.proto); the rest decode
# as unknown fields. Per message, (name, number, scalar type or message name, repeated).
_MESSAGES = {
    "KvEventCursor": [("next_sequence_number", 2, _FIELD.TYPE_UINT64, False)],
    "SubscribeKvEventsRequest": [("resume", 2, "KvEventCursor", False)],
    "KvEventStreamStart": [],
    "AllBlocksCleared": [],
    "BlockHash": [("signed_value", 2, _FIELD.TYPE_INT64, False)],
    "BlockStored": [("block_hashes", 1, "BlockHash", True)],
    "KvCacheEvent": [
        ("block_stored", 1, "BlockStored", False),
        ("all_blocks_cleared", 3, "AllBlocksCleared", False),
    ],
    "KvEventBatch": [
        ("sequence_number", 1, _FIELD.TYPE_UINT64, False),
        ("events", 4, "KvCacheEvent", True),
    ],
    "SubscribeKvEventsResponse": [
        ("start", 1, "KvEventStreamStart", False),
        ("batch", 2, "KvEventBatch", False),
    ],
    "GetKvSnapshotRequest": [],
    "KvSnapshotBlocks": [("stored", 1, "BlockStored", True)],
    "KvSnapshotEnd": [
        ("resume", 1, "KvEventCursor", False),
        ("complete", 2, _FIELD.TYPE_BOOL, False),
    ],
    "GetKvSnapshotResponse": [
        ("blocks", 1, "KvSnapshotBlocks", False),
        ("end", 2, "KvSnapshotEnd", False),
    ],
}


def _message_classes() -> dict:
    file = descriptor_pb2.FileDescriptorProto(
        name="sglang/kv/v1/kv_events_test.proto",
        package="sglang.kv.v1",
        syntax="proto3",
    )
    for name, fields in _MESSAGES.items():
        message = file.message_type.add(name=name)
        for field_name, number, kind, repeated in fields:
            field = message.field.add(
                name=field_name,
                number=number,
                label=_FIELD.LABEL_REPEATED if repeated else _FIELD.LABEL_OPTIONAL,
            )
            if isinstance(kind, str):
                field.type = _FIELD.TYPE_MESSAGE
                field.type_name = f".sglang.kv.v1.{kind}"
            else:
                field.type = kind
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return {
        name: message_factory.GetMessageClass(
            pool.FindMessageTypeByName(f"sglang.kv.v1.{name}")
        )
        for name in _MESSAGES
    }


class KvEventStream:
    """`SubscribeKvEvents` and `GetKvSnapshot` on the Rust server's gRPC port."""

    def __init__(self, grpc_port: int):
        self.messages = _message_classes()
        self.channel = grpc.insecure_channel(
            f"127.0.0.1:{grpc_port}", options=[("grpc.enable_http_proxy", 0)]
        )
        self._subscribe = self.channel.unary_stream(
            "/sglang.kv.v1.KvEvents/SubscribeKvEvents",
            request_serializer=lambda request: request.SerializeToString(),
            response_deserializer=self.messages["SubscribeKvEventsResponse"].FromString,
        )
        self._snapshot = self.channel.unary_stream(
            "/sglang.kv.v1.KvEvents/GetKvSnapshot",
            request_serializer=lambda request: request.SerializeToString(),
            response_deserializer=self.messages["GetKvSnapshotResponse"].FromString,
        )

    def subscribe(self, resume: Optional[int] = None):
        """Rank 0's responses; `resume` is a sequence number in the current stream."""
        request = self.messages["SubscribeKvEventsRequest"]()
        if resume is not None:
            request.resume.SetInParent()
            request.resume.next_sequence_number = resume
        return self._subscribe(request, timeout=60)

    def snapshot(self):
        """Rank 0's snapshot: the hashes of its blocks, and its end."""
        request = self.messages["GetKvSnapshotRequest"]()
        *blocks, last = self._snapshot(request, timeout=60)
        hashes = [
            block_hash.signed_value
            for response in blocks
            for stored in response.blocks.stored
            for block_hash in stored.block_hashes
        ]
        return hashes, last.end

    def close(self) -> None:
        self.channel.close()


@unittest.skipUnless(is_rust_server_built(), "embedded rust server extension not built")
class TestRustServerKvEvents(CustomTestCase):
    @classmethod
    def setUpClass(cls):
        cls.process = None
        cls.zmq_port = get_free_port()
        cls.grpc_port = get_free_port()
        kv_events_config = {"publisher": "zmq", "endpoint": f"tcp://*:{cls.zmq_port}"}
        cls.process = popen_launch_server(
            DEFAULT_SMALL_MODEL_NAME_FOR_TEST,
            DEFAULT_URL_FOR_TEST,
            timeout=DEFAULT_TIMEOUT_FOR_SERVER_LAUNCH,
            other_args=[
                "--grpc-port",
                str(cls.grpc_port),
                "--kv-events-config",
                json.dumps(kv_events_config),
            ],
            env={"SGLANG_RUST_SERVER": "1"},
        )

    @classmethod
    def tearDownClass(cls):
        if cls.process is not None:
            terminate_and_kill_process_tree(cls.process)

    def _post(self, path: str, body=None):
        response = requests.post(f"{DEFAULT_URL_FOR_TEST}{path}", json=body, timeout=60)
        self.assertEqual(response.status_code, 200, response.text)

    def _generate(self, text: str):
        sampling_params = {"temperature": 0, "max_new_tokens": 8}
        self._post("/generate", {"text": text, "sampling_params": sampling_params})

    def _numbers_to_the_clear(self, responses, cleared: int) -> list:
        """The stream's next sequence numbers up to `cleared`, the cache clear's
        number on ZMQ; the batch with that number must hold the clear here too."""
        batches = [next(responses).batch]
        while batches[-1].sequence_number < cleared:
            batches.append(next(responses).batch)
        self.assertTrue(
            any(event.HasField("all_blocks_cleared") for event in batches[-1].events)
        )
        return [batch.sequence_number for batch in batches]

    def test_grpc_stream_carries_the_zmq_sequence_numbers(self):
        stream = KvEventStream(self.grpc_port)
        self.addCleanup(stream.close)
        with zmq.Context() as context, context.socket(zmq.SUB) as subscriber:
            subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
            subscriber.setsockopt(zmq.RCVTIMEO, 30_000)
            subscriber.connect(f"tcp://127.0.0.1:{self.zmq_port}")
            # A SUB socket misses what is published before it joins.
            for attempt in range(60):
                if subscriber.poll(500):
                    break
                self._generate(f"Warm-up {attempt}: name three colors.")
            else:
                self.fail("no KV event batch reached the ZMQ subscriber")

            live = stream.subscribe()
            self.addCleanup(live.cancel)
            self.assertTrue(next(live).HasField("start"))
            self._generate("The capital of France is Paris. The capital of Italy is")
            self._post("/flush_cache")
            while True:
                _, seq, payload = subscriber.recv_multipart()
                batch = msgspec.msgpack.decode(payload, type=KVEventBatch)
                if AllBlocksCleared() in batch.events:
                    break
        cleared = int.from_bytes(seq, "big")

        numbers = self._numbers_to_the_clear(live, cleared)
        self.assertEqual(numbers, list(range(numbers[0], cleared + 1)))

        # From the engine's replay buffer.
        resumed = stream.subscribe(resume=0)
        self.addCleanup(resumed.cancel)
        self.assertTrue(next(resumed).HasField("start"))
        numbers = self._numbers_to_the_clear(resumed, cleared)
        self.assertEqual(numbers, list(range(cleared + 1)))

        # The snapshot holds the blocks the stream stored since the clear, up
        # to its cursor; the blocks follow the stream on their own task.
        self._generate("The capital of Spain is Madrid. The capital of Peru is")
        for _ in range(300):
            snapshot, end = stream.snapshot()
            if snapshot:
                break
            time.sleep(0.1)
        else:
            self.fail("the snapshot holds no block of the request")
        self.assertTrue(end.complete)
        stored = [
            block_hash.signed_value
            for _ in range(cleared + 1, end.resume.next_sequence_number)
            for event in next(resumed).batch.events
            for block_hash in event.block_stored.block_hashes
        ]
        self.assertEqual(sorted(snapshot), sorted(stored))


if __name__ == "__main__":
    unittest.main()
