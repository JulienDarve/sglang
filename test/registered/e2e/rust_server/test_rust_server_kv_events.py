"""The Rust server's `sglang.kv.v1.KvEvents` stream on a real engine carries the
batches of the scheduler's ZMQ endpoint, with the same sequence numbers."""

import json
import unittest

import msgspec
import requests
import zmq

from sglang.srt.disaggregation.kv_events import BlockRemoved, BlockStored, KVEventBatch
from sglang.srt.utils.network import get_free_port
from sglang.test.ci.ci_register import register_cuda_ci
from sglang.test.kv_event_stream import KvEventStream
from sglang.test.test_utils import (
    DEFAULT_SMALL_MODEL_NAME_FOR_TEST,
    DEFAULT_TIMEOUT_FOR_SERVER_LAUNCH,
    DEFAULT_URL_FOR_TEST,
    CustomTestCase,
    is_rust_server_built,
    popen_launch_server,
    terminate_and_kill_process_tree,
)

register_cuda_ci(est_time=90, stage="extra-a", runner_config="1-gpu-small")


def _zmq_events(payload: bytes) -> list:
    return [
        (
            type(event).__name__,
            (
                event.block_hashes
                if isinstance(event, (BlockStored, BlockRemoved))
                else []
            ),
        )
        for event in msgspec.msgpack.decode(payload, type=KVEventBatch).events
    ]


def _grpc_events(batch) -> list:
    events = []
    for event in batch.events:
        if event.HasField("block_stored"):
            hashes = event.block_stored.block_hashes
            events.append(("BlockStored", [h.signed_value for h in hashes]))
        elif event.HasField("block_removed"):
            hashes = event.block_removed.block_hashes
            events.append(("BlockRemoved", [h.signed_value for h in hashes]))
        else:
            events.append(("AllBlocksCleared", []))
    return events


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

    def test_grpc_stream_matches_zmq(self):
        stream = KvEventStream(self.grpc_port)
        self.addCleanup(stream.close)
        live = stream.subscribe(data_parallel_rank=0)
        self.addCleanup(live.cancel)
        self.assertTrue(next(live).HasField("start"))

        zmq_batches = {}
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
            self._generate("The capital of France is Paris. The capital of Italy is")
            self._post("/flush_cache")
            while True:
                _, seq, payload = subscriber.recv_multipart()
                events = _zmq_events(payload)
                zmq_batches[int.from_bytes(seq, "big")] = events
                if ("AllBlocksCleared", []) in events:
                    break
        last = max(zmq_batches)

        grpc_batches = {}
        while not grpc_batches or max(grpc_batches) < last:
            batch = next(live).batch
            grpc_batches[batch.sequence_number] = _grpc_events(batch)
        first = min(grpc_batches)
        self.assertEqual(sorted(grpc_batches), list(range(first, last + 1)))
        self.assertEqual({seq: grpc_batches[seq] for seq in zmq_batches}, zmq_batches)
        kinds = {kind for events in zmq_batches.values() for kind, _ in events}
        self.assertIn("BlockStored", kinds)

        # From the engine's replay buffer.
        resumed = stream.subscribe(data_parallel_rank=0, resume=0)
        self.addCleanup(resumed.cancel)
        next(resumed)
        replayed = {}
        while not replayed or max(replayed) < last:
            batch = next(resumed).batch
            replayed[batch.sequence_number] = _grpc_events(batch)
        self.assertEqual(sorted(replayed), list(range(last + 1)))
        self.assertEqual({seq: replayed[seq] for seq in grpc_batches}, grpc_batches)


if __name__ == "__main__":
    unittest.main()
