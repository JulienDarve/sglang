"""The Rust server's request records and crash dump through the real pyo3
boundary: a real `Server` built from `ServerArgs(request_log=..., crash_dump=True)`,
one request completed through the real ingress and egress calls, no model."""

import glob
import importlib
import json
import os
import pickle
import socket
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

import msgspec
import numpy as np
import requests

from sglang.srt.managers.io_struct import GenerateReqInput
from sglang.srt.rust_server import server as server_module
from sglang.srt.rust_server.config import request_log_settings
from sglang.srt.rust_server.server import RustServer
from sglang.test.ci.ci_register import register_cuda_ci
from sglang.test.rust_server_args import standalone_server_args
from sglang.test.test_utils import CustomTestCase, is_rust_server_built

# Where the Rust server extension is built; the test needs no GPU.
register_cuda_ci(est_time=10, stage="base-b", runner_config="1-gpu-small")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _server_args(ext, port: int):
    return standalone_server_args(
        ext,
        port=port,
        request_log=ext.RequestLogConfig(**request_log_settings(1, GenerateReqInput)),
        crash_dump=True,
    )


@unittest.skipUnless(is_rust_server_built(), "embedded rust server extension not built")
class TestRustServerCrashDumpBoundary(CustomTestCase):
    def test_a_completed_request_reaches_the_dump(self):
        ext = importlib.import_module("sglang.srt.rust_extensions._server")
        port = _free_port()
        server = ext.Server(_server_args(ext, port))
        try:
            # With the warmup skipped, the first scheduler message is the
            # post-startup FreezeGCReq; take it so the test sees its request.
            self.assertTrue(server.wait_request(timeout_ms=15000))
            (freeze_gc,) = server.recv_requests(1)
            self.assertEqual(msgspec.msgpack.decode(freeze_gc.header)[0], "FreezeGCReq")
            bridge = RustServer(server, http_port=port)
            bridge.server_args = SimpleNamespace(
                watermark_config=None, resolved_dict=dict
            )
            self.assertEqual(
                json.loads(server.crash_dump_snapshot()),
                {"bodies": {}, "finished": [], "unfinished": []},
            )
            payload = {
                "rid": "boundary",
                "input_ids": [1, 2, 3],
                "sampling_params": {"max_new_tokens": 1},
            }
            with requests.Session() as http, ThreadPoolExecutor(1) as client:
                http.trust_env = False
                response = client.submit(
                    http.post,
                    f"http://127.0.0.1:{port}/generate",
                    json=payload,
                    headers={"x-sgl-routing-key": "k"},
                    timeout=20,
                )
                self.assertTrue(server.wait_request(timeout_ms=15000))
                (ingress,) = server.recv_requests(1)
                admitted = bridge._admit(ingress.header, dict(ingress.buffers))
                in_flight = json.loads(server.crash_dump_snapshot())
                self.assertEqual(
                    [r["rid"] for r in in_flight["unfinished"]], ["boundary"]
                )
                header = msgspec.msgpack.encode(
                    [
                        [admitted.rid],
                        [{"type": "length", "length": 1}],
                        [len(admitted.input_ids)],
                        [1],
                        [0],
                        [0],
                    ]
                )
                self.assertTrue(
                    server.push_decode_result_batch(
                        header, [np.array([2], dtype="<i8").tobytes()]
                    )
                )
                self.assertEqual(response.result(timeout=5).status_code, 200)

            snapshot = json.loads(server.crash_dump_snapshot())
            (finished,) = snapshot["finished"]
            self.assertEqual(finished["rid"], "boundary")
            self.assertEqual(snapshot["bodies"][str(finished["body"])], payload)
            self.assertEqual(finished["out"]["output_ids"], [2])

            with (
                tempfile.TemporaryDirectory() as folder,
                patch.object(
                    server_module,
                    "get_observability",
                    return_value=SimpleNamespace(crash_dump_folder=folder),
                ),
            ):
                bridge.dump_requests_before_crash()
                (path,) = glob.glob(os.path.join(folder, "*", "crash_dump_*.pkl"))
                with open(path, "rb") as f:
                    ((obj, out, _, _),) = pickle.load(f)["requests"]
            self.assertIsInstance(obj, GenerateReqInput)
            self.assertEqual(obj.rid, "boundary")
            self.assertEqual(obj.input_ids, (1, 2, 3))
            self.assertEqual(out["meta_info"]["id"], "boundary")
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
