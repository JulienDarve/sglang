"""A test client for the Rust server's `sglang.kv.v1.KvEvents` stream, built from
the fields tests read (proto/sglang/kv/v1/kv_events.proto); the rest decode as
unknown fields."""

from typing import Optional

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

_FIELD = descriptor_pb2.FieldDescriptorProto

# Per message, (name, number, scalar type or message name, repeated).
_MESSAGES = {
    "KvEventCursor": [
        ("stream_id", 1, _FIELD.TYPE_STRING, False),
        ("next_sequence_number", 2, _FIELD.TYPE_UINT64, False),
    ],
    "SubscribeKvEventsRequest": [
        ("data_parallel_rank", 1, _FIELD.TYPE_UINT32, False),
        ("resume", 2, "KvEventCursor", False),
    ],
    "KvEventStreamStart": [
        ("stream_id", 1, _FIELD.TYPE_STRING, False),
        ("data_parallel_rank", 2, _FIELD.TYPE_UINT32, False),
    ],
    "BlockHash": [("signed_value", 2, _FIELD.TYPE_INT64, False)],
    "BlockStored": [("block_hashes", 1, "BlockHash", True)],
    "BlockRemoved": [("block_hashes", 1, "BlockHash", True)],
    "AllBlocksCleared": [],
    "KvCacheEvent": [
        ("block_stored", 1, "BlockStored", False),
        ("block_removed", 2, "BlockRemoved", False),
        ("all_blocks_cleared", 3, "AllBlocksCleared", False),
    ],
    "KvEventBatch": [
        ("sequence_number", 1, _FIELD.TYPE_UINT64, False),
        ("ts", 2, _FIELD.TYPE_DOUBLE, False),
        ("data_parallel_rank", 3, _FIELD.TYPE_UINT32, False),
        ("events", 4, "KvCacheEvent", True),
    ],
    "SubscribeKvEventsResponse": [
        ("start", 1, "KvEventStreamStart", False),
        ("batch", 2, "KvEventBatch", False),
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
    """`SubscribeKvEvents` on the Rust server's gRPC port."""

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

    def subscribe(
        self, data_parallel_rank: int, resume: Optional[int] = None, timeout=60
    ):
        """The responses; `resume` is a sequence number in the current stream."""
        request = self.messages["SubscribeKvEventsRequest"](
            data_parallel_rank=data_parallel_rank
        )
        if resume is not None:
            request.resume.SetInParent()
            request.resume.next_sequence_number = resume
        return self._subscribe(request, timeout=timeout)

    def close(self) -> None:
        self.channel.close()
