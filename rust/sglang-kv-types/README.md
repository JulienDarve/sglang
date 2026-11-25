# sglang-kv-types crate

Rust bindings for SGLang's `sglang.kv.v1` API: the prost messages and the tonic
client and server for `KvEvents`. `SubscribeKvEvents` streams a scheduler's KV
cache events with the same sequence numbers as its ZMQ publisher, and
`GetKvSnapshot` returns the blocks its device cache holds, with the cursor to
subscribe from. The bindings are generated
from `proto/sglang/kv/v1` by `cargo gen-api` and checked in, so building the
crate needs no `protoc` or build script. SGLang's own Rust server uses this same
crate.

The gRPC client lives at
`sglang_kv_types::kv::v1::kv_events_client::KvEventsClient`. The public API uses
tonic 0.14 and prost 0.14 types.

`sglang-kv-types` is versioned on its own semver track, independent of SGLang
and `sglang-api-types` releases: a minor bump for any schema, generated-API or
dependency change, a patch bump for documentation or metadata only. No workflow
publishes it yet: its first release to crates.io is a maintainer step.
