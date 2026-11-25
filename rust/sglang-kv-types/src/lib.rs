//! Generated bindings for SGLang's `sglang.kv.v1` package, the KV cache event
//! service (prost + tonic only). `src/generated/` is written by `cargo gen-api`
//! from `proto/sglang/kv/v1/`; edit the .proto file and regenerate, never
//! these files.

/// Version of this crate, which versions the `sglang.kv.v1` schema
/// independently of SGLang releases.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod kv {
    // Generated code optimizes for template uniformity, not lint cleanliness.
    #[allow(clippy::all)]
    pub mod v1 {
        include!("generated/sglang.kv.v1.rs");
        include!("generated/sglang.kv.v1.tonic.rs");
    }
}
