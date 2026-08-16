//! Host-side bindings generated from the canonical WIT world.
//!
//! The generator reads [`crate::WIT_WORLD`] out of the same `wit/` directory the
//! contract tests parse, so host glue and contract evidence cannot drift apart.

wasmtime::component::bindgen!({
    path: "wit",
    world: "conformance",
});
