//! Host-side bindings generated from the canonical WIT world.
//!
//! The generator reads [`crate::WIT_WORLD`] out of the same `wit/` directory the
//! contract tests parse, so host glue and contract evidence cannot drift apart.
//!
//! Exports are generated async so a guest call runs on its own stack rather
//! than on the thread polling its future. The WIT world itself stays
//! synchronous: nothing here declares `async func`, so this is Wasmtime's fiber
//! support, not Component Model async. That distinction is what keeps the
//! JavaScript toolchain able to build a guest for this world.
//!
//! Imports stay synchronous. `logging` and `cancellation` return without
//! waiting on anything, so making them async would buy a suspension point
//! neither of them can use.

wasmtime::component::bindgen!({
    path: "wit",
    world: "conformance",
    exports: { default: async },
});
