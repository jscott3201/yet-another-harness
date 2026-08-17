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
//! waiting on anything, and `capabilities` must stay synchronous for a reason
//! beyond symmetry: a synchronous host import cannot suspend the fiber its
//! guest call runs on, so a capability call can never be parked while holding
//! a scope-activity admission, and the activation's pre-cleanup drain can
//! never wait on a future nobody polls. Generating these imports async would
//! reintroduce exactly that hang.
//!
//! The `with` remap makes the generated host traits speak
//! [`crate::capability::GrantedCapability`] directly, so the store's resource
//! table holds the real entry type rather than an uninhabited marker. The key
//! is the versioned interface ID joined to the resource name with a dot.

wasmtime::component::bindgen!({
    path: "wit",
    world: "conformance",
    exports: { default: async },
    with: { "yah:plugin/capabilities@0.1.0.capability": crate::capability::GrantedCapability },
});
