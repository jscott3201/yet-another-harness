//! MILE-001 kernel (gate G01): command/state/effect/cancellation semantics
//! proven with zero live model calls and zero real external effects.
//!
//! Obligation map (MILE-001 card §"Exit criteria" → module):
//! - `ids`, `error` — shared vocabulary every fence obligation asserts against
//!   (rows 3, A3b, A4, A10)
//! - `provider` — normalized events, two wire dialects, deterministic fake
//!   provider (Component 1; the 13-row fixture catalog)
//! - `effect` — external-effect ledger + fake-effect backend (Component 2;
//!   rows 5, A5, A13, A16)
//! - `cancel` — §5 cancellation records and the scope rules that are pure
//!   functions of a frozen scope (obligation 4; rows A6, A14)
//! - `funnel`, `state` — single mutation funnel over Selene (rows 1, 2, 7, 8,
//!   A8–A11, I16, V1)
//! - `adapter` — in-process serialization-round-trip adapter + ADR-002 §17
//!   corpus (Component 3; rows 6a, 6b)
//!
//! Modules land in dependency order; a module absent from the tree is not
//! built yet, not implied.

pub mod cancel;
pub mod effect;
pub mod error;
pub mod funnel;
pub mod ids;
pub mod protocol;
pub mod provider;
pub mod store;
