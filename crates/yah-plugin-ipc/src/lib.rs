//! The bounded framed bidirectional worker protocol (v1).
//!
//! This crate is the wire contract between the YAH host and a supervised
//! worker process (Node or CPython), specified before either worker runtime
//! exists. The Rust types here are the source of truth: they generate the
//! checked-in JSON Schema and TypeScript under `generated/worker-protocol/`,
//! and the fixture corpus in `tests/` pins the semantics a worker SDK must
//! satisfy — handshake, calls, streams, cancellation, errors, artifact
//! spill, and resource handles.
//!
//! Everything is sans-io on purpose. [`frame`] turns bytes into frames and
//! back, incrementally and with every bound checked before allocation;
//! [`session`] is a pure state machine over decoded frames with an injected
//! millisecond clock. The process driver (IPC-002) supplies the socket or
//! pipe, the spawned child, and the kill path; nothing in this crate can
//! block, sleep, or spawn.
//!
//! What this crate does not do: authenticate the peer (the bootstrap fd and
//! peer-credential checks belong to the process driver), supervise or
//! restart a worker, or define any capability's semantics. Capability
//! refusals are answers inside a successful call, exactly as in the Wasm
//! lane; the closed [`types::WireErrorKind`] family names only failures of
//! the protocol boundary itself.

pub mod constants;
pub mod frame;
pub mod generate;
pub mod session;
pub mod strict;
pub mod types;

pub use constants::*;
