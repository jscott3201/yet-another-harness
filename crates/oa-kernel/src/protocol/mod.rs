//! ADR-002 application protocol and Adapter 1.
//!
//! The protocol's re-exported Rust types are the source of truth. [`InProcessAdapter`]
//! still crosses a JSON encode/decode boundary before it invokes the funnel;
//! direct Rust calls are not a conforming Adapter 1 path.

mod adapter;
pub mod generate;
mod json;
mod translate;
mod types;

pub use adapter::{InProcessAdapter, request_digest};
pub use types::*;

pub const PROTOCOL_VERSION: BoundedU32 = BoundedU32::new(1);
pub const PAYLOAD_SCHEMA_VERSION: BoundedU32 = BoundedU32::new(1);
pub const RECEIPT_VERSION: BoundedU32 = BoundedU32::new(1);
pub const EVENT_VERSION: BoundedU32 = BoundedU32::new(1);

pub const MAX_COMMAND_BYTES: usize = 1024 * 1024;
pub const MAX_FRAME_BYTES: usize = MAX_COMMAND_BYTES + 4096;
pub const MAX_CONTROL_FRAME_BYTES: usize = 16 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 256 * 1024;
pub const MAX_RESULT_BYTES: usize = 4096;
pub const MAX_ERROR_DETAIL_CHARS: usize = 512;
pub const MAX_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 16 * 1024;
pub const DEFAULT_DURABLE_QUEUE_CAPACITY: usize = 1024;
pub const DEFAULT_PROGRESS_QUEUE_CAPACITY: usize = 256;
pub const MAX_RESUME_EVENTS: usize = 1024;
pub const MAX_RESUME_BYTES: usize = MAX_FRAME_BYTES;
pub const MAX_DURABLE_SUBSCRIPTIONS: usize = 64;
pub const MAX_DURABLE_SUBSCRIPTION_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_AGE_SECONDS: u64 = 1_209_600;
pub const DEFAULT_MAX_EVENTS_PER_PROJECT: u64 = 1_000_000;
