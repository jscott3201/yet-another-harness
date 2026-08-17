//! A minimal example plugin for the `yah:plugin@0.1.0` conformance world.
//!
//! This is an authoring example, not a fixture. The fixtures in
//! `crates/yah-plugin-wasm/guests` are hand-written component text chosen to
//! make one host bound observable; this is what a plugin author would actually
//! write, compiled by a real toolchain from the same WIT the host is generated
//! from. Its TypeScript counterpart answers identically, which is what makes
//! the world a contract rather than a Rust convention.
//!
//! It calls the logging and cancellation imports on purpose - a guest that
//! never calls back leaves the host's guest-to-host path unexercised, and that
//! path carries its own bounds. It does not call `capabilities`, so no
//! toolchain-portability claim exists for capability transport yet.

wit_bindgen::generate!({
    path: "../../../crates/yah-plugin-wasm/wit",
    world: "conformance",
});

use exports::yah::plugin::{fixture_tool, lifecycle};
use yah::plugin::cancellation::is_cancelled;
use yah::plugin::logging::{LogField, LogLevel, log};

struct Example;

impl lifecycle::Guest for Example {
    fn activate() -> Result<(), lifecycle::GuestError> {
        log(LogLevel::Info, "rust example activated", &[]);
        Ok(())
    }
}

impl fixture_tool::Guest for Example {
    /// Echo the request back inside a fixed envelope.
    ///
    /// The answer is built by hand rather than with a JSON library: what this
    /// example demonstrates is the world and the host's imports, and a
    /// serialiser here would be the largest thing in the component without
    /// showing anything the host cares about.
    fn invoke(input_json: String) -> Result<String, fixture_tool::GuestError> {
        // Cancellation is advisory and read-only, so a guest that means to be
        // interruptible has to ask. The host's own teardown does not depend on
        // this answer.
        if is_cancelled() {
            return Err(fixture_tool::GuestError {
                code: fixture_tool::ErrorCode::Cancelled,
                message: "host asked the guest to stop".to_owned(),
            });
        }
        if input_json.is_empty() {
            return Err(fixture_tool::GuestError {
                code: fixture_tool::ErrorCode::InvalidInput,
                message: "input-json was empty".to_owned(),
            });
        }
        // UTF-8 bytes. `String::len` is already that here; the TypeScript guest
        // has to ask for it, because a JavaScript string's length is UTF-16
        // code units. Both report the same quantity under the same name.
        log(
            LogLevel::Debug,
            "rust example invoked",
            &[LogField {
                key: "bytes".to_owned(),
                value: input_json.len().to_string(),
            }],
        );
        Ok(format!("{{\"echo\":{input_json},\"from\":\"rust\"}}"))
    }
}

export!(Example);
