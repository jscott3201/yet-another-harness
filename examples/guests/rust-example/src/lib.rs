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
//! path carries its own bounds. It calls `capabilities` for the same reason:
//! `cap:`-prefixed requests acquire the brokered capability, invoke it, and
//! release it by scope drop, so the resource half of the world is entered by
//! authored code under a real toolchain, not only by the hand-written fixture.

wit_bindgen::generate!({
    path: "../../../crates/yah-plugin-wasm/wit",
    world: "conformance",
});

use exports::yah::plugin::{fixture_tool, lifecycle};
use yah::plugin::cancellation::is_cancelled;
use yah::plugin::capabilities::{AcquireErrorCode, CallErrorCode, acquire};
use yah::plugin::logging::{LogField, LogLevel, log};

/// The capability both example guests consume, granted or not by the test rig.
const CAPABILITY_ID: &str = "example.text-echo/v1";

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
        if let Some(request) = input_json.strip_prefix("cap:") {
            return Ok(answer_through_capability(request));
        }
        Ok(format!("{{\"echo\":{input_json},\"from\":\"rust\"}}"))
    }
}

/// Acquire the capability, invoke it once, and answer - never trap.
///
/// A refusal is an answer, not a guest error: the world already carries the
/// broker's decision in the error record, and a guest that turned it into a
/// trap would erase the difference between "the host said no" and "the guest
/// broke". The handle is released by scope drop at the end of this function,
/// which is the whole release story in Rust - the generated `Capability` owns
/// its handle and its `Drop` emits the resource drop the host counts.
///
/// The output is spliced raw into a quoted position, so the answer is
/// well-formed JSON only while the provider's output needs no string
/// escaping - true of every `echo:` answer. Both guests share that limit on
/// purpose: an output that needed escaping would malform both answers
/// identically rather than split them.
fn answer_through_capability(request: &str) -> String {
    let capability = match acquire(CAPABILITY_ID) {
        Ok(capability) => capability,
        Err(refusal) => {
            return format!(
                "{{\"capability-refused\":\"{}\",\"from\":\"rust\"}}",
                acquire_code(refusal.code)
            );
        }
    };
    match capability.invoke(request) {
        Ok(output) => format!("{{\"capability\":\"{output}\",\"from\":\"rust\"}}"),
        Err(failure) => format!(
            "{{\"capability-failed\":\"{}\",\"from\":\"rust\"}}",
            call_code(failure.code)
        ),
    }
}

/// The WIT names, spelled by hand.
///
/// wit-bindgen renders enum cases as UpperCamelCase Rust variants, and the
/// TypeScript guest receives the same cases as kebab-case strings with nothing
/// to map. A `Debug`-format shortcut here would answer `NotGranted` where the
/// other guest answers `not-granted`. The call codes' guard is the corpus's
/// two provider refusals; the acquire codes' is the ungranted activation's
/// exact `not-granted` answer - one arm of these six, which is as far as the
/// tests reach today.
fn acquire_code(code: AcquireErrorCode) -> &'static str {
    match code {
        AcquireErrorCode::InvalidId => "invalid-id",
        AcquireErrorCode::NotGranted => "not-granted",
        AcquireErrorCode::Revoked => "revoked",
        AcquireErrorCode::Unavailable => "unavailable",
        AcquireErrorCode::Mismatched => "mismatched",
        AcquireErrorCode::HandleLimit => "handle-limit",
    }
}

fn call_code(code: CallErrorCode) -> &'static str {
    match code {
        CallErrorCode::Revoked => "revoked",
        CallErrorCode::Exhausted => "exhausted",
        CallErrorCode::InvalidInput => "invalid-input",
        CallErrorCode::Failed => "failed",
    }
}

export!(Example);
