//! Checked-in artifact generation, the kernel protocol's pattern: the Rust
//! types are the source of truth, and the generated JSON Schema and
//! TypeScript under `generated/worker-protocol/` make every wire-shape
//! change visible in code review. `tools/protocol-codegen --check` fails
//! the gate when they drift.

// `super`, not `crate`: in this crate the parent is the crate root, and in
// `tools/protocol-codegen` this file is `#[path]`-mounted beside `types` in
// a `worker` module, where `crate::` would resolve to the tool.
use super::constants::*;
use super::types::*;
use schemars::schema_for;
use std::path::Path;
use ts_rs::{Config, TS};

pub fn worker_schema() -> String {
    let schema =
        serde_json::to_value(schema_for!(WorkerMessage)).expect("worker schema converts to JSON");
    serde_json::to_string_pretty(&schema).expect("worker schema serializes") + "\n"
}

pub fn host_schema() -> String {
    let schema =
        serde_json::to_value(schema_for!(HostMessage)).expect("host schema converts to JSON");
    serde_json::to_string_pretty(&schema).expect("host schema serializes") + "\n"
}

pub fn typescript() -> String {
    let config = Config::default();
    let declarations = [
        CallId::decl(&config),
        HandleId::decl(&config),
        Hello::decl(&config),
        Accept::decl(&config),
        Refuse::decl(&config),
        WireLimits::decl(&config),
        Ceilings::decl(&config),
        Call::decl(&config),
        Reply::decl(&config),
        Outcome::decl(&config),
        CancelReason::decl(&config),
        StreamOpen::decl(&config),
        StreamData::decl(&config),
        StreamClass::decl(&config),
        Credit::decl(&config),
        Cancel::decl(&config),
        CancelTarget::decl(&config),
        Release::decl(&config),
        ReleaseAck::decl(&config),
        HandleKind::decl(&config),
        ArtifactOffer::decl(&config),
        Goodbye::decl(&config),
        WireError::decl(&config),
        WireErrorKind::decl(&config),
        WorkerMessage::decl(&config),
        HostMessage::decl(&config),
    ];
    let declarations = declarations.map(export_type);
    let metadata = format!(
        concat!(
            "export const PROTOCOL_VERSION = {PROTOCOL_VERSION} as const;\n\n",
            "export const FRAME_PREFIX_BYTES = {FRAME_PREFIX_BYTES} as const;\n\n",
            "export const MAX_WIRE_ID = {MAX_WIRE_ID} as const;\n\n",
            "export const MAX_WIRE_UINT32 = {MAX_WIRE_UINT32} as const;\n\n",
            "export const DEFAULT_WIRE_LIMITS = Object.freeze({{\n",
            "  max_frame_bytes: {MAX_FRAME_BYTES},\n",
            "  max_control_frame_bytes: {MAX_CONTROL_FRAME_BYTES},\n",
            "  max_call_payload_bytes: {MAX_CALL_PAYLOAD_BYTES},\n",
            "  max_inline_result_bytes: {MAX_INLINE_RESULT_BYTES},\n",
            "  max_stream_data_bytes: {MAX_STREAM_DATA_BYTES},\n",
            "  max_artifact_read_bytes: {MAX_ARTIFACT_READ_BYTES},\n",
            "}} as const satisfies Readonly<WireLimits>);\n\n",
            "export const DEFAULT_CEILINGS = Object.freeze({{\n",
            "  host_calls_in_flight: {DEFAULT_HOST_CALLS_IN_FLIGHT},\n",
            "  worker_calls_in_flight: {DEFAULT_WORKER_CALLS_IN_FLIGHT},\n",
            "  live_handles: {DEFAULT_LIVE_HANDLES},\n",
            "  initial_stream_credit: {INITIAL_STREAM_CREDIT},\n",
            "  max_stream_credit: {MAX_STREAM_CREDIT},\n",
            "}} as const satisfies Readonly<Ceilings>);\n\n",
            "export const WIRE_FIELD_LIMITS = Object.freeze({{\n",
            "  error_detail_chars: {MAX_ERROR_DETAIL_CHARS},\n",
            "  method_chars: {MAX_METHOD_CHARS},\n",
            "  media_type_chars: {MAX_MEDIA_TYPE_CHARS},\n",
            "  sdk_identity_chars: {MAX_SDK_IDENTITY_CHARS},\n",
            "  goodbye_reason_chars: {MAX_GOODBYE_REASON_CHARS},\n",
            "}} as const);",
        ),
        PROTOCOL_VERSION = PROTOCOL_VERSION,
        FRAME_PREFIX_BYTES = FRAME_PREFIX_BYTES,
        MAX_WIRE_ID = MAX_WIRE_ID,
        MAX_WIRE_UINT32 = MAX_WIRE_UINT32,
        MAX_FRAME_BYTES = MAX_FRAME_BYTES,
        MAX_CONTROL_FRAME_BYTES = MAX_CONTROL_FRAME_BYTES,
        MAX_CALL_PAYLOAD_BYTES = MAX_CALL_PAYLOAD_BYTES,
        MAX_INLINE_RESULT_BYTES = MAX_INLINE_RESULT_BYTES,
        MAX_STREAM_DATA_BYTES = MAX_STREAM_DATA_BYTES,
        MAX_ARTIFACT_READ_BYTES = MAX_ARTIFACT_READ_BYTES,
        DEFAULT_HOST_CALLS_IN_FLIGHT = DEFAULT_HOST_CALLS_IN_FLIGHT,
        DEFAULT_WORKER_CALLS_IN_FLIGHT = DEFAULT_WORKER_CALLS_IN_FLIGHT,
        DEFAULT_LIVE_HANDLES = DEFAULT_LIVE_HANDLES,
        INITIAL_STREAM_CREDIT = INITIAL_STREAM_CREDIT,
        MAX_STREAM_CREDIT = MAX_STREAM_CREDIT,
        MAX_ERROR_DETAIL_CHARS = MAX_ERROR_DETAIL_CHARS,
        MAX_METHOD_CHARS = MAX_METHOD_CHARS,
        MAX_MEDIA_TYPE_CHARS = MAX_MEDIA_TYPE_CHARS,
        MAX_SDK_IDENTITY_CHARS = MAX_SDK_IDENTITY_CHARS,
        MAX_GOODBYE_REASON_CHARS = MAX_GOODBYE_REASON_CHARS,
    );
    format!(
        "// Generated from yah-plugin-ipc Rust protocol types and constants. Do not edit.\n\n{metadata}\n\nexport type JsonValue = null | boolean | number | string | JsonValue[] | {{ [key: string]: JsonValue }};\n\n{}\n",
        declarations.join("\n\n"),
    )
}

fn export_type(declaration: String) -> String {
    declaration.replacen("type ", "export type ", 1)
}

pub fn check_checked_in(root: &Path) -> Result<(), String> {
    check(
        &root.join("generated/worker-protocol/worker.schema.json"),
        &worker_schema(),
    )?;
    check(
        &root.join("generated/worker-protocol/host.schema.json"),
        &host_schema(),
    )?;
    check(
        &root.join("generated/worker-protocol/protocol.ts"),
        &typescript(),
    )
}

fn check(path: &Path, expected: &str) -> Result<(), String> {
    let actual = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read generated artifact {}: {e}", path.display()))?;
    if actual != expected {
        return Err(format!(
            "generated artifact {} differs; run cargo run --locked -p yah-plugin-ipc --bin generate-worker-protocol",
            path.display()
        ));
    }
    Ok(())
}
