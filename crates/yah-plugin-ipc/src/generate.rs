//! Checked-in artifact generation, the kernel protocol's pattern: the Rust
//! types are the source of truth, and the generated JSON Schema and
//! TypeScript under `generated/worker-protocol/` make every wire-shape
//! change visible in code review. `tools/protocol-codegen --check` fails
//! the gate when they drift.

// `super`, not `crate`: in this crate the parent is the crate root, and in
// `tools/protocol-codegen` this file is `#[path]`-mounted beside `types` in
// a `worker` module, where `crate::` would resolve to the tool.
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
    format!(
        "// Generated from yah-plugin-ipc Rust protocol types. Do not edit.\n\nexport type JsonValue = null | boolean | number | string | JsonValue[] | {{ [key: string]: JsonValue }};\n\n{}\n",
        declarations.join("\n\n")
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
