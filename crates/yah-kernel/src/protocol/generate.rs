use super::{ClientMessage, ProtocolFrame, ServerMessage};
use schemars::schema_for;
use std::path::Path;
use ts_rs::{Config, TS};

pub fn client_schema() -> String {
    let mut schema =
        serde_json::to_value(schema_for!(ClientMessage)).expect("client schema converts to JSON");
    constrain_protocol_versions(&mut schema);
    serde_json::to_string_pretty(&schema).expect("client schema serializes") + "\n"
}

fn constrain_protocol_versions(schema: &mut serde_json::Value) {
    let Some(command) = schema.pointer_mut("/$defs/Command") else {
        return;
    };
    for field in ["protocol_version", "payload_schema_version"] {
        if let Some(property) = command.pointer_mut(&format!("/properties/{field}")) {
            *property = serde_json::json!({ "type": "integer", "const": 1 });
        }
    }
}

pub fn server_schema() -> String {
    let mut schema =
        serde_json::to_value(schema_for!(ServerMessage)).expect("server schema converts to JSON");
    for (definition, field) in [("Receipt", "receipt_version"), ("Event", "event_version")] {
        if let Some(property) =
            schema.pointer_mut(&format!("/$defs/{definition}/properties/{field}"))
        {
            *property = serde_json::json!({ "type": "integer", "const": 1 });
        }
    }
    serde_json::to_string_pretty(&schema).expect("protocol schema serializes") + "\n"
}

pub fn typescript() -> String {
    let config = Config::default();
    let declarations = [
        super::ScopeKind::decl(&config),
        super::Scope::decl(&config),
        super::Target::decl(&config),
        super::DecimalU64::decl(&config),
        super::Rfc3339Timestamp::decl(&config),
        super::BoundedU32::decl(&config),
        super::ExpectedVersion::decl(&config),
        super::RunOpenPayload::decl(&config),
        super::RunCloseOutcome::decl(&config),
        super::RunClosePayload::decl(&config),
        super::WorkItemCreatePayload::decl(&config),
        super::UnitAdmitPayload::decl(&config),
        super::UnitDispatchPayload::decl(&config),
        super::ProgressReportPayload::decl(&config),
        super::UnitPayload::decl(&config),
        super::CommandBody::decl(&config),
        super::Command::decl(&config),
        super::ReceiptOutcome::decl(&config),
        super::ErrorKind::decl(&config),
        super::Error::decl(&config),
        super::Receipt::decl(&config),
        super::StreamClass::decl(&config),
        super::Actor::decl(&config),
        super::Event::decl(&config),
        super::CursorExpired::decl(&config),
        super::AdapterLimits::decl(&config),
        super::Retention::decl(&config),
        super::SubscriptionOpened::decl(&config),
        super::SubscriptionPending::decl(&config),
        super::SlowConsumer::decl(&config),
        super::SubscriptionClosed::decl(&config),
        ClientMessage::decl(&config),
        ServerMessage::decl(&config),
        ProtocolFrame::decl(&config),
    ];
    let declarations = declarations.map(export_type);
    format!(
        "// Generated from yah-kernel Rust protocol types. Do not edit.\n\nexport type JsonValue = null | boolean | number | string | JsonValue[] | {{ [key: string]: JsonValue }};\n\n{}\n",
        declarations.join("\n\n")
    )
}

fn export_type(declaration: String) -> String {
    declaration.replacen("type ", "export type ", 1)
}

pub fn check_checked_in(root: &Path) -> Result<(), String> {
    check(
        &root.join("generated/protocol/client.schema.json"),
        &client_schema(),
    )?;
    check(
        &root.join("generated/protocol/server.schema.json"),
        &server_schema(),
    )?;
    check(&root.join("generated/protocol/protocol.ts"), &typescript())
}

fn check(path: &Path, expected: &str) -> Result<(), String> {
    let actual = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read generated artifact {}: {e}", path.display()))?;
    if actual != expected {
        return Err(format!(
            "generated artifact {} differs; run cargo run --locked -p yah-kernel --bin generate-protocol",
            path.display()
        ));
    }
    Ok(())
}
