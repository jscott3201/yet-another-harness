use chrono::DateTime;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;
use ts_rs::TS;

pub type ExtraFields = BTreeMap<String, Value>;
pub type JsonObject = BTreeMap<String, Value>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct Scope {
    pub scope_kind: ScopeKind,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub scope_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum ScopeKind {
    Global,
    Project,
    Run,
    Unit,
}

impl ScopeKind {
    pub(crate) fn wire(self) -> &'static str {
        match self {
            ScopeKind::Global => "global",
            ScopeKind::Project => "project",
            ScopeKind::Run => "run",
            ScopeKind::Unit => "unit",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct Target {
    pub aggregate_kind: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub aggregate_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct ExpectedVersion {
    pub aggregate_kind: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub aggregate_id: String,
    pub version: DecimalU64,
}

#[derive(Clone, Debug, PartialEq, Eq, TS)]
#[ts(type = "string")]
pub struct DecimalU64(String);

impl DecimalU64 {
    pub fn new(value: u64) -> Self {
        Self(value.to_string())
    }

    pub fn get(&self) -> u64 {
        self.0.parse().expect("validated decimal u64")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, TS)]
#[ts(type = "string")]
pub struct Rfc3339Timestamp(String);

impl Rfc3339Timestamp {
    pub fn new(value: String) -> Result<Self, String> {
        let parsed = DateTime::parse_from_rfc3339(&value)
            .map_err(|e| format!("invalid RFC 3339 timestamp: {e}"))?;
        if parsed.offset().local_minus_utc() != 0
            || parsed.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string() != value
        {
            return Err("timestamp must be UTC with millisecond precision".into());
        }
        Ok(Self(value))
    }
}

impl Serialize for Rfc3339Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Rfc3339Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for Rfc3339Timestamp {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "Rfc3339Timestamp".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        serde_json::from_value(serde_json::json!({
            "type": "string",
            "format": "date-time",
            "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$"
        }))
        .expect("RFC 3339 timestamp schema is valid")
    }
}

impl TryFrom<String> for DecimalU64 {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty()
            || (value.len() > 1 && value.starts_with('0'))
            || !value.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(format!("non-canonical u64 string: {value:?}"));
        }
        value
            .parse::<u64>()
            .map_err(|_| format!("u64 string out of range: {value:?}"))?;
        Ok(Self(value))
    }
}

impl From<DecimalU64> for String {
    fn from(value: DecimalU64) -> Self {
        value.0
    }
}

impl Serialize for DecimalU64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DecimalU64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        DecimalU64::try_from(value).map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for DecimalU64 {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "DecimalU64".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        serde_json::from_value(serde_json::json!({
            "type": "string",
            "pattern": "^(0|[1-9][0-9]{0,18}|1[0-7][0-9]{18}|18[0-3][0-9]{17}|184[0-3][0-9]{16}|1844[0-5][0-9]{15}|18446[0-6][0-9]{14}|184467[0-3][0-9]{13}|1844674[0-3][0-9]{12}|184467440[0-6][0-9]{10}|1844674407[0-2][0-9]{9}|18446744073[0-6][0-9]{8}|1844674407370[0-8][0-9]{6}|18446744073709[0-4][0-9]{5}|184467440737095[0-4][0-9]{4}|18446744073709550[0-9]{3}|18446744073709551[0-5][0-9]{2}|1844674407370955160[0-9]|1844674407370955161[0-5])$",
            "maxLength": 20
        }))
        .expect("decimal u64 schema is valid")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
pub struct Command {
    #[ts(type = "1")]
    pub protocol_version: BoundedU32,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub command_id: String,
    pub scope: Scope,
    #[ts(type = "1")]
    pub payload_schema_version: BoundedU32,
    pub target: Target,
    #[schemars(length(max = 8))]
    pub expected_versions: Vec<ExpectedVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub attempt_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub authority_epoch: Option<DecimalU64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub deadline: Option<Rfc3339Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub causation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub trace_context: Option<String>,
    #[serde(flatten)]
    #[ts(flatten)]
    pub body: CommandBody,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^blake3:[0-9a-f]{64}$"))]
    pub request_digest: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, TS)]
#[ts(type = "number")]
pub struct BoundedU32(u32);

impl BoundedU32 {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Serialize for BoundedU32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.0)
    }
}

impl<'de> Deserialize<'de> for BoundedU32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        u32::deserialize(deserializer).map(Self)
    }
}

impl JsonSchema for BoundedU32 {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "BoundedU32".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        serde_json::from_value(serde_json::json!({
            "type": "integer",
            "minimum": 0,
            "maximum": 4_294_967_295_u64
        }))
        .expect("bounded u32 schema is valid")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(tag = "command_type", content = "payload")]
pub enum CommandBody {
    #[serde(rename = "run.open")]
    RunOpen(RunOpenPayload),
    #[serde(rename = "run.close")]
    RunClose(RunClosePayload),
    #[serde(rename = "work_item.create")]
    WorkItemCreate(WorkItemCreatePayload),
    #[serde(rename = "unit.admit")]
    UnitAdmit(UnitAdmitPayload),
    #[serde(rename = "unit.dispatch")]
    UnitDispatch(UnitDispatchPayload),
    #[serde(rename = "unit.progress_report")]
    ProgressReport(ProgressReportPayload),
    #[serde(rename = "unit.stamp_bump")]
    StampBump(UnitPayload),
}

impl CommandBody {
    pub(crate) fn command_type(&self) -> CommandType {
        match self {
            CommandBody::RunOpen(_) => CommandType::RunOpen,
            CommandBody::RunClose(_) => CommandType::RunClose,
            CommandBody::WorkItemCreate(_) => CommandType::WorkItemCreate,
            CommandBody::UnitAdmit(_) => CommandType::UnitAdmit,
            CommandBody::UnitDispatch(_) => CommandType::UnitDispatch,
            CommandBody::ProgressReport(_) => CommandType::ProgressReport,
            CommandBody::StampBump(_) => CommandType::StampBump,
        }
    }

    pub(crate) fn payload_json(&self) -> Value {
        match self {
            CommandBody::RunOpen(payload) => serde_json::to_value(payload),
            CommandBody::RunClose(payload) => serde_json::to_value(payload),
            CommandBody::WorkItemCreate(payload) => serde_json::to_value(payload),
            CommandBody::UnitAdmit(payload) => serde_json::to_value(payload),
            CommandBody::UnitDispatch(payload) => serde_json::to_value(payload),
            CommandBody::ProgressReport(payload) => serde_json::to_value(payload),
            CommandBody::StampBump(payload) => serde_json::to_value(payload),
        }
        .expect("command payload serializes")
    }

    pub(crate) fn extra_fields(&self) -> &ExtraFields {
        match self {
            CommandBody::RunOpen(payload) => &payload.extra,
            CommandBody::RunClose(payload) => &payload.extra,
            CommandBody::WorkItemCreate(payload) => &payload.extra,
            CommandBody::UnitAdmit(payload) => &payload.extra,
            CommandBody::UnitDispatch(payload) => &payload.extra,
            CommandBody::ProgressReport(_) => empty_extra_fields(),
            CommandBody::StampBump(payload) => &payload.extra,
        }
    }
}

fn empty_extra_fields() -> &'static ExtraFields {
    static EMPTY: std::sync::OnceLock<ExtraFields> = std::sync::OnceLock::new();
    EMPTY.get_or_init(ExtraFields::new)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct RunOpenPayload {
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub run_id: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub goal_work_item_id: String,
    #[serde(flatten)]
    #[ts(flatten)]
    pub extra: ExtraFields,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct RunClosePayload {
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub run_id: String,
    pub outcome: RunCloseOutcome,
    #[serde(flatten)]
    #[ts(flatten)]
    pub extra: ExtraFields,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum RunCloseOutcome {
    ClosedSuccess,
    ClosedFailure,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct WorkItemCreatePayload {
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub work_item_id: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^blake3:[0-9a-f]{64}$"))]
    pub acceptance_contract_digest: String,
    pub declared_write_scope: Vec<String>,
    #[serde(flatten)]
    #[ts(flatten)]
    pub extra: ExtraFields,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct UnitAdmitPayload {
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub unit_id: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub work_item_id: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub run_id: String,
    #[serde(flatten)]
    #[ts(flatten)]
    pub extra: ExtraFields,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct UnitDispatchPayload {
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub unit_id: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub holder_id: String,
    #[serde(flatten)]
    #[ts(flatten)]
    pub extra: ExtraFields,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct ProgressReportPayload {
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub unit_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct UnitPayload {
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_.:-]+$"))]
    pub unit_id: String,
    #[serde(flatten)]
    #[ts(flatten)]
    pub extra: ExtraFields,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CommandType {
    RunOpen,
    RunClose,
    WorkItemCreate,
    UnitAdmit,
    UnitDispatch,
    ProgressReport,
    StampBump,
}

impl CommandType {
    pub(crate) fn wire(&self) -> &'static str {
        match self {
            CommandType::RunOpen => "run.open",
            CommandType::RunClose => "run.close",
            CommandType::WorkItemCreate => "work_item.create",
            CommandType::UnitAdmit => "unit.admit",
            CommandType::UnitDispatch => "unit.dispatch",
            CommandType::ProgressReport => "unit.progress_report",
            CommandType::StampBump => "unit.stamp_bump",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
pub struct Receipt {
    pub command_id: String,
    pub scope: Scope,
    pub outcome: ReceiptOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub state_version: Option<DecimalU64>,
    pub event_cursors: Vec<DecimalU64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub error: Option<Error>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub result: Option<JsonObject>,
    #[ts(type = "1")]
    pub receipt_version: BoundedU32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptOutcome {
    Accepted,
    Completed,
    Replayed,
    Rejected,
    OutcomeUnknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
pub struct Error {
    pub kind: ErrorKind,
    pub reason: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub retry_after_ms: Option<BoundedU32>,
    pub reconcile_required: bool,
    pub occurrence_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub details: Option<JsonObject>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    InvalidRequest,
    Unauthenticated,
    Unauthorized,
    NotFound,
    VersionConflict,
    IdempotencyConflict,
    FenceRejected,
    JournalImmutable,
    PolicyDenied,
    CapabilityUnsupported,
    ApprovalRequired,
    ResourceExhausted,
    PayloadTooLarge,
    CursorExpired,
    SlowConsumer,
    Cancelled,
    DeadlineExceeded,
    Unavailable,
    OutcomeUnknown,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
pub struct Event {
    pub event_id: String,
    pub cursor: DecimalU64,
    pub stream_class: StreamClass,
    pub aggregate_kind: String,
    pub aggregate_id: String,
    pub aggregate_version: DecimalU64,
    pub ordinal: BoundedU32,
    pub event_kind: String,
    #[ts(type = "1")]
    pub event_version: BoundedU32,
    pub occurred_at: Rfc3339Timestamp,
    pub actor: Actor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub command_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub causation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub correlation_id: Option<String>,
    pub payload: JsonObject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum StreamClass {
    DurableSemantic,
    Progress,
    Telemetry,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct Actor {
    pub principal_kind: String,
    pub principal_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct CursorExpired {
    pub min_retained_cursor: DecimalU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct AdapterLimits {
    pub durable_event_queue_capacity: BoundedU32,
    pub progress_event_queue_capacity: BoundedU32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct Retention {
    pub min_retained_cursor: DecimalU64,
    pub max_age_seconds: DecimalU64,
    pub max_events_per_project: DecimalU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct SubscriptionOpened {
    pub subscription_id: String,
    pub limits: AdapterLimits,
    pub retention: Retention,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct SubscriptionPending {
    pub subscription_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct SlowConsumer {
    pub min_retained_cursor: DecimalU64,
    pub last_delivered_cursor: DecimalU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
pub struct SubscriptionClosed {
    pub subscription_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SubscriptionPoll {
    Event(Box<Event>),
    Pending,
    SlowConsumer(SlowConsumer),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum ClientMessage {
    Command(Box<Command>),
    Resume {
        project_id: String,
        after_cursor: DecimalU64,
    },
    Subscribe {
        project_id: String,
        after_cursor: DecimalU64,
    },
    SubscriptionPoll {
        subscription_id: String,
    },
    SubscriptionClose {
        subscription_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum ServerMessage {
    Receipt(Receipt),
    Events(Vec<Event>),
    Event(Event),
    CursorExpired(CursorExpired),
    SubscriptionOpened(SubscriptionOpened),
    SubscriptionPending(SubscriptionPending),
    SlowConsumer(SlowConsumer),
    SubscriptionClosed(SubscriptionClosed),
    Error(Error),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(untagged)]
pub enum ProtocolFrame {
    Client(ClientMessage),
    Server(Box<ServerMessage>),
}
