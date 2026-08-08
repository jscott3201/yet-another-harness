// Generated from oa-kernel Rust protocol types. Do not edit.

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type ScopeKind = "global" | "project" | "run" | "unit";

export type Scope = { scope_kind: ScopeKind, scope_id: string, };

export type Target = { aggregate_kind: string, aggregate_id: string, };

export type DecimalU64 = string;

export type Rfc3339Timestamp = string;

export type BoundedU32 = number;

export type ExpectedVersion = { aggregate_kind: string, aggregate_id: string, version: DecimalU64, };

export type RunOpenPayload = { run_id: string, goal_work_item_id: string, } & ({ [key in string]: number | string | boolean | Array<JsonValue> | { [key in string]: JsonValue } | null });

export type RunCloseOutcome = "closed_success" | "closed_failure" | "cancelled";

export type RunClosePayload = { run_id: string, outcome: RunCloseOutcome, } & ({ [key in string]: number | string | boolean | Array<JsonValue> | { [key in string]: JsonValue } | null });

export type WorkItemCreatePayload = { work_item_id: string, acceptance_contract_digest: string, declared_write_scope: Array<string>, } & ({ [key in string]: number | string | boolean | Array<JsonValue> | { [key in string]: JsonValue } | null });

export type UnitAdmitPayload = { unit_id: string, work_item_id: string, run_id: string, } & ({ [key in string]: number | string | boolean | Array<JsonValue> | { [key in string]: JsonValue } | null });

export type UnitDispatchPayload = { unit_id: string, holder_id: string, } & ({ [key in string]: number | string | boolean | Array<JsonValue> | { [key in string]: JsonValue } | null });

export type ProgressReportPayload = { unit_id: string, };

export type UnitPayload = { unit_id: string, } & ({ [key in string]: number | string | boolean | Array<JsonValue> | { [key in string]: JsonValue } | null });

export type CommandBody = { "command_type": "run.open", "payload": RunOpenPayload } | { "command_type": "run.close", "payload": RunClosePayload } | { "command_type": "work_item.create", "payload": WorkItemCreatePayload } | { "command_type": "unit.admit", "payload": UnitAdmitPayload } | { "command_type": "unit.dispatch", "payload": UnitDispatchPayload } | { "command_type": "unit.progress_report", "payload": ProgressReportPayload } | { "command_type": "unit.stamp_bump", "payload": UnitPayload };

export type Command = { protocol_version: 1, command_id: string, scope: Scope, payload_schema_version: 1, target: Target, expected_versions: Array<ExpectedVersion>, attempt_token?: string | null, authority_epoch?: DecimalU64 | null, deadline?: Rfc3339Timestamp | null, causation_id?: string | null, correlation_id?: string | null, trace_context?: string | null, request_digest: string, } & ({ "command_type": "run.open", "payload": RunOpenPayload } | { "command_type": "run.close", "payload": RunClosePayload } | { "command_type": "work_item.create", "payload": WorkItemCreatePayload } | { "command_type": "unit.admit", "payload": UnitAdmitPayload } | { "command_type": "unit.dispatch", "payload": UnitDispatchPayload } | { "command_type": "unit.progress_report", "payload": ProgressReportPayload } | { "command_type": "unit.stamp_bump", "payload": UnitPayload });

export type ReceiptOutcome = "accepted" | "completed" | "replayed" | "rejected" | "outcome_unknown";

export type ErrorKind = "invalid_request" | "unauthenticated" | "unauthorized" | "not_found" | "version_conflict" | "idempotency_conflict" | "fence_rejected" | "journal_immutable" | "policy_denied" | "capability_unsupported" | "approval_required" | "resource_exhausted" | "payload_too_large" | "cursor_expired" | "slow_consumer" | "cancelled" | "deadline_exceeded" | "unavailable" | "outcome_unknown" | "internal";

export type Error = { kind: ErrorKind, reason: string, message: string, retryable: boolean, retry_after_ms?: BoundedU32 | null, reconcile_required: boolean, occurrence_id: string, details?: { [key in string]: JsonValue } | null, };

export type Receipt = { command_id: string, scope: Scope, outcome: ReceiptOutcome, state_version?: DecimalU64 | null, event_cursors: Array<DecimalU64>, error?: Error | null, result?: { [key in string]: JsonValue } | null, receipt_version: 1, };

export type StreamClass = "durable_semantic" | "progress" | "telemetry";

export type Actor = { principal_kind: string, principal_id: string, };

export type Event = { event_id: string, cursor: DecimalU64, stream_class: StreamClass, aggregate_kind: string, aggregate_id: string, aggregate_version: DecimalU64, ordinal: BoundedU32, event_kind: string, event_version: 1, occurred_at: Rfc3339Timestamp, actor: Actor, command_id?: string | null, causation_id?: string | null, correlation_id?: string | null, payload: { [key in string]: JsonValue }, };

export type CursorExpired = { min_retained_cursor: DecimalU64, };

export type AdapterLimits = { durable_event_queue_capacity: BoundedU32, progress_event_queue_capacity: BoundedU32, };

export type Retention = { min_retained_cursor: DecimalU64, max_age_seconds: DecimalU64, max_events_per_project: DecimalU64, };

export type SubscriptionOpened = { subscription_id: string, limits: AdapterLimits, retention: Retention, };

export type SubscriptionPending = { subscription_id: string, };

export type SlowConsumer = { min_retained_cursor: DecimalU64, last_delivered_cursor: DecimalU64, };

export type SubscriptionClosed = { subscription_id: string, };

export type ClientMessage = { "kind": "command", "message": Command } | { "kind": "resume", "message": { project_id: string, after_cursor: DecimalU64, } } | { "kind": "subscribe", "message": { project_id: string, after_cursor: DecimalU64, } } | { "kind": "subscription_poll", "message": { subscription_id: string, } } | { "kind": "subscription_close", "message": { subscription_id: string, } };

export type ServerMessage = { "kind": "receipt", "message": Receipt } | { "kind": "events", "message": Array<Event> } | { "kind": "event", "message": Event } | { "kind": "cursor_expired", "message": CursorExpired } | { "kind": "subscription_opened", "message": SubscriptionOpened } | { "kind": "subscription_pending", "message": SubscriptionPending } | { "kind": "slow_consumer", "message": SlowConsumer } | { "kind": "subscription_closed", "message": SubscriptionClosed } | { "kind": "error", "message": Error };

export type ProtocolFrame = ClientMessage | ServerMessage;
