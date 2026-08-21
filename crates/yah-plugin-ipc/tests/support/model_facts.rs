#![allow(dead_code)]

use serde::{Deserialize, Serialize};

pub const HOST_VERSION: u32 = 1;
pub const MAX_STREAM_CREDIT: u32 = 1024;

/// What one queued host frame means, at comparison granularity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireFact {
    Accept,
    Refuse(&'static str),
    Call {
        call_id: u64,
    },
    Reply {
        call_id: u64,
        outcome: OutcomeFact,
    },
    StreamOpen {
        call_id: u64,
        credit: u32,
    },
    StreamData {
        call_id: u64,
        seq: u64,
        more: bool,
        lossless: bool,
        dropped: u64,
    },
    Credit {
        call_id: u64,
        additional: u32,
    },
    Cancel {
        call_id: u64,
        target_call: bool,
    },
    Release {
        handle: u64,
        kind: Kind,
    },
    ReleaseAck {
        handle: u64,
        kind: Kind,
    },
    Goodbye,
}

/// What one session event means, at comparison granularity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventFact {
    Negotiated,
    CallDelivered {
        call_id: u64,
    },
    HostCallSettled {
        call_id: u64,
        outcome: OutcomeFact,
    },
    HostCallLost {
        call_id: u64,
        kind: &'static str,
        reconcile: bool,
    },
    StreamOpened {
        call_id: u64,
        credit: u32,
    },
    StreamItem {
        call_id: u64,
        seq: u64,
        more: bool,
        lossless: bool,
        dropped: u64,
    },
    CreditGranted {
        call_id: u64,
        additional: u32,
    },
    CancelRequested {
        call_id: u64,
        target_call: bool,
    },
    HandleReleased {
        handle: u64,
        kind: Kind,
    },
    WorkerHandleReleased {
        handle: u64,
        kind: Kind,
    },
    HandlesReclaimed {
        count: u32,
    },
    WorkerGoodbye,
    CallRefused {
        call_id: u64,
        kind: &'static str,
    },
    Fatal {
        kind: &'static str,
    },
    /// A host application call was refused; the string is the AppError
    /// discriminant the adapter maps, not a diagnostic string.
    AppErr(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    Capability,
    Artifact,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeFact {
    Ok,
    Spilled { handle: u64 },
    Err { kind: &'static str, retryable: bool },
    Cancelled,
}

/// The generated action vocabulary. Worker-side actions feed real frames;
/// host-side actions call the application API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    Hello,
    HelloBadVersion,
    HelloUnknownRequired,
    /// A decodable non-hello frame before negotiation (a goodbye).
    NonHelloFirst,
    /// A second hello while active.
    HelloAgain,
    WorkerCall {
        id: u64,
        stream: bool,
    },
    ArtifactRead {
        id: u64,
        handle: u64,
        ok_range: bool,
    },
    WorkerReply {
        id: u64,
        outcome: WOutcome,
    },
    StreamOpen {
        id: u64,
        credit: u32,
    },
    StreamData {
        id: u64,
        seq: u64,
        more: bool,
        lossless: bool,
        dropped: u64,
    },
    Credit {
        id: u64,
        additional: u32,
    },
    WorkerCancel {
        id: u64,
        target_call: bool,
    },
    Release {
        handle: u64,
        kind: Kind,
    },
    ReleaseAck {
        handle: u64,
        kind: Kind,
    },
    Goodbye,
    /// Clean end-of-input (never fed mid-frame by the generator).
    Eof,
    HostCall {
        deadline_ms: Option<u32>,
    },
    Tick {
        now_ms: u64,
    },
    HostCancel {
        id: u64,
        target_call: bool,
    },
    AnswerWorkerCall {
        id: u64,
        outcome: WOutcome,
    },
    Mint {
        id: u64,
    },
    OfferArtifact {
        id: u64,
        bytes: u32,
    },
    HostRelease {
        handle: u64,
        kind: Kind,
    },
    HostOpenStream {
        id: u64,
        credit: u32,
    },
    HostGrantCredit {
        id: u64,
        additional: u32,
    },
    HostStreamItem {
        id: u64,
        lossless: bool,
        more: bool,
    },
    HostNoteDrops {
        id: u64,
        dropped: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WOutcome {
    Ok,
    Spilled { handle: u64, bytes: u32 },
    ErrUnknownCall,
    ErrInternal,
    Cancelled,
}

impl Action {
    /// The wire id the action carries, if any, for the bounds gate.
    pub fn wire_id(&self) -> Option<u64> {
        match *self {
            Action::WorkerCall { id, .. }
            | Action::ArtifactRead { id, .. }
            | Action::WorkerReply { id, .. }
            | Action::StreamOpen { id, .. }
            | Action::StreamData { id, .. }
            | Action::Credit { id, .. }
            | Action::WorkerCancel { id, .. } => Some(id),
            Action::Release { handle, .. }
            | Action::ReleaseAck { handle, .. }
            | Action::HostRelease { handle, .. } => Some(handle),
            _ => None,
        }
    }
}
