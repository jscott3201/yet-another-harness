# Architecture

Open Agent separates durable runtime truth from disposable processes and
connections. A worker can disappear; accepted commands, authority changes,
effect observations, and cancellation state cannot.

## Runtime Model

```text
client or embedded caller
          |
          | versioned JSON protocol
          v
  in-process adapter (Adapter 1)
          |
          | typed command
          v
     mutation funnel
       /    |     \
      /     |      \
 current  receipt  semantic
  state             journal
      \      |      /
       one Selene transaction
```

The adapter does not own state or authority. It validates and translates a
wire command, then sends it through the same mutation funnel used by the
kernel. State changes, command receipts, and semantic events commit together.

## Authority and Fencing

The kernel uses several independent axes:

- `authority_epoch` changes when a new daemon lifetime claims the project.
- `attempt_epoch` changes when work is dispatched or retried.
- `stamp` invalidates existing holder tokens without creating a new attempt.
- the active lease binds the holder identity to the attempt.

Holder-authorized writes must pass every current check. Authority methods use
the current authority epoch and do not accept holder tokens. A stale callback
can be retained as diagnostic input where policy permits, but it cannot advance
current state.

## Commands and Receipts

The idempotency key is `(scope_kind, scope_id, command_id)`. Repeating the same
command digest returns its stored receipt. Reusing the key with another digest
is rejected without a second transition.

Each accepted transition writes:

- the aggregate state change;
- its semantic event batch; and
- the durable command receipt.

The receipt records the event cursor range, so clients can reconnect and find
the effects of their command without relying on a surviving connection.

## External Effects

Filesystem, process, Git, network, tool, and model actions are treated as
external effects. A non-read-only action needs a committed effect intent before
dispatch. Dispatch is evidence that the action may have happened; it is not
evidence of success.

If the result cannot be proven after a crash, the effect remains uncertain and
enters reconciliation. The kernel currently enforces the `no_retry` class;
query-before-retry reconciliation remains unfinished.

## Cancellation

Cancellation is a durable tree operation. The request and its frozen member
scope commit before signals are sent. One immutable delivery record carries
the delivery time, optional observation time, and outcome because sending a
signal does not prove that a child stopped.

A dispatched effect with no authoritative outcome remains reconciling after
cancellation. The kernel does not manufacture a cancelled outcome for an
action that may have escaped.

## Storage Boundary

One control graph represents one project. The store persists project identity,
authority epoch, retention floor, aggregate state, receipts, semantic events,
effect records, and cancellation records. Connections and adapter instances
are reconstructible indexes over that durable state.

The passed G02 gate proves the storage transaction and recovery substrate. It
does not prove the unfinished daemon, execution, or protocol-conformance work.

## Configuration and Extensions

The planned configuration layer separates preferences from authority. Built-in,
user, project, and one-run values resolve by typed field rules, and an effective
value reports where it came from. Repository configuration is untrusted: it may
request a provider, backend, tool, or extension, but it cannot grant itself
credentials, network access, broader file access, or weaker approval policy.

The draft contract would require explicit user opt-in for experimental features,
remote data or credential use, executable third-party extensions, optional
listeners, and non-baseline backends. It would bind active work to an immutable
effective-configuration digest so later edits cannot reinterpret an existing
attempt or prepared tool call.

Future extensions attach through capability-scoped provider, tool, context,
execution, VCS, artifact, and client seams. They cannot receive direct store or
integration authority. This configuration layer and its public schema are
designed but not implemented.

## Terminal Client

Ratatui is the selected TUI library. The planned full-screen client has three
primary destinations: Work, Approvals, and Configuration. Runs, effects,
evidence, review, and configuration provenance appear as contextual panes or
subviews rather than independent runtime state.

The client owns focus, selection, scroll offsets, visible tabs, overlays, and
unsaved drafts. The daemon owns every workflow, approval, config revision,
effect outcome, evidence packet, receipt, and cursor. Reconnect replaces or
resumes the daemon projection; it never infers success from local screen state.
The TUI and its test matrix are designed but not implemented.
