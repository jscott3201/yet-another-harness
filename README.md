# Open Agent

A local-first daemon that runs and supervises coding agents, built so the
runtime's record of what happened survives crashes, retries, and concurrent
writers without lying about any of it.

**Status: pre-0.1.** No releases, no published crates. The kernel is under
construction; the gate table below is the real state of the project and is
updated as code lands. Design decisions live in a local planning corpus owned by
the maintainer — this README is its public projection.

## Objectives

- **Durable execution truth.** Current state, an immutable semantic journal, and
  command receipts commit in one transaction. Recovery is proven by kill/reopen
  tests at every commit-pipeline stage, not assumed.
- **Explicit external-effect uncertainty.** An effect whose outcome was never
  observed stays `unknown` until reconciled. It is never blindly retried and
  never marked done by guess; irreversible effects escalate immediately.
- **Small-blast-radius multi-agent coordination.** Depth-one delegation, at most
  two concurrent writer agents with disjoint write ownership, context-fresh
  review, and one human-controlled integration authority.
- **Provider neutrality.** Normalized provider contracts (streaming, tool calls,
  typed errors, usage) exercised against a deterministic fake provider — in both
  Responses and Anthropic Messages dialects — before any live endpoint.
- **Honest sandboxing.** Execution backends declare what they actually enforce.
  The first backend, `local.trusted`, claims no isolation it does not have.

## Gate tracking

Every claim above must pass a model-free gate — deterministic tests with zero
live model calls and zero real external effects — before any live-model proof
runs.

| Gate | Proves | Status |
|---|---|---|
| G02 — storage fan-in and crash recovery | Atomic CAS + receipt + journal append under process kill, writer-takeover contest, corruption drill | **In progress** |
| G01 — model-free kernel milestone | Command/state/effect/cancellation semantics against fake provider and fake effects; 23 deterministic exit obligations | Not started |
| Protocol conformance, adapters 2–4 | Same conformance corpus over UDS, named pipe, loopback HTTP/SSE | Not started |
| Install and negative tests | Clean-machine install, daemon lifecycle, honest `local.trusted` negative tests | Not started |
| Multi-agent evaluation freeze | Frozen arms, budgets, and thresholds for the first live multi-agent proof | Blocked on G01/G02 |

A gate is **Passed** only when its full obligation table runs deterministically;
a reproducible failure of G02's hard bars falsifies the storage design rather
than delaying it, and the fallback design track opens instead.

## What is not here yet

No GUI, no live provider adapter, no published protocol spec, no sandbox
guarantees. Those arrive behind their gates, in the order above.
