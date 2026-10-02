# ADR 002: Postgres ownership and at-least-once delivery

## Status
Accepted

## Date
2026 10 01

## Context
A webhook can be acknowledged before Redis receives its job; delivery retries, worker restarts, and remote API timeouts can repeat effects. Queue-level locks cannot enforce ownership during Redis loss or after cancellation.

## Decision
Adopt RFC-0001's transactional inbox/outbox and stable logical jobs. Acknowledge receipt only after inbox commit. Route each delivery under a task lock and commit state, disposition, jobs, and outbox together. Redis delivers at least once. Postgres task generation plus unique renewable lease tokens authorize dequeue, results, and broker admission. Pause, terminal states, and snapshot changes fence previous ownership. Request/operation IDs distinguish logical work from delivery and execution attempts.

Persist GitHub mutation intents before sending them. Treat ambiguous responses as UNKNOWN and reconcile the remote outcome before retry. Confirm pushes atomically with snapshot/generation transitions and successor reply jobs; stale worker leases never regain authority. Require aggregate CI and reviews on the actual target/head snapshot. Merge admission serializes with controls and supplies expected head SHA; repository rules protect base integration.

## Consequences
There is no distributed exactly-once promise. Repair loops and fault injection are essential. Redis loss is recoverable from Postgres. Unknown remote writes and unsettled billing remain durable obligations rather than blindly retried operations. See RFC-0001 sections 4.3, 5, 7.3, and 8.
