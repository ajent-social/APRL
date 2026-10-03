# ADR 008: Durable process reservations

Status: Accepted for bounded foundation implementation.

A timed-out or canceled execution can leave an unreaped owned process. Marking its job retryable releases an execution lease but does not prove resource cleanup. Local monitors do not survive restart, and extending a lease would preserve stale execution authority.

Persist a separate run-bound process reservation before attempting launch. Trusted host configuration supplies resource scope, positive maximum active reservations, and bounded evidence-verifier timeout; there is no default activation. Every unresolved reservation counts against capacity, including canceled and older-generation runs. A task cannot acquire replacement execution while its reservation is unresolved.

Reservations progress from RESERVED to STARTED or UNKNOWN, then REAPED only with trusted evidence. Before launch, a one-time atomic BeginStart transition persists UNKNOWN launch intent; only the transition winner may attempt launch. RESERVED replay alone never grants permission to start. A retry of UNKNOWN, STARTED or REAPED cannot create another process. A later process observation enriches UNKNOWN without changing execution authority. Late identity observations can enrich UNKNOWN but cannot reopen execution. Immutable task/job/run/workspace/supervisor identity and hashed lease fence prevent rebinding. Positive PID/PGID alone is insufficient: the host must establish process-start identity and prove never-started or whole owned group drainage before reaping. Missing, stale, or ambiguous proof keeps the reservation held. Native process identifiers do not establish OCI containment or permit signaling unrelated processes.

Use task lock, resource-scope advisory lock, then reservation row lock consistently. When a transaction also touches organization budget, acquire that lock first; preserve existing task/PR then job/run ordering and never acquire task or budget after a scope lock. Cancellation, pause, generation changes and lease expiry revoke execution independently and cannot delete unresolved reservations. Startup reconciliation must occur before lease sweeping or dispatch. No paid execution, provider activation or production qualification follows from these primitives.

First-class author and independent review tasks are recorded in T8 of the implementation plan. E1 task acceptance remains required; this extension does not count recovered source as delivered.
