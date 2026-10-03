# APRL service contracts

This document freezes the version 1 JSON boundaries shared by the API,
control, queue, worker, result and review packages. The Go definitions live in
`internal/contracts`; `schemas/job-v1.json`, `result-v1.json` and
`review-v1.json` describe the corresponding JSON payloads. JSON object keys are
case-sensitive. Consumers must use the strict `DecodeEvent`, `DecodeJob`,
`DecodeResult` or `DecodeReview` entry point: unknown fields, missing required
keys, `null` in place of an object, extra trailing JSON, malformed values and
unsupported versions are rejected.

## Shared identities and authority

Identifiers are non-zero UUID strings. A task is identified by `task_id`; a
stable logical operation by `job_id`; an execution by `run_id`; a lease by
`lease_token`; and a durable operation/replay by `operation_id`. Every message
also carries a `correlation_id` UUID used to follow the operation across API,
control, queue and worker boundaries. GitHub webhook `delivery_id` remains a
separate external delivery identity and is not a job or execution identity.

`generation` is a required, non-negative PostgreSQL `BIGINT` integer.
Explicit zero is valid for a new task at its initial generation; omission is
invalid and cannot silently turn a decoded message into generation zero. A consumer authorizes execution
only when the task, job, generation, active lease token and snapshot all match
durable state. Validating a UUID or generation's shape does not itself grant
authority. A worker result's idempotency key is `(task_id, job_id,
operation_id)`; an identical replay is acknowledged, while reuse with different
meaning must be rejected by the durable result store.

`snapshot` is always an object. An unattached authoring task may carry `{}`.
When present, `head_sha` and `base_sha` must appear together and be lowercase
40-character Git SHAs. `integration_sha` is optional and requires that pair.
Reviews always require an attached head/base snapshot. Result handling permits
an empty snapshot for pre-PR authoring; attached-task result admission must
compare the supplied snapshot with the current durable snapshot.

Lease tokens are non-zero UUIDs on execution jobs, results and reviews. The
token is an authority input and is compared to the live lease for the same
task/job/generation; stale, expired, revoked or mismatched leases are rejected.
Events represent routed durable work and do not assert a worker lease. Attempts
are positive 32-bit integers and count execution attempts for that logical
job.

## Version 1 JSON types

`contracts.Event` is the normalized, routed event record: `version`, `event_id`,
`event_type`, `task_id`, `job_id`, `generation`, `snapshot`, `operation_id` and
`correlation_id`. It carries no worker lease because webhook receipt and
routing precede execution ownership. Raw GitHub receipt metadata is not this
type: delivery ID and event type are captured at the HTTP boundary before task
enrollment, and are not treated as task/job authority.

`contracts.Job` is a logical operation message: `version`, `task_id`, `job_id`,
`generation`, `snapshot`, `attempt`, `operation_id`, `correlation_id` and
`operation`. Redis carries this message before a worker has claimed a Postgres
lease, so `lease_token` and `run_id` are both absent on a queued job. After a
successful durable lease claim, both are added together; `ValidateForExecution`
rejects an unclaimed message, and the worker must still compare them with live
Postgres authority. Supplying only one, a zero UUID or a malformed UUID is
invalid. `budget_envelope` is optional for control/reconciliation work that
does not invoke inference; when present, it is validated. Any inference
admission must separately require an approved envelope before execution. The
envelope has
`max_cost_micro_usd`, `max_input_tokens`, `max_output_tokens`, `max_calls` and
`pricing_version`. Money is an integer count of micro-USD; no float conversion
or downward rounding is part of this contract. Token/call bounds are positive
integers no greater than 10,000,000 input tokens, 1,000,000 output tokens and
10,000 calls. `attempt` is a positive PostgreSQL `INTEGER`. Pricing identity is explicit. The contract does not enable paid
execution; unsupported billing coverage remains disabled under ADR 003.

`contracts.Result` is a completion record: `version`, `task_id`, `job_id`,
`run_id`, `generation`, `lease_token`, `snapshot`, `attempt`, `operation_id`,
`correlation_id`, `status` and `summary`. Status is `succeeded`, `failed` or
`cancelled`. The durable result boundary checks the same operation identity on
replay and rejects a conflicting payload.

`contracts.Review` carries the result identity fields through
`correlation_id`, then `verdict`, `summary` and `findings`. Verdict is `approve`
or `request_changes`; optional `requested_change` carries an explicit change
request when findings do not. A finding has a unique UUID `id`, severity (`blocker`,
`warning` or `nit`), non-empty `comment`, optional `suggestion` and optional
`anchor`. An anchor is an all-or-nothing `{ "file": string, "line": positive
integer, "side": "LEFT"|"RIGHT" }` object. Findings may be unanchored,
including blockers; validation never drops or downgrades an unanchored blocker.
`approve` with any blocker is invalid. `request_changes` needs an actionable
finding or a non-empty explicit `requested_change`. Duplicate finding IDs are
invalid.

## HTTP API

The API uses JSON response bodies with `Content-Type: application/json`. Error bodies use
`{"error":"<stable_code>"}`. Health bodies contain no credentials, URLs or
other secrets.

### `POST /webhooks/github`

The request body is the exact raw GitHub payload bytes. GitHub App HMAC
verification is performed over those bytes before JSON parsing, normalization
or persistence. The `X-GitHub-Delivery` header supplies `delivery_id` and
`X-GitHub-Event` supplies the event type. The service acknowledges only after
the signed delivery is durably recorded in the inbox. Routing is asynchronous
and may retry independently after receipt commits.

| Status | Body | Meaning |
| --- | --- | --- |
| `202 Accepted` | `{"delivery_id":"<id>","accepted":true}` | A new signed delivery committed. |
| `200 OK` | `{"delivery_id":"<id>","accepted":true,"duplicate":true}` | This delivery ID was already committed; no second transition is created. |
| `400 Bad Request` | `{"error":"invalid_request"}` | Missing delivery/event headers or invalid payload after signature verification. |
| `401 Unauthorized` | `{"error":"invalid_signature"}` | Missing or invalid GitHub HMAC. |
| `503 Service Unavailable` | `{"error":"storage_unavailable"}` | Durable delivery commit failed; sender may retry. |

### `POST /internal/results`

The body is a `contracts.Result` JSON object and must pass `DecodeResult`.
Authenticate the host supervisor identity; repository credentials do not
authenticate this boundary. Admission binds supervisor identity to task, job,
run, lease and generation. Replays use the same operation ID and are
idempotent only when their meaning matches the accepted result.

| Status | Body | Meaning |
| --- | --- | --- |
| `200 OK` | `{"accepted":true,"operation_id":"<uuid>"}` | Result committed, or identical replay recognized. |
| `400 Bad Request` | `{"error":"invalid_result"}` | Malformed or inconsistent versioned result. |
| `401 Unauthorized` | `{"error":"unauthorized"}` | Missing or invalid host service authentication. |
| `403 Forbidden` | `{"error":"forbidden"}` | Authenticated supervisor is not bound to the submitted task/run/lease. |
| `409 Conflict` | `{"error":"stale_result"}` | Lease, generation or attached snapshot no longer matches, or replay conflicts. |
| `503 Service Unavailable` | `{"error":"storage_unavailable"}` | Result could not be durably committed. |

### `GET /healthz` and `GET /readyz`

`GET /healthz` reports process liveness only: `200 OK` with
`{"status":"alive"}`. It does not claim backing services are ready.

`GET /readyz` is role-aware readiness. It returns `200 OK` with
`{"status":"ready"}` when required dependencies for the role are available.
Otherwise it returns `503 Service Unavailable` with
`{"status":"not_ready","dependencies":["postgres"]}`. The dependency list
contains stable dependency names only (for example `postgres` and `redis`),
never connection strings or credentials.

## Foundation persistence seam

`storage.WithUnitOfWork(ctx, pool, clock, callback)` binds a callback to one transaction. `Repositories.Queries()` provides Exec/Query/QueryRow without commit control. Lock organization budget before task; LockTask then locks the attached PR. Use returned `LockedTask.Record()` for persisted generation/snapshot/PR identity; stale handles cannot create jobs after a generation change. Re-lock after direct SQL updates to generation or PR snapshot. Receipt insertion, final disposition, task state, logical job and DISPATCH outbox writes commit together. Execution `Job.Attempt` and task-wide remediation attempts are separate counters.

Go serialization omits a zero optional `budget_envelope` using `omitzero`, preserving strict queued-job round trips while retaining nonzero bounds. Explicit empty envelopes still reject. The installed Go JSON semantics and [official Marshal documentation](https://pkg.go.dev/encoding/json#Marshal) were checked; the regression test failed before the tag correction and passes afterward. Version 1 wire fields are unchanged.

## Confirmed push correlation

The GitHub operation UUID is `github_operations.id`. A push request records its immutable run/lease binding, assigned `branch`, planned `new_head_sha`, optional `addressed_finding_ids`, and `reply_intents` containing finding ID and body. Signed webhook observations correlate to that persisted task/job/generation and old snapshot plus planned new head; trusting a bot actor alone is insufficient. Confirmation records `result.head_sha`.

Under the task lock, `result.handoff_generation` marks the snapshot/generation change exactly once. It does not mean result completion or successor replies have finalized. Result handling separately commits `reply_jobs_created`, original C completion, and successor-generation jobs/outbox. A webhook arriving before a result must preserve the intents and original admission tuple; it must not strand a recognized confirmed-push completion solely because handoff advanced generation. Paused/escalated observations preserve their state and queue no execution.

## Durable execution leases

`leases.Claim` locks task, PR, job and run state, admitting one live job per task. Its immutable run tuple binds role, prompt hash, generation, snapshot, supervisor identity and opaque credential ID. Claiming records ownership; the supervisor must obtain budget admission before starting inference. `Lease.InferenceRequired` distinguishes author/review/fix from CI reconciliation and replies. Supported operation roles are author A, review B, fix C, CI reconciliation A and reply C; unknown operations reject.

Consumers performing durable changes in their own transaction use `ValidateLocked` and `CompleteLocked` with the injected clock. These read time after acquiring locks and reject expiry, replaced tokens, mismatched jobs/runs, snapshot changes and forbidden task states. Completion records job/run disposition; callers own lifecycle transitions.

## Durable event routing

`router.New` requires explicit repository enrollment policy and trusted producer sets. `RouteDelivery` routes committed inbox receipts and records final disposition in the same transaction as state, stable logical jobs and outbox rows. Organization budget accounts must be provisioned separately. An authorized issue enrollment queues author work; an opted-in human PR queues CI reconciliation. CI webhooks are hints for full aggregate reconciliation, never an individual pass granting review or merge. GitHub status events read top-level `sha` and `sender`.

Snapshot changes invalidate approval and CI, advance generation and preserve pause/escalation; uncorrelated pushes pause active work. Terminal states remain terminal. Normalized completion observations require the durable job/task/operation/correlation tuple and current generation/snapshot; authenticated result persistence owns actual completion transitions. Confirmed correlated pushes preserve the original job while the result boundary finalizes it, and queue one successor CI job.

## Deferred executions and truthful billing

`leases.RetryLocked(ctx, repos, lease, clock, nextAttemptAt, executionStatus)` validates the current tuple, finishes the run, returns the same logical job to PENDING with no lease, and commits a DISPATCH outbox row due in the future. Its payload carries `job_id` and `retry_from_run_id`. `Retry` provides a standalone transaction. Claim returns `ErrNotDue` before the latest retry's persisted deadline, so duplicate Redis hints cannot bypass scheduling. A consumer may acknowledge its old execution after the corresponding run is finished and matching retry outbox exists; publication may occur later. A fresh retry delivery must claim the current job rather than treating its predecessor's completion as completion of the new attempt.

CI integration pinning completes its valid reconciliation lease before changing the generation/snapshot and queuing new pinned work. Pending aggregate observations use durable retry, releasing execution ownership rather than holding a worker while CI runs.

Admission limits govern new reservations under organization/task locks. They do not suppress actual billing history: task spend can exceed its ceiling after a real overage, and further admission must fail closed with organization emergency mode. Reservations are unique per run; their identity, envelope and pricing are immutable and their coverage cannot be deleted. Cost entries are append-only. E1's budget API accepts trusted host final-usage attestation; arbitrary worker output alone cannot justify releasing unknown coverage. E2 must prove the real provider request/metering boundary. No paid adapter is enabled by these local contracts.

The budget reservation also stores immutable `admission_envelope` JSON containing the complete host-supplied input/output token, call, cost and pricing bounds. `Reserve`/`ReserveLocked` take that typed envelope explicitly; a nonzero queued job envelope must match it exactly. An absent queued envelope is resolved by the trusted host before execution, without rewriting the stable job payload. Admission retries compare the full stored tuple. Empty schema fixture envelopes do not prove executable admission.

The version 1 BudgetEnvelope contains max cost, input/output tokens, calls and pricing version; model identity is recorded with each charge. It does not prove a provider/model-specific gateway or allowlist. That live contract remains an E2 prerequisite.

## Atomic budget package

`budget.Reserve` and `ReserveLocked` take the current lease and explicit typed envelope. They lock organization then task, validate durable job/run authority, and admit only supported inference operations with capacity. Existing reservation replay requires the exact envelope, active RESERVED state and no organization emergency; UNKNOWN or settling coverage cannot grant another start. Admission uses numeric ledger aggregates, including all-time task charges and active reservations, and rolling organization charges in `(now - 24h, now]` plus outstanding coverage.

`RecordCharge` and its locked variant preserve authoritative historical provider billing after revocation. Request ID replay compares reservation/run, model, JSON usage and amount; conflicting reuse rejects. Late new charges after settlement reopen conservative UNKNOWN coverage and enter emergency mode. Amounts too large for the cached BIGINT counter retain exact ledger aggregates and expose saturation rather than silently authorizing more work. `MarkUnknown` retains coverage. `SettleReservation` and its locked variant require a trusted host final-usage attestation and a non-running historical run; repeated settlement of the same tuple is a no-op.


## Redis delivery boundary

`queue.NewStreams` requires a client configured with `ContextTimeoutEnabled: true`; the caller owns client shutdown. Entries carry the stable job UUID. `DispatchDue` publishes due Postgres outbox rows under outbox-only locks; a failed commit after XADD can duplicate a hint. Unsupported outbox kinds remain unpublished and back off unless a typed handler is injected. No task or run locks span Redis calls.

Consumers claim through Postgres before executing, retain entries while another lease is valid, and require injected budget admission plus matching durable RESERVED coverage before inference. An executor returning nil is not durable completion proof. XACK requires durable terminal/rejected state, or completion of this delivery's own run together with its persisted retry outbox. Fresh retry hints must claim a new run and honor the due fence. Unknown or malformed hints without durable rejection remain pending.


## Aggregate CI and stable successor identity

`ci.Evaluate` checks the full head/base/integration tuple and configured exact producer for every required check. Empty requirements deny admission. `PinIntegration` completes the current CI run before advancing the snapshot and requeues a pinned successor. `Reconcile` queues review once on pass, consumes the shared task cycle counter for bounded fix work, or releases pending work through `RetryLocked`. The persisted deadline escalates before accepting a new outcome. Protected-target merge policy also requires an allowed target and current human approval.

Router and CI derive stable UUIDs from SHA256 of `aprl.router.v1:` plus `job:`, `operation:` or `correlation:` plus the logical key, with UUID version/variant bits applied. Logical keys are `task:<uuid>:generation:<g>:<operation>`; fix work adds `cycle:<n>:` before the operation. New producers must reuse the exact domain and key, so duplicate hints coalesce without an identity conflict.

Recorded GitHub intent ID, task/job/generation, operation, identity, expected snapshot, request JSON and creation time are immutable and append-only. Status, remote ID and result remain mutable for confirmation. Push requests must contain the admitted `run_id` and `lease_token`, validated against the immutable run tuple by host-owned broker/results code; mere JSON presence does not grant authority.


Expected delivery-level errors, including not-yet-due retries and undisposed malformed hints, retain their PEL entries without stopping unrelated consumption. Infrastructure and unexpected executor errors remain visible failures. Consumer validates the current lease again after admission; supervisor validates independently immediately before starting a process. Neither historical coverage nor Redis ownership grants a stale execution.

Successor reply jobs keep the frozen v1 payload. Their authoritative Postgres logical key is `task:<uuid>:generation:<g>:reply:push:<parent_operation_uuid>:finding:<finding_uuid>`. Host reply execution resolves this pointer against the confirmed parent's immutable `reply_intents`, then admits a fresh run-bound broker intent. It does not precreate a mutation with an unknown future lease. Suppressed replies preserve intents and `reply_jobs_created=false`; an injected transaction-bound materializer on authorized resume may queue only intents whose confirmed head matches the refreshed current head.


## Process registration and broker identity

`agent_runs` has optional complete process registration: `process_id`, `process_group_id`, `workspace_id`, `process_started_at`, `last_heartbeat_at` and `execution_deadline_at`. Initial registration requires a RUNNING admission; process/workspace identity and deadline become immutable, and heartbeat is monotonic. A supervisor uses owned process handles and workspace markers; it cannot kill an arbitrary persisted PID after restart.

New host-admitted `github_operations.identity` records logical app role A, B or C, separately from supervisor service identity and opaque supervisor credential ID. The configured transport binds that role to its distinct real App/installation. Existing generic migration fixtures are not mutation admissions. C push handoff is associated with a `fix` job and its exact operation UUID; successful result status alone never confirms the planned GitHub head. Both result-first and webhook-first completion require durable remote confirmation.

The broker must resolve an assigned source branch from trusted host policy or a snapshot-bound remote PR read, never from worker-requested branch text. A pre-PR author branch may be deterministically assigned from the persisted task UUID. GitHub's [merge endpoint](https://docs.github.com/en/rest/pulls/pulls#merge-a-pull-request) expected `sha` guards the head; it does not provide base-SHA CAS. [Updating a ref](https://docs.github.com/en/rest/git/refs#update-a-reference) with `force=false` protects fast-forward history rather than expected-old-SHA CAS. A future real adapter must prove the stronger snapshot contract before autonomous mutations are enabled. Foundation transport doubles are test injection only.

### Unstarted admission cleanup

A queue budget-admission callback may only reserve coverage; it cannot start a provider request or process. If the subsequent host lease check expires before executor invocation, the consumer locks the original admission and disposes it without altering a replacement owner. A host-proven unstarted admission settles unused coverage and creates a durable future retry when the same active task snapshot still applies. Any recorded process, cost, result receipt, or GitHub intent retains UNKNOWN coverage instead. Worker completion alone never attests billing finality.

### Shared CI and control fencing

CI timeout completes its own current reconciliation run before calling the shared escalation controller. Escalation advances generation, invalidates approvals and cancels outstanding work atomically. CI failure consumes a C attempt through the same `AdmitFixLocked` and `QueueFixLocked` helpers as review remediation; an exhausted limit escalates without incrementing the counter or creating another fix. Callers hold organization-budget then task locks for the entire unit of work. CI timeout keeps the PR's ERROR observation; exhausted remediation keeps FAILURE while task authority is revoked.

### Authenticated result completion

The host authenticates a supervisor Principal containing task/run identity and opaque credential reference before consulting accepted receipts. Wrong identity returns 403 even on an exact body replay. New results match immutable original job/run/lease/snapshot and current live authority; the only webhook-first exception is the exact confirmed push and immediately following generation with an unexpired original run. Push changes clear integration identity on the new snapshot while retaining the immutable original result snapshot. Old-generation jobs already cancelled by routing stay cancelled; their run and accepted receipt still provide durable disposition. Receipt, run completion, UNKNOWN budget coverage, handoff and successor reply jobs/outbox commit atomically. Reply intents suppressed by pause retain a false materialization marker for explicit resume after a fresh matching remote snapshot. Worker succeeded does not confirm a remote operation or final billing.

Admission errors and missing reservation proof use the same unstarted-host cleanup as lease expiry. They terminate only the run, preserve a still-current logical job as PENDING, and persist a future retry before a later execution/ack. Cleanup uses a fresh bounded context when admission was cancelled. An ambiguous reservation commit is reconciled from durable evidence; recorded execution evidence retains UNKNOWN coverage. Revoked generations and replacement owners are never reauthorized.
# Whole code-change lifecycle integration

**Status: semantic direction accepted; external wire schema/version pending architecture freeze.** This contract records lifecycle ownership and task semantics. It intentionally does not define payload fields, endpoints, serialization, authentication headers, or compatibility numbers.

APRL is the canonical scheduler and lifecycle eligibility/admission authority for every enrolled code-change lifecycle. It supports both complete standalone operation and delegated operation initiated by a neutral product workflow controller. In delegated mode, the controller owns its approved product graph, aggregate admission, product qualification, and deployment decisions. APRL owns canonical child IDs, child readiness and scheduling, lifecycle stage routing, and service-authoritative admission/result handling. The same lifecycle must not be advanced by a second scheduler. Generic plan/apply/claim representations and stage routing remain reusable; APRL-specific authority is supplied through its adapter. A claim means pickup only.

An author or fixer task that changes code must produce a pull request and place its URL and exact head on the dependent review task before completing at PR handoff. Every code-changing PR requires an executable independent review task selected through ordinary apply+claim. The reviewer binds evidence to exact head and base. Approval proceeds through guarded merge and reviewer-owned verification of the merged landing. A blocking review creates bounded fix and dependent re-review children. These rows are normal executable work with their own claims and evidence, not decorative checklist items.

Ordinary descendants are not ready until required review, merge, and landed verification complete. Only an explicitly marked speculative dependency with recorded inputs may start earlier; that work does not release downstream readiness or authorize merge. No exemption may bypass independent review for a code-changing PR. A blocked review or fix must expose bounded correction and re-review children; exhausted bounds remain blocked/escalated for owner policy rather than silently releasing descendants.

Protocol adoption must preserve idempotency, authenticated authoritative admission/result facts, lifecycle and snapshot fencing, bounded child expansion, cancellation and ambiguous-outcome reconciliation, and explicit version compatibility. Exact schema and mapping of these semantics to wire fields are pending the architecture lane; reconcile this section with the frozen schema before publication or implementation. See [ADR 007](adr/007-whole-code-change-lifecycle.md).
