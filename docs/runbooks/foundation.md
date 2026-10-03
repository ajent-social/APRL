# Foundation qualification and recovery

The foundation assembles real durable components behind explicit trusted host interfaces. Local system tests use owned PostgreSQL 16 and Redis 7, signed HTTP ingress, authenticated result HTTP, a local fake child process and fake GitHub/provider transports. These fixtures prove local behavior; they establish no production authentication, OCI containment, billing finality, external wire interoperability or deployment.

## Preconditions and bounded checks

Use an isolated checkout and task-owned external storage for caches and artifacts. Set `TEST_RESOURCE_OWNER`, `TEST_DATABASE_URL` and `TEST_REDIS_URL` explicitly for resources you own. Fixture setup fails when services or ownership are absent; it must never skip required integration cases. Each test owns a random database schema and Redis namespace. Never flush shared Redis or drop an unverified schema.

Before a multi-package build/test/vet/lint, check host load and claim the shared build lease according to the repository/global instructions. Run normal and full race tests serially, release the exact lease immediately afterwards, and retain logs outside the checkout. The focused system command is:

```sh
go test -json ./tests/system -run '^TestFoundation' -count=1
```

Full qualification includes `go test -json ./... -count=1`, then `go test -race -json ./... -count=1`, build, vet, lint and empty gofmt/goimports diffs. Count terminal JSON events with a nonempty `Test` field; distinguish package-level `no test files` entries. Require positive executed counts, zero failed tests and zero skipped tests. Source function counts are not execution evidence. Local Mac process checks do not establish Linux native process behavior; hosted Linux qualification belongs to CI.

## Role startup and health

The real constructors require explicit authentication, inventory/recovery, cancellation, broker, label and execution adapters for their roles. No production factory registry is supplied. The `api`, `control` and `worker` CLI subcommands validate their bounded options and fail closed with stable missing-adapter errors; they cannot start production roles by enabling fixture flags.

Initial recovery must complete within its configured bound before any mutation, expiry, dispatch, queue claim or budget reservation. Initial failure returns a role error and requires a host supervisor restart. After successful startup, current dependency checks gate each new effect. `/healthz` remains `200 {"status":"alive"}` while the role is running. `/readyz` returns `200 {"status":"ready"}` only with current prerequisites; otherwise it returns `503` with sorted safe unavailable dependency names. Responses disclose no URLs, credentials or raw adapter errors.

Control and worker loop errors back off and retry only after current prerequisites pass. Public readiness reports the unavailable `control_loop` or `worker_loop`; its own loop-status bit is excluded only from the restart prerequisite check. Redis group recreation occurs only after trusted recovery/readiness. Shutdown cancels and joins bounded loops while leaving caller-owned database and Redis clients open.

## Persisted recovery authority

PostgreSQL owns task generations, jobs, due outbox intents, leases, immutable run identities, process holds, confirmed operations and accounting. Redis stream entries are delivery hints. Restart constructors against the same owned database state and namespace; do not clear generation, lease, hold or UNKNOWN accounting state to make a retry available.

If an owned stream is lost, repair pending hints from the latest eligible PostgreSQL job/outbox state with persisted backoff. Duplicate receipts, hints and results must reconcile the same logical identity. Cancellation targets the captured task/job/run tuple; it cannot retarget a replacement run or authorize signalling a persisted numeric PID.

An unresolved process group retains capacity, workspace and budget exposure. Missing PID or missing result is not proof of termination. Only fresh trusted never-started or drained-group evidence can release the relevant hold. Startup inventory precedes sweep and dispatch. Native local process identity/group tests do not establish OCI isolation or restart-safe host handles.

A possibly applied remote write remains UNKNOWN. Reconciliation uses read-only lookup and confirmed receipts; it must not re-execute the write. Worker usage/result JSON does not settle provider billing. Retain UNKNOWN coverage until a trusted settlement resolves the same immutable attempt.

## Failure injection and evidence

Use only the task-owned isolated checkout and owned fixtures. Match the genuine-red mutation to the predicate exercised by the test: authenticated result generation/lease fencing and routed completion-event fencing are separate paths. Disable the intended guard, observe the specific failing assertion, restore exact source bytes in a finally path, and rerun successfully. A test kept green by another defense is not a genuine-red receipt.

An actual backing-service outage probe must verify ownership, ensure no sibling test/compiler is using the fixture, stop only that fixture, observe readiness 503/liveness 200, restore it in a finally path, and wait for its actual startup readiness. Record an initial fixture-start timing failure separately from a source regression. Preserve original failure logs and restored verification.

## Delivery record

Local system execution and independent review receipts are added here only after verification. Author handoff completion does not unblock downstream work; the first-class review task must independently accept the exact PR head, merge with the expected head and verify the actual landed tree/base ancestry. Hosted CI and production readiness remain separate evidence.

The startup-hold integration audit identified that injectable callbacks alone do not prove persisted inventory. System fixtures may explicitly connect startup recovery to the real process-hold store and a bounded test host ledger; record that as local integration evidence. A production host inventory/factory remains unavailable until separately implemented and qualified. Do not mark production recovery ready from an always-success callback.

### Stale-result negative control

The T1.18 negative control temporarily changes the stale-generation rejection branch after `validateWebhookFirst` to return success. `TestFoundationFaultsDuplicateAndStaleHTTPResults` then fails at the real HTTP assertion: a revoked generation is falsely acknowledged with 200 instead of 409. Exact source restoration passes the same test. This demonstrates sensitivity to stale HTTP rejection; it does not claim removal of every durable lease or accounting fence. The mutation is never committed, and the production source is restored before final verification.

### Local system-test qualification

On the isolated T1.18 candidate, the six `TestFoundation` cases passed with zero skips. Full normal and serialized race suites each passed 315 terminal test events with zero skips; command build, vet, lint, and all five changed Go files passed formatting checks. The signed-receipt vertical slice records AUTHORING/COMPLETED/SUCCESS, a REAPED native process hold, one authenticated result receipt, and an acknowledged stream delivery. Its provider usage reservation remains UNKNOWN with the full fixture allowance and zero charges because no trusted provider settlement evidence exists. Linux CI, production host inventory and provider/OCI activation remain pending.
