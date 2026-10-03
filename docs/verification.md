# Foundation verification record

This record tracks fixture-backed foundation acceptance. It does not claim production rollout, paid runtime support, or live GitHub behavior.

## Wave 1 — Bootstrap

- Dispatch: T1.1, one GPT-6-Luna worker, isolated feature worktree.
- Baseline: observer-only acceptance failed because the Go module and bootstrap package did not exist; no coding harness was dispatched by the observer.
- Fixtures: dedicated PostgreSQL 16 and Redis 7 services provisioned for this execution; task tests use generated owned schemas and key namespaces.
- Acceptance: PASS. Eight test/subtest executions, no skips; missing database subprocess produced the required failure. Module download, per-package vet/lint, formatters and linter configuration validation passed.
- Observer: baseline FAIL (absent module); implemented PASS, no coding dispatch.
- Coordinator review: security/quality/performance PASS after fixing all-connection search-path isolation and bounded fixture contexts. Helpers have actual configuration and connectivity behavior, not production fake wiring.
- Build: `go build -p 2 ./...` PASS under the acquired shared build lease; lease released immediately.
- Integration: worker commit `016b27f`; owned code matched byte-for-byte after merge.
- Ship status: locally accepted foundation component; CI and production rollout remain pending.

## Boundaries

The original RFC and planning files were preserved. Implementation copies are maintained on the integration branch. Maximum prescribed coding concurrency is three workers. The coordinator owns module changes, review, integration, and the single full-suite race lane.

## Wave 2 — Runtime probe and contracts

- Dispatch: T1.2 and T1.3, two GPT-6-Luna workers in isolated feature worktrees.
- Baseline: all ten named capability predicates FAIL, with no coding dispatch by the observer. The contract guard rejects zero executed cases and skipped cases.
- Scope: read-only installed CLI probes and owned Redis fixtures; no provider requests or live GitHub mutation.
- Acceptance: PASS. Runtime probe: five test executions; contracts: eight test executions; all ten named capability predicates passed. The combined `go test -json ./... -count=1` gate passed 21 test cases under the shared build lease.
- Coordinator review: security/quality/performance PASS after fixing queued-versus-execution authority, required-field presence, explicit-zero generations, and receipt-versus-routing commit boundaries.
- Wire check: CLI exercised real Redis and installed local help; contracts are exercised through strict public decoders and matching schemas. Full live use cases remain deferred.
- Integration: owned files matched both worker branches exactly after merge.
- Deviations: user-selected agent coding lanes override the default coding harness; T1.4 gains an embedding wrapper to retain one SQL source.
- Ship status: local foundation components accepted; CI and live rollout pending.

Wave 2 integrated `go build ./...` also passed under the acquired/released shared build lease.

## Wave 3 — Core schema

- T1.4 accepted locally: PostgreSQL 16 migration applies atomically and idempotently in an owned schema. Five actual test executions pass with no skips.
- Duplicate source/delivery/job identities and cross-task result operation rebinding reject. Invalid states, negative reservations, absent active leases and immutable admission changes reject; closure retains audit rows.
- Observer: absent integration package baseline FAIL, all four named implemented predicates PASS. Per-package vet/lint and formatting passed. Coordinator reviewed bounded rollback, embedded single SQL source, composite receipt references and integer micro-USD constraints.
- Coordinator genuine-red check: allowing `NOT_A_STATE` in the owned migration made the invalid-state test fail; restoring the reviewed bytes restored the named test to PASS.
- Worker commit `4a427a4`; all four owned files match after integration. CI and live rollout remain pending.

## Wave 4 — Transactional persistence

- T1.5 accepted locally: one actual real-Postgres `TestUnitOfWork` covers atomic success/rollback, receipt/disposition/job replay and conflicts, lock ordering, stale lock handles, attached PR snapshots and existing cross-task PR rejection. Manual clock advances without sleeps; remediation attempt 2 accepts execution attempt 1.
- Observer absent-test baseline FAIL → implemented PASS. Coordinator mutation committed a failed callback and made the test FAIL; restored rollback behavior returns PASS.
- All three owned package vet/lint checks and format/import diffs passed. Worker `fc7a377` merged with exact file equality.
- Coordinator fixed optional zero-envelope serialization with a failing-before/passing-after round-trip test; contracts now have nine actual passing cases.
- Integrated `go build ./...` and `go test -json ./... -count=1` pass under the shared build lease, released immediately afterward: 28 actual cases, no skipped cases.

## Wave 5 — Webhooks accepted; routing/leases pending

- T1.6 worker `e20f6c4` integrated with exact file equality. Nine real-HTTP/Postgres test events passed with no skips, including exact-byte signature ordering, missing/bad signatures, replay/conflict, body limits and deferred commit failure with zero persisted rows. Early-ack mutation returned 202 incorrectly and the test failed; restored handler returns 503.
- Coordinator-owned retest, both package vet/lint checks, formatting and acceptance observer all PASS under verified shared lease ownership. Absent-test observer baseline was FAIL.
- Build ownership incident and resulting execution guard are recorded in ADR 004. Lease checks run during interrupted ownership are not certified; coordinator retesting supplies fresh evidence.


## Wave 6 — Durable delivery, budgets and aggregate CI

- Three prescribed Luna coding lanes implemented T1.9, T1.10 and T1.12 in isolated task worktrees; all owned files match merged commits exactly.
- Task checks: delivery 8 test events, budgets 10 race test events, aggregate CI 12 test events; zero skips. All affected package vet/lint, empty formatter diffs and observer-only acceptance checks passed.
- Independent genuine-red checks detected wrong published job identity, missing organization capacity and stale CI snapshot admission; exact source restoration returned each selected case to green.
- Shared retry/immutable admission/accounting amendments were tested against actual Postgres. New GitHub immutable-intent regression failed before its trigger, then 7 migration cases passed; mutable remote outcomes remain supported.
- Integrated module build and 92 actual test cases pass, zero skips. Full-suite race remains serialized through T1.19. No paid provider or product GitHub adapter is enabled.
- Wire review: actual Redis XADD/XREADGROUP/XAUTOCLAIM and Postgres rows decide transport/ownership; injected host bounds and trusted CI observations remain explicit seams for later real-adapter proof.

## Wave 7 — Authorized controls and results dispatched

Two prescribed coding lanes own controls and results separately. The coordinator retains shared schema/docs, integration and build-gate ownership. Accepted receipt replay cannot bypass supervisor/run binding; confirmed push completion requires immutable original-run proof. Unknown usage retains budget coverage. Acceptance is pending.

Independent wave-6 review reopened the delivery boundary for a concrete consumer-starvation defect and expiry-before-handoff gap. Both new regressions failed against previous behavior. The restored implementation passes 13 delivery cases, all affected vet/lint/observer gates and six focused race events, with no skips. Integration will be rechecked with the next wave.

## First-class review task primitives (T7 author handoff)

- Verified source: `5b634b9`; the delivery branch carries identical Go, migration and test bytes on top of current main.
- Build, vet and lint pass. Regular and serialized race suites each execute 203 test pass events with zero skips against owned PostgreSQL and Redis fixtures. The final source includes all declared parallel contributors in review exclusions and preserves historical verdicts across later corrections.
- Buffered provider/store tests cover atomic PR publication, revision races, append-only receipt audit, transaction rollback, actor/claim/policy fencing, correction expansion and host-qualified final delivery.
- Genuine-red: disabling the host outcome verifier caused the actual forged-merge integration case to fail; exact source restoration passes. No compile error or skipped fixture is counted as the negative proof.
- Independent source reviews accepted contract/adapter and coordinator state/store changes. The PR delivery task remains pending current-head review, guarded merge and actual landed verification.
- The shared generic provider/parser/pool bridge has independent review, 19 focused Python passes and verified repository landing. No provider endpoint, credentials or runtime profile is activated by this slice; external protocol/runtime interoperability and the remaining E1/E6 gates are pending.


Broker source qualification (2026-10-03): source 4df8973 passed 22 actual race-test cases, zero skips/failures, affected broker/integration vet and lint. The valid C-role out-of-branch denial regression genuinely failed with only the branch guard removed, then passed after exact source restoration. No remote transport mutations were performed by tests; no production credentials or runtime activation. Owned gofmt/goimports diffs are empty. Independent PR delivery remains pending.


Durable process hold source qualification (2026-10-03): integration source67e0912 passed whole-module build, 251 regular and 251 race test events with zero skips/failures, vet and lint. Real PostgreSQL fixtures cover capacity, reservation replay, single launch intent, cancellation/expiry fencing, pause/resume, stale/late reaping evidence, and ambiguous admission coverage. UNKNOWN and REAPED/GROUP_DRAINED retain budget coverage; trusted NEVER_STARTED permits settlement. Removing only the process-hold billing predicate genuinely fails the drained-process regression; exact restored source passes. Revoked launch-intent regression also genuinely failed when only the live-lease guard was disabled and passed after exact restoration. Owned gofmt/goimports diffs are empty. Independent store/lease source review accepted the corrected boundary. Native fixture ownership, startup reconciliation, OCI and actual service/provider activation remain separate pending gates. No T1.15/T1.16/E1/E6 completion claim.

Process reservation correction qualification: source9b7c6bf passed full-module build, 253 regular and 253 race test events with zero skips/failures, vet and lint. RESERVED→STARTED bypass is denied at both store and SQL boundary; BeginStart fixtures now consume launch permission. UNKNOWN reason changes increment revision without reopening or clearing identity, and identical observations are idempotent. PR5 remains subject to fresh independent re-review.

The corrective direct-publication regression genuinely failed when only the SQL RESERVED→STARTED guard was relaxed, with the expected database-bypass assertion; exact migration restoration passed. Owned format/import diffs remain empty.

Broker source-head correction: sourcee0d1072 passed27 actual targeted race test events with zero skips/failures, affected vet and lint. Exact reviewed PR source head is mandatory on confirmed merge Execute/Lookup. A source-head-only mutation genuinely failed mismatch regression; restored source passed. Independent corrective PR review/landing remains required.

Supervisor source qualification:6fb1a7c passed whole-module build,270 regular and270 serialized race test pass events, zero skipped tests, vet and lint. Owned native fixtures cover exact lease/budget gates at launch and activation, emergency/pause races, UNKNOWN ownership, TERM-ignore/KILL, heartbeat, signed-boundary stale results, workspace ownership and trusted never-started retry/zero settlement. A primary-KILL-only mutation failed the actual reap-bound assertion; exact restoration passed, with owned reassert KILL kept for cleanup. The later timeout assertion is test-only and passed the restored targeted case; production Go bytes match the full-suite source. Source review found no blocking issue. Production OCI/restart-safe handles and service activation remain unavailable; independent exact-head PR delivery is still required.
