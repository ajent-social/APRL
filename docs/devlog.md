# Development Log
Investigation findings, debugging sessions, and benchmark results.
Entries are newest-first. Prune entries older than 90 days during `/tidy --trim`.
---

## 2026-10-01: Redis Streams and local Codex capability probe
**Type:** finding
**Tags:** Redis Streams, XREADGROUP, XAUTOCLAIM, Codex CLI, billing admission
**Problem:** Verify at-least-once Redis delivery and discover local Codex execution options without making provider requests or claiming unproven cost bounds.
**Investigation:** Ran `go test ./tests/spikes -run '^TestRuntimeProbe' -count=1` against the owned Redis fixture; 5 test/subtest functions ran with no skips. With `TEST_REDIS_URL` unset, the fixture failed immediately with `required integration service URL TEST_REDIS_URL is unset`. A consumer read an entry without acknowledging it, replayed its own pending entry, closed, and a replacement consumer reclaimed the entry with zero minimum idle time. The test asserted the stream entry ID and logical job UUID stayed stable and checked pending ownership before acknowledgement. The CLI read only version/help output from installed codex-cli 0.160.0; it did not inspect configuration values or invoke a provider. An injected unsupported billing envelope was also tested: a temporary unsafe admission behavior failed the test, then the restored policy passed.
**Root cause:** Redis preserves pending entries for consumer-group recovery, while each stream-entry ID remains distinct from the stable logical job UUID. Local help exposes JSONL, output-schema, ephemeral, config-override, and strict-config options. These options do not prove a hard billing ceiling.
**Fix:** Added a bounded disposable Redis probe with random resource names, explicit fixture ownership, and cleanup limited to its stream and consumer group. Paid execution remains disabled for absent and unsupported billing envelopes because no hard-budget enforcement proof exists. The restored full selector, direct `go run ./cmd/aprl-probe --local-only`, go vet, and both scoped golangci-lint checks passed.
**Impact:** T1.9 can build on the verified Streams redelivery and XAUTOCLAIM behavior. E2 still requires a real gateway/pricing-envelope proof before paid runtime admission.

### 2026 10 01 — T1.8 fenced ownership accepted

The coordinator ran all 15 focused lease race cases with real Postgres and zero skips, then both package vet/lint gates and the acceptance observer. An independent expiry-guard mutation made the intended strict-TTL case fail; restoration passed. The implementation binds execution ownership to the durable task/job/run tuple and fails closed on unsupported operation roles. Claim creates the run before budget admission; no process may start until reservation succeeds.

### 2026 10 01 — Intake, routing and lease wave accepted

The integrated module builds and all 59 fixture-backed test cases pass with no skips. Seven router cases cover explicit issue/human-PR enrollment, trusted CI hints including actual GitHub status payloads, unexpected human push, terminal fencing, recorded push handoff, stale completion and fail-closed policy. Review found and fixed an expected missing-PR lookup error and SQL polymorphic JSON parameter typing. Distinct delivery IDs coalesce to stable logical work. Independent completion-fence mutation failed the intended test, then restoration passed. The independent webhook early-ack mutation likewise failed its real commit-error case; all nine restored API cases passed. Production rollout and paid adapters remain disabled.

### 2026 10 01 — Shared billing and retry seams refined

The budget lane identified a conflict between admission ceilings and truthful accounting: the initial task cache constraints rejected actual overspend. A new migration test genuinely failed against the old schema. The unreleased core schema now preserves nonnegative amounts while allowing the ledger to record overage; admission still belongs to atomic budget checks. Reservation identity and pricing are immutable, runs have one reservation, and charge history is append-only. No deployed schema or live billing was changed.

CI's pending-check path now has transaction-composable retry: release the lease, finish its run and persist a future outbox while retaining the logical job UUID. Claim checks that deadline against the latest run. The coordinator ran 22 schema/lease race cases with no skips and all affected vet/lint gates. Disabling the due-time fence made the early duplicate test fail; restoration passed.

### 2026 10 01 — T1.10 atomic budget package accepted

Ten focused real-Postgres race cases executed with zero skips. Both package vet/lint gates and the acceptance observer passed. Disabling organization-capacity enforcement admitted excess concurrent reservations and caused the intended test failure; exact restoration passed. The ledger records overage and late post-settlement charges while retaining unknown coverage, and new admission uses authoritative numeric aggregates. Complete typed host bounds are persisted without rewriting queued jobs. Runtime-specific model/pricing enforcement remains an E2 proof.


### 2026 10 01 — T1.9 durable Redis delivery accepted

Eight focused real-service test events passed with zero skips, together with all affected vet/lint gates and the acceptance observer. Real deferred commit failure proves duplicate publication retains one logical job identity. Fresh markerless retry hints now claim a successor run instead of incorrectly acknowledging a predecessor's completion. Missing budget admission retains pending work without starting inference. An independent wrong-identity mutation failed the publication case; exact restoration passed.


### 2026 10 01 — Delivery/budget/CI wave accepted

Integrated build and 92 real-service test events pass with zero skips. CI's 12 focused cases and all affected vet/lint gates pass; removing the complete snapshot fence made stale-head admission fail as intended, and restoration passed. Pinning integration completes the old run before generation advancement, and subsequent router hints coalesce with the exact stable job identity. Pending checks persist a due retry without holding a worker; deadline wins even over a newly passing observation. A protected-target fixture was corrected to explicitly allow its target while retaining the human-approval guard.

The results probe found GitHub mutation request identity was not database-immutable. A new real-Postgres regression failed before adding the unreleased-schema intent trigger, then all seven migration cases passed. Intent identity/request/expected snapshot and creation time cannot change or be deleted; transport status, remote ID and confirmation result can change. No installed external schema was touched. Push request run/token content must additionally be host-validated by the broker and results boundary.


### 2026 10 01 — Independent delivery review correction

Read-only review found an early retry or undisposed malformed hint stopped the whole consumer, starving unrelated work. A real Redis run test failed for unknown jobs, early retries and missing admission. Expected entry-level errors now retain pending hints and continue; infrastructure or unexpected executor failures still surface to the host. Slow admission also consumed the lease lifetime before executor handoff; its manual-clock regression failed before adding fresh lease validation. All 13 delivery cases, package vet/lint and observer passed after correction; six focused race events passed with zero skips. The supervisor must independently validate immediately before process launch.


### 2026 10 01 — Supervisor process evidence seam

Read-only supervisor planning found no durable process/heartbeat/deadline/workspace evidence. A focused migration regression failed before adding optional complete registration fields, immutable process identity and monotonic heartbeat guards to the unreleased schema. All 24 migration/lease race cases passed with zero skips; affected storage/migration/test vet and lint passed. Actual process launch and owned group cleanup remain T1.16, with OCI/provider proof deferred to E2.

## 2026 10 04 — Full SDLC execution preflight

Reconciled main d97a89d and successful hosted CI37192937020. T9.1 records known deployment inputs/blocks without admitting AWS spend. E1/E6/E7/E8 historical source and fixture receipts are delivered; CLI production factories remain intentionally unavailable. T2.0 expands a credential-free first E2 horizon: run-scoped host result authentication plus bounded HTTPS submission. Local load above 10 holds local compiler/testing; coordinator verification uses the existing hosted Linux CI, never bypassing local lease rules. Default AWS identity is accessible but chosen account/region/cost and model/App credentials await operator inputs. DNS currently NXDOMAIN with Cloudflare authority; configured Cloudflare operation tools are absent. No production resource or model request made.
