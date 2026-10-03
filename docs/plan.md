# APRL Implementation Plan

Change Summary: 2026 10 03 - Durable control-plane foundation PR1 is present on current main (commit 6feaf92); E1 remains incomplete. T1.1-T1.12 and T1.14 have accepted task evidence; T1.13 and T1.15-T1.19 remain pending. The complete standalone/delegated lifecycle integration contract is documented below; external wire schema remains pending architecture freeze. No live paid execution or production rollout is claimed.

## 1. Context

Build APRL in Go, as a continuously available service that orchestrates separate author, reviewer, and fixer GitHub App identities. Webhooks feed a Postgres-authoritative state machine; Redis Streams delivers fenced jobs to ephemeral headless Codex workers. Routine PRs may merge only after current-snapshot CI/review and target policy pass. Human pause, bounded attempts, and enforceable budgets are product requirements.

The repository has an accepted Go/Postgres/Redis durable control-plane foundation on current main, with implementation and task receipts under `internal/`, `docs/tasks/`, and `docs/contracts.md`. E1 broker policy, recovery, process supervision, assembled service roles, end-to-end foundation proof, and quality-gate handoff remain incomplete as tracked below. This plan preserves the existing RFC and historical task evidence; it does not claim live provider execution or production deployment.

The user requires Go. Use standard-library net/http and flag, pgx/v5 for Postgres, and go-redis/v9 for Redis Streams. The RFC has been aligned to this Go/Redis Streams stack; the authoritative Postgres ownership and budget protocols are preserved. Pin dependency versions after the compatibility probe and pin the supported Go toolchain (local planning environment: Go 1.27.1). See [ADR 001](adr/001-service-runtime.md), [ADR 002](adr/002-durable-control-plane.md), and [ADR 003](adr/003-metered-execution.md). Budget bounds on real Codex/provider calls are unproven and block live paid agents, not fixture-based foundation work. No production host or credentials are supplied.

Success is measured by loss-free durable replay, stale-worker rejection, complete snapshot CI gating, zero admitted reservation overshoot, bounded remediation, and honest reconciliation of remote outcomes. Later rollout benchmark goals follow RFC section 11; false negatives must be measured alongside the <15% false-positive and >=80% two-attempt resolution targets. Production verification is required before a rollout milestone is shipped.

## 2. Discovery Summary and Use Case Summary

Ten planned use cases: seven P0 and three P1. The durable foundation implements and tests bounded portions of these use cases; the complete standalone/delegated lifecycle and live runtime remain incomplete. Catalog: `.claude/scratch/usecases-manifest.json`. Use-case coverage must be read with the task acceptance and evidence below, not inferred from the lifecycle RFC alone.

| ID | Priority | User outcome |
| --- | --- | --- |
| UC-001 | issue label aprl:implement | Implement an explicit issue/spec task |
| UC-002 | GitHub PR review | Receive a commit-specific review |
| UC-003 | GitHub PR branch | Resolve review or CI failures |
| UC-004 | GitHub PR merge | Merge an eligible reviewed snapshot |
| UC-005 | GitHub slash commands and labels | Pause and explicitly resume automation |
| UC-006 | .aprl/config.yaml | Configure trusted repository policy |
| UC-007 | operator policy and task budget ledger | Limit spending before any inference |
| UC-008 | GitHub webhook and service restart | Recover after webhook, queue, or worker failure |
| UC-009 | PR labels/comments and structured logs | Inspect and act on an escalation |
| UC-010 | GitHub push and review events | Preserve human branch changes and protected-target authority |

Accepted foundation contracts cover inbox/outbox boundaries, Postgres lease/generation ownership, current-head/base aggregate CI, pre-PR budget accounting, pause/merge races, authenticated result acceptance, and successor-generation replies. Broker, remote-operation recovery, service assembly, and end-to-end acceptance remain pending. Redis delivery is not ownership: Redis Streams provides transport redelivery while Postgres owns lifecycle state and due-time scheduling. CLI output support does not prove financial bounds.

## 3. Scope and Deliverables

In scope: durable control-plane foundation, review-only runtime integration, bounded remediation, authoring/guarded merging, operator observability, and production rollout gates. Outside scope: a dashboard UI, general triage, workflow-file modification, fork remediation, native unattended auto-merge, and unattended protected-production merges during initial rollout.

| ID | Deliverable | Owner | Acceptance |
| --- | --- | --- | --- |
| D1 | Fixture-backed running control plane | Coordinator | E1 recovery/cancellation vertical slice passes with real Postgres/Redis |
| D2 | Live review-only B | Coordinator + runtime worker | E2 proves isolation, metering and GitHub review correctness |
| D3 | B/C remediation loop | Coordinator + implementation workers | E3 meets attempt, stall and cancellation gates |
| D4 | A/B/C guarded PR lifecycle | Coordinator | E4 enforces target/human/CI/review merge admission |
| D5 | Operated production rollout | Operator + coordinator | E5 live deployment/recovery and escalation evidence recorded |

Proposed code ownership: `cmd/aprl` for the standard-library CLI; `internal/httpapi` for HTTP adapters; `internal/lifecycle`, `router`, `control`, `ci`, and `policy` for behavior; `internal/storage` and `migrations` for persistence; `internal/queue` and `dispatch` for delivery; `internal/leases`, `budget`, `broker`, and `results` for authority; `internal/supervisor` and `reconcile` for recovery. `internal/contracts` holds versioned data, while each consuming package defines narrow interfaces. No worker may edit another task's files without a coordinator-approved contract change.

Shared go.mod/go.sum and tool pins are coordinator-owned. T1.1 pins pgx and go-redis; T1.3 requests coordinator-owned YAML/schema dependency additions after validation. Test doubles stay in tests, never production wiring.

The initial service has three supervised roles: API, control, and worker. The control role runs routing, dispatch and reconciliation loops; worker supervisors run one-shot agents. Postgres and Redis are persistent backing services. Public webhook traffic and authenticated internal results are distinct boundaries. Health responses contain no secrets. There is no product UI in the frontier.

Queue contract: XADD carries a stable logical job UUID in its fields; Redis assigns an independent stream-entry ID. Duplicate stream entries are safe because Postgres deduplicates completed/owned logical work. XREADGROUP delivers, XACK follows durable completion/rejection, and XAUTOCLAIM restores abandoned pending delivery. Every reclaimed delivery must still obtain a valid Postgres lease. Delayed retries are due Postgres outbox rows, not an assumed Redis Streams scheduler. Do not trim unacknowledged entries; the durable reconciler rebuilds delivery after Redis loss.

## 4. Checkable Work Breakdown

### Lifecycle operating contract

APRL owns one canonical scheduler and eligibility/admission authority for each enrolled code-change lifecycle in both standalone and delegated modes. Standalone mode retains the full APRL lifecycle from authorized task through authoring, independent review, bounded fixes/re-reviews, and guarded merge. Delegated mode receives an approved lifecycle intent from a neutral product workflow controller; it uses APRL's same canonical child IDs, task state, readiness, scheduling, and stage routing. The controller retains its product graph and aggregate/product acceptance authority, but does not create a second scheduler for APRL stages. APRL's authoritative admission/result adapter governs an enrolled lifecycle. A developer claim is work pickup only and grants no runtime, provider, or GitHub mutation authority.

Each author/fix task that changes code produces a PR and records PR URL plus exact head on its dependent review task, then completes at that handoff. Every code-changing PR has an executable independent review task, selected and claimed through the ordinary apply+claim loop. The reviewer checks the exact head and base; on approval, the review lane owns guarded merge and verifies landing. A blocking review creates explicit bounded fix and dependent re-review tasks. Review without a PR URL/head is not eligible. Review, fix, or PR-handoff completion alone never releases an ordinary downstream dependency: it becomes ready only after the required PR is reviewed, merged, and landing is verified. An explicitly speculative dependency may start earlier only with its speculative status and inputs recorded; it grants no release authority. There is no no-review exemption path.

Shared plan/apply/claim representation, readiness, and stage routing remain generic and reusable. APRL's service-specific adapter supplies authoritative lifecycle admission/result facts. See [ADR 007](adr/007-whole-code-change-lifecycle.md) and [the integration contract](contracts.md#whole-code-change-lifecycle-integration). Exact external wire fields and schema version are pending architecture freeze and must be reconciled before publication or implementation.

### E1 - Durable control-plane foundation
fidelity: executable
Acceptance: T1.1-T1.19 pass on owned test services; a signed event completes through fixture execution after injected restarts, with stale work denied and budgets retained. Real model/GitHub mutations remain disabled until E2's runtime proof. Initial production deployment is E5, not an implied result of foundation tests.

- [x] T1.1 Bootstrap the Go module and fail-closed test harness  Owner: coordinator  Est: 60m  kind: agent  verifies: [infrastructure]  deps: []  acc: [Go module loads; integration test fixtures fail with a useful error when required DB/Redis URLs are absent; tool versions and scratch/cache paths are documented.]  lane: agent
  - Scope/contract: [docs/tasks/T1.1.md](tasks/T1.1.md); exact owned files and verification commands are listed there.
  - S1.1.1 Verify: Unset the integration database URL and observe a fixture setup error, not a skip. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.2 Probe Redis Streams and the installed Codex execution contract  Owner: runtime  Est: 90m  kind: agent  verifies: [UC-007, UC-008]  deps: [T1.1]  acc: [A bounded disposable Redis probe proves consumer-group redelivery, pending reclamation and client shutdown while preserving the logical job UUID; local CLI help/config probes record JSONL/schema/ephemeral support, billing-envelope unknowns, and a disabled paid-execution capability when bounds are unproven. Do not send paid provider requests in this task.]  lane: agent  blocked-by: [T1.1]
  - Scope/contract: [docs/tasks/T1.2.md](tasks/T1.2.md); exact owned files and verification commands are listed there.
  - S1.2.1 Verify: Inject an unsupported billing envelope and verify capability admission remains disabled. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.3 Freeze versioned job, event, result and review contracts  Owner: contracts  Est: 90m  kind: agent  verifies: [UC-002, UC-005, UC-008]  deps: [T1.1]  acc: [Version 1 carries task/job/generation, lease token, snapshot, attempt, operation ID and correlation; malformed payloads, inconsistent verdicts, and incomplete anchors reject; unanchored blockers validate.]  lane: agent  blocked-by: [T1.1]
  - Scope/contract: [docs/tasks/T1.3.md](tasks/T1.3.md); exact owned files and verification commands are listed there.
  - S1.3.1 Verify: Remove a required generation field from a fixture and observe validation failure. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.4 Create core Postgres migrations and constraints  Owner: database  Est: 90m  kind: agent  verifies: [UC-007, UC-008, UC-009]  deps: [T1.3]  acc: [RFC tables migrate into an empty Postgres 16 database; duplicate source/delivery/job keys reject; amount/state constraints reject invalid rows; audit records survive PR closure. Schema reset is confined to an owned test schema.]  blocked-by: [T1.3]  lane: agent
  - Scope/contract: [docs/tasks/T1.4.md](tasks/T1.4.md); exact owned files and verification commands are listed there.
  - S1.4.1 Verify: Insert a negative reservation and observe a database constraint failure. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.5 Implement transactional persistence and an injectable clock  Owner: database  Est: 90m  kind: agent  verifies: [UC-007, UC-008]  deps: [T1.4]  acc: [A unit of work commits event disposition/state/jobs/outbox together or rolls all back; row locking and lock order are documented; test clock never depends on wall-time sleeps.]  blocked-by: [T1.4]  lane: agent
  - Scope/contract: [docs/tasks/T1.5.md](tasks/T1.5.md); exact owned files and verification commands are listed there.
  - S1.5.1 Verify: Raise after creating a job but before commit and assert no partial rows survive. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.6 Accept signed webhook deliveries durably  Owner: api  Est: 90m  kind: agent  verifies: [UC-008]  deps: [T1.5]  acc: [POST /webhooks/github returns 202 with delivery_id/accepted on a committed signed body, 200 with duplicate true on replay, 401 with error invalid_signature on bad HMAC, and 503 with error storage_unavailable when commit fails; exact-byte HMAC precedes parsing/sanitization.]  blocked-by: [T1.5]  lane: agent
  - Scope/contract: [docs/tasks/T1.6.md](tasks/T1.6.md); exact owned files and verification commands are listed there.
  - S1.6.1 Verify: Inject a commit failure and assert no 2xx acknowledgement. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.7 Route events through guarded lifecycle transitions  Owner: control  Est: 90m  kind: agent  verifies: [UC-005, UC-008, UC-010]  deps: [T1.5]  acc: [Router records event disposition and logical jobs once, trusts configured event/app combinations, invalidates snapshot approvals on head/base changes, preserves paused/escalated observations, and keeps closed/merged terminal.]  blocked-by: [T1.5]  lane: agent
  - Scope/contract: [docs/tasks/T1.7.md](tasks/T1.7.md); exact owned files and verification commands are listed there.
  - S1.7.1 Verify: Deliver an old-generation completion and assert it creates no active-state transition/job. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.8 Claim and renew fenced Postgres execution leases  Owner: leases  Est: 90m  kind: agent  verifies: [UC-005, UC-008]  deps: [T1.5]  acc: [Two concurrent claims yield one owner per task; expired owner cannot heartbeat, complete, or admit a mutation after replacement; tokens and generation are checked atomically.]  blocked-by: [T1.5]  lane: agent
  - Scope/contract: [docs/tasks/T1.8.md](tasks/T1.8.md); exact owned files and verification commands are listed there.
  - S1.8.1 Verify: Expire and replace a lease, then assert the old token is denied. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.9 Dispatch the transactional outbox through Redis Streams  Owner: queue  Est: 90m  kind: agent  verifies: [UC-008]  deps: [T1.2, T1.5, T1.8]  acc: [Outbox publishes stream entries carrying stable UUID logical job IDs; crash after publish/before acknowledgement redelivers safely; queue records are hints, never proof of completion; all clients close during shutdown. Consumer delivery, XACK after durable disposition, and XAUTOCLAIM reclamation are owned here; reclaim must not steal a still-valid Postgres lease. Delayed retries use due Postgres outbox rows.]  blocked-by: [T1.2, T1.5, T1.8]  lane: agent
  - Scope/contract: [docs/tasks/T1.9.md](tasks/T1.9.md); exact owned files and verification commands are listed there.
  - S1.9.1 Verify: Fault after queue publication and verify retry preserves one logical job identity. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.10 Reserve and settle task/org budgets atomically  Owner: budget  Est: 90m  kind: agent  verifies: [UC-007]  deps: [T1.5]  acc: [Task exists before PR; concurrent admissions lock org then task and include rolling spend plus outstanding reservations; settlement is request-id idempotent; cancellation/unknown usage retains coverage; unknown pricing envelopes deny admission. Money uses integer micro-USD with conservative rounding, never float64. Duplicate settlement is a no-op only for identical reservation/run/usage/cost; conflicting request-ID reuse rejects. Test rolling-window boundaries and concurrent settlement/admission.]  blocked-by: [T1.5]  lane: agent
  - Scope/contract: [docs/tasks/T1.10.md](tasks/T1.10.md); exact owned files and verification commands are listed there.
  - S1.10.1 Verify: Race reservations exceeding remaining capacity and assert at least one is denied without overshoot. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.11 Apply authorized pause/resume and bounded attempt controls  Owner: control  Est: 90m  kind: agent  verifies: [UC-003, UC-005, UC-010]  deps: [T1.7, T1.8]  acc: [Unauthorized controls reject; pause records generation/cancellation atomically during Redis outage; label removal never resumes; resume preserves spend/attempt counters; C admission rejects at >= configured maximum for CI and review attempts.]  blocked-by: [T1.7, T1.8]  lane: agent
  - Scope/contract: [docs/tasks/T1.11.md](tasks/T1.11.md); exact owned files and verification commands are listed there.
  - S1.11.1 Verify: Replay CI-only failure admissions at the attempt limit and assert no additional C job. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.12 Aggregate CI by trusted head/base integration snapshot  Owner: ci  Est: 90m  kind: agent  verifies: [UC-002, UC-004, UC-006]  deps: [T1.3, T1.7, T1.8]  acc: [All required checks must succeed on the pinned integration snapshot with trusted producer; stale/missing/pending/cancelled/neutral/skipped checks deny admission; empty policy denies autonomous merge; duplicate completions queue B once; timeout escalates.]  blocked-by: [T1.3, T1.7, T1.8]  lane: agent
  - Scope/contract: [docs/tasks/T1.12.md](tasks/T1.12.md); exact owned files and verification commands are listed there.
  - S1.12.1 Verify: Mix a current passing lint check with old-SHA passing tests and assert review admission stays closed. Run the scoped tests, then formatter/linter checks after code changes.
- [ ] T1.13 Build broker policy and durable mutation admission  Owner: broker  Est: 90m  kind: agent  verifies: [UC-002, UC-004, UC-010]  deps: [T1.8, T1.11, T1.12]  acc: [Fake transport enforces A/B/C capability matrix, lease/generation/head/base gates and serialized pause/merge admission; expected head SHA is in merge request; protected targets require current human approval; no token reaches worker payload. No real GitHub writes yet. Fake transport exists only in tests and is constructor-injected; production broker construction requires a configured real transport or fails closed, never defaults to fake success.]  lane: agent  blocked-by: [T1.8, T1.11, T1.12]
  - Scope/contract: [docs/tasks/T1.13.md](tasks/T1.13.md); exact owned files and verification commands are listed there.
  - S1.13.1 Verify: Try C approval/merge/out-of-branch push with a valid lease and assert denial. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.14 Accept authenticated worker results and atomic push handoffs  Owner: results  Est: 90m  kind: agent  verifies: [UC-002, UC-003, UC-008]  deps: [T1.7, T1.8, T1.10]  acc: [POST /internal/results authenticates a host service identity (not a repository token): 401 error unauthorized on missing auth, 409 error stale_result on bad lease/generation/snapshot, and 200 accepted with operation_id on idempotent valid completion. Confirmed push bumps generation once and creates successor reply jobs once. Supervisor authentication is bound to the task/run/lease/generation admission; wrong-task/run credentials reject with 403 forbidden even if a different lease is otherwise valid. Identical replay is idempotent and conflicting replay rejects.]  blocked-by: [T1.7, T1.8, T1.10]  lane: agent
  - Scope/contract: [docs/tasks/T1.14.md](tasks/T1.14.md); exact owned files and verification commands are listed there.
  - S1.14.1 Verify: Race push webhook and completion for one operation and assert exactly one generation transition/reply intent set. Run the scoped tests, then formatter/linter checks after code changes.
- [ ] T1.15 Reconcile durable jobs and ambiguous remote operations  Owner: recovery  Est: 90m  kind: agent  verifies: [UC-008, UC-009]  deps: [T1.9, T1.13, T1.14]  acc: [Queue loss republishes unfinished work; expired leases fence before replacement; UNKNOWN GitHub writes query fake remote state before retry; labels repair without state advancement; merged state requires remote confirmation.]  blocked-by: [T1.9, T1.13, T1.14]  lane: agent
  - Scope/contract: [docs/tasks/T1.15.md](tasks/T1.15.md); exact owned files and verification commands are listed there.
  - S1.15.1 Verify: Simulate accepted merge plus timeout and assert reconciliation confirms instead of issuing a second merge. Run the scoped tests, then formatter/linter checks after code changes.
- [ ] T1.16 Supervise bounded fixture worker processes  Owner: supervisor  Est: 90m  kind: agent  verifies: [UC-005, UC-007, UC-008]  deps: [T1.8, T1.10, T1.11]  acc: [Supervisor launches only the owned fake-agent fixture, records process/run identity, heartbeat, timeout and result; pause kills process group after TERM/KILL grace and fences IPC; cleans only owned task workspaces. Actual OCI/Codex sandbox is deferred to E2. Fixture execution is available only through test injection; production configuration cannot select it. Test production startup rejects a missing OCI adapter instead of running a fake worker.]  blocked-by: [T1.8, T1.10, T1.11]  lane: agent
  - Scope/contract: [docs/tasks/T1.16.md](tasks/T1.16.md); exact owned files and verification commands are listed there.
  - S1.16.1 Verify: Make the fixture ignore TERM and verify KILL plus stale-result denial after the configured grace. Run the scoped tests, then formatter/linter checks after code changes.
- [ ] T1.17 Assemble separately supervised service roles and health APIs  Owner: coordinator  Est: 90m  kind: agent  verifies: [UC-008, UC-009]  deps: [T1.6, T1.9, T1.14, T1.15, T1.16]  acc: [Commands api/control/worker start separate roles with shared contracts. GET /healthz returns 200 status alive; GET /readyz returns 200 status ready only with DB/role dependencies available, otherwise 503 status not_ready with dependency names and no secrets. Control loops use bounded polling, persisted deadlines, signal-driven shutdown and no queue-held locks while waiting on CI. API/control roles can run in the foundation; production worker startup fails closed until E2 supplies the real OCI/provider adapters. Tests may inject fixture adapters but deployment flags cannot enable them.]  blocked-by: [T1.6, T1.9, T1.14, T1.15, T1.16]  lane: agent
  - Scope/contract: [docs/tasks/T1.17.md](tasks/T1.17.md); exact owned files and verification commands are listed there.
  - S1.17.1 Verify: Stop a required backing service and assert readiness 503 while liveness remains 200. Run the scoped tests, then formatter/linter checks after code changes.
- [ ] T1.18 Prove the foundation crash/cancellation vertical slice  Owner: coordinator  Est: 90m  kind: agent  verifies: [UC-005, UC-007, UC-008, UC-010]  deps: [T1.17]  acc: [Real Postgres/Redis plus fake GitHub/inference execute signed receipt -> routing -> dispatch -> lease -> result; restart at each seam, Redis loss, duplicate delivery, stale result, budget race, pause, and ambiguous write all satisfy invariants. No live agents or merges enabled.]  lane: agent  blocked-by: [T1.17]
  - Scope/contract: [docs/tasks/T1.18.md](tasks/T1.18.md); exact owned files and verification commands are listed there.
  - S1.18.1 Verify: Disable the generation guard in an owned test worktree and observe the stale-result regression fail; restore and report both outcomes. Run the scoped tests, then formatter/linter checks after code changes.
- [ ] T1.19 Install scoped quality gates and record foundation handoff  Owner: coordinator  Est: 60m  kind: agent  verifies: [infrastructure, UC-008]  deps: [T1.18]  acc: [CI runs gofmt/goimports, go vet, golangci-lint and unit/API/integration/system tests with real service fixtures and no silent integration skips; unsupported paid-execution capability stays disabled; record exact green commands and all pending live integration prerequisites. The CI workflow provisions owned Postgres/Redis service fixtures, sets required URLs, verifies executed test counts, and serializes the full-suite race lane.]  blocked-by: [T1.18]  lane: agent
  - Scope/contract: [docs/tasks/T1.19.md](tasks/T1.19.md); exact owned files and verification commands are listed there.
  - S1.19.1 Verify: Make a required integration fixture absent and assert CI fails rather than passes/skips. Run the scoped tests, then formatter/linter checks after code changes.

### E2 - Review-only B with verified runtime and metering
fidelity: outline
Build the real OCI supervisor, external metered gateway and credential broker adapter, trusted checkout/config, review output validation, thread/finding persistence, and GitHub review posting. Prove provider request bounds before admitting paid execution.
Acceptance: A live sandbox review preserves unanchored blockers; no fixer/merge is enabled; credentials/egress/cancellation and billing envelopes are proven for pinned runtime versions.
- [ ] T2.0 PLAN: expand E2 after its trigger evidence  Owner: coordinator  Est: 60m  kind: plan  delivers: [E2 executable tasks, contracts, and updated use cases]  deps: [T1.19]  acc: [E2 is executable with resolved dependencies, owned file scopes and falsifiable acceptance for every row]  blocked-by: [T1.19]  blocked: Prior epic implementation exit evidence is not yet available; coordinator-only planning
Trigger: Dependency planning-task completion alone never permits downstream coding; require the prior epic's implementation acceptance, substitute its resulting milestone task IDs, then groom this epic. E5 may start with review-only production while E3/E4 remain disabled.

### E3 - Bounded B/C remediation
fidelity: outline
Expand after E2 live acceptance, adding fixer inputs for CI and persisted findings, target merges without force-push, local correction limits, successor reply handoffs, thread resolution and fingerprint normalization.
Acceptance: CI-only and review loops stop at shared attempt/cost limits; human pushes and pauses fence all work; benchmark evidence records resolution and review error rates.
- [ ] T3.0 PLAN: expand E3 after its trigger evidence  Owner: coordinator  Est: 60m  kind: plan  delivers: [E3 executable tasks, contracts, and updated use cases]  deps: [T2.0]  acc: [E3 is executable with resolved dependencies, owned file scopes and falsifiable acceptance for every row]  blocked-by: [T2.0]  blocked: Prior epic implementation exit evidence is not yet available; coordinator-only planning
Trigger: Dependency planning-task completion alone never permits downstream coding; require the prior epic's implementation acceptance, substitute its resulting milestone task IDs, then groom this epic. E5 may start with review-only production while E3/E4 remain disabled.

### E4 - Authoring and guarded merging
fidelity: outline
Expand after E3 acceptance, adding issue/spec task enrollment, author retries/escalation, pre-PR budgets, target selection, current-snapshot human approvals and guarded squash merge reconciliation.
Acceptance: Sandbox issue-to-PR succeeds; no stale/unreviewed/protected-without-human snapshot merges; GitHub rules are verified and never bypassed.
- [ ] T4.0 PLAN: expand E4 after its trigger evidence  Owner: coordinator  Est: 60m  kind: plan  delivers: [E4 executable tasks, contracts, and updated use cases]  deps: [T3.0]  acc: [E4 is executable with resolved dependencies, owned file scopes and falsifiable acceptance for every row]  blocked-by: [T3.0]  blocked: Prior epic implementation exit evidence is not yet available; coordinator-only planning
Trigger: Dependency planning-task completion alone never permits downstream coding; require the prior epic's implementation acceptance, substitute its resulting milestone task IDs, then groom this epic. E5 may start with review-only production while E3/E4 remain disabled.

### E5 - Production service operations and phased rollout
fidelity: outline
Expand when the operator provides an owned production target and E2 live integration evidence. Add provisioning/supervision, TLS/webhook routing, secrets, DB backup/restore, retention, monitoring, escalation notification and phased enablement.
Acceptance: Deploy to production and observe health, signed deliveries, recovery and control behavior live; approve Phase 2/3 separately after E3/E4 gates; document rollback as disabling autonomy without deleting audit data.
- [ ] T5.0 PLAN: expand E5 after its trigger evidence  Owner: coordinator  Est: 60m  kind: plan  delivers: [E5 executable tasks, contracts, and updated use cases]  deps: [T2.0]  acc: [E5 is executable with resolved dependencies, owned file scopes and falsifiable acceptance for every row]  blocked-by: [T2.0]  blocked: Prior epic implementation exit evidence is not yet available; coordinator-only planning

### E6 - Whole-lifecycle integration and executable review flow

E6 integrates the complete lifecycle adapter with generic plan/apply/claim workflows while preserving standalone APRL operation. It does not assert live readiness. E6 implementation depends on E1 foundation handoff and the frozen external protocol. Exact wire schema is pending; do not implement guessed fields.

- [ ] T6.0 PLAN: freeze whole-lifecycle integration contract  Owner: coordinator  Est: 60m  kind: plan  delivers: [versioned protocol, compatibility policy, lifecycle/readiness mapping, standalone and delegated acceptance]  deps: [T1.19]  acc: [Architecture-frozen schema and service-specific authoritative admission/result adapter are documented; no second scheduler is introduced; standalone flow remains complete]  blocked-by: [T1.19]  blocked: Exact external wire schema remains pending architecture lane
- [ ] T6.1 Implement the lifecycle protocol adapter  Owner: coordinator  Est: 90m  kind: agent  verifies: [UC-001, UC-002, UC-003, UC-004, UC-008]  deps: [T6.0]  acc: [Approved lifecycle intent maps to APRL canonical child IDs/state/readiness; result and admission facts are authenticated, idempotent, fenced and reconciled; protocol version compatibility is explicit]  blocked-by: [T6.0]
- [ ] T6.2 Route standalone and delegated lifecycles through one scheduler  Owner: coordinator  Est: 90m  kind: agent  verifies: [UC-001, UC-002, UC-003, UC-004]  deps: [T6.1]  acc: [Standalone author/review/fix/re-review/merge remains executable; delegated mode uses the same APRL scheduler; review is ordinary apply+claim work; blocking findings expand bounded fix/re-review children; reviewer verifies exact head/base, merges and verifies landing]  blocked-by: [T6.1]
- [ ] T6.3 Integrate generic task tooling with lifecycle admission  Owner: shared-tooling maintainer + coordinator  Est: 90m  kind: agent  verifies: [infrastructure]  deps: [T6.1]  acc: [Generic plan/apply/claim task representation, readiness and stage routing remain reusable; ordinary descendants wait for verified landing; explicit speculative dependencies are recorded and confer no release authority; service adapter remains authoritative; no review exemption exists]  blocked-by: [T6.1]
- [ ] T6.4 Independently review lifecycle integration changes  Owner: independent reviewer  Est: 60m  kind: agent  verifies: [infrastructure]  stage: review  deps: [T6.2, T6.3]  acc: [Review task is executable by normal apply+claim; records PR URL and exact head; reviewer is independent; blocker findings create bounded fix/re-review tasks; approval covers exact head/base; merge and verified landing are recorded]  blocked-by: [T6.2, T6.3]
Trigger: Dependency planning-task completion alone never permits downstream coding; require the prior epic's implementation acceptance, substitute its resulting milestone task IDs, then groom this epic. E5 may start with review-only production while E3/E4 remain disabled.

## 5. Parallel Work and Waves

The coordinator owns shared contracts, integration, plan/roadmap and ADR amendments. Use up to three GPT-6-Luna implementation workers plus one coordinator, matching this session's four slots. Each implementation worker requires its own unique external-SSD worktree and exact task file scope. Read-only planning probes do not own or edit files. Go skill conventions apply: context-first I/O, wrapped errors, no library panics, bounded goroutine lifetimes, consumer-defined interfaces and no fabricated success in production paths. Test doubles live only in _test.go or explicit testutil packages; unavailable runtime adapters fail closed. Model overrides for ambiguous contract/runtime decisions stay on the coordinator. This APRL-specific three-worker limit remains unchanged by this lifecycle documentation update.

| Track | Tasks | Sync boundary |
| --- | --- | --- |
| Persistence/durability | T1.4-T1.6, T1.8-T1.9 | Frozen T1.3 contracts; shared T1.5 repositories |
| Control/policy | T1.7, T1.11-T1.12 | Persisted transitions and lease APIs |
| Admission/recovery | T1.10, T1.13-T1.16 | Budget, broker and result contracts |
| Integration/quality | T1.17-T1.19 | All tracks verified before assembly |

### Waves

#### Wave 1: Bootstrap (1 worker)
- [x] T1.1 Scheduling reference; acceptance and scope are in the WBS.
T1.1. Coordinator reviews skeleton and integration fixture failures.
#### Wave 2: Contracts and feasibility (2 workers)
- [x] T1.2 Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.3 Scheduling reference; acceptance and scope are in the WBS.
T1.2 and T1.3. Freeze interfaces before any consuming lane begins.
#### Wave 3: Schema (1 worker)
- [x] T1.4 Scheduling reference; acceptance and scope are in the WBS.
T1.4.
#### Wave 4: Persistence (1 worker)
- [x] T1.5 Scheduling reference; acceptance and scope are in the WBS.
T1.5.
#### Wave 5: Independent intake/control/leases (3 workers)
- [x] T1.6 Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.7 Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.8 Scheduling reference; acceptance and scope are in the WBS.
T1.6, T1.7, T1.8.
#### Wave 6: Delivery/budget/CI (3 workers)
- [x] T1.9 Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.10 Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.12 Scheduling reference; acceptance and scope are in the WBS.
T1.9, T1.10, T1.12.
#### Wave 7: Authorized controls and results (2 workers)
- [x] T1.11 Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.14 Scheduling reference; acceptance and scope are in the WBS.
T1.11, T1.14.
#### Wave 8: Broker and fixture supervisor (2 workers)
- [ ] T1.13 Scheduling reference; acceptance and scope are in the WBS.
- [ ] T1.16 Scheduling reference; acceptance and scope are in the WBS.
T1.13, T1.16.
#### Wave 9: Recovery (1 worker)
- [ ] T1.15 Scheduling reference; acceptance and scope are in the WBS.
T1.15.
#### Wave 10: Assembly (1 worker)
- [ ] T1.17 Scheduling reference; acceptance and scope are in the WBS.
T1.17.
#### Wave 11: Fault verification (1 worker)
- [ ] T1.18 Scheduling reference; acceptance and scope are in the WBS.
T1.18.
#### Wave 12: Quality gate (1 worker)
- [ ] T1.19 Scheduling reference; acceptance and scope are in the WBS.
T1.19.
#### Wave 13: Next frontier planning (1 coordinator)
- [ ] T2.0 Scheduling reference; acceptance and scope are in the WBS.
T2.0. Later planning waves are scheduled after the previous epic's actual exit, not just its planning row. Lower parallel counts reflect real schema/interface/integration dependencies; do not fill them with unsupported future implementation.

#### Wave 14: Deferred remediation planning (1 coordinator)
- [ ] T3.0 Scheduling reference; blocked until expanded E2 implementation exits.
#### Wave 15: Deferred authoring/merge planning (1 coordinator)
- [ ] T4.0 Scheduling reference; blocked until expanded E3 implementation exits.
#### Wave 16: Deferred production planning (1 coordinator)
- [ ] T5.0 Scheduling reference; may be groomed after E2 evidence and target selection, before E3/E4 if review-only deployment is desired.
These deferred planning waves are triggers, not a fixed calendar. Expansion inserts the actual implementation tasks and revises downstream scheduling.

## 6. Timeline and Milestones

| ID | Milestone | Dependencies | Exit evidence |
| --- | --- | --- | --- |
| M1 | Foundation ready | T1.19 | Real DB/Redis fault slice and scoped CI green; paid runtime disabled if unproven |
| M2 | Review-only pilot | Expanded E2 exit task | Actual sandbox review, isolation/metering proof and review benchmark |
| M3 | Closed-loop pilot | Expanded E3 exit task | Shared attempt bounds, verified fixes, pause and human-push tests |
| M4 | End-to-end pilot | Expanded E4 exit task | Issue-to-PR and guarded merge tests with actual GitHub rules |
| M5 | Production rollout | Expanded E5 exits plus applicable M2-M4 | Live deployment and recovery evidence for each enabled phase |

Foundation estimates total 1650 worker-minutes (27.5 worker-hours), excluding environment setup, review and integration overhead. Plan roughly 4-6 working days for M1 with up to three workers and one coordinator; this is a sequencing estimate, not a deadline commitment. Re-estimate E2-E5 from observed runtime and production evidence rather than assigning speculative dates.

## 7. Risk Register

| ID | Risk | Impact | Likelihood | Mitigation |
| --- | --- | --- | --- | --- |
| R1 | Provider/Codex billing envelope cannot be enforced | Blocks paid autonomy | Unknown | T1.2 capability report; E2 real gateway proof; fail closed |
| R2 | Redis pending delivery and Postgres retry semantics diverge | Delivery bugs | Medium | T1.2 redelivery probe; Postgres due-times and fencing; pending-message reclaim tests |
| R3 | Pause or lease replacement races with remote write | Stale mutations | High | Generation/lease checks; serialized broker; reconcile accepted outcomes |
| R4 | CI tests head but not current target integration | Unsafe readiness | High | Head/base/integration identity and repository-rule gates |
| R5 | Green tests silently skip DB/Redis | False safety evidence | Medium | Fail-closed fixtures and explicit CI services |
| R6 | Production target/App credentials unavailable | Live gates blocked | Unknown | E5 operator-owned prerequisites; no implied account access |
| R7 | Shared schema/contracts drift across workers | Integration churn | Medium | T1.3 freeze, single coordinator, explicit file ownership |
| R8 | Real sandbox cannot isolate secrets, host sockets or egress | Unauthorized code authority | High | External broker/gateway, OCI isolation/egress acceptance before E2 |

## 8. Operating Procedure

Changes involving concurrency require the coordinator to run the full go test -race ./... gate on the single serialized race lane before completion; workers run their scoped race commands and do not launch competing full-suite race lanes.

A frontier task is complete when its scoped tests and paired table-driven regression test pass, the stated genuine-red probe fails as predicted then passes after restoration, relevant Go formatting, vet and lint checks pass, and integration review accepts the contract. API changes require real HTTP status/body assertions. No UI is planned, so browser tests are inapplicable until scope changes. Contracts are hypotheses: no worker contract is certified today. Certification requires the intended worker tier to execute it successfully with recorded evidence.

Implementation happens in isolated task-specific worktrees on an owned external volume; verify mount, writability and capacity at dispatch. Task-specific GOCACHE, GOMODCACHE, GOTMPDIR, temp paths, test databases/Redis namespaces and generated artifacts stay on that volume; do not repurpose HOME or CODEX_HOME. Never clean another lane's files. Obtain the shared build lease for multi-package go build/go test/golangci-lint commands covered by the global build rule, check load first, and release immediately with the returned claim SHA. Run go test -race ./... on exactly one lane, with at most two heavy build lanes per project. Serialize heavy integration resource use as needed.

Use the claim skill to own tasks and shared plan edits; release only the acquired SHA. Before rewriting the plan, re-read and merge current content. Commit only authorized, task-owned changes; this planning turn makes no commit or PR. Future code merges follow repository CI/rules and require an explicit review. No production environment currently exists, so frontier completion is local/CI foundation acceptance; an epic's user-facing rollout is not shipped until E5 deploys and verifies it live. Deployment, account creation, paid calls and notifications need task-specific authorization at execution time.

## 9. Progress Log

- 2026 10 01: Created E1/T1.1-T1.19 executable frontier, E2-E5 deferred planning rows, ADRs 001-003, use-case manifest and 19 pending Go task contracts; aligned RFC/ADR 001 to the user's Go requirement; no code or runtime acceptance claimed.

- 2026 10 01: Execution routing follows the user's GPT-6-Luna requirement; coding rows use agent lanes and acceptance observation remains separate. T1.4 also owns `migrations/migrations.go` to embed the SQL source without duplication.

## 10. Hand off Notes

T1.1-T1.12 are locally accepted and integrated. T1.14 is also accepted. Wave 7 integrated build, regular/race suites (136 pass events each, zero skips), vet/lint and formatting pass. Broker/supervisor are active on two owned Luna lanes. The production APRL service is not deployed. Do not confuse an existing local agent proxy with APRL's future gateway. The installed shared plan parser recognizes task IDs/dependencies but does not understand kind: plan, fidelity, or acc fields. Outline planning rows are therefore explicitly blocked and must be handled by the coordinator; do not dispatch them as implementation workers from legacy parsed JSON. Clear that external planning guard only after actual preceding exit evidence and re-grooming. The raw Markdown/contract files remain authoritative.

All engineering rows execute on the user-selected GPT-6-Luna agent lanes. Read-only acceptance observation is separate from coding dispatch. The prescribed waves cap coding concurrency at three workers.

Required execution inputs: owned Postgres 16 and Redis test instances/URLs; Linux OCI runtime for E2; three separately scoped GitHub App installations and a dedicated sandbox repository; approved required-check integration conventions; supported model/pricing/gateway contract; operator escalation channel; and an owned production target for E5. These are placeholders, not proven entitlements. Do not store credentials in docs or worker payloads. Outline planning tasks must replace their trigger with actual preceding exit task IDs when expanded.

## 11. Appendix

- [RFC-0001](rfc/rfc-0001.md): behavior and guardrails.
- [ADR 001](adr/001-service-runtime.md): Go service roles and OCI runtime choice.
- [ADR 002](adr/002-durable-control-plane.md): durable ownership and recovery.
- [ADR 003](adr/003-metered-execution.md): hard-budget prerequisite.
- [ADR 004](adr/004-build-ownership.md): coordinator executes strict shared-lease quality gates while coding lanes remain parallel.
- [Redis Go client](https://redis.io/docs/latest/develop/clients/go/): go-redis/v9.
- [Redis XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/): pending-message redelivery.
- [pgx/v5](https://pkg.go.dev/github.com/jackc/pgx/v5): PostgreSQL driver and pool.
- [Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference#configtoml): provider configuration; experimental rollout tracking is not a USD-cap guarantee.
- Task contracts: `docs/tasks/T1.1.md` through `docs/tasks/T1.19.md`; T1.1-T1.12 and T1.14 locally certified; T1.13 and T1.15-T1.19 pending.

- Execution checkpoint 2026 10 01: Wave 6 accepted locally. Integrated build and 92 real-service test cases pass, with zero skips. Next prescribed wave is T1.11/T1.14 on two Luna lanes; compiler checks remain coordinator-only under ADR 004.

- Delivery integration review: expired admissions before executor invocation now release unused coverage only with host evidence of no execution; metered or ambiguous admissions retain UNKNOWN coverage. Genuine-red regression failed on the previous behavior. All 15 focused delivery cases and eight focused race events pass with no skips; affected package vet/lint and acceptance observation pass.

- T1.11 controls locally accepted: thirteen real-service race events, both affected packages vet/lint, formatting and observer pass; genuine-red attempt-limit probe fails as predicted and exact restoration passes. T1.14 remains under verification.

- Recovery checkpoint 2026 10 02: machine restart preserved integration commits, pending shared CI changes and all results source. Owned services restarted; source snapshots and Git history salvaged to the task artifact volume. Shared CI/control integration has 26 race-test events, zero skips, affected-package vet/lint and independent review passing. The CI generation-fence regression failed against the previous implementation. Results remain pending final genuine-red restoration and source gates.

## Completed-work delivery checkpoint

The user requested merging completed work while E1 implementation continues. These are coordinator stages for the accepted slice, not additional implementation lanes or evidence of E1 completion. The 19 engineering task rows remain the implementation progress denominator.

- [x] S0.1 Verify accepted foundation slice  Owner: coordinator  stage: verify  deps: [T1.1, T1.2, T1.3, T1.4, T1.5, T1.6, T1.7, T1.8, T1.9, T1.10, T1.11, T1.12, T1.14]  acc: [Candidate build, actual-service suite, serialized race checks, formatting, vet and lint pass with no skipped tests]
- [x] S0.2 Independently review merge candidate  Owner: coordinator  stage: review  deps: [S0.1]  acc: [Reviewed source revision recorded and blocking findings resolved]
- [x] S0.3 Rebase merge completed slice  Owner: coordinator  stage: merge  deps: [S0.2]  acc: [Foundation PR1 is present on origin/main at 6feaf927897e2044717afa02c4a659b33f8fa42e; accepted candidate verification and review evidence remains in docs/verification.md; this does not complete E1]
- [x] S0.4 Verify landed completed slice  Owner: coordinator  stage: verify-landed  deps: [S0.3]  acc: [The exact landed main commit is the foundation tree with accepted verification evidence; current main checkout resolves to that commit; E1 follow-on rows remain pending]

- T1.14 results locally accepted after recovery: 17 race events, zero skips; three packages vet/lint, formatting and observer pass. Reviewer-controlled suppression of reply materialization failed the actual webhook/result race; exact restoration passes. Original snapshot includes integration identity and new pushed snapshot clears it. Old-generation cancelled job is not reauthorized by result receipt.

- Historical completed-slice verification/review 2026 10 02: candidate b4255df had 136 regular and 136 race pass events, zero skips, build, vet, lint and formatting passing. Independent reviewers resolved one admission-retry blocker with a genuine-red emergency regression and verified all four retry/error cases. The then-active broker/supervisor work and rebase-merge wording are historical; PR1 is now present on current main, while E1 remains incomplete.

## Versioned whole-lifecycle protocol adoption

The [code-delivery/v1 wire contract](contracts/code-delivery-v1.md) and canonical request/digest fixtures freeze the neutral caller-facing protocol. Implementation and interoperability remain future work: E6 must prove subscription-only admission, first-class executable apply+claim children, independent review, bounded explicit corrections, and authenticated verified-landing receipts before a caller claims adoption. Schema freeze does not complete E1 or qualify remote execution.
