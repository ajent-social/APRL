# APRL Implementation Plan

Change Summary: 2026 10 03 - E1 foundation tasks T1.1-T1.19 have accepted/delivered evidence; current main is 5bb7556 after PR12. Owned-service tests and hosted Linux CI pass 315 normal/315 race events with zero failures/skips. The complete standalone/delegated lifecycle integration contract is documented below; neutral code-delivery/v1 wire schema is frozen; adapter and interoperability proof remain pending. No live paid execution or production rollout is claimed.

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

Shared plan/apply/claim representation, readiness, and stage routing remain generic and reusable. APRL's service-specific adapter supplies authoritative lifecycle admission/result facts. See [ADR 007](adr/007-whole-code-change-lifecycle.md) and [the integration contract](contracts.md#whole-code-change-lifecycle-integration). Exact external wire fields and schema version are frozen in [code-delivery/v1](contracts/code-delivery-v1.md); adapter implementation and interoperability remain pending.

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
- [x] T1.13 Build broker policy and durable mutation admission  Owner: broker  Est: 90m  kind: agent stage: author delivery-gate: T1.13.R  verifies: [UC-002, UC-004, UC-010]  deps: [T1.8, T1.11, T1.12]  acc: [Fake transport enforces A/B/C capability matrix, lease/generation/head/base gates and serialized pause/merge admission; expected head SHA is in merge request; protected targets require current human approval; no token reaches worker payload. No real GitHub writes yet. Fake transport exists only in tests and is constructor-injected; production broker construction requires a configured real transport or fails closed, never defaults to fake success.]  lane: agent  blocked-by: [T1.8, T1.11, T1.12]
  - Scope/contract: [docs/tasks/T1.13.md](tasks/T1.13.md); exact owned files and verification commands are listed there.
- [x] T1.13.R Independently review and deliver broker  Owner: independent-reviewer kind: agent stage: review lane: agent blocked-by: [T1.13] pr-url: https://github.com/ajent-social/APRL/pull/4 acc: [exact PR head accepted, normal guarded merge, actual landing verified]
  - S1.13.1 Verify: Try C approval/merge/out-of-branch push with a valid lease and assert denial. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.14 Accept authenticated worker results and atomic push handoffs  Owner: results  Est: 90m  kind: agent  verifies: [UC-002, UC-003, UC-008]  deps: [T1.7, T1.8, T1.10]  acc: [POST /internal/results authenticates a host service identity (not a repository token): 401 error unauthorized on missing auth, 409 error stale_result on bad lease/generation/snapshot, and 200 accepted with operation_id on idempotent valid completion. Confirmed push bumps generation once and creates successor reply jobs once. Supervisor authentication is bound to the task/run/lease/generation admission; wrong-task/run credentials reject with 403 forbidden even if a different lease is otherwise valid. Identical replay is idempotent and conflicting replay rejects.]  blocked-by: [T1.7, T1.8, T1.10]  lane: agent
  - Scope/contract: [docs/tasks/T1.14.md](tasks/T1.14.md); exact owned files and verification commands are listed there.
  - S1.14.1 Verify: Race push webhook and completion for one operation and assert exactly one generation transition/reply intent set. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.13.F1 Bind confirmed merge receipts to the reviewed source head  Owner: broker kind: agent stage: fix delivery-gate: T1.13.F1.R lane: agent blocked-by: [T1.13.R] acc: [merge confirmation requires nonempty exact reviewed source head; mismatched Execute and Lookup remain UNKNOWN; real PostgreSQL regressions and corrective PR handoff]
- [x] T1.13.F1.R Independently review and deliver exact-head merge receipt correction  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/6 blocked-by: [T1.13.F1] acc: [fresh exact-head source review, guarded merge and actual landing verified]
- [x] T1.15 Reconcile durable jobs and ambiguous remote operations  Owner: recovery  Est: 90m  kind: agent stage: author delivery-gate: T1.15.R  verifies: [UC-008, UC-009]  deps: [T1.9, T1.13.R, T1.13.F1.R, T1.14]  acc: [Queue loss republishes unfinished work; expired leases fence before replacement; UNKNOWN GitHub writes query fake remote state before retry; labels repair without state advancement; merged state requires remote confirmation.]  blocked-by: [T1.9, T1.13.R, T1.13.F1.R, T1.14]  lane: agent
  - Scope/contract: [docs/tasks/T1.15.md](tasks/T1.15.md); exact owned files and verification commands are listed there.
- [x] T1.15.R Independently review and deliver T1.15 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/8 blocked-by: [T1.15] acc: [independently accepted PR head 79e0cad against base c703bdc; shared-account review comment recorded; guarded ordinary rebase landed at 286fa1b with exact reviewed tree]
  - S1.15.1 Verify: Simulate accepted merge plus timeout and assert reconciliation confirms instead of issuing a second merge. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.16 Supervise bounded fixture worker processes  Owner: supervisor  Est: 90m  kind: agent stage: author delivery-gate: T1.16.R  verifies: [UC-005, UC-007, UC-008]  deps: [T1.8, T1.10, T1.11]  acc: [Supervisor launches only the owned fake-agent fixture, records process/run identity, heartbeat, timeout and result; pause kills process group after TERM/KILL grace and fences IPC; cleans only owned task workspaces. Actual OCI/Codex sandbox is deferred to E2. Fixture execution is available only through test injection; production configuration cannot select it. Test production startup rejects a missing OCI adapter instead of running a fake worker.]  blocked-by: [T1.8, T1.10, T1.11]  lane: agent
  - Scope/contract: [docs/tasks/T1.16.md](tasks/T1.16.md); exact owned files and verification commands are listed there.
- [x] T1.16.R Independently review and deliver T1.16 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/7 blocked-by: [T1.16] acc: [exact-head independent acceptance, guarded merge and actual landing verified]
  - S1.16.1 Verify: Make the fixture ignore TERM and verify KILL plus stale-result denial after the configured grace. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.17 Assemble separately supervised service roles and health APIs  Owner: coordinator  Est: 90m  kind: agent stage: author delivery-gate: T1.17.R  verifies: [UC-008, UC-009]  deps: [T1.6, T1.9, T1.14, T1.15.R, T1.16.R, T8.6.R]  acc: [Explicit trusted host injection constructs separately supervised api/control/worker roles with shared contracts; unconfigured CLI commands fail closed with stable missing-adapter errors. GET /healthz returns 200 status alive; GET /readyz returns 200 status ready only with DB/role dependencies available, otherwise 503 status not_ready with dependency names and no secrets. Control loops use bounded polling, persisted deadlines, signal-driven shutdown and no queue-held locks while waiting on CI. Real API/control constructors run with explicit trusted host or test injection; no production factory registry exists, so unconfigured CLI startup remains unavailable for every role. Production worker execution additionally requires E2 OCI/provider adapters. Tests may inject fixture adapters but deployment flags cannot enable them.]  blocked-by: [T1.6, T1.9, T1.14, T1.15.R, T1.16.R, T8.6.R]  lane: agent
  - Scope/contract: [docs/tasks/T1.17.md](tasks/T1.17.md); exact owned files and verification commands are listed there.
- [x] T1.17.R Independently review and deliver T1.17 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/10 blocked-by: [T1.17] acc: [exact-head independent acceptance, guarded merge and actual landing verified]
  - S1.17.1 Verify: Stop a required backing service and assert readiness 503 while liveness remains 200. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.18 Prove the foundation crash/cancellation vertical slice  Owner: coordinator  Est: 90m  kind: agent stage: author delivery-gate: T1.18.R  verifies: [UC-005, UC-007, UC-008, UC-010]  deps: [T1.17.R]  acc: [Real Postgres/Redis plus fake GitHub/inference execute signed receipt -> routing -> dispatch -> lease -> result; restart at each seam, Redis loss, duplicate delivery, stale result, budget race, pause, and ambiguous write all satisfy invariants. No live agents or merges enabled.]  lane: agent  blocked-by: [T1.17.R]
  - Scope/contract: [docs/tasks/T1.18.md](tasks/T1.18.md); exact owned files and verification commands are listed there.
- [x] T1.18.R Independently review and deliver T1.18 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/11 blocked-by: [T1.18] acc: [exact-head independent acceptance, guarded merge and actual landing verified]
  - S1.18.1 Verify: Disable the generation guard in an owned test worktree and observe the stale-result regression fail; restore and report both outcomes. Run the scoped tests, then formatter/linter checks after code changes.
- [x] T1.19 Install scoped quality gates and record foundation handoff  Owner: coordinator  Est: 60m  kind: agent stage: author delivery-gate: T1.19.R  verifies: [infrastructure, UC-008]  deps: [T1.18.R]  acc: [CI runs gofmt/goimports, go vet, golangci-lint and unit/API/integration/system tests with real service fixtures and no silent integration skips; unsupported paid-execution capability stays disabled; record exact green commands and all pending live integration prerequisites. The CI workflow provisions owned Postgres/Redis service fixtures, sets required URLs, verifies executed test counts, and serializes the full-suite race lane.]  blocked-by: [T1.18.R]  lane: agent
  - Scope/contract: [docs/tasks/T1.19.md](tasks/T1.19.md); exact owned files and verification commands are listed there.
- [x] T1.19.R Independently review and deliver T1.19 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/12 blocked-by: [T1.19] acc: [exact-head independent acceptance, guarded merge and actual landing verified]
  - S1.19.1 Verify: Make a required integration fixture absent and assert CI fails rather than passes/skips. Run the scoped tests, then formatter/linter checks after code changes.

### E2 - Review-only B with verified runtime and metering
fidelity: outline
Build the real OCI supervisor, external metered gateway and credential broker adapter, trusted checkout/config, review output validation, thread/finding persistence, and GitHub review posting. Prove provider request bounds before admitting paid execution.
Acceptance: A live sandbox review preserves unanchored blockers; no fixer/merge is enabled; credentials/egress/cancellation and billing envelopes are proven for pinned runtime versions.
- [ ] T2.0 PLAN: expand E2 after its trigger evidence  Owner: coordinator  Est: 60m  kind: plan  delivers: [E2 executable tasks, contracts, and updated use cases]  deps: [T1.19.R]  acc: [E2 is executable with resolved dependencies, owned file scopes and falsifiable acceptance for every row]  blocked-by: [T1.19.R]  blocked: Prior epic implementation exit evidence is not yet available; coordinator-only planning
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

E6 integrates the complete lifecycle adapter with generic plan/apply/claim workflows while preserving standalone APRL operation. It does not assert live readiness. E6 implementation depends on E1 foundation handoff and the frozen external protocol. The exact wire schema is frozen in [code-delivery/v1](contracts/code-delivery-v1.md); do not implement guessed fields.

- [x] T6.0 PLAN: map frozen whole-lifecycle integration contract to service adapter  Owner: coordinator Est: 60m kind: plan stage: author delivery-gate: T6.0.R deps: [T1.19.R] blocked-by: [T1.19.R] acc: [Frozen wire-to-service mapping and five bounded implementation/review pairs documented; preserve standalone/product flow and generic tooling; no second scheduler or runtime activation]
  - Scope/contract: [docs/tasks/T6.0.md](tasks/T6.0.md).
- [x] T6.0.R Independently review and deliver the lifecycle adapter plan  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/13 blocked-by: [T6.0, T6.0.F1.R] acc: [exact-head plan acceptance, complete acyclic ownership/DAG and preserved wire/scope, guarded merge and actual landing verified]
- [x] T6.0.F1 Clarify admitted, denied and pending intent replay after blocking review  Owner: coordinator kind: agent stage: fix delivery-gate: T6.0.F1.R lane: agent blocked-by: [T6.0] acc: [Resolve finding D1 on PR13 head6d106de; admitted repeats observation, denied repeats403 without lifecycle, pending repeats durable intent503; no wire or implementation scope change]
- [x] T6.0.F1.R Independently re-review and deliver corrected adapter plan  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/13 blocked-by: [T6.0.F1] acc: [Exact corrected head acceptance and green CI; guarded merge and full landed verification; stable T6.0.R gate releases only after this delivery]
- [x] T6.1 Implement strict code-delivery v1 codec  Owner: coordinator Est: 90m kind: agent stage: author delivery-gate: T6.1.R lane: agent deps: [T6.0.R] blocked-by: [T6.0.R] acc: [Exact frozen canonical bytes/digest; strict bounded JSON, required fields/enums/graphs and opaque external identifiers; zero-cost schema without fabricated admission or settlement]
  - Scope/contract: [docs/tasks/T6.1.md](tasks/T6.1.md).
- [x] T6.1.R Independently review and deliver T6.1 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/14 blocked-by: [T6.1, T6.1.F1.R] acc: [exact-head independent acceptance, bounded explicit fix/re-review after blockers, guarded merge and actual landing verified]
- [x] T6.1.F1 Restore caller-compatible dotted repository and PR URL segments  Owner: coordinator kind: agent stage: fix delivery-gate: T6.1.F1.R lane: agent blocked-by: [T6.1] acc: [Resolve formal URL compatibility finding on PR14 head7cbf2d3; allow embedded dots while denying exact dot/dotdot segments; request and observation regressions fail before correction and pass after; no wider codec or authority changes]
- [x] T6.1.F1.R Independently re-review and deliver corrected codec  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/14 blocked-by: [T6.1.F1] acc: [Exact corrected head acceptance and final-head green CI; guarded merge and complete actual landing verification; stable T6.1.R releases only after this delivery]
- [x] T6.2 Persist atomic delegation binding through the canonical scheduler  Owner: coordinator Est: 90m kind: agent stage: author delivery-gate: T6.2.R lane: agent deps: [T6.1.R] blocked-by: [T6.1.R] acc: [Real PostgreSQL caller/id/digest idempotency and lifecycle creation; stable denial binding, aggregate admissions/concurrency and exact expiry/cancel fences; standalone canonical flow remains unchanged]
  - Scope/contract: [docs/tasks/T6.2.md](tasks/T6.2.md).
- [x] T6.2.R Independently review and deliver T6.2 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/15 blocked-by: [T6.2] acc: [exact-head independent acceptance, bounded explicit fix/re-review after blockers, guarded merge and actual landing verified]
- [x] T6.3 Expose caller-scoped HTTPS lifecycle handlers  Owner: coordinator Est: 90m kind: agent stage: author delivery-gate: T6.3.R lane: agent deps: [T6.2.R] blocked-by: [T6.2.R] acc: [Exact PUT/GET/cancel routes, mandatory trusted authentication/authorization, one MiB strict JSON, caller-scoped read/denial and finite redacted failures; same store/adapter, no production-selectable fixture]
  - Scope/contract: [docs/tasks/T6.3.md](tasks/T6.3.md).
- [x] T6.3.R Independently review and deliver T6.3 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/16 blocked-by: [T6.3] acc: [exact-head independent acceptance, bounded explicit fix/re-review after blockers, guarded merge and actual landing verified]
- [x] T6.4 Persist trusted landing evidence and project sequenced observations  Owner: coordinator Est: 90m kind: agent stage: author delivery-gate: T6.4.R lane: agent deps: [T6.2.R, T6.3.R] blocked-by: [T6.2.R, T6.3.R] acc: [Full host-returned evidence persisted atomically; actual canonical child crosswalk, positive monotonic replay-safe sequence, current independent exact-head/base/policy landing alone releases; cancel/unknown and zero-cost non-settlement preserved]
  - Scope/contract: [docs/tasks/T6.4.md](tasks/T6.4.md).
- [x] T6.4.R Independently review and deliver T6.4 implementation  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/17 blocked-by: [T6.4] acc: [exact-head independent acceptance, bounded explicit fix/re-review after blockers, guarded merge and actual landing verified]
- [x] T6.5 Compose delegated and standalone delivery with ordinary apply and claims  Owner: coordinator Est: 90m kind: agent stage: author delivery-gate: T6.5.R lane: agent deps: [T6.1.R, T6.2.R, T6.3.R, T6.4.R] blocked-by: [T6.1.R, T6.2.R, T6.3.R, T6.4.R] acc: [Real PostgreSQL/loopback HTTP and published generic shim snapshot/WON/fresh admission/stage/fenced acknowledgement fixtures; standalone and delegated author/review/fix/re-review/verified landing regressions; immutable grants and no runtime/profile activation]
  - Scope/contract: [docs/tasks/T6.5.md](tasks/T6.5.md).
- [ ] T6.5.R Independently review and deliver T6.5 implementation pr-url: https://github.com/ajent-social/APRL/pull/19 Owner: independent-reviewer kind: agent stage: review lane: agent blocked-by: [T6.5, T6.5.F1.R] acc: [exact-head independent acceptance, bounded explicit fix/re-review after blockers, guarded merge and actual landing verified]
- [ ] T6.5.F1 Assert host-qualified zero accounting and non-settlement after UNKNOWN  Owner: coordinator kind: agent stage: fix delivery-gate: T6.5.F1.R lane: agent blocked-by: [T6.5] acc: [Resolve blocking F1 on PR19 head e8f0814; initial UNKNOWN late-fenced and landed observations explicitly require trusted subscription-only zero-exposure grant and empty settlement ID; no production/accounting scope expansion]
- [ ] T6.5.F1.R Independently re-review and deliver corrected composed fixture  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/19 blocked-by: [T6.5.F1] acc: [Fresh corrected exact-head independent acceptance and final-head CI plus local tagged normal/race; guarded merge and full landed verification; stable T6.5.R releases only after this delivery]

Trigger: Dependency planning-task completion alone never permits downstream coding; require the prior epic's implementation acceptance, substitute its resulting milestone task IDs, then groom this epic. E5 may start with review-only production while E3/E4 remain disabled.


### E7 - First-class review task foundation

- [x] T7.1 Specify first-class plan review contracts and policy  Owner: design  kind: agent stage: implement lane: agent  acc: [neutral ADR and runbook define PR handoff, actor independence, bounded correction and stable delivery gates]
- [x] T7.2 Implement versioned plan task and receipt validation  Owner: contracts  kind: agent stage: implement lane: agent  acc: [invalid PR snapshots, negative review outcomes and invalid correction envelopes reject]
- [x] T7.3 Implement deterministic lifecycle progression and eligibility  Owner: coordinator  kind: agent stage: implement lane: agent  blocked-by: [T7.2]  acc: [author handoff releases review; self-review and stale approval reject; fixes and re-reviews remain explicit; only verified landing releases ordinary descendants]
- [x] T7.4 Add durable admission and shared apply adapter  Owner: coordinator  kind: agent stage: implement lane: agent  blocked-by: [T7.3]  acc: [duplicate claims cannot duplicate admission; fenced receipts atomically update visible successor tasks and survive restart]
- [x] T7.5 Integrate shared parser and stage routing  Owner: shared-tooling  kind: agent stage: implement lane: agent  blocked-by: [T7.4]  acc: [ordinary apply loops claim review/fix/re-review tasks with current PR and admission; coding checkbox alone cannot authorize mutation]
- [x] T7.6 Verify revision and create PR handoff  Owner: coordinator  kind: agent stage: verify lane: agent  blocked-by: [T7.1, T7.5]  acc: [targeted tests and required checks pass; PR URL and exact head recorded on T7.7]
- [x] T7.7 Independently review and deliver review-task revision  Owner: independent-reviewer  kind: agent stage: review lane: agent  blocked-by: [T7.6]  pr-url: https://github.com/ajent-social/APRL/pull/3  acc: [independent current-head review accepted; guarded PR merge confirmed; landed revision verified]
- [x] T7.8 Preserve review and correction identifiers in generic plan parsing  Owner: shared-tooling kind: agent stage: author delivery-gate: T7.8.R lane: agent blocked-by: [T7.7] acc: [R/F suffixed review/fix task IDs parse as ordinary tasks with exact dependencies; scheduling references do not duplicate executable rows; focused Python regressions and author PR handoff]
- [x] T7.8.R Independently review and land parser compatibility correction  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/dndungu/skills/pull/129 head-sha: 61d8150dd3d2fe4e35ed06c120b201893c322745 base-sha: 04469547804f99a1292fd937bb7be70be9fd4796 blocked-by: [T7.8] acc: [independent exact-head review, guarded merge and actual tree/ancestry verification in private shared tooling repository]

### E8 - Durable process ownership integration

- [x] T8.1 Implement durable process reservations and admission gates  Owner: process-holds  kind: agent stage: author delivery-gate: T8.1.R lane: agent  acc: [immutable run-bound reservations, finite trusted scope capacity, cancellation-safe unresolved states, trusted bounded reaping evidence; real PostgreSQL regressions]
- [x] T8.1.R Record initial independent process hold review  Owner: independent-reviewer kind: agent stage: review lane: agent blocked-by: [T8.1] pr-url: https://github.com/ajent-social/APRL/pull/5 acc: [exact-head initial verdict recorded; changes requested, no merge or successful delivery]
- [x] T8.1.F1 Correct direct process identity publication bypass  Owner: process-holds kind: agent stage: fix delivery-gate: T8.1.R lane: agent blocked-by: [T8.1.R] pr-url: https://github.com/ajent-social/APRL/pull/5 acc: [Started and SQL reject RESERVED bypass; legitimate fixtures consume BeginStart; required checks and corrective PR handoff]
- [x] T8.1.R2 Independently re-review and deliver process hold primitives  Owner: independent-reviewer kind: agent stage: rereview delivery-gate: T8.1.R lane: agent blocked-by: [T8.1.F1] pr-url: https://github.com/ajent-social/APRL/pull/5 acc: [corrective exact-head review accepted, guarded merge and actual landing verified]
- [x] T8.2 Verify startup recovery process reservation integration  Owner: coordinator kind: agent stage: verify lane: agent blocked-by: [T8.1.R2, T1.15.R, T1.17.R]  acc: [unresolved reservations block replacement across generations; startup reconciles before sweep or dispatch; ambiguous ownership remains held]
- [x] T8.3 Verify supervisor process ownership integration  Owner: coordinator kind: agent stage: verify lane: agent blocked-by: [T8.1.R2, T1.16.R]  acc: [reservation before launch, exact trusted ownership identity, bounded shutdown, unknown retains capacity, only proven reaped releases]
- [x] T8.4 Verify corrected foundation and publish PR  Owner: coordinator  kind: agent stage: verify lane: agent blocked-by: [T8.2, T8.3]  acc: [real database/process boundary regressions, required checks and PR handoff]
- [x] T8.5 Independently review and land process reservation correction  Owner: independent-reviewer  kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/18 blocked-by: [T8.4]  acc: [exact PR head accepted, guarded merge and actual landed verification]
- [x] T8.6 Route outbox work by owner and bind cancellation to exact runs  Owner: control-routing kind: agent stage: author delivery-gate: T8.6.R lane: agent blocked-by: [T1.9, T1.11, T8.1.R2] acc: [dispatcher never consumes LABEL_SYNC; router/control cancel intents capture original task/job/run before revocation; no task-only or persisted-PID cancellation; real PostgreSQL regression and corrective handoff]
- [x] T8.6.R Independently review and deliver outbox ownership correction  Owner: independent-reviewer kind: agent stage: review lane: agent pr-url: https://github.com/ajent-social/APRL/pull/9 blocked-by: [T8.6] acc: [exact-head independent acceptance, guarded merge and verified actual landing]

## 5. Parallel Work and Waves

The coordinator owns shared contracts, integration, plan/roadmap and ADR amendments. Use up to three GPT-6-Luna implementation workers plus one coordinator, matching this session's four slots. Each implementation worker requires its own unique external-SSD worktree and exact task file scope. Read-only planning probes do not own or edit files. Go skill conventions apply: context-first I/O, wrapped errors, no library panics, bounded goroutine lifetimes, consumer-defined interfaces and no fabricated success in production paths. Test doubles live only in _test.go or explicit testutil packages; unavailable runtime adapters fail closed. Model overrides for ambiguous contract/runtime decisions stay on the coordinator. This APRL-specific three-worker limit remains unchanged by this lifecycle documentation update.

Numbered wave headings retain the historical grouping. Apply selects tasks by current dependency readiness, not by a barrier that waits for a lower-numbered blocked author: a review runs as soon as its author handoff is complete, and descendants still require verified landing. No wave number grants speculative execution authority.

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
- [x] T1.13 Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.16 Scheduling reference; acceptance and scope are in the WBS.
T1.13, T1.16.
#### Wave 9: Recovery (1 worker)
- [x] T1.15 Scheduling reference; acceptance and scope are in the WBS.
T1.15.
#### Wave 10: Assembly (1 worker)
- [x] T1.17 Scheduling reference; acceptance and scope are in the WBS.
T1.17.
#### Wave 11: Fault verification (1 worker)
- [x] T1.18 Scheduling reference; acceptance and scope are in the WBS.
T1.18.
#### Wave 12: Quality gate (1 worker)
- [x] T1.19 Scheduling reference; acceptance and scope are in the WBS.
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

#### Wave 17: First-class review foundation and parser handoff

- [x] T7.1 Scheduling reference; acceptance and scope are in the WBS.
- [x] T7.2 Scheduling reference; acceptance and scope are in the WBS.
- [x] T7.3 Scheduling reference; acceptance and scope are in the WBS.
- [x] T7.4 Scheduling reference; acceptance and scope are in the WBS.
- [x] T7.5 Scheduling reference; acceptance and scope are in the WBS.
- [x] T7.6 Scheduling reference; acceptance and scope are in the WBS.
- [x] T7.7 Scheduling reference; acceptance and scope are in the WBS.
- [x] T7.8 Scheduling reference; acceptance and scope are in the WBS.
- [x] T7.8.R Scheduling reference; acceptance and scope are in the WBS.

#### Wave 18: Durable process ownership correction

- [x] T8.1 Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.1.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.1.F1 Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.1.R2 Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.2 Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.3 Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.4 Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.5 Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.6 Scheduling reference; acceptance and scope are in the WBS.
- [x] T8.6.R Scheduling reference; acceptance and scope are in the WBS.

#### Wave 19: Independent E1 delivery gates

- [x] T1.13.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.13.F1 Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.13.F1.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.15.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.16.R Scheduling reference; acceptance and scope are in the WBS.

- [x] T1.17.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.18.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T1.19.R Scheduling reference; acceptance and scope are in the WBS.

#### Wave 20: Caller protocol integration after E1 delivery

- [x] T6.0 Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.1 Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.2 Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.3 Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.4 Scheduling reference; acceptance and scope are in the WBS.

#### Wave 21: E6 planning and implementation delivery gates

- [x] T6.0.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.0.F1 Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.0.F1.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.1.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.1.F1 Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.1.F1.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.2.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.3.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.4.R Scheduling reference; acceptance and scope are in the WBS.
- [x] T6.5 Scheduling reference; acceptance and scope are in the WBS.
- [ ] T6.5.R Scheduling reference; acceptance and scope are in the WBS.
- [ ] T6.5.F1 Scheduling reference; acceptance and scope are in the WBS.
- [ ] T6.5.F1.R Scheduling reference; acceptance and scope are in the WBS.

## 6. Timeline and Milestones

| ID | Milestone | Dependencies | Exit evidence |
| --- | --- | --- | --- |
| M1 | Foundation ready | T1.19.R | Real DB/Redis fault slice and scoped CI green; paid runtime disabled if unproven |
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
- Task contracts: `docs/tasks/T1.1.md` through `docs/tasks/T1.19.md`; T1.1-T1.16 accepted and landed; T1.17-T1.19 pending.

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

## Review-task revision (2026 10 03)

Owner-authorized scope: revise design, plan and code for PR-first executable review tasks. Latest accepted policy requires independent review for every code-changing PR; ordinary downstream work waits for verified landing. Public contracts remain caller/provider neutral. Existing E1 acceptance and unfinished source are preserved.


Revision waves: design and contracts plus read-only compatibility discovery in parallel; coordinator progression then durable adapter; shared-tooling integration after contract agreement; verification/PR handoff; independent review and delivery. No paid runtime or production activation follows from this revision.

The T7 review-task primitives are an additive foundation slice. They do not satisfy the caller-facing code-delivery/v1 service protocol, subscription/runtime qualification or E6 end-to-end scheduler integration. Existing E6 IDs and gates are preserved.

Historical review-task author handoff: T7.1-T7.6 completed local acceptance and PR publication before T7.7 independent delivery. Its current PR head/base must be captured from GitHub at admission and rechecked before merge. Code was qualified at local source 5b634b9 with 203 regular and 203 race pass events, zero skips, build/vet/lint and restored host-proof regression; the delivery branch preserves byte-identical Go/migration/test source. Generic helper landed separately with 19 focused Python tests. The handoff itself did not complete T7.7; the subsequent landed receipt below completes that review. E1/E6 remain incomplete.

T7 delivery receipt: PR3 independently reviewed at exact head 9759d834dd4312f00cf3efae6f61f6fae193b17b and merged by guarded rebase. Actual main landing bebad8295ab86fedffd0f6e3171a43ec522c3b05 has reviewed tree 87c85a2abf35c3bf0f4529c43b342b714473636e. Independent agent acceptance was recorded as a GitHub COMMENT because the shared account authored the PR; no formal GitHub approval or hosted CI success is claimed. T7.7 claim released. No runtime activation or E1/E6 completion.

### T8: Durable process reservations supporting E1 recovery

This necessary bounded foundation correction preserves the original E1 acceptance gates. Execution authority and resource ownership are separate: cancellation revokes execution immediately, while unresolved owned processes retain capacity until trusted host proof.


Broker delivery receipt: PR4 independently reviewed at exact head 69535b5ee7cd3c7d08de64a6722956ce4d867557 and normally rebase merged to75c7a058a04b0e67767b33f5c0cc9f1143848ad5. Actual landed tree a3220fa0508276cea03c548064e0de7c93b172fd equals reviewed candidate tree. T1.13.R claim released; independent agent acceptance recorded as COMMENT with shared author login. No hosted CI or live transport activation claim.

Process reservation author handoff: PR5 carries qualified source67e0912 and unchanged Go/migration/test bytes. T8.1.R must capture actual current PR head/base and independently deliver the exact candidate. This handoff completes the author task only; startup recovery, supervisor integration and E1/E6 remain pending.

Initial PR5 review receipt: T8.1.R completed its review execution with changes requested at head4a528f22326a4661c9ffaf831dffd15c376531c8, review5401113450. Its checked box records the negative execution only and does not establish delivery; downstream work waits T8.1.R2. The stable lifecycle delivery gate remains closed through fix/re-review. Corrective scope is bounded to the direct Started/SQL transition, matching launch fixtures and conservative UNKNOWN reason observations; no extra funding or activation.

Corrective PR5 author handoff: source9b7c6bf passed module build, 253 regular and 253 serialized race test events (zero skips), vet and lint. Direct RESERVED process publication is denied by both store and database; new UNKNOWN reason observations preserve identity and charged capacity. The corrective head requires fresh T8.1.R2 acceptance and actual landing; initial negative review remains historical. No runtime activation.

T8.1.R2 verified delivery receipt: independent exact-head review5401219806 accepted corrective PR5 head2de76afee6a959381a0e7937875e44e038f919a0; normal guarded rebase landed1a816312ae58346e78db56742ca5034f7223c384. Actual main ancestry and full tree6afe0f4496144f891289fef65a27c67508e7cb15 match the qualified candidate. Initial negative review5401113450 remains historical. No hosted checks/protection or runtime activation claimed.

Broker corrective author qualification: sourcee0d1072 executed 27 targeted race test events, zero skips/failures, affected vet and lint. Mismatched or absent reviewed-source head cannot confirm an Execute or Applied Lookup; durable UNKNOWN and one mutation attempt remain. Removing only the merge source-head guard genuinely failed the mismatch regression; exact source restoration passes. T1.13.F1.R must independently deliver the corrective PR before T1.15 delivery. No real remote transport activation.

Broker correction author handoff: PR6 contains the qualified two-file source and review-gated plan. T1.13.F1 is complete as a coding handoff only; T1.13.F1.R must record exact head/base, independently accept, merge and verify actual landing before T1.15 delivery.


T8.6 bounded scope: internal/dispatch/dispatch.go, internal/control/control.go, internal/router/router.go and corresponding integration dispatch/controls/router tests plus tests/api/results_test.go for the revoked paused-webhook result regression. Coordinator owns ADR/plan and integration. Reconciliation owns LABEL_SYNC external I/O and subsequent task/outbox acknowledgement; dispatch never locks a label row around its callback. Cancellation conveys the captured immutable run, not a later replacement, and grants no authority to signal numeric persisted PIDs. Missing authenticated cross-process cancellation transport remains unavailable until assembly supplies it. T1.17 delivery additionally waits T8.6.R.


T7.8 scope: shared generic plan/scripts/parse_plan.py and its focused tests; coordinator owns this APRL plan and keeps historical scheduling references outside the canonical WBS while preserving their text. No runtime provider/profile activation or second scheduler. Canonical task identifiers and historical claim receipts remain unchanged.

Broker correction delivery: independent review5401300273 accepted PR6 head1725b26ab2b212b9bd861712b6f755095040374a and guarded rebase landed5a74dd138c1f838a840a2cbeb214c4c8cb8ecc29. Actual main ancestry and complete tree9e89229df01a2076ab018b4c7c417ad9b7260e08 equal the reviewed candidate. No hosted check or runtime activation claim.

T7.8 author handoff: private shared tooling PR129 preserves ordinary review/fix task IDs; tested sourcef1368bc published as61d8150dd3d2fe4e35ed06c120b201893c322745/tree38181744b498b1364eb7b8908550ac7f0372821a.18 focused and2 compatibility Python tests, footprint ratchet and genuine-red parser regression passed. Actual APRL plan now has55 unique canonical tasks,77 valid acyclic dependency edges and55 wave assignments. First-class review definitions are inside the canonical WBS; historical scheduling references remain outside it. T7.8.R must independently deliver the private PR; no installed profile/provider/runtime activation.

T7.8.R actual delivery: private shared skills PR129 independently accepted exact head61d8150dd3d2fe4e35ed06c120b201893c322745 and guarded rebase landed2674b6d442874e07b4ea98c370ac14cc24a46fa4. Actual private main ancestry and full tree38181744b498b1364eb7b8908550ac7f0372821a match the reviewed source. Reviewer reran18 focused and2 compatibility Python tests. No hosted checks, installed provider profile or runtime activation claimed.

Supervisor T1.16 author qualification: full-module source6fb1a7c passed build,270 regular and270 race test events with no skipped tests, vet and lint. Production Go bytes are unchanged by later test-only timeout assertione4a075e; that assertion passed the exact restored targeted native test. Disabling only primary KILL genuinely failed the TERM-ignoring reap-bound test while owned reassert KILL remained for cleanup; exact restoration passed. Native boot/start identity, ambiguous leader retention, stale HTTP result rejection, heartbeat and trusted never-started retry/settlement are exercised. Author checkbox does not establish delivery; T1.16.R must independently accept current PR head and verify actual landing. No OCI containment/restart handle/provider or runtime activation.

T1.16 coding handoff: PR7 contains the qualified native fixture supervisor and canonical review-gated plan. T1.16 author is complete; reviewer T1.16.R must read actual current head/base from GitHub, independently accept, guarded-merge and verify landing. The source plan embeds the stable PR URL, avoiding self-referential head hashes. E1 acceptance remains14/19 until delivery receipts establish additional tasks.

T1.15 author qualification: source7583261 composed with independently merged supervisor passed full build,284 regular and284 race executed test cases, zero skipped tests, vet/lint and two-file formatting/import checks. Future repair deadline predicate genuine-red caught premature same-pass retry; restored source passed. Author handoff is complete, but T1.15.R independent exact-head acceptance and actual verified landing remain required before descendants.

T1.16.R delivered: independent review accepted PR7 exactheadce860ac3c4ad81ea9035bb4079a6942e86abc211; guarded ordinary rebase landedc703bdc263653873a7493cab6de0efbd34e4c781. Actual landing tree7831952c56bcc41278c53d5b6c96b8bda6eb71bc equals reviewed tree and ancestry is verified. Shared-account review is COMMENTED, not formal GitHub APPROVED; no hosted checks are claimed. Exact review claimcfb436a57a440d2783916d16fbc652753028d6d2 released. Current E1 delivered count15/19; T1.15 remains authorhandoff pending its independent review.

T1.15.R delivered: independently accepted PR8 head79e0cad668809820ede61acfaa95267f57b39fce against reviewed basec703bdc263653873a7493cab6de0efbd34e4c781. Shared-account review comment https://github.com/ajent-social/APRL/pull/8#issuecomment-5971079936 records the verdict; it is not a formal GitHub APPROVED review. Guarded ordinary rebase merged as286fa1b945da357c1524ea6f3e5838de54f2dc01; reviewed base is an ancestor of main and landed tree d8b0c9118d5d30facf0e18a58253e4417e28bd0f exactly matches the reviewed candidate. GitHub reports dndungu as merger. No hosted checks or live provider/GitHub mutations are claimed. Exact review claim0ba1b0e22daa86679247a5796d4cfe9bf247ca67 released after verification. E1 delivered count16/19.

T8.6 pre-handoff correction: independent source pre-review identified uncleared router lease fields; author fixed atomic clearing and added real-DB leased-run/UNKNOWN-hold/exact-cancel-target coverage. Full composition exposed an older paused-webhook result expectation; the directly affected API regression now requires409 stale_result and retains TERMINATED/CANCELLED/UNKNOWN with no replies, rather than relaxing result authentication. Formal T8.6.R review still starts after the PR handoff. Startup integration verification T8.2 also waits for T1.17.R so a recovery callback alone is not claimed as assembled startup evidence.

T8.6 author qualification: production build passed; final source4cbfb0f passed286 regular and286 race test cases with zero skipped tests, whole-module vet/lint and seven owned Go-file formatting/import checks. Captured cancellation run binding and paired router lease-clearing mutations each genuinely failed their real-DB assertions; exact restoration passed. Independent pre-handoff source review found no remaining blockers. Formal T8.6.R acceptance and verified landing remain mandatory; no production cancellation transport activation. E1 verified delivery is16/19; T1.17-T1.19 remain pending.

T8.6.R delivered: independent exact-head review5401853533 accepted PR9 headd0c59d30b2ba817bde1eb15354252fc633c50032. Guarded ordinary rebase landede9d7245afbb717ae996674b96d4ab6f784e260ed; complete tree0d2da14f35048781ca53f463d9d12ea2d009a862 equals the reviewed candidate and reviewed base286fa1b is an ancestor. Exact review claimae3ddd381a9626f68d51b872c43e5c09438a61fa released. Historical77-edge snapshot is preserved; current plan has78 after the assembled startup dependency. T1.17 is now eligible from actual verified review deliveries, with two source/test authors under frozen separate file ownership and a third independent review agent. No hosted CI or runtime activation claim.

T1.17 assembly clarification: no production trust/factory registry currently exists. Qualification covers real role constructors and loops with explicit trusted test injection, not production-runnable CLI commands. The CLI validates finite role/options and reports stable missing-adapter errors; it cannot invent authentication, inventory or execution authority. A future authorized host assembly must supply those adapters.

T1.17 author handoff: PR10 contains the combined implementation and separate test-author source. Local verification passed 309 normal and 309 race test events, zero skips, build/vet/lint/four-file formatting; owned PostgreSQL outage and genuine-red readiness predicate removal/restoration passed. T1.17 is complete only as author handoff. T1.17.R remains open and downstream delivery remains blocked until independent exact-head acceptance, guarded merge and verified actual landing. Unconfigured CLI production startup remains unavailable for every role.

T1.17.R verified delivery: independent review accepted PR10 exact head ac48a6cb against base e9d7245; shared-account COMMENT recorded. Guarded ordinary rebase landed 752014f01e6ffc31fda8a935fc57d1e384586d50 with reviewed whole tree 1fdedcce0518899fd93de3f557d8e59dffd74fe3 and verified base ancestry. No hosted checks exist yet; local 309 normal/309 race tests and compiler checks are the bounded qualification. Downstream T1.18 may now claim.

T1.18 author handoff: PR11 contains six owned-service system cases and test-only native identity helpers. Local verification passed315normal/315race terminal test events, zero skips, build/vet/lint/five-file formatting and stale HTTP negative-control failure/exact-restoration pass. Successful execution retains unverified usage UNKNOWN; native group drain does not imply provider settlement. T1.18.R remains open and downstream delivery remains blocked until exact-head independent acceptance, guarded merge and verified landing. Production host inventory and Linux hosted qualification remain pending.

T1.18.R verified delivery: independent review accepted PR11 exact head6b68ead471a2b90044ffff6f43d1c13f4a3dd703 against base752014f, recorded a truthful shared-account COMMENT, and guarded rebase landed0728071d333cbb9daf9b73c633711c9c3539d309. Actual main has reviewed whole tree69ad59c9a10abd2c283202b350fe76eba68fbdda and verified base ancestry; review claim released. Local315normal/315race events, zero skips qualify the owned fixtures; hosted Linux and production host/provider evidence remain pending. T1.19 may now claim.

T8.3 verification receipt: the independently landed supervisor and PR11 system fixtures satisfy reservation-before-launch, exact native leader identity, bounded TERM/KILL shutdown, UNKNOWN capacity retention and proof-only reaping. Both full normal and serialized race logs contain12passing TestSupervisor terminal events (including their subtests), with no skips; the complete suites each have315passing events. The real PostgreSQL/native process cases cover ambiguous start, activation fences, unverifiable descendants, owned cleanup, pause and timeout. T1.18 additionally records an exact-ledger REAPED hold while usage stays UNKNOWN. This is local E1 fixture verification; T8.2 remains open for its separate startup bridge, and Linux hosted, production host inventory, OCI and provider qualification remain distinct pending work. No new implementation is introduced by this verification task.

T1.19 author handoff: PR12 installs service-backed Ubuntu24.04 CI with immutable pins, complete tracked-Go formatting, both pipeline statuses and complete JSON/package accounting. Hosted run37152786186 on functional headb363f02 passed315regular/315race terminal test events, zero failures/skips, vet/lint/format; Linux-only identifier corrections and help-only pinned CLI prerequisite are included. The final handoff documentation head must receive its own green hosted check before independent T1.19.R acceptance, guarded merge and actual landing. Linux-native fixture qualification does not establish OCI containment, durable production host inventory, provider settlement or runtime activation.

T1.19.R verified delivery: independent review accepted PR12 exact headea6865b7e2effbc8d6809f135976d0c7d6734eca with final hosted run37153113270 green315normal/315race events, zero failures/skips. Guarded rebase landed5bb75568c60d26800dbfd7cac2be669f23391b25 with whole treef75288ed22d1f572423b2496311641437ae4b58b and reviewed base ancestry verified; review claim released. E1 code/owned-service/native Linux fixture qualification is complete19/19. Production host inventory, OCI/provider/subscription and settlement authority remain separate pending work.

T6.0 groom mapping: [implementation plan](contracts/code-delivery-v1-implementation.md) preserves the frozen external schema and ordinary canonical apply/claim path. The five author/review pairs replace the coarse E6 rows and retain standalone/product regressions plus generic tooling acceptance. T6.0.R must deliver this plan before implementation; all downstream ordinary dependencies use review delivery gates. No profile, provider, factory or production service is activated.

T6.0 author handoff: PR13 contains the mapping and five bounded author/review contracts. Parser verified62 unique task definitions/62 wave assignments and88 valid acyclic dependency edges; frozen wire/golden fixture unchanged. Hosted final-head CI and independent T6.0.R acceptance, guarded merge and verified landing remain required before T6.1 coding.

T6.0.R negative review D1 on PR13 head6d106de: replay wording incorrectly promised a lifecycle for denied/pending intent. Explicit T6.0.F1 correction and dependent T6.0.F1.R are required; stable T6.0.R remains blocked and releases no downstream coding until corrected review verifies landing.

T6.0.F1 author handoff: PR13 corrects D1 by explicitly separating ADMITTED observation replay, terminal DENIED/no lifecycle replay and unresolved INTENT/503 serialized decision. Wire and implementation scope remain unchanged; dependent T6.0.F1.R must accept the corrected exact head and verify actual landing before stable T6.0.R delivery.

T6.0.F1.R verified delivery: independent re-review accepted PR13 exact head1970a32536f2a415c4b265bb31520f26676be7a7/tree6a69b014cf6a4dd11c405a20da91ddef9cf601ee against base5bb75568c60d26800dbfd7cac2be669f23391b25. Final-head CI37155380986 passed315normal/315race zero failures/skips. Guarded rebase landed73098182e2985a0a4d662a3617e4311a9e8ce907; reviewer and coordinator fetched main, verified full tree equality and base ancestry, released review claim. D1 is resolved; stable T6.0.R delivered and T6.1 may be claimed. This is plan delivery, not running adapter or live interoperability evidence.

T6.1 author handoff: PR14 implements only the strict frozen wire codec and pure tests. Local full suites passed420normal/420race terminal test events, zero failures/test skips and31complete package terminals; build/vet passed and final comment corrections passed lint/goimports/diff checks. Deliberately swapping canonical request fields failed the exact golden test, then byte-exact restoration passed. Initial invalid test fixtures/expectations and lint findings were corrected; failed logs retained. Caller synthetic request bytes/digest remain identical; malformed unpaired Unicode escapes are rejected per the recorded decision. Final-head hosted CI and independent T6.1.R acceptance/guarded merge/verified landing remain required before T6.2. No admission, runtime/profile activation or live interoperability is claimed.

T6.1.R negative review: PR14 exact head7cbf2d35f086bae16bdf3b9248818e49e7d175b8 rejects embedded dots in URL segments permitted by the frozen caller. Finding comment5973999054 requires explicit T6.1.F1 correction and dependent T6.1.F1.R; stable T6.1.R remains blocked and releases no T6.2 coding until verified corrected delivery. Hosted green run37157513726 does not resolve the semantic finding.

T6.1.F1 author handoff: PR14 restores embedded dots in repository/PR URL segments and continues rejecting exact dot/dotdot segments. Both new named request/observation regressions failed on reviewed source7cbf2d3 and passed after the minimal correction. Fresh full suites passed422normal/422race events, zero failures/test skips and31complete package terminals; build/vet/lint/goimports/diff checks passed. Load/other-owner lease holds deferred stages without overriding ownership; all own build leases released. T6.1.F1.R must accept corrected exact head/final-head green CI, merge and verify actual landing before stable T6.1.R releases T6.2.

T6.1.F1.R verified delivery: independent re-review accepted PR14 exact head88ae9df2263212d9f083f84afefbde50e7ae373c/tree360de5f4a03c0f26f4faa14ad0d9aacd20fbcc57 against base73098182e2985a0a4d662a3617e4311a9e8ce907. Final-head CI37159131741 passed422normal/422race, zero failures/test skips. Actual guarded rebase landing is f6dce5173415a16945c1edc235f4be0ff3894ca0; coordinator and reviewer freshly verified remote main, whole tree equality and base ancestry. Earlier provisional review receipt was corrected append-only. Both review gates delivered; T6.2 can be admitted. Qualification remains codec/fixture only, without runtime or provider activation.

T6.2 draft candidate: PR15 publishes the source and real-PostgreSQL regression candidate for Linux CI while local Go checks wait for another project shared build lease. Author task remains unchecked/claimed, review remains dependency-blocked, and neither verification nor review acceptance is claimed. Author preflight is closing a post-insert exact-expiry race before final handoff.

T6.2 author handoff: PR15 persists caller/delegation INTENT before policy I/O, immutable canonical scope/digest, terminal denial or one atomically admitted canonical lifecycle, and binding-first shared attempt/concurrency/exact-expiry/cancel fences on every existing Store.Update route. Fresh full local suites passed442normal/442race events, zero failures/test skips and31complete package terminals; build/vet/lint and empty goimports/diff checks passed. The named fourth-admission regression failed with only the aggregate cap disabled, then exact SHA256 restoration passed. Real PostgreSQL advisory-lock coordination crossed expiry during lifecycle INSERT and verified savepoint rollback to denial with no orphan; actual receipt/binding-update rollback also passed. Earlier fixture import/type/lint errors, migration reserved-name error, invalid test assumption and control-script assertion mismatch were corrected and failed evidence retained. Exact final-head hosted CI and independent T6.2.R acceptance/guarded merge/actual landed proof remain required; downstream work stays blocked. No runtime/profile/provider activation or production qualification.

T6.2 delivery receipt: PR15 independently accepted at exact head fe11bb10cd92397b8e554e4fb489b300b963daf6 (comment5974967554). Actual guarded rebase landing7ce9d6a0c9cd7d5e27d358a67a7c2ef0e0922fc4 has reviewed tree18b2408fec8df8f987d0f92b9e5c890ab172b91e and reviewed basef6dce5173415a16945c1edc235f4be0ff3894ca0 as ancestor. Root independently fetched and verified both. Final-head CI37164462502 confirms442 regular and442 race passes, zero failures/test skips; local full442/442 plus build/vet/lint and genuine aggregate-cap red/restored pass. T6.2.R claim released; T6.3 becomes available. This is bounded source/storage qualification, not production authentication, endpoint, provider or runtime activation.

T6.3 author handoff: PR16 publishes the bounded caller-scoped TLS handler and real PostgreSQL/TLS API fixtures. Local candidatea9e1ec60d62349ecf96b30cc03ddd9f2391cc623 has470 regular and470 race passes, zero failures/test skips and31 complete package terminals; build/vet/lint and empty goimports diff pass. Disabling only the path/body guard fails the actual named path rejection and persists the forbidden binding/lifecycle; byte-exact source restoration passes the named regression. Hosted CI37166573621 passes that source candidate. This documentation-only handoff preserves its Go bytes; T6.3.R must capture the actual final PR head/base, final-head hosted CI, independently accept, merge and verify landing. Author completion alone does not release T6.4. The required projector is an explicit fixture seam here, with durable trusted projection still gated on T6.4; no endpoint, identity provider, runtime or profile is activated.

T6.3.R delivery receipt: PR16 final head5e5323905a9252d46337991b666204116a5f3193 received independent acceptance5975244466 and hosted CI37166813723 (470 regular and470 race passes, zero failures/skips). Actual main2218b23d1d01a1d36db91a85abbf9c3fe1bf61d6 has reviewed treef7525ab5f3bcd70f23ccbffb16419d619ce0cc5a and reviewed base7ce9d6a0c9cd7d5e27d358a67a7c2ef0e0922fc4 as ancestor. Root independently fetched and checked both. T6.4 is admitted; its source integration is not yet verified or handed off.

T8.2/T8.4 verification and author handoff: PR18 adds an assembled control-startup fixture with real PostgreSQL hold inventory, a recovery barrier before sweep/dispatch, fail-closed late recovery, and an explicitly seeded generation+1 replacement job. The old UNKNOWN run blocks replacement without creating a second run or reservation; its hold revision and budget remain UNKNOWN. Hosted CI37171091101 at8f0ce135935bb42323c75da29365e2d806adb613 passes472 regular and472 race terminal test events, zero failures/skips and31 complete package terminals in each lane; build/vet/lint/format gates pass. Local focused reruns were held by shared load or another owner's build lease, so the verification receipt relies on the hosted service-backed execution and does not claim an additional local pass. This documentation-only handoff preserves the qualified Go bytes. T8.5 must independently accept the final head/base, final-head CI, guarded merge and actual landing. Inventory and control-resume state are explicit test fixtures; production host inventory/recovery factories remain unqualified.

T8.5 delivery receipt: independent acceptance5975858929 reviewed PR18 head523b5629a4660bc114a87d74561df17b15ae08a1/base2218b23d1d01a1d36db91a85abbf9c3fe1bf61d6 with final-head hosted CI37171505963 passing472 normal/472 race, zero failures/skips. Guarded landingc138b10717494137d50db1d2db728428c2e5ea12 has reviewed treec76364d8074b309d43a7996e65b90f7e351943c1 and reviewed base as ancestor; reviewer and Root freshly fetched and checked both. The review claim is released. This completes the bounded startup fixture verification, not production inventory or recovery qualification.

T6.4 author handoff: PR17 persists full typed host landing evidence with the canonical transition, rejects proof from workers or public callbacks, and projects canonical task IDs and historical coding-handoff PRs using persisted transition time. Host-authenticated late facts retain complete proof as bounded non-releasing audit records; exact authenticated replay does not write or advance sequence. Source candidatee9942f6 has green hosted CI37171350032; the earlier f8fc6da source suite passed484 normal/484 race, and the final historical-author regression adds one test. Local focused database suites pass14 normal/14 race, zero failures/skips; source unit tests/vet passed. Disabling the exact reviewed head, base, public proofless-update, projection-sequence and historical-author guards each failed its actual named assertion; byte-exact source restoration passed each. Pre-handoff compiler/fixture/lint and controller-assertion errors were corrected and their failed evidence retained. The current candidate includes separately delivered startup PR18 on actual basec138b10717494137d50db1d2db728428c2e5ea12; T6.4.R must require final-head hosted checks, independently accept that current snapshot, merge and verify actual landing. No production verifier, identity provider, installed registry/profile or runtime is activated.

T6.4.R delivery receipt: independent acceptance5975931671 reviewed PR17 head73347c44ce18ff583cbde9dcec7be89192a9f34a/basec138b10717494137d50db1d2db728428c2e5ea12 with final-head hosted CI37171883603 passing487 normal/487 race, zero failures/skips and31 complete packages each. GitHub refused rebase for the reviewed base-sync merge; the authorized guarded non-squash merge preserved that history and landed6ca9cfb3f58b19a5c234f6f77f1a9540e8be6dc0. Reviewer and Root freshly fetched and verified reviewed tree40c16409b992a17af1a4ec087c6874ac068e0308 and base ancestry. Claims are released. T6.5 is now admitted as an explicit tagged local host fixture; production verifier/provider/identity/runtime remain unqualified.

T6.5 author handoff: PR19 composes the actual published generic shim with real PostgreSQL/TLS, caller-scoped lost-reply recovery/restart replay, literal ordinary Git WON claims, fresh admission and fenced acknowledgement. Standalone and delegated author/review/negative/fix/re-review/approval/merge/typed-landing cases retain one stable gate; an ordinary downstream pool row becomes ready only after landing. The composition reproduced null leaf dependencies; the adapter emits an empty array, protected by the public protocol regression. Final source bcb95f1 passed17 selected tagged normal/17 race cases, zero failures/skips, plus tagged vet/lint and empty goimports. Missing trusted host artifact failed the named smoke test rather than skipping. Disabled authorization, claim verification, stale revision, cancellation, expiry, dependency-array, reviewed head/base and historical-author predicates each failed their actual named assertions and passed after byte-exact restoration. Earlier empty-selector/controller/fixture failures were retained and excluded from qualification. Published provider artifact SHA2567a8958617edc66e45a372e718dfceedff026e824c4946cce59bf7043c805f99b and claim primitive SHA2561db72f13c5cc071578a63a6c8ed3455d9c5c0aee3c782edc7adf1a43a182c609 were pinned; public CI independently qualifies ordinary public source (initial run37175103766:487 regular/487 race, zero failures/skips). T6.5.R must require final-head CI and independent acceptance before guarded merge/exact-tree landing. No production provider, verifier, profile, runtime factory or external interoperability is qualified.

T6.5.R negative review F1: independent comment5976367110 on PR19 exact head e8f0814a2e0b0b35a6547bbc61efe2e4cb7e88f5/base6ca9cfb3f58b19a5c234f6f77f1a9540e8be6dc0 identified missing explicit accounting/non-settlement assertions despite the task acceptance requirement. T6.5.F1 must bind the zero accounting assertion to the actual trusted subscription-only no-exposure grant and prove no fabricated settlement after initial/UNKNOWN/late-fenced/landed observations. The correction remains fixture-only, with no monetary/runtime scope expansion. T6.5.F1.R is dependent on the fix; stable T6.5.R remains blocked. The original review claim was released and PR19 was not merged.
