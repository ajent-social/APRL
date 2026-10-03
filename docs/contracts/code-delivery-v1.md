# Code delivery v1 wire contract

Status: frozen caller-side schema; service interoperability requires matching
implementation and the golden fixture. This protocol delegates one bounded code
change to one canonical lifecycle service; it is not a child scheduler API.

Version: `code-delivery/v1`. UTF-8 JSON, explicit keys below in the listed order.
The request digest is lowercase hex SHA-256 of compact JSON bytes in this order,
without trailing newline. Strings use Go encoding/json escaping. No field in
Request, Spec or Envelope is omitted. UTC times encode as RFC3339Nano with Z.

| Object | Ordered keys and types |
|---|---|
| Request | version:string, delegation_id:string, caller_id:string, project_id:string, job_id:string, task_id:string, plan_revision:int, plan_digest:string, spec:Spec |
| Spec | repository:string, target_branch:string, source_commit:string, acceptance:[]string, policy_revision:string, profile_revision:string, execution_mode:string, envelope:Envelope |
| Envelope | max_attempts:int, max_concurrent:int, max_cost_cents:int64, expires_at:time |
| Observation | version:string, delegation_id:string, request_digest:string, lifecycle_id:string, sequence:uint64, state:string, children:[]ChildTask, accounting:Accounting, landed:*LandedReceipt (omit only when nil), reason:string |
| ChildTask | id:string, kind:string, state:string, depends_on:[]string, finding_ids:[]string, pr_url:string, head_commit:string |
| Accounting | spent_cents:int64, reserved_cents:int64, unknown_cents:int64, settlement_id:string |
| LandedReceipt | repository:string, target_branch:string, pr_url:string, reviewed_head:string, reviewed_base:string, policy_revision:string, landed_commit:string, source_digest:string, reviewer:string, author:string, verifier:string, verified_at:time |

`execution_mode` must be `subscription_only`; `max_cost_cents` must be zero in
v1. No paid inference, paid fallback or cloud allocation is admitted. Positive
attempts/concurrency (at most six concurrent), fixed expiry and immutable acceptance
scope remain mandatory. The service independently qualifies subscription execution.

PUT `/v1/delegations/{delegation_id}` accepts Request and returns Observation
(200/201/202). GET the same route returns Observation (200/202). POST the route
with `/cancel` suffix accepts `{}` and returns Observation (200/202). Bearer-scoped
caller authentication over HTTPS is required; test-only loopback HTTP may be
explicitly enabled. Other statuses, non-JSON, unknown fields, oversized responses,
invalid bindings or transport ambiguity are errors. Cross-origin redirects cannot
forward credentials. Request/response body maximum is 1 MiB.

PUT idempotency binds caller plus delegation ID to one exact digest. Same input
returns/reconciles the same lifecycle; changed input conflicts. A not-found GET is
not evidence that a timed-out PUT failed. The caller durably records intent before
PUT, retains unknown exposure and reconciles rather than starting another lifecycle.

Observation states: admitted, running, changes_requested, landed, failed, paused,
canceled, unknown. Child kinds: author, review, fix, re_review, delivery. Child
states: pending, ready, running, completed, blocked, canceled. Sequence is positive
and monotonically increasing; same-sequence identical replay is idempotent and
conflicting replay fails. Lifecycle identity cannot change. Task IDs are stable
and unique, dependency/finding IDs explicit. These are actual canonical executable
tasks for ordinary apply/claim, not an independently executable caller graph.

Only authenticated `landed` with a complete matching receipt can release ordinary
dependencies. Independent reviewer differs from author; verifier, reviewed head/base,
trusted policy, actual landed commit and source digest are explicit. Coding handoff,
changes_requested, canceled, unknown and a completed checkbox cannot release it.
Head/base/policy changes require fresh review. Fix/re-review children remain within
the same immutable scope and finite envelope. Cancellation prevents new admission
and still reconciles previously admitted effects; late landing is recorded without
releasing descendants of a canceled parent.

Accounting values are nonnegative, overflow-safe and within the grant. Over-budget
or malformed reports preserve unknown exposure and block release; they must not
be silently discarded as if no remote work happened. Settlement is separately
authoritative; a landing receipt never refunds an allowance. Artifact/customer
qualification and production approval are independent of repository landing.

Golden fixture: `fixtures/code-delivery-v1-request.json`; expected digest is in
`fixtures/code-delivery-v1-request.sha256`. IDs and URLs are synthetic. A matching
fixture proves serialization only, not transport or lifecycle qualification.
