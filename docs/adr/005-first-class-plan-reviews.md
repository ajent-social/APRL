# ADR 005: First-class plan reviews and lifecycle delivery gates

Status: Accepted architecture; implementation and integration are not yet verified.

## Context

Code review must be an executable, independently claimable unit of work. Hiding review in an automatic loop makes it hard to schedule, audit, retry, or distinguish an approved delivery from a successful but negative review attempt. At the same time, shared plan tooling should not take over a caller's domain-specific authority to admit work or decide when its lifecycle has advanced.

## Decision

Every code-changing pull request requires an independent review before merge. There is no code-merge exemption based on task labels, file types, actor role, or documentation-only claims. Trusted policy and revision provenance determine whether a task changes code.

The generic plan/apply/claim layer represents review, fix, and re-review as ordinary visible executable tasks, parses their dependencies, routes the appropriate stage, and atomically claims work. A domain caller or provider adapter owns lifecycle-specific admission, readiness, outcomes, and receipts. In APRL, APRL remains the canonical authority for those lifecycle decisions and GitHub mutations. Adapters bind generic claims to the current domain admission and release only claims they own when admission is denied. A task claim, stale plan row, or checked box alone never authorizes execution or mutation.

The lifecycle proceeds as follows:

1. An author completes required checks, creates or updates the pull request through authorized operations, and records its URL and exact current head on the dependent review task. The author task completes at this handoff; that does not mean the delivery is complete.
2. The independent review task becomes claimable through normal apply/claim. The reviewer checks the current head and base, trusted policy revision, and required CI evidence, then records an approval, a changes-requested result, or a failure against that exact snapshot.
3. Approval permits guarded merge only while the reviewed head, base, policy, CI evidence, and repository rules remain current. The merge uses an expected-head guard. Delivery completes only after the actual landed revision is verified and recorded.
4. A blocking or otherwise corrective review outcome creates an explicit bounded fix task and dependent re-review task, linked to stable finding IDs, the pull request, and affected revision. The fix depends on the finding/handoff record and current admission, not on successful completion of the negative review task. Re-review depends on the fix's updated PR handoff. A changed head, base, or policy invalidates approval and requires a new review.

Each lifecycle has one stable delivery gate across review and correction attempts. Review execution success is distinct from review approval. A negative review cannot satisfy the gate; unresolved or exhausted correction keeps the gate blocked. Ordinary downstream work is ready only after independent review, confirmed merge, and verified landing. Earlier speculative work requires an explicit dependency on an immutable isolated PR-head input and grants no delivery or release authority.

Corrective expansion is limited by a preapproved envelope covering scope, attempts, duration, concurrency, and budget. Fix and re-review tasks remain visible and append-only under that envelope. Exceeding it requires escalation and revised authority. This decision does not create an allowance.

The provider boundary stays generic: shared tooling handles representation and claim/routing mechanics, while the caller/provider adapter binds admission and result handoff to its authoritative lifecycle. There is no shared database or distributed exactly-once assumption. Durable idempotency, reconciliation, and authenticated outcome receipts belong to the integration contract. Physical execution has one durable launch/stop/reconciliation owner; a provider may allocate capacity but does not become a second PR scheduler or lifecycle controller.

## Consequences

- Review, fix, and re-review are visible plan tasks executed through normal apply/claim.
- Code-changing PRs cannot bypass independent review.
- The lifecycle owner remains authoritative for readiness, correction progression, repository policy, merge admission, and verified landing.
- A stable delivery gate blocks descendants until the current reviewed snapshot is actually landed and verified.
- Concrete schemas, migrations, adapters, and runtime qualification require separate implementation and verification. This ADR does not claim the integration currently works.


The [whole-lifecycle boundary](007-whole-code-change-lifecycle.md) and [caller-facing wire contract](../contracts/code-delivery-v1.md) remain authoritative for external delegation. The internal plan-task protocol is a separate representation and does not qualify that external runtime.
