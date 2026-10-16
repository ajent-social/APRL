# ADR 011: Preserve the APRL kernel while retiring the separate product

Date: 2026-10-16
Status: Accepted portfolio destination; cutover and retirement are not executed.

## Decision

Foundry is the independently reusable engineering system beneath Zati and other
authorized clients. Absorb APRL's useful lifecycle kernel and retire APRL as a
separate product after an evidence-based transition. Product consolidation does
not require an immediate repository, process or storage merge. Preserve existing
source, authored work, unique branches/worktrees and implementation evidence.

ADR007 remains controlling for every presently enrolled lifecycle: APRL alone
owns canonical PR children, scheduling, readiness, stage routing and mutation
admission/results. Foundry owns product orchestration and qualification; Zati owns
organizational intent, policy and reporting. Neither creates competing dispatches
for APRL-owned stages. Generic portable skills and task tools remain supported,
but a developer claim is not runtime or GitHub authority.

The [Foundry portfolio plan](https://github.com/sirerun/foundry/blob/main/docs/portfolio-plan.md) owns the one cross-repository transition
graph. APRL's native plan and frozen code-delivery/v1 contract retain their task
identities, statuses, evidence and current lifecycle authority. Their existence
is not a command to launch an independent new APRL product or finish every old
standalone production milestone before observability can ship. Scope needed for
consumer safety or a transition is selected explicitly through the portfolio
plan and the existing native admission mechanism, without double scheduling.

## Cutover acceptance

Before changing ownership, inventory real consumers and active/unknown lifecycles,
canonical IDs, callbacks, credentials, budgets, deployments and recovery state.
Qualify the selected Foundry-facing boundary using independent review and real
consumer evidence. Preserve exact-head/base review, bounded fix/re-review,
current CI/target policy, guarded merge and verified landing; ordinary descendants
must not run on review completion alone.

Choose and record a bounded routing/cutover boundary. Existing work drains under
its authoritative owner unless a separately reviewed transfer preserves identity,
fencing, reservations and outcomes. Reconcile unknown GitHub/provider effects
before any retry; never replay mutations merely because the old service stopped
responding. Prove no dual writer/scheduler, duplicate merge, abandoned reservation
or lost callback. Keep a recovery route; rollback cannot re-enable two owners.
The current ADR007 ownership ends only for the explicitly verified cutover scope.

Retire the separate service only after consumers and durable work have drained or
been safely transferred, recovery/retention is accepted, and explicit shutdown
authority identifies the affected resources. Do not delete repositories, worktrees,
state or credentials based on this document. No direct cross-service database
access or silent consumer migration is introduced.

## Priority and limits

Bora observability and ASSI infrastructure order live within Foundry. Install and
collect across all production projects first; full APRL consolidation is not a
prerequisite. The observability allowance is $100/month in AWS using Kubernetes;
Foundry hosting and AWS coding workers have a separate $150/month ceiling. No
budget is combined or expanded, and this decision authorizes no cloud writes or
paid runs. AMOS and preserved Griffon application-runtime boundaries are unchanged.

Routine release/recovery can be automatic within configured scope and policy;
destructive actions, privilege increases, budget expansion and exceptions require
the human. Existing admissions and human-required rules remain effective until an
explicitly verified policy change. Source/fixture tests, live interoperability,
installed operation and safe retirement remain separate evidence claims.
