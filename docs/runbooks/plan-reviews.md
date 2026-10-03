# Plan review and delivery runbook

This runbook describes the required lifecycle behavior. It does not assert that a particular integration or runtime is currently implemented or qualified.

## Author handoff

1. Claim the admitted coding task through the normal apply/claim path.
2. Implement the requested change and run the checks required by trusted policy.
3. Create or update the pull request using authorized repository operations.
4. Record the pull request URL and exact head revision on the dependent review task, together with the required handoff evidence.
5. Complete the coding task at this handoff. The review task is now eligible for normal apply/claim. Do not mark the delivery gate complete.

## Independent review

1. An eligible reviewer claims the visible review task through normal apply/claim. Verify independence from actual actor and authored-revision provenance; task pickup alone does not prove independence.
2. Load the current pull-request head, base, trusted policy revision, and required CI evidence. Confirm they refer to the same review snapshot.
3. Record findings with stable identifiers and a snapshot-bound verdict: approved, changes requested, or failed. Execution success alone is not approval.
4. Before an approval can authorize merge, recheck that the head, base, policy, required checks, and repository rules are still current. Any relevant movement invalidates the approval.

## Approved delivery

1. The independent reviewer requests guarded merge through the authorized mutation boundary, binding the operation to the approved expected head and current target/policy state.
2. Reconcile ambiguous outcomes before retrying. Do not treat a timeout or cancellation as evidence that merge did not occur.
3. Verify the actual landed revision and record a receipt binding the repository, pull request, reviewed head/base, policy, landed commit, provenance, and verifier.
4. Satisfy the stable delivery gate only after merge is confirmed and landing is verified. Release ordinary downstream tasks only from that gate. A speculative dependency must explicitly name an immutable isolated PR-head input and must not imply delivery or release readiness.

## Negative review and bounded correction

1. Keep the delivery gate blocked when review requests changes or fails.
2. Create a visible fix task linked to the stable findings, pull request, and affected revision. It depends on the recorded handoff/findings and current admission; it must not depend on successful completion of the negative review task.
3. Create a re-review task dependent on the fix's updated PR handoff. The fix repeats required checks and records its new exact head before completing.
4. Run re-review through normal apply/claim with a reviewer independent of the relevant authored revision. Review the new current snapshot; prior approval cannot carry forward after head, base, or policy changes.
5. Repeat only within the preapproved scope, attempts, duration, concurrency, and budget. Record child-task expansion append-only. If correction is exhausted or scope must grow, leave delivery blocked and escalate for revised authority.

## Pause, cancellation, and recovery

Stop new admissions under a withdrawn or paused grant and honor the lifecycle's local pause control. Fence stale tasks and claims. Separately reconcile physical execution, repository operations, and charges; record unknown outcomes as unknown until observed. A late observation may establish a merge or charge but must not advance a canceled lifecycle. Retry unknown GitHub mutations only after reconciliation shows that retry is safe.


All declared coding contributors are published with the PR handoff and excluded from reviewing that revision. An earlier reviewer may execute a later fix task, but a different reviewer must review the corrected head; historical verdicts remain immutable.

An exact already-recorded live receipt is not a new transition: the store returns a conflict rather than advancing its revision. Reconcile the persisted receipt through the trusted host before treating an uncertain retry as delivered. Cancellation or expiry rejects replayed success evidence. The buffered protocol is an embedding bridge, and requires host-selected authentication, fresh claims, current policy and qualified remote outcomes; it provides no default executable endpoint or credentials.
