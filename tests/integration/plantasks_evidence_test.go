package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/plantasks"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

const plantasksEvidenceIDPrefix = "f2000000-0000-4000-8000-"

var plantasksEvidenceNextID atomic.Uint64

func plantasksEvidenceID() string {
	return fmt.Sprintf("%s%012x", plantasksEvidenceIDPrefix, plantasksEvidenceNextID.Add(1))
}

type plantasksEvidenceFixture struct {
	ctx       context.Context
	pool      *pgxpool.Pool
	manual    *clock.Manual
	store     *plantasks.Store
	binding   plantasks.DelegationBinding
	delegates *plantasks.Delegations
	actors    *plantasksTestAuthenticator
	claims    *plantasksTestClaimVerifier
	policy    *plantasksTestRuntimePolicy
	outcomes  *plantasksEvidenceOutcomeVerifier
	landing   *plantasksEvidenceLandingVerifier
	hostAuth  *plantasksEvidenceHostAuthenticator
	adapter   *plantasks.Adapter
	request   plantasks.DeliveryV1Request
	author    plantasks.Provenance
	reviewer  plantasks.Provenance
	fixer     plantasks.Provenance
	reviewID  string
	claimID   string
	pr        plantasks.PRBinding
	merged    plantasks.Receipt
}

func newPlantasksEvidenceFixture(t *testing.T) *plantasksEvidenceFixture {
	t.Helper()
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate landing evidence schema: %v", err)
	}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	manual := clock.NewManual(now)
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "evidence-caller-" + plantasksEvidenceID()}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	delegates, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct evidence delegation service: %v", err)
	}
	_, request := plantasksDelegationRequest(t)
	request.DelegationID = plantasksEvidenceID()
	request.CallerID = auth.caller
	// The real corrected workflow admits exactly four lifecycle tasks: initial
	// author, initial review, one fix, and its independent re-review.
	request.Spec.Envelope.MaxAttempts = 4
	request.Spec.Envelope.ExpiresAt = now.Add(24 * time.Hour)
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode evidence request: %v", err)
	}
	binding, err := delegates.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("admit evidence request: %v", err)
	}
	actors := &plantasksTestAuthenticator{}
	claims := &plantasksTestClaimVerifier{allowed: make(map[string]plantasksTestClaim)}
	policyRuntime := &plantasksTestRuntimePolicy{lifecycleID: binding.LifecycleID,
		taskIDs:  make(map[string]bool),
		actorIDs: make(map[string]bool),
	}
	for id := range binding.State.Tasks {
		policyRuntime.taskIDs[id] = true
	}
	landing := &plantasksEvidenceLandingVerifier{}
	outcomes := &plantasksEvidenceOutcomeVerifier{allow: make(map[string]struct{}), landing: landing}
	hostAuth := &plantasksEvidenceHostAuthenticator{actor: "host-ledger-verifier"}
	adapter, err := plantasks.NewAdapter(store, claims, actors, policyRuntime, outcomes)
	if err != nil {
		t.Fatalf("construct evidence adapter: %v", err)
	}
	author := plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: now}
	reviewer := plantasks.Provenance{ActorID: "reviewer-1", ActorKind: "human", AuthoredAt: now}
	fixer := plantasks.Provenance{ActorID: "fixer-1", ActorKind: "agent", AuthoredAt: now}
	for _, actor := range []plantasks.Provenance{author, reviewer, fixer} {
		policyRuntime.actorIDs[actor.ActorID] = true
	}
	f := &plantasksEvidenceFixture{ctx: ctx, pool: db.Pool, manual: manual, store: store, binding: binding,
		delegates: delegates, actors: actors, claims: claims, policy: policyRuntime, outcomes: outcomes,
		landing: landing, hostAuth: hostAuth, adapter: adapter, request: request,
		author: author, reviewer: reviewer, fixer: fixer,
	}
	f.authorHandoff(t)
	f.requestReview(t)
	f.fixAndRereview(t)
	f.approveAndMerge(t)
	return f
}

func (f *plantasksEvidenceFixture) load(t *testing.T) plantasks.State {
	t.Helper()
	state, err := f.store.Load(f.ctx, f.binding.LifecycleID)
	if err != nil {
		t.Fatalf("load evidence lifecycle: %v", err)
	}
	return state
}

func (f *plantasksEvidenceFixture) admit(t *testing.T, taskID string, actor plantasks.Provenance, suffix string) string {
	t.Helper()
	claim := strings.Repeat("a", 39) + fmt.Sprintf("%x", plantasksEvidenceNextID.Add(1)%16)
	if len(claim) != 40 {
		claim = strings.Repeat("b", 40)
	}
	f.actors.actor = actor
	f.claims.allow(taskID, claim, actor.ActorID)
	f.policy.taskIDs[taskID] = true
	f.policy.actorIDs[actor.ActorID] = true
	state := f.load(t)
	if err := f.adapter.Admit(f.ctx, state.Lifecycle.ID, taskID, claim, state.Revision, f.manual.Now().Add(12*time.Hour)); err != nil {
		t.Fatalf("admit %s task: %v", suffix, err)
	}
	return claim
}

func (f *plantasksEvidenceFixture) result(t *testing.T, receipt plantasks.Receipt, claim string) {
	t.Helper()
	state := f.load(t)
	if err := f.adapter.Result(f.ctx, state.Lifecycle.ID, receipt, claim, state.Revision); err != nil {
		t.Fatalf("record %s result: %v", receipt.Outcome, err)
	}
}

func (f *plantasksEvidenceFixture) authorHandoff(t *testing.T) {
	t.Helper()
	state := f.load(t)
	authorTask := state.Tasks[plantasksStoreTaskID]
	if authorTask.ID == "" {
		for _, task := range state.Tasks {
			if task.Stage == plantasks.StageAuthor {
				authorTask = task
				break
			}
		}
	}
	claim := f.admit(t, authorTask.ID, f.author, "author")
	f.pr = plantasksStorePR()
	f.pr.URL = "https://github.com/example/project/pull/1"
	f.author.SourceRevision = f.pr.HeadSHA
	f.actors.actor = f.author
	receipt := plantasks.Receipt{Version: plantasks.VersionV1, ID: plantasksEvidenceID(),
		LifecycleID: state.Lifecycle.ID, TaskID: authorTask.ID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor: f.author, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &f.pr, CreatedAt: f.manual.Now()}
	f.result(t, receipt, claim)
}

func (f *plantasksEvidenceFixture) requestReview(t *testing.T) {
	t.Helper()
	state := f.load(t)
	for _, task := range state.Tasks {
		if task.Stage == plantasks.StageReview && task.Correction == 0 {
			f.reviewID = task.ID
			break
		}
	}
	if f.reviewID == "" {
		t.Fatal("initial delivery review task missing after author handoff")
	}
	claim := f.admit(t, f.reviewID, f.reviewer, "review")
	f.reviewer.SourceRevision = f.pr.HeadSHA
	f.actors.actor = f.reviewer
	receipt := plantasks.Receipt{Version: plantasks.VersionV1, ID: plantasksEvidenceID(),
		LifecycleID: state.Lifecycle.ID, TaskID: f.reviewID, Outcome: plantasks.OutcomeChangesRequest,
		Actor: f.reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &f.pr,
		FindingIDs: []string{plantasksEvidenceID()}, CreatedAt: f.manual.Now(), Detail: "one bounded correction"}
	f.result(t, receipt, claim)
}

func (f *plantasksEvidenceFixture) fixAndRereview(t *testing.T) {
	t.Helper()
	state := f.load(t)
	var fixTask, rereview plantasks.Task
	for _, task := range state.Tasks {
		if task.Correction == 1 && task.Stage == plantasks.StageFix {
			fixTask = task
		}
		if task.Correction == 1 && task.Stage == plantasks.StageRereview {
			rereview = task
		}
	}
	if fixTask.ID == "" || rereview.ID == "" {
		t.Fatalf("correction tasks missing: fix=%+v rereview=%+v", fixTask, rereview)
	}
	claim := f.admit(t, fixTask.ID, f.fixer, "fix")
	newPR := f.pr
	newPR.HeadSHA = strings.Repeat("c", 40)
	f.fixer.SourceRevision = newPR.HeadSHA
	f.actors.actor = f.fixer
	receipt := plantasks.Receipt{Version: plantasks.VersionV1, ID: plantasksEvidenceID(),
		LifecycleID: state.Lifecycle.ID, TaskID: fixTask.ID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor: f.fixer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &newPR, CreatedAt: f.manual.Now()}
	f.result(t, receipt, claim)
	f.pr = newPR
	f.reviewID = rereview.ID
}

func (f *plantasksEvidenceFixture) approveAndMerge(t *testing.T) {
	t.Helper()
	state := f.load(t)
	claim := f.admit(t, f.reviewID, f.reviewer, "rereview")
	f.reviewer.SourceRevision = f.pr.HeadSHA
	f.actors.actor = f.reviewer
	approved := plantasks.Receipt{Version: plantasks.VersionV1, ID: plantasksEvidenceID(),
		LifecycleID: state.Lifecycle.ID, TaskID: f.reviewID, Outcome: plantasks.OutcomeApproved,
		Actor: f.reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &f.pr, CreatedAt: f.manual.Now()}
	f.result(t, approved, claim)
	state = f.load(t)
	f.merged = plantasks.Receipt{Version: plantasks.VersionV1, ID: plantasksEvidenceID(),
		LifecycleID: state.Lifecycle.ID, TaskID: f.reviewID, Outcome: plantasks.OutcomeMerged,
		Actor: f.reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &f.pr,
		MergeCommit: strings.Repeat("d", 40), CreatedAt: f.manual.Now()}
	f.outcomes.allow[f.merged.ID] = struct{}{}
	f.result(t, f.merged, claim)
	f.claimID = claim
}

func (f *plantasksEvidenceFixture) landingReceipt(t *testing.T) plantasks.Receipt {
	t.Helper()
	state := f.load(t)
	return plantasks.Receipt{Version: plantasks.VersionV1, ID: plantasksEvidenceID(),
		LifecycleID: state.Lifecycle.ID, TaskID: f.reviewID, Outcome: plantasks.OutcomeLanded,
		Actor: f.reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &f.pr,
		MergeCommit: f.merged.MergeCommit, LandedCommit: f.merged.MergeCommit, CreatedAt: f.manual.Now()}
}

func (f *plantasksEvidenceFixture) evidence(receipt plantasks.Receipt, revision int64) plantasks.LandedEvidence {
	return plantasks.LandedEvidence{LifecycleID: receipt.LifecycleID, TaskID: receipt.TaskID,
		ReceiptID: receipt.ID, Revision: revision, PRNumber: f.pr.Number, MergeCommit: receipt.MergeCommit,
		Receipt: plantasks.DeliveryV1LandedReceipt{Repository: f.request.Spec.Repository, TargetBranch: f.request.Spec.TargetBranch,
			PRURL: f.pr.URL, ReviewedHead: f.pr.HeadSHA, ReviewedBase: f.pr.BaseSHA,
			PolicyRevision: f.binding.State.Lifecycle.PolicyRevision, LandedCommit: receipt.LandedCommit,
			SourceDigest: strings.Repeat("a", 64), Reviewer: f.reviewer.ActorID, Author: f.author.ActorID,
			Verifier: f.hostAuth.actor, VerifiedAt: f.manual.Now().UTC()}}
}

func (f *plantasksEvidenceFixture) authorizeLanding(receipt plantasks.Receipt) {
	state, _ := f.store.Load(f.ctx, f.binding.LifecycleID)
	evidence := f.evidence(receipt, state.Revision)
	f.landing.set(evidence)
	f.outcomes.allow[receipt.ID] = struct{}{}
}

func (f *plantasksEvidenceFixture) recordLanding(t *testing.T, receipt plantasks.Receipt) {
	t.Helper()
	f.authorizeLanding(receipt)
	f.actors.actor = f.reviewer
	f.result(t, receipt, f.claimID)
}

type plantasksEvidenceOutcomeVerifier struct {
	allow   map[string]struct{}
	landing *plantasksEvidenceLandingVerifier
}

func (v *plantasksEvidenceOutcomeVerifier) Verify(_ context.Context, _ plantasks.Lifecycle, _ plantasks.Task, receipt plantasks.Receipt) error {
	if receipt.Outcome == plantasks.OutcomeMerged || receipt.Outcome == plantasks.OutcomeLanded {
		if _, ok := v.allow[receipt.ID]; !ok {
			return plantasks.ErrOutcomeDenied
		}
	}
	return nil
}

func (v *plantasksEvidenceOutcomeVerifier) FetchLanding(ctx context.Context, state plantasks.State, task plantasks.Task, receipt plantasks.Receipt) (plantasks.LandedEvidence, error) {
	return v.landing.FetchLanding(ctx, state, task, receipt)
}

func (v *plantasksEvidenceOutcomeVerifier) VerifyLanding(ctx context.Context, state plantasks.State, task plantasks.Task, receipt plantasks.Receipt, evidence plantasks.LandedEvidence, now time.Time) error {
	return v.landing.VerifyLanding(ctx, state, task, receipt, evidence, now)
}

type plantasksEvidenceLandingVerifier struct {
	evidence      plantasks.LandedEvidence
	trustedDigest string
	err           error
}

func (v *plantasksEvidenceLandingVerifier) set(evidence plantasks.LandedEvidence) {
	v.evidence = evidence
	v.trustedDigest = evidence.Receipt.SourceDigest
}

func (v *plantasksEvidenceLandingVerifier) FetchLanding(_ context.Context, _ plantasks.State, _ plantasks.Task, _ plantasks.Receipt) (plantasks.LandedEvidence, error) {
	return v.evidence, v.err
}

func (v *plantasksEvidenceLandingVerifier) VerifyLanding(_ context.Context, _ plantasks.State, _ plantasks.Task, _ plantasks.Receipt, evidence plantasks.LandedEvidence, _ time.Time) error {
	if v.err != nil {
		return v.err
	}
	if evidence.Receipt.SourceDigest != v.trustedDigest {
		return errors.New("fixture host verifier found an untrusted landed tree digest")
	}
	if !reflect.DeepEqual(v.evidence, evidence) {
		return errors.New("fixture host proof signature mismatch")
	}
	return nil
}

type plantasksEvidenceHostAuthenticator struct {
	actor string
	err   error
}

func (a *plantasksEvidenceHostAuthenticator) AuthenticateHost(context.Context) (string, error) {
	return a.actor, a.err
}

func TestPlanTaskLandingEvidenceCommitsTypedProofAndProjectsStableSnapshot(t *testing.T) {
	f := newPlantasksEvidenceFixture(t)
	receipt := f.landingReceipt(t)
	f.recordLanding(t, receipt)
	state := f.load(t)
	if state.DeliveryReceiptID != receipt.ID || state.Completed[receipt.TaskID] != plantasks.OutcomeLanded {
		t.Fatalf("typed landing did not satisfy stable delivery gate: receipt=%q completed=%q", state.DeliveryReceiptID, state.Completed[receipt.TaskID])
	}
	var stored *plantasks.LandedEvidence
	for _, actual := range state.Receipts {
		if actual.ID == receipt.ID {
			stored = actual.LandedEvidence
			break
		}
	}
	if stored == nil || !reflect.DeepEqual(*stored, f.landing.evidence) {
		t.Fatalf("trusted typed landing proof not persisted with receipt: %+v", stored)
	}
	binding, err := f.delegates.Get(f.ctx, f.request.DelegationID)
	if err != nil {
		t.Fatalf("reload delivered delegation: %v", err)
	}
	projector, err := plantasks.NewDeliveryV1Projector(f.store)
	if err != nil {
		t.Fatalf("construct delivery projector: %v", err)
	}
	first, err := projector.Project(f.ctx, binding)
	if err != nil {
		t.Fatalf("project delivered observation: %v", err)
	}
	firstBytes, err := plantasks.EncodeDeliveryV1Observation(first, binding.Request)
	if err != nil {
		t.Fatalf("encode delivered observation: %v", err)
	}
	if first.Landed == nil || first.Landed.LandedCommit != receipt.LandedCommit || first.State != "landed" {
		t.Fatalf("projection omitted qualified landing: %+v", first)
	}
	staleBinding := binding
	staleBinding.Sequence--
	if _, err := projector.Project(f.ctx, staleBinding); err == nil {
		t.Fatal("projector upgraded a stale caller binding to the current observation")
	}
	if len(first.Children) != 4 {
		t.Fatalf("correction projection has %d children, want actual author, review, fix, and re-review tasks without a synthetic delivery row; %+v", len(first.Children), first.Children)
	}
	f.manual.Advance(2 * time.Hour)
	second, err := projector.Project(f.ctx, binding)
	if err != nil {
		t.Fatalf("repeat projection after wall clock advanced: %v", err)
	}
	secondBytes, err := plantasks.EncodeDeliveryV1Observation(second, binding.Request)
	if err != nil || string(firstBytes) != string(secondBytes) {
		t.Fatalf("same-sequence projection changed after wall time moved: equal=%t err=%v", string(firstBytes) == string(secondBytes), err)
	}
}

func TestPlanTaskLandingEvidenceRejectsWrongReviewedHead(t *testing.T) {
	plantasksEvidenceRejectsMutation(t, "wrong reviewed head", func(e *plantasks.LandedEvidence) {
		e.Receipt.ReviewedHead = strings.Repeat("c", 40)
	})
}

func TestPlanTaskLandingEvidenceRejectsWrongReviewedBase(t *testing.T) {
	plantasksEvidenceRejectsMutation(t, "wrong reviewed base", func(e *plantasks.LandedEvidence) {
		e.Receipt.ReviewedBase = strings.Repeat("c", 40)
	})
}

func TestPlanTaskLandingEvidenceRejectsWrongPolicyRevision(t *testing.T) {
	plantasksEvidenceRejectsMutation(t, "wrong policy revision", func(e *plantasks.LandedEvidence) {
		e.Receipt.PolicyRevision = "policy-revoked"
	})
}

func TestPlanTaskLandingEvidenceRejectsWrongSourceDigest(t *testing.T) {
	f := newPlantasksEvidenceFixture(t)
	receipt := f.landingReceipt(t)
	state := f.load(t)
	evidence := f.evidence(receipt, state.Revision)
	f.landing.set(evidence)
	// The trusted test host signs the original Git-tree digest. Return a second
	// syntactically valid digest to prove the local host verifier rejects it.
	f.landing.evidence.Receipt.SourceDigest = strings.Repeat("c", 64)
	f.outcomes.allow[receipt.ID] = struct{}{}
	f.actors.actor = f.reviewer
	before := f.load(t)
	if err := f.adapter.Result(f.ctx, before.Lifecycle.ID, receipt, f.claimID, before.Revision); err == nil {
		t.Fatal("landing with a different Git-tree digest passed trusted host verification")
	}
	assertPlantasksEvidenceUnchanged(t, f, before)
}

func TestPlanTaskLandingEvidenceRequiresReviewerIndependentOfAllAuthors(t *testing.T) {
	f := newPlantasksEvidenceFixture(t)
	for _, authorID := range []string{f.author.ActorID, f.fixer.ActorID} {
		t.Run(authorID, func(t *testing.T) {
			receipt := f.landingReceipt(t)
			state := f.load(t)
			evidence := f.evidence(receipt, state.Revision)
			evidence.Receipt.Reviewer = authorID
			f.landing.set(evidence)
			f.outcomes.allow[receipt.ID] = struct{}{}
			f.actors.actor = f.reviewer
			before := f.load(t)
			if err := f.adapter.Result(f.ctx, before.Lifecycle.ID, receipt, f.claimID, before.Revision); err == nil {
				t.Fatal("landing accepted reviewer who authored part of the delivered change")
			}
			assertPlantasksEvidenceUnchanged(t, f, before)
		})
	}
}

func TestPlanTaskLandingEvidenceCannotBeForgedByWorkerOrPublicStoreUpdate(t *testing.T) {
	f := newPlantasksEvidenceFixture(t)
	receipt := f.landingReceipt(t)
	state := f.load(t)
	evidence := f.evidence(receipt, state.Revision)
	f.landing.set(evidence)
	f.outcomes.allow[receipt.ID] = struct{}{}
	f.actors.actor = f.reviewer
	workerForgery := receipt
	workerForgery.LandedEvidence = &evidence
	before := f.load(t)
	if err := f.adapter.Result(f.ctx, before.Lifecycle.ID, workerForgery, f.claimID, before.Revision); err == nil {
		t.Fatal("worker-supplied landing proof was accepted")
	}
	assertPlantasksEvidenceUnchanged(t, f, before)
	beforeBinding, err := f.delegates.Get(f.ctx, f.request.DelegationID)
	if err != nil {
		t.Fatalf("load delegated binding before proofless public store attempt: %v", err)
	}
	if err := f.store.Update(f.ctx, before.Lifecycle.ID, before.Revision, func(current *plantasks.State) error {
		// This is intentionally proofless: the verified capability is private
		// to Adapter.Result, so a public callback cannot satisfy the delivery gate.
		return current.Record(receipt, f.claimID, f.manual.Now())
	}); err == nil {
		t.Fatal("public store callback forged host landing without verified capability")
	}
	assertPlantasksEvidenceUnchanged(t, f, before)
	afterBinding, err := f.delegates.Get(f.ctx, f.request.DelegationID)
	if err != nil {
		t.Fatalf("load binding after proofless public store attempt: %v", err)
	}
	if beforeBinding.Sequence != afterBinding.Sequence || beforeBinding.LifecycleRevision != afterBinding.LifecycleRevision {
		t.Fatalf("proofless public store attempt churned binding sequence/revision: before=%+v after=%+v", beforeBinding, afterBinding)
	}
}

func TestPlanTaskLandingEvidenceBindingFailureRollsBackReceiptAndProof(t *testing.T) {
	f := newPlantasksEvidenceFixture(t)
	receipt := f.landingReceipt(t)
	f.authorizeLanding(receipt)
	beforeState := f.load(t)
	beforeBinding, err := f.delegates.Get(f.ctx, f.request.DelegationID)
	if err != nil {
		t.Fatalf("load binding before forced final-write failure: %v", err)
	}
	if _, err := f.pool.Exec(f.ctx, `CREATE FUNCTION reject_landing_binding_update() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced landing binding failure'; END; $$`); err != nil {
		t.Fatalf("create binding failure function: %v", err)
	}
	if _, err := f.pool.Exec(f.ctx, `CREATE TRIGGER reject_landing_binding_update BEFORE UPDATE OF lifecycle_revision ON plan_delegations
		FOR EACH ROW EXECUTE FUNCTION reject_landing_binding_update()`); err != nil {
		_, _ = f.pool.Exec(f.ctx, `DROP FUNCTION reject_landing_binding_update()`)
		t.Fatalf("create binding failure trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_landing_binding_update ON plan_delegations`)
		_, _ = f.pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS reject_landing_binding_update()`)
	})
	f.actors.actor = f.reviewer
	if err := f.adapter.Result(f.ctx, beforeState.Lifecycle.ID, receipt, f.claimID, beforeState.Revision); err == nil {
		t.Fatal("landing committed despite forced final binding write failure")
	}
	afterState := f.load(t)
	afterBinding, err := f.delegates.Get(f.ctx, f.request.DelegationID)
	if err != nil {
		t.Fatalf("load binding after failed landing transaction: %v", err)
	}
	var receiptRows int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM plan_task_receipts WHERE lifecycle_id=$1::uuid`, beforeState.Lifecycle.ID).Scan(&receiptRows); err != nil {
		t.Fatalf("count receipt rows after rollback: %v", err)
	}
	if !reflect.DeepEqual(beforeState, afterState) || beforeBinding.Sequence != afterBinding.Sequence ||
		beforeBinding.LifecycleRevision != afterBinding.LifecycleRevision || receiptRows != len(beforeState.Receipts) {
		t.Fatalf("failed landing bind left partial proof/receipt/state: before=%+v after=%+v bindingSeq=%d/%d receiptRows=%d", beforeState, afterState, beforeBinding.Sequence, afterBinding.Sequence, receiptRows)
	}
}

func TestPlanTaskLandingEvidenceStaleSequenceCannotMutate(t *testing.T) {
	f := newPlantasksEvidenceFixture(t)
	receipt := f.landingReceipt(t)
	f.authorizeLanding(receipt)
	before := f.load(t)
	if err := f.adapter.Result(f.ctx, before.Lifecycle.ID, receipt, f.claimID, before.Revision-1); err == nil {
		t.Fatal("stale lifecycle revision accepted landing")
	}
	assertPlantasksEvidenceUnchanged(t, f, before)
}

func TestPlanTaskLateLandingFactAfterCancellationOrExpiryDoesNotRelease(t *testing.T) {
	for _, mode := range []string{"cancelled", "expired"} {
		t.Run(mode, func(t *testing.T) {
			f := newPlantasksEvidenceFixture(t)
			var canceled bool
			if mode == "cancelled" {
				if _, err := f.delegates.Cancel(f.ctx, f.request.DelegationID); err != nil {
					t.Fatalf("cancel admitted delegation: %v", err)
				}
				canceled = true
			} else {
				f.manual.Advance(24*time.Hour + time.Nanosecond)
			}
			before := f.load(t)
			receipt := f.landingReceipt(t)
			f.landing.set(f.evidence(receipt, before.Revision))
			recorder, err := plantasks.NewLateLandingRecorder(f.store, f.hostAuth, f.outcomes)
			if err != nil {
				t.Fatalf("construct host late-landing recorder: %v", err)
			}
			if err := recorder.Record(f.ctx, before.Lifecycle.ID, receipt, before.Revision); err != nil {
				t.Fatalf("record trusted late landing fact after %s: %v", mode, err)
			}
			after := f.load(t)
			if after.Revision != before.Revision+1 || after.DeliveryReceiptID != before.DeliveryReceiptID || after.Completed[f.reviewID] == plantasks.OutcomeLanded {
				t.Fatalf("late fact advanced workflow authority: before=%+v after=%+v", before, after)
			}
			if after.Cancelled != canceled || !reflect.DeepEqual(before.Tasks, after.Tasks) || !reflect.DeepEqual(before.Claims, after.Claims) || !reflect.DeepEqual(before.Receipts, after.Receipts) {
				t.Fatal("late landing fact changed cancellation, tasks, claims, or normal receipt history")
			}
			bindingForProjection, err := f.delegates.Get(f.ctx, f.request.DelegationID)
			if err != nil {
				t.Fatalf("read binding for late-fact projection: %v", err)
			}
			projector, err := plantasks.NewDeliveryV1Projector(f.store)
			if err != nil {
				t.Fatalf("construct late-fact projector: %v", err)
			}
			observation, err := projector.Project(f.ctx, bindingForProjection)
			if err != nil {
				t.Fatalf("project late landing audit: %v", err)
			}
			wantStatus, wantReason := "canceled", ""
			if mode == "expired" {
				wantStatus, wantReason = "unknown", "scope_expired"
			}
			if observation.State != wantStatus || observation.Reason != wantReason || observation.Landed != nil || observation.Accounting.SpentCents != 0 || observation.Accounting.ReservedCents != 0 || observation.Accounting.UnknownCents != 0 || observation.Accounting.SettlementID != "" {
				t.Fatalf("late audit changed workflow or accounting authority: %+v", observation)
			}
			fact, ok := after.LateLandedFacts[receipt.ID]
			if !ok || fact.ID != receipt.ID || fact.HostActor != f.hostAuth.actor || fact.Receipt.LandedEvidence == nil ||
				!reflect.DeepEqual(*fact.Receipt.LandedEvidence, f.landing.evidence) {
				t.Fatalf("separate audit fact lacks its verified host proof: %+v", fact)
			}
			want := before
			want.Revision = after.Revision
			want.ProjectionTime = after.ProjectionTime
			want.LateLandedFacts = after.LateLandedFacts
			if !reflect.DeepEqual(want, after) {
				t.Fatal("late fact changed non-audit lifecycle state")
			}
			firstBinding, err := f.delegates.Get(f.ctx, f.request.DelegationID)
			if err != nil {
				t.Fatalf("read binding after late fact: %v", err)
			}
			reopenedStore, err := plantasks.NewStore(f.pool, f.manual)
			if err != nil {
				t.Fatalf("reopen store after late fact: %v", err)
			}
			reopenedRecorder, err := plantasks.NewLateLandingRecorder(reopenedStore, f.hostAuth, f.outcomes)
			if err != nil {
				t.Fatalf("reopen late-landing recorder: %v", err)
			}
			if err := reopenedRecorder.Record(f.ctx, before.Lifecycle.ID, receipt, before.Revision); err != nil {
				t.Fatalf("exact late-fact replay after reopen: %v", err)
			}
			reopenedDelegates, err := plantasks.NewDelegations(reopenedStore,
				&plantasksDelegationAuthenticator{caller: f.request.CallerID}, &plantasksDelegationPolicy{}, &plantasksDelegationInitialFactory{})
			if err != nil {
				t.Fatalf("reopen delegation reader: %v", err)
			}
			afterReplay, err := reopenedDelegates.Get(f.ctx, f.request.DelegationID)
			if err != nil {
				t.Fatalf("read binding after late-fact replay: %v", err)
			}
			if afterReplay.Sequence != firstBinding.Sequence || afterReplay.LifecycleRevision != firstBinding.LifecycleRevision {
				t.Fatalf("exact late-fact replay churned persisted sequence/revision: first=%+v replay=%+v", firstBinding, afterReplay)
			}
			changedProof := f.landing.evidence
			changedProof.Receipt.SourceDigest = strings.Repeat("c", 64)
			f.landing.set(changedProof)
			if err := reopenedRecorder.Record(f.ctx, before.Lifecycle.ID, receipt, before.Revision); err == nil {
				t.Fatal("same late-fact ID accepted a changed trusted proof")
			}
			finalState, err := reopenedStore.Load(f.ctx, before.Lifecycle.ID)
			if err != nil || !reflect.DeepEqual(after, finalState) {
				t.Fatalf("conflicting late fact changed state: err=%v", err)
			}
		})
	}
}

func plantasksEvidenceRejectsMutation(t *testing.T, name string, mutate func(*plantasks.LandedEvidence)) {
	t.Helper()
	f := newPlantasksEvidenceFixture(t)
	receipt := f.landingReceipt(t)
	state := f.load(t)
	evidence := f.evidence(receipt, state.Revision)
	mutate(&evidence)
	// The injected verifier attests the fixture as a trusted host response;
	// field-to-lifecycle matching must still be enforced by the production path.
	f.landing.set(evidence)
	f.outcomes.allow[receipt.ID] = struct{}{}
	f.actors.actor = f.reviewer
	before := f.load(t)
	if err := f.adapter.Result(f.ctx, before.Lifecycle.ID, receipt, f.claimID, before.Revision); err == nil {
		t.Fatalf("%s accepted", name)
	}
	assertPlantasksEvidenceUnchanged(t, f, before)
}

func assertPlantasksEvidenceUnchanged(t *testing.T, f *plantasksEvidenceFixture, before plantasks.State) {
	t.Helper()
	after := f.load(t)
	if !reflect.DeepEqual(before, after) {
		beforeJSON, _ := json.Marshal(before)
		afterJSON, _ := json.Marshal(after)
		t.Fatalf("denied landing changed durable state: before=%s after=%s", beforeJSON, afterJSON)
	}
}
