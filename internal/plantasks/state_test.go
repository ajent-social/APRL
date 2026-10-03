package plantasks

import (
	"strings"
	"testing"
	"time"
)

func initialState() State {
	l := contractValidLifecycle()
	l.DeliveryGateID = contractReviewTaskID
	a := Task{Version: VersionV1, ID: contractAuthorTaskID, LifecycleID: l.ID, Stage: StageAuthor, CreatedAt: contractNow}
	r := contractValidReviewTask()
	r.PR = nil
	return State{Lifecycle: l, Tasks: map[string]Task{a.ID: a, r.ID: r}, StartedAt: contractNow}
}

func admitTest(t *testing.T, s *State, id, actor string) {
	t.Helper()
	if err := s.Admit(Admission{TaskID: id, ClaimSHA: contractSHA, ActorID: actor, Revision: s.Revision, ExpiresAt: contractNow.Add(time.Minute)}, contractNow); err != nil {
		t.Fatal(err)
	}
}

func recordTest(t *testing.T, s *State, id, actor string, o Outcome, n int) Receipt {
	t.Helper()
	r := Receipt{Version: VersionV1, ID: "30000000-0000-4000-8000-00000000000" + string(rune('0'+n)), LifecycleID: s.Lifecycle.ID, TaskID: id, Outcome: o, Actor: Provenance{ActorID: actor, ActorKind: "agent", AuthoredAt: contractNow, SourceRevision: contractSHA}, PolicyRevision: s.Lifecycle.PolicyRevision, PR: contractValidPR(), CreatedAt: contractNow}
	if o == OutcomeChangesRequest {
		r.FindingIDs = []string{contractFindingID}
	}
	if o == OutcomeMerged || o == OutcomeLanded {
		r.MergeCommit = contractSHA
	}
	if o == OutcomeLanded {
		r.LandedCommit = contractSHA
	}
	if err := s.Record(r, contractSHA, contractNow); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPlanTaskHandoffAndIndependentDelivery(t *testing.T) {
	s := initialState()
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := s.Eligible(contractReviewTaskID, "reviewer", contractNow); err == nil {
		t.Fatal("review ready before author handoff")
	}
	admitTest(t, &s, contractAuthorTaskID, "author-1")
	recordTest(t, &s, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
	if s.DeliveryReceiptID != "" {
		t.Fatal("coding completion released delivery")
	}
	if err := s.Eligible(contractReviewTaskID, "author-1", contractNow); err == nil {
		t.Fatal("self review admitted")
	}
	admitTest(t, &s, contractReviewTaskID, "reviewer")
	recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeApproved, 2)
	if _, ok := s.Completed[contractReviewTaskID]; ok {
		t.Fatal("approval marked delivery complete")
	}
	recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeMerged, 3)
	if _, ok := s.Completed[contractReviewTaskID]; ok {
		t.Fatal("merge marked delivery verified")
	}
	r := recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeLanded, 4)
	if s.DeliveryReceiptID != r.ID {
		t.Fatal("verified landing absent")
	}
	if err := s.Record(r, contractSHA, contractNow.Add(30*time.Second)); err != nil {
		t.Fatalf("exact historical replay: %v", err)
	}
	r.Detail = "changed"
	if err := s.Record(r, contractSHA, contractNow); err == nil {
		t.Fatal("receipt ID rebound")
	}
	s.DeliveryReceiptID = ""
	if err := s.Validate(); err == nil {
		t.Fatal("delivered gate lost receipt pointer")
	}
}

func TestPlanTaskSnapshotAndCancellationFences(t *testing.T) {
	for _, mode := range []string{"snapshot", "cancel", "expired"} {
		t.Run(mode, func(t *testing.T) {
			s := initialState()
			admitTest(t, &s, contractAuthorTaskID, "author-1")
			recordTest(t, &s, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
			admitTest(t, &s, contractReviewTaskID, "reviewer")
			recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeApproved, 2)
			now := contractNow
			switch mode {
			case "snapshot":
				pr := *contractValidPR()
				pr.BaseSHA = contractSHA
				s.CurrentPR = &pr
			case "cancel":
				s.Cancelled = true
			case "expired":
				now = now.Add(time.Minute)
			}
			r := Receipt{Version: VersionV1, ID: contractReceiptID[:len(contractReceiptID)-1] + "9", LifecycleID: s.Lifecycle.ID, TaskID: contractReviewTaskID, Outcome: OutcomeMerged, Actor: Provenance{ActorID: "reviewer", ActorKind: "agent", AuthoredAt: contractNow, SourceRevision: contractSHA}, PolicyRevision: s.Lifecycle.PolicyRevision, PR: contractValidPR(), MergeCommit: contractSHA, CreatedAt: contractNow}
			if err := s.Record(r, contractSHA, now); err == nil {
				t.Fatal("stale merge accepted")
			}
		})
	}
}

func TestPlanTaskNegativeReviewDoesNotDeliver(t *testing.T) {
	s := initialState()
	admitTest(t, &s, contractAuthorTaskID, "author-1")
	recordTest(t, &s, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
	admitTest(t, &s, contractReviewTaskID, "reviewer")
	recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeChangesRequest, 2)
	if s.DeliveryReceiptID != "" {
		t.Fatal("negative review released delivery")
	}
	fixID, rrID := derivedTaskID(contractReviewTaskID+":fix"), derivedTaskID(contractReviewTaskID+":rereview")
	if _, ok := s.Tasks[fixID]; !ok {
		t.Fatal("negative review did not publish fix")
	}
	if _, ok := s.Tasks[rrID]; !ok {
		t.Fatal("negative review did not publish re-review")
	}
	if err := s.Eligible(fixID, "fixer", contractNow); err != nil {
		t.Fatal(err)
	}
	if err := s.Eligible(rrID, "another-reviewer", contractNow); err == nil {
		t.Fatal("re-review ready before fix")
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPlanTaskCorrectionAdvancesSnapshotAndStableGate(t *testing.T) {
	s := initialState()
	admitTest(t, &s, contractAuthorTaskID, "author-1")
	recordTest(t, &s, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
	admitTest(t, &s, contractReviewTaskID, "reviewer")
	recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeChangesRequest, 2)
	fixID, rrID := derivedTaskID(contractReviewTaskID+":fix"), derivedTaskID(contractReviewTaskID+":rereview")
	admitTest(t, &s, fixID, "fixer")
	pr := *contractValidPR()
	pr.HeadSHA = strings.Repeat("b", 40)
	r := Receipt{Version: VersionV1, ID: "30000000-0000-4000-8000-000000000003", LifecycleID: s.Lifecycle.ID, TaskID: fixID, Outcome: OutcomeCodingHandoff, Actor: Provenance{ActorID: "fixer", ActorKind: "agent", AuthoredAt: contractNow, SourceRevision: pr.HeadSHA}, PolicyRevision: s.Lifecycle.PolicyRevision, PR: &pr, CreatedAt: contractNow}
	if err := s.Record(r, contractSHA, contractNow); err != nil {
		t.Fatal(err)
	}
	if s.Tasks[rrID].PR.HeadSHA != pr.HeadSHA {
		t.Fatal("fix failed to publish review head")
	}
	for _, actor := range []string{"author-1", "fixer"} {
		if err := s.Eligible(rrID, actor, contractNow); err == nil {
			t.Fatalf("author %s self-reviewed", actor)
		}
	}
	stale := s
	oldPR := contractValidPR()
	stale.CurrentPR = oldPR
	if err := stale.Validate(); err == nil {
		t.Fatal("historical head rollback validated")
	}
	admitTest(t, &s, rrID, "reviewer")
	for n, outcome := range []Outcome{OutcomeApproved, OutcomeMerged, OutcomeLanded} {
		r.ID = "30000000-0000-4000-8000-00000000000" + string(rune('4'+n))
		r.TaskID = rrID
		r.Actor.ActorID = "reviewer"
		r.Outcome = outcome
		if outcome == OutcomeMerged || outcome == OutcomeLanded {
			r.MergeCommit = contractSHA
		}
		if outcome == OutcomeLanded {
			r.LandedCommit = contractSHA
		}
		if err := s.Record(r, contractSHA, contractNow); err != nil {
			t.Fatal(err)
		}
	}
	if s.DeliveryReceiptID != r.ID {
		t.Fatal("corrective landing did not satisfy stable gate")
	}
	if s.Completed[contractReviewTaskID] != OutcomeChangesRequest {
		t.Fatal("negative historical review was rewritten")
	}
}

func TestPlanTaskCorrectionExhaustionEscalates(t *testing.T) {
	s := initialState()
	s.Lifecycle.Limits.MaxCorrections = 0
	admitTest(t, &s, contractAuthorTaskID, "author-1")
	recordTest(t, &s, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
	admitTest(t, &s, contractReviewTaskID, "reviewer")
	recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeChangesRequest, 2)
	if s.EscalationReason == "" || len(s.Tasks) != 2 || s.DeliveryReceiptID != "" {
		t.Fatal("exhausted correction expanded or delivered")
	}
}

func TestPlanTaskAssignedActorAndCancel(t *testing.T) {
	s := initialState()
	task := s.Tasks[contractAuthorTaskID]
	task.Actor = Provenance{ActorID: "author-1", ActorKind: "agent", AuthoredAt: contractNow}
	s.Tasks[task.ID] = task
	if err := s.Eligible(task.ID, "other", contractNow); err == nil {
		t.Fatal("assigned actor ignored")
	}
	admitTest(t, &s, task.ID, "author-1")
	r := Receipt{Version: VersionV1, ID: "30000000-0000-4000-8000-000000000009", LifecycleID: s.Lifecycle.ID, TaskID: task.ID, Outcome: OutcomeCancel, Actor: task.Actor, PolicyRevision: s.Lifecycle.PolicyRevision, CreatedAt: contractNow}
	if err := s.Record(r, contractSHA, contractNow); err != nil {
		t.Fatal(err)
	}
	if !s.Cancelled {
		t.Fatal("cancel receipt left lifecycle open")
	}
	if s.DeliveryReceiptID != "" {
		t.Fatal("cancel released delivery")
	}
	if err := s.Eligible(task.ID, "author-1", contractNow); err == nil {
		t.Fatal("cancelled task remained eligible")
	}
}

func TestPlanTaskAdmissionEnvelope(t *testing.T) {
	s := initialState()
	s.Lifecycle.Limits.MaxAttempts = 1
	claim := Admission{TaskID: contractAuthorTaskID, ClaimSHA: contractSHA, ActorID: "author-1", Revision: s.Revision, ExpiresAt: contractNow.Add(time.Minute)}
	deadline := s.StartedAt.Add(time.Duration(s.Lifecycle.Limits.MaxDurationSeconds) * time.Second)
	beyond := claim
	beyond.ExpiresAt = deadline.Add(time.Second)
	if err := s.Admit(beyond, contractNow); err == nil {
		t.Fatal("admission exceeded lifecycle deadline")
	}
	if err := s.Admit(claim, contractNow); err != nil {
		t.Fatal(err)
	}
	retry := claim
	retry.ExpiresAt = contractNow.Add(3 * time.Minute)
	if err := s.Admit(retry, contractNow.Add(2*time.Minute)); err == nil {
		t.Fatal("exhausted task attempt envelope reopened")
	}
	if err := s.Eligible(contractAuthorTaskID, "author-1", deadline); err == nil {
		t.Fatal("expired lifecycle remained eligible")
	}
	if s.Attempts[contractAuthorTaskID] != 1 {
		t.Fatal("rejected admission changed attempt count")
	}
}

func TestPlanTaskPersistedCorrectionGraphFences(t *testing.T) {
	s := initialState()
	admitTest(t, &s, contractAuthorTaskID, "author-1")
	recordTest(t, &s, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
	admitTest(t, &s, contractReviewTaskID, "reviewer")
	recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeChangesRequest, 2)
	fixID := derivedTaskID(contractReviewTaskID + ":fix")
	original := s.Tasks[fixID]
	delete(s.Tasks, fixID)
	if err := s.Validate(); err == nil {
		t.Fatal("missing correction accepted")
	}
	s.Tasks[fixID] = original
	extra := original
	extra.ID = "40000000-0000-4000-8000-000000000099"
	s.Tasks[extra.ID] = extra
	if err := s.Validate(); err == nil {
		t.Fatal("duplicate noncanonical correction accepted")
	}
	delete(s.Tasks, extra.ID)
	for id, r := range s.Receipts {
		if r.Outcome == OutcomeChangesRequest {
			r.Actor.ActorID = "author-1"
			s.Receipts[id] = r
		}
	}
	if err := s.Validate(); err == nil {
		t.Fatal("persisted self verdict accepted")
	}
}

func TestPlanTaskReplayCannotAdvanceCancelledOrExpiredLifecycle(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		s := initialState()
		admitTest(t, &s, contractAuthorTaskID, "author-1")
		recordTest(t, &s, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
		var receipt Receipt
		for _, r := range s.Receipts {
			receipt = r
		}
		now := s.StartedAt.Add(time.Duration(s.Lifecycle.Limits.MaxDurationSeconds) * time.Second)
		if cancelled {
			s.Cancelled = true
			now = contractNow
		}
		if err := s.Record(receipt, contractSHA, now); err == nil {
			t.Fatal("late historical success was acknowledged")
		}
	}
}

func TestPlanTaskParallelContributorsCannotReview(t *testing.T) {
	s := initialState()
	task := s.Tasks[contractAuthorTaskID]
	contributor := Provenance{ActorID: "parallel-author", ActorKind: "agent", AuthoredAt: contractNow, SourceRevision: contractSHA}
	task.Authors = []Provenance{contributor}
	s.Tasks[task.ID] = task
	admitTest(t, &s, task.ID, "author-1")
	recordTest(t, &s, task.ID, "author-1", OutcomeCodingHandoff, 1)
	review := s.Tasks[contractReviewTaskID]
	if !containsAuthor(review.Authors, contributor) {
		t.Fatal("parallel contributor lost at handoff")
	}
	if err := s.Eligible(review.ID, contributor.ActorID, contractNow); err == nil {
		t.Fatal("parallel contributor admitted for review")
	}
	review.Authors = nil
	s.Tasks[review.ID] = review
	if err := s.Validate(); err == nil {
		t.Fatal("persisted review omitted contributors")
	}
}

func TestPlanTaskReviewerCanFixButCannotReviewTheirCorrection(t *testing.T) {
	s := initialState()
	admitTest(t, &s, contractAuthorTaskID, "author-1")
	recordTest(t, &s, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
	admitTest(t, &s, contractReviewTaskID, "reviewer")
	recordTest(t, &s, contractReviewTaskID, "reviewer", OutcomeChangesRequest, 2)
	fixID := derivedTaskID(contractReviewTaskID + ":fix")
	admitTest(t, &s, fixID, "reviewer")
	newPR := *contractValidPR()
	newPR.HeadSHA = strings.Repeat("c", 40)
	receipt := Receipt{Version: VersionV1, ID: "30000000-0000-4000-8000-000000000003", LifecycleID: s.Lifecycle.ID, TaskID: fixID, Outcome: OutcomeCodingHandoff, Actor: Provenance{ActorID: "reviewer", ActorKind: "agent", AuthoredAt: contractNow, SourceRevision: newPR.HeadSHA}, PolicyRevision: s.Lifecycle.PolicyRevision, PR: &newPR, CreatedAt: contractNow}
	if err := s.Record(receipt, contractSHA, contractNow); err != nil {
		t.Fatal(err)
	}
	reviewID := derivedTaskID(contractReviewTaskID + ":rereview")
	if err := s.Eligible(reviewID, "reviewer", contractNow); err == nil {
		t.Fatal("fix author admitted to rereview")
	}
	if err := s.Eligible(reviewID, "independent-rereviewer", contractNow); err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("valid prior review history invalidated by later fix: %v", err)
	}
}
