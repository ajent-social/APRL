package plantasks

import (
	"strings"
	"testing"
	"time"
)

var (
	contractLifecycleID  = "10000000-0000-4000-8000-000000000001"
	contractGateID       = "20000000-0000-4000-8000-000000000002"
	contractAuthorTaskID = "20000000-0000-4000-8000-000000000001"
	contractReviewTaskID = "20000000-0000-4000-8000-000000000002"
	contractReceiptID    = "30000000-0000-4000-8000-000000000001"
	contractFindingID    = "40000000-0000-4000-8000-000000000001"
	contractSHA          = "0123456789abcdef0123456789abcdef01234567"
	contractOtherSHA     = "abcdef0123456789abcdef0123456789abcdef01"
	contractNow          = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
)

func contractValidLifecycle() Lifecycle {
	return Lifecycle{
		Version: VersionV1, ID: contractLifecycleID, DeliveryGateID: contractGateID,
		Repository:     Repository{Owner: "sample-org", Name: "sample-repo", Target: "main"},
		PolicyRevision: "policy-r7",
		Authored:       Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: contractNow, SourceRevision: contractSHA},
		Limits:         CorrectionLimits{MaxCorrections: 3, MaxAttempts: 8, MaxDurationSeconds: 3600, MaxConcurrentTasks: 2},
		CreatedAt:      contractNow,
	}
}

func contractValidPR() *PRBinding {
	return &PRBinding{Number: 17, URL: "https://github.com/sample-org/sample-repo/pull/17", HeadSHA: contractSHA,
		BaseSHA: "abcdef0123456789abcdef0123456789abcdef01", PolicyRevision: "policy-r7"}
}

func contractValidReviewTask() Task {
	return Task{Version: VersionV1, ID: contractReviewTaskID, LifecycleID: contractLifecycleID, Stage: StageReview,
		Dependencies: []Dependency{{TaskID: contractAuthorTaskID, Kind: DependencyHandoff}}, PR: contractValidPR(), CreatedAt: contractNow}
}

func TestValidateTaskRequiresExactHandoffBinding(t *testing.T) {
	lifecycle := contractValidLifecycle()
	tests := []struct {
		name string
		task Task
		ok   bool
	}{
		{"author starts without a PR", Task{Version: VersionV1, ID: contractAuthorTaskID, LifecycleID: contractLifecycleID, Stage: StageAuthor, CreatedAt: contractNow}, true},
		{"review requires handoff", contractValidReviewTask(), true},
		{"pending review awaits PR handoff", Task{Version: VersionV1, ID: contractReviewTaskID, LifecycleID: contractLifecycleID, Stage: StageReview, Dependencies: []Dependency{{TaskID: contractAuthorTaskID, Kind: DependencyHandoff}}, CreatedAt: contractNow}, true},
		{"non-gate review is rejected", func() Task {
			task := contractValidReviewTask()
			task.ID = "20000000-0000-4000-8000-000000000003"
			return task
		}(), false},
		{"review correction must use re-review stage", func() Task { task := contractValidReviewTask(); task.Correction = 1; return task }(), false},
		{"fix requires finding and correction", Task{Version: VersionV1, ID: contractReviewTaskID, LifecycleID: contractLifecycleID, Stage: StageFix, Correction: 1, FindingIDs: []string{contractFindingID}, Dependencies: []Dependency{{TaskID: contractAuthorTaskID, Kind: DependencyHandoff}}, PR: contractValidPR(), CreatedAt: contractNow}, true},
		{"correction limit is enforced", Task{Version: VersionV1, ID: contractReviewTaskID, LifecycleID: contractLifecycleID, Stage: StageFix, Correction: 4, FindingIDs: []string{contractFindingID}, Dependencies: []Dependency{{TaskID: contractAuthorTaskID, Kind: DependencyHandoff}}, PR: contractValidPR(), CreatedAt: contractNow}, false},
		{"repository binding is enforced", func() Task {
			task := contractValidReviewTask()
			task.PR.URL = "https://github.com/other/repo/pull/17"
			return task
		}(), false},
		{"policy binding is enforced", func() Task { task := contractValidReviewTask(); task.PR.PolicyRevision = "policy-old"; return task }(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTask(lifecycle, tt.task)
			if (err == nil) != tt.ok {
				t.Fatalf("ValidateTask() error = %v, want success %v", err, tt.ok)
			}
		})
	}
}

func TestValidateReceiptKeepsOutcomesDistinct(t *testing.T) {
	lifecycle := contractValidLifecycle()
	task := contractValidReviewTask()
	base := Receipt{Version: VersionV1, ID: contractReceiptID, LifecycleID: contractLifecycleID, TaskID: contractReviewTaskID,
		Actor:          Provenance{ActorID: "reviewer-1", ActorKind: "human", AuthoredAt: contractNow, SourceRevision: contractSHA},
		PolicyRevision: "policy-r7", PR: contractValidPR(), CreatedAt: contractNow}
	tests := []struct {
		name    string
		receipt Receipt
		ok      bool
	}{
		{"approved exact snapshot", func() Receipt { r := base; r.Outcome = OutcomeApproved; return r }(), true},
		{"approved source must equal exact head", func() Receipt {
			r := base
			r.Outcome = OutcomeApproved
			r.Actor.SourceRevision = contractOtherSHA
			return r
		}(), false},
		{"changes requested binds findings", func() Receipt {
			r := base
			r.Outcome = OutcomeChangesRequest
			r.FindingIDs = []string{contractFindingID}
			return r
		}(), true},
		{"changes requested without findings rejected", func() Receipt { r := base; r.Outcome = OutcomeChangesRequest; return r }(), false},
		{"findings on approval rejected", func() Receipt {
			r := base
			r.Outcome = OutcomeApproved
			r.FindingIDs = []string{contractFindingID}
			return r
		}(), false},
		{"merged requires commit", func() Receipt { r := base; r.Outcome = OutcomeMerged; return r }(), false},
		{"merged is distinct", func() Receipt { r := base; r.Outcome = OutcomeMerged; r.MergeCommit = contractSHA; return r }(), true},
		{"landed binds both commits", func() Receipt {
			r := base
			r.Outcome = OutcomeLanded
			r.MergeCommit = contractSHA
			r.LandedCommit = contractSHA
			return r
		}(), true},
		{"unknown cannot be success", func() Receipt { r := base; r.Outcome = OutcomeUnknown; r.MergeCommit = contractSHA; return r }(), false},
		{"execution failure remains distinct", func() Receipt { r := base; r.Outcome = OutcomeFailed; r.PR = nil; return r }(), true},
		{"cancel may observe late landing", func() Receipt {
			r := base
			r.Outcome = OutcomeCancel
			r.ObservedOutcome = OutcomeLanded
			r.PR = contractValidPR()
			r.MergeCommit = contractSHA
			r.LandedCommit = contractSHA
			return r
		}(), true},
		{"late landing source must equal observed head", func() Receipt {
			r := base
			r.Outcome = OutcomeCancel
			r.ObservedOutcome = OutcomeLanded
			r.MergeCommit = contractSHA
			r.LandedCommit = contractSHA
			r.Actor.SourceRevision = contractOtherSHA
			return r
		}(), false},
		{"cancel observation requires bound evidence", func() Receipt {
			r := base
			r.Outcome = OutcomeCancel
			r.ObservedOutcome = OutcomeLanded
			r.PR = nil
			r.MergeCommit = contractSHA
			r.LandedCommit = contractSHA
			return r
		}(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateReceipt(lifecycle, task, tt.receipt)
			if (err == nil) != tt.ok {
				t.Fatalf("ValidateReceipt() error = %v, want success %v", err, tt.ok)
			}
		})
	}
}

func TestReceiptDetailIsBounded(t *testing.T) {
	receipt := Receipt{Version: VersionV1, ID: contractReceiptID, LifecycleID: contractLifecycleID, TaskID: contractReviewTaskID,
		Outcome: OutcomeFailed, Actor: Provenance{ActorID: "reviewer-1", ActorKind: "human", AuthoredAt: contractNow},
		PolicyRevision: "policy-r7", CreatedAt: contractNow, Detail: strings.Repeat("x", maxReceiptDetailBytes)}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("receipt at detail limit rejected: %v", err)
	}
	receipt.Detail += "x"
	if err := receipt.Validate(); err == nil {
		t.Fatal("receipt detail over 4096 bytes accepted")
	}
}

func TestFixHandoffMayAdvanceCommitOnSamePullRequest(t *testing.T) {
	lifecycle := contractValidLifecycle()
	pr := contractValidPR()
	task := Task{Version: VersionV1, ID: contractReviewTaskID, LifecycleID: contractLifecycleID, Stage: StageFix,
		Dependencies: []Dependency{{TaskID: contractAuthorTaskID, Kind: DependencyHandoff}}, Correction: 1,
		FindingIDs: []string{contractFindingID}, PR: pr, CreatedAt: contractNow}
	receipt := Receipt{Version: VersionV1, ID: contractReceiptID, LifecycleID: contractLifecycleID, TaskID: contractReviewTaskID,
		Outcome: OutcomeCodingHandoff, Actor: Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: contractNow, SourceRevision: contractOtherSHA},
		PolicyRevision: "policy-r7", PR: func() *PRBinding {
			updated := *pr
			updated.HeadSHA = contractOtherSHA
			return &updated
		}(), CreatedAt: contractNow}
	if err := ValidateReceipt(lifecycle, task, receipt); err != nil {
		t.Fatalf("same PR with updated fix revision rejected: %v", err)
	}
	receipt.Actor.SourceRevision = contractSHA
	if err := ValidateReceipt(lifecycle, task, receipt); err == nil {
		t.Fatal("fix handoff with mismatching valid source revision accepted")
	}
	receipt.Actor.SourceRevision = contractOtherSHA
	receipt.PR.Number++
	if err := ValidateReceipt(lifecycle, task, receipt); err == nil {
		t.Fatal("fix handoff with a different PR identity accepted")
	}
}

func TestStrictContractRejectsUnsupportedVersionsAndDependencyKinds(t *testing.T) {
	lifecycle := contractValidLifecycle()
	if err := lifecycle.Validate(); err != nil {
		t.Fatalf("valid lifecycle rejected: %v", err)
	}
	lifecycle.Version++
	if err := lifecycle.Validate(); err == nil {
		t.Fatal("unsupported lifecycle version accepted")
	}

	task := contractValidReviewTask()
	task.Dependencies[0].Kind = "ready"
	if err := task.Validate(); err == nil {
		t.Fatal("unknown dependency kind accepted")
	}
}
