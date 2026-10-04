package plantasks

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateLandedEvidenceRejectsReviewerFromAnyHistoricalTask(t *testing.T) {
	state := initialState()
	admitTest(t, &state, contractAuthorTaskID, "author-1")
	recordTest(t, &state, contractAuthorTaskID, "author-1", OutcomeCodingHandoff, 1)
	admitTest(t, &state, contractReviewTaskID, "reviewer-1")
	recordTest(t, &state, contractReviewTaskID, "reviewer-1", OutcomeApproved, 2)
	recordTest(t, &state, contractReviewTaskID, "reviewer-1", OutcomeMerged, 3)

	receipt := Receipt{
		Version: VersionV1, ID: "30000000-0000-4000-8000-000000000005", LifecycleID: state.Lifecycle.ID,
		TaskID: contractReviewTaskID, Outcome: OutcomeLanded,
		Actor:          Provenance{ActorID: "reviewer-1", ActorKind: "agent", AuthoredAt: contractNow, SourceRevision: contractSHA},
		PolicyRevision: state.Lifecycle.PolicyRevision, PR: contractValidPR(), MergeCommit: contractSHA,
		LandedCommit: contractSHA, CreatedAt: contractNow,
	}
	evidence := LandedEvidence{
		LifecycleID: state.Lifecycle.ID, TaskID: contractReviewTaskID, ReceiptID: receipt.ID,
		Revision: state.Revision, PRNumber: contractValidPR().Number, MergeCommit: contractSHA,
		Receipt: DeliveryV1LandedReceipt{
			Repository: "https://github.com/sample-org/sample-repo", TargetBranch: "main",
			PRURL: contractValidPR().URL, ReviewedHead: contractSHA, ReviewedBase: contractValidPR().BaseSHA,
			PolicyRevision: state.Lifecycle.PolicyRevision, LandedCommit: contractSHA,
			SourceDigest: strings.Repeat("a", 64), Reviewer: "reviewer-1", Author: "author-1",
			Verifier: "independent-verifier", VerifiedAt: contractNow,
		},
	}
	if err := validateLandedEvidence(state, receipt, evidence, contractNow, false); err != nil {
		t.Fatalf("valid approved and merged baseline rejected: %v", err)
	}

	// This historical task is outside the delivery gate. The gate's author list
	// still correctly contains only the initial author, but its reviewer also
	// appears in another task's persisted authorship history.
	state.Tasks["20000000-0000-4000-8000-000000000003"] = Task{
		Version: VersionV1, ID: "20000000-0000-4000-8000-000000000003", LifecycleID: state.Lifecycle.ID,
		Stage: StageFix, Dependencies: []Dependency{{TaskID: contractReviewTaskID, Kind: DependencyHandoff}},
		Authors:    []Provenance{{ActorID: "reviewer-1", ActorKind: "agent", AuthoredAt: contractNow, SourceRevision: contractSHA}},
		Correction: 1, FindingIDs: []string{contractFindingID}, PR: contractValidPR(), CreatedAt: contractNow,
	}

	if err := validateLandedEvidence(state, receipt, evidence, contractNow, false); !errors.Is(err, ErrReceiptAudit) {
		t.Fatalf("landing with a reviewer in another historical task authorship was accepted: %v", err)
	}
}
