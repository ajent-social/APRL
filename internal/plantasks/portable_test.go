package plantasks

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	portableTestLifecycleID = "11111111-1111-4111-8111-111111111111"
	portableTestAuthorID    = "22222222-2222-4222-8222-222222222222"
	portableTestGateID      = "33333333-3333-4333-8333-333333333333"
	portableTestSourceSHA   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	portableTestBaseSHA     = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	portableTestFixSHA      = "cccccccccccccccccccccccccccccccccccccccc"
)

var portableTestStart = time.Date(2026, time.October, 4, 12, 0, 0, 0, time.UTC)

func portableTestState(t *testing.T) State {
	t.Helper()
	author := Provenance{ActorID: "agent:author-a", ActorKind: "agent", AuthoredAt: portableTestStart, SourceRevision: portableTestSourceSHA}
	coauthor := Provenance{ActorID: "agent:author-b", ActorKind: "agent", AuthoredAt: portableTestStart, SourceRevision: portableTestSourceSHA}
	state := State{
		Lifecycle: Lifecycle{
			Version: VersionV1, ID: portableTestLifecycleID, DeliveryGateID: portableTestGateID,
			Repository:     Repository{Owner: "example", Name: "portable-project", Target: "main"},
			PolicyRevision: "policy:portable-test-v1", Authored: author,
			Limits:    CorrectionLimits{MaxCorrections: 1, MaxAttempts: 8, MaxDurationSeconds: 3600, MaxConcurrentTasks: 2},
			CreatedAt: portableTestStart,
		},
		Tasks: map[string]Task{
			portableTestAuthorID: {
				Version: VersionV1, ID: portableTestAuthorID, LifecycleID: portableTestLifecycleID,
				Stage: StageAuthor, Authors: []Provenance{author, coauthor}, CreatedAt: portableTestStart,
			},
			portableTestGateID: {
				Version: VersionV1, ID: portableTestGateID, LifecycleID: portableTestLifecycleID,
				Stage: StageReview, Dependencies: []Dependency{{TaskID: portableTestAuthorID, Kind: DependencyHandoff}},
				CreatedAt: portableTestStart,
			},
		},
		Receipts: map[string]Receipt{}, Completed: map[string]Outcome{}, Claims: map[string]Admission{},
		Attempts: map[string]int{}, StartedAt: portableTestStart, Revision: 0,
	}
	if err := state.ValidateAt(portableTestStart); err != nil {
		t.Fatalf("construct canonical portable lifecycle fixture: %v", err)
	}
	return state
}

func portableTestAuthoredSource(state State) PortableAuthoredSource {
	raw := []byte("# APRL portable adapter fixture\n\n## Portable parent task\n\n- [ ] Portable parent task: Preserve reviewed delivery authority\n")
	return PortableAuthoredSource{
		PlanTitle: "APRL portable adapter fixture", TaskTitle: "Portable parent task",
		Stage: "author", AuthoredStatus: "pending", Acceptance: "Preserve reviewed delivery authority",
		SourceRef:      "https://github.com/example/portable-project/blob/" + portableTestSourceSHA + "/docs/plan.md",
		SourceRevision: state.Lifecycle.Authored.SourceRevision, SourceBytes: raw,
		TaskRaw: "- [ ] Portable parent task: Preserve reviewed delivery authority",
	}
}

func portableTestApply(t *testing.T, state *State, taskID string, actor Provenance, outcome Outcome, pr *PRBinding, findings []string, at time.Time) Receipt {
	t.Helper()
	claim := strings.Repeat("f", 40)
	deadline := state.StartedAt.Add(time.Duration(state.Lifecycle.Limits.MaxDurationSeconds) * time.Second)
	expires := at.Add(5 * time.Minute)
	if expires.After(deadline) {
		expires = deadline
	}
	if existing, ok := state.Claims[taskID]; ok {
		if existing.ActorID != actor.ActorID || !at.Before(existing.ExpiresAt) {
			t.Fatalf("canonical claim for %s is not reusable by %s at %s: %+v", taskID, actor.ActorID, at, existing)
		}
		claim = existing.ClaimSHA
	} else if err := state.Admit(Admission{TaskID: taskID, ClaimSHA: claim, ActorID: actor.ActorID, Revision: state.Revision, ExpiresAt: expires}, at); err != nil {
		t.Fatalf("canonical admission for %s: %v", taskID, err)
	}
	receiptID := portableTestReceiptID(len(state.Receipts) + 1)
	receipt := Receipt{Version: VersionV1, ID: receiptID, LifecycleID: state.Lifecycle.ID, TaskID: taskID,
		Outcome: outcome, Actor: actor, PolicyRevision: state.Lifecycle.PolicyRevision, PR: pr,
		FindingIDs: append([]string(nil), findings...), CreatedAt: at.UTC(), Detail: "portable test transition"}
	if outcome == OutcomeMerged || outcome == OutcomeLanded {
		receipt.MergeCommit = strings.Repeat("d", 40)
	}
	if outcome == OutcomeLanded {
		receipt.LandedCommit = receipt.MergeCommit
	}
	if err := state.Record(receipt, claim, at); err != nil {
		t.Fatalf("canonical %s transition for %s: %v", outcome, taskID, err)
	}
	state.Revision++
	projectionTime := at.UTC()
	state.ProjectionTime = &projectionTime
	if err := state.ValidateAt(at); err != nil {
		t.Fatalf("validate canonical state after %s: %v", outcome, err)
	}
	return receipt
}

func portableTestReceiptID(n int) string {
	return fmt.Sprintf("%08x-4444-4444-8444-%012x", n+1, n+1)
}

func portableTestPR(head string) *PRBinding {
	return &PRBinding{Number: 7, URL: "https://github.com/example/portable-project/pull/7", HeadSHA: head, BaseSHA: portableTestBaseSHA, PolicyRevision: "policy:portable-test-v1"}
}

func portableTestDocument(t *testing.T, bundle PortableBundle) map[string]any {
	t.Helper()
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal portable bundle: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode portable bundle: %v", err)
	}
	return document
}

func TestProjectPortableUsesCanonicalLifecycleWithoutInventingAuthoredChildren(t *testing.T) {
	state := portableTestState(t)
	bundle, err := ProjectPortable(state, portableTestAuthoredSource(state))
	if err != nil {
		t.Fatalf("project initial canonical lifecycle: %v", err)
	}
	document := portableTestDocument(t, bundle)
	definition, ok := document["definition"].(map[string]any)
	if !ok || definition["title"] != "APRL portable adapter fixture" {
		t.Fatalf("portable definition lost authored plan title: %#v", document["definition"])
	}
	tasks, ok := definition["tasks"].([]any)
	if !ok || len(tasks) != 1 {
		t.Fatalf("portable definition fabricated or omitted authored tasks: %#v", definition["tasks"])
	}
	task, ok := tasks[0].(map[string]any)
	if !ok || task["title"] != "Portable parent task" || task["stage"] != "author" || task["acceptance"] != "Preserve reviewed delivery authority" {
		t.Fatalf("portable task did not preserve authored fields: %#v", tasks[0])
	}
	units, ok := definition["executionUnits"].([]any)
	if !ok || len(units) != 1 {
		t.Fatalf("native lifecycle must project as one compound execution unit: %#v", definition["executionUnits"])
	}
	if evidence, ok := document["evidence"].([]any); !ok || len(evidence) != 0 {
		t.Fatalf("projection invented evidence or omitted an empty evidence list: %#v", document["evidence"])
	}
	if evaluations, ok := document["evaluations"].([]any); !ok || len(evaluations) != 0 {
		t.Fatalf("projection invented portable qualifications: %#v", document["evaluations"])
	}
	if document["contractVersion"] != "0.0.1" {
		t.Fatalf("portable contract version=%v, want 0.0.1", document["contractVersion"])
	}
}

func TestProjectPortableCoreSourceIdentityBindsExactCallerBytes(t *testing.T) {
	state := portableTestState(t)
	firstInput := portableTestAuthoredSource(state)
	first, err := ProjectPortable(state, firstInput)
	if err != nil {
		t.Fatalf("project original authored source bytes: %v", err)
	}
	secondInput := firstInput
	secondInput.SourceBytes = append(append([]byte(nil), firstInput.SourceBytes...), []byte("\n<!-- source-only change -->\n")...)
	second, err := ProjectPortable(state, secondInput)
	if err != nil {
		t.Fatalf("project altered authored source bytes: %v", err)
	}
	identity := func(input PortableAuthoredSource, bundle PortableBundle) string {
		t.Helper()
		sum := sha256.Sum256(input.SourceBytes)
		digest := "sha256:" + hex.EncodeToString(sum[:])
		wantRef := "urn:aprl:caller-source:" + digest
		if bundle.Definition.Source.Ref != wantRef || bundle.Definition.Source.Revision != digest || bundle.Definition.Source.Digest != digest {
			t.Fatalf("portable core source identity is not content-addressed: source=%+v want ref=%q revision/digest=%q", bundle.Definition.Source, wantRef, digest)
		}
		if bundle.Definition.Revision != digest || bundle.Definition.Digest != digest || bundle.Definition.Tasks[0].Source.Ref != wantRef {
			t.Fatalf("portable plan/task identity does not bind exact source bytes: definition=%+v taskSource=%+v", bundle.Definition, bundle.Definition.Tasks[0].Source)
		}
		document := portableTestDocument(t, bundle)
		definition := document["definition"].(map[string]any)
		metadata := definition["metadata"].(map[string]any)["aprl"].(map[string]any)
		caller := metadata["callerSource"].(map[string]any)
		if caller["classification"] != "caller-supplied-unverified" || caller["claimedRef"] != input.SourceRef || caller["claimedRevision"] != input.SourceRevision || caller["digest"] != digest {
			t.Fatalf("unverified caller claims were not retained separately from portable core identity: %#v", caller)
		}
		if metadata["lifecycleAuthorSourceRevision"] != state.Lifecycle.Authored.SourceRevision {
			t.Fatalf("native lifecycle code provenance was lost or confused with caller source metadata: %#v", metadata)
		}
		return digest
	}
	firstDigest := identity(firstInput, first)
	secondDigest := identity(secondInput, second)
	if firstDigest == secondDigest || first.Definition.ID != second.Definition.ID {
		t.Fatalf("altered exact bytes must change content identity while preserving native lifecycle identity: first=%q/%q second=%q/%q", first.Definition.ID, firstDigest, second.Definition.ID, secondDigest)
	}
}

func TestProjectPortableMatchesCanonicalGolden(t *testing.T) {
	state := portableTestState(t)
	bundle, err := ProjectPortable(state, portableTestAuthoredSource(state))
	if err != nil {
		t.Fatalf("project initial canonical lifecycle: %v", err)
	}
	actual, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		t.Fatalf("marshal canonical portable golden: %v", err)
	}
	actual = append(actual, '\n')
	goldenPath := filepath.Join("..", "..", "tests", "portableplan", "aprl-lifecycle.json")
	if os.Getenv("UPDATE_PORTABLE_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatalf("create portable golden directory: %v", err)
		}
		if err := os.WriteFile(goldenPath, actual, 0o644); err != nil {
			t.Fatalf("write canonical portable golden: %v", err)
		}
	}
	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read canonical portable golden %s: %v\nactual projection:\n%s", goldenPath, err, actual)
	}
	if string(actual) != string(expected) {
		t.Fatalf("portable projection differs from canonical golden %s\nactual projection:\n%s", goldenPath, actual)
	}
}

func TestProjectPortablePreservesStableGateAndEveryContributorAcrossCorrection(t *testing.T) {
	state := portableTestState(t)
	initial := portableTestAuthoredSource(state)
	author := state.Tasks[portableTestAuthorID].Authors[0]
	initialPR := portableTestPR(portableTestSourceSHA)
	portableTestApply(t, &state, portableTestAuthorID, author, OutcomeCodingHandoff, initialPR, nil, portableTestStart.Add(time.Minute))
	reviewer := Provenance{ActorID: "agent:reviewer", ActorKind: "agent", AuthoredAt: portableTestStart.Add(2 * time.Minute), SourceRevision: portableTestSourceSHA}
	findings := []string{"44444444-4444-4444-8444-444444444444"}
	portableTestApply(t, &state, portableTestGateID, reviewer, OutcomeChangesRequest, initialPR, findings, portableTestStart.Add(2*time.Minute))
	if state.Completed[portableTestGateID] != OutcomeChangesRequest {
		t.Fatalf("fixture did not retain negative review as an execution outcome: %v", state.Completed)
	}
	fixTaskID, rereviewTaskID := portableCorrectionTaskIDs(t, state)
	fixer := Provenance{ActorID: "agent:fixer", ActorKind: "agent", AuthoredAt: portableTestStart.Add(3 * time.Minute), SourceRevision: portableTestFixSHA}
	correctedPR := portableTestPR(portableTestFixSHA)
	portableTestApply(t, &state, fixTaskID, fixer, OutcomeCodingHandoff, correctedPR, nil, portableTestStart.Add(3*time.Minute))

	bundle, err := ProjectPortable(state, initial)
	if err != nil {
		t.Fatalf("project corrected canonical lifecycle: %v", err)
	}
	document := portableTestDocument(t, bundle)
	definition := document["definition"].(map[string]any)
	if tasks := definition["tasks"].([]any); len(tasks) != 1 {
		t.Fatalf("canonical fix/re-review children became authored tasks: %#v", tasks)
	}
	if evidence, ok := document["evidence"].([]any); !ok || len(evidence) != 0 {
		t.Fatalf("native transitions must not become unverified portable evidence: %#v", document["evidence"])
	}
	if evaluations, ok := document["evaluations"].([]any); !ok || len(evaluations) != 0 {
		t.Fatalf("canonical execution outcomes must not become qualifications: %#v", document["evaluations"])
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("marshal portable correction projection: %v", err)
	}
	for _, value := range []string{portableTestGateID, fixTaskID, rereviewTaskID, author.ActorID, "agent:author-b", fixer.ActorID, correctedPR.HeadSHA, correctedPR.BaseSHA} {
		if !strings.Contains(string(encoded), value) {
			t.Errorf("namespaced native metadata omitted %q", value)
		}
	}
	if strings.Contains(string(encoded), "claim_sha") || strings.Contains(string(encoded), strings.Repeat("f", 40)) {
		t.Fatal("portable projection leaked host claim credentials")
	}
}

func TestProjectPortableKeepsUnknownAndCancellationOutcomesNonSuccess(t *testing.T) {
	t.Run("unknown retains unknown state", func(t *testing.T) {
		state := portableTestState(t)
		author := state.Tasks[portableTestAuthorID].Authors[0]
		portableTestApply(t, &state, portableTestAuthorID, author, OutcomeUnknown, nil, nil, portableTestStart.Add(time.Minute))
		bundle, err := ProjectPortable(state, portableTestAuthoredSource(state))
		if err != nil {
			t.Fatalf("project unknown canonical lifecycle: %v", err)
		}
		if got := bundle.Snapshot.Executions[0].State; got != "unknown" {
			t.Fatalf("unknown lifecycle projected as %q", got)
		}
		if len(bundle.Evidence) != 0 || len(bundle.Evaluations) != 0 || len(bundle.Definition.Requirements) != 0 {
			t.Fatalf("unknown receipt was promoted to portable proof: %+v", bundle)
		}
	})

	t.Run("cancel remains canceled", func(t *testing.T) {
		state := portableTestState(t)
		author := state.Tasks[portableTestAuthorID].Authors[0]
		portableTestApply(t, &state, portableTestAuthorID, author, OutcomeCodingHandoff, portableTestPR(portableTestSourceSHA), nil, portableTestStart.Add(time.Minute))
		operator := Provenance{ActorID: "agent:operator", ActorKind: "operator", AuthoredAt: portableTestStart.Add(2 * time.Minute), SourceRevision: portableTestSourceSHA}
		portableTestApply(t, &state, portableTestGateID, operator, OutcomeCancel, nil, nil, portableTestStart.Add(2*time.Minute))
		bundle, err := ProjectPortable(state, portableTestAuthoredSource(state))
		if err != nil {
			t.Fatalf("project canceled canonical lifecycle: %v", err)
		}
		if got := bundle.Snapshot.Executions[0].State; got != "canceled" {
			t.Fatalf("canceled lifecycle projected as %q", got)
		}
		if len(bundle.Evidence) != 0 || len(bundle.Evaluations) != 0 {
			t.Fatalf("cancel outcome was promoted to portable proof: %+v", bundle)
		}
	})
}

func TestProjectPortableLateLandingIsAuditOnlyAndExpiryRemainsVisible(t *testing.T) {
	state := portableTestState(t)
	author := state.Tasks[portableTestAuthorID].Authors[0]
	pr := portableTestPR(portableTestSourceSHA)
	portableTestApply(t, &state, portableTestAuthorID, author, OutcomeCodingHandoff, pr, nil, portableTestStart.Add(time.Minute))
	reviewer := Provenance{ActorID: "agent:reviewer", ActorKind: "agent", AuthoredAt: portableTestStart.Add(2 * time.Minute), SourceRevision: portableTestSourceSHA}
	portableTestApply(t, &state, portableTestGateID, reviewer, OutcomeApproved, pr, nil, portableTestStart.Add(2*time.Minute))
	portableTestApply(t, &state, portableTestGateID, reviewer, OutcomeMerged, pr, nil, portableTestStart.Add(3*time.Minute))
	merge := strings.Repeat("d", 40)
	receipt := Receipt{
		Version: VersionV1, ID: portableTestReceiptID(4), LifecycleID: state.Lifecycle.ID,
		TaskID: portableTestGateID, Outcome: OutcomeLanded, Actor: reviewer,
		PolicyRevision: state.Lifecycle.PolicyRevision, PR: pr, MergeCommit: merge, LandedCommit: merge,
		CreatedAt: portableTestStart.Add(61 * time.Minute),
	}
	verifiedAt := portableTestStart.Add(61 * time.Minute)
	evidence := LandedEvidence{
		LifecycleID: state.Lifecycle.ID, TaskID: portableTestGateID, ReceiptID: receipt.ID,
		Revision: state.Revision, PRNumber: pr.Number, MergeCommit: merge,
		Receipt: DeliveryV1LandedReceipt{
			Repository: "https://github.com/example/portable-project", TargetBranch: "main", PRURL: pr.URL,
			ReviewedHead: pr.HeadSHA, ReviewedBase: pr.BaseSHA, PolicyRevision: state.Lifecycle.PolicyRevision,
			LandedCommit: merge, SourceDigest: strings.Repeat("a", 64), Reviewer: reviewer.ActorID,
			Author: author.ActorID, Verifier: "host:portable-test", VerifiedAt: verifiedAt,
		},
	}
	receipt.LandedEvidence = &evidence
	factAt := portableTestStart.Add(62 * time.Minute)
	state.LateLandedFacts = map[string]LateLandingFact{
		receipt.ID: {ID: receipt.ID, HostActor: "host:portable-test", Receipt: receipt, ObservedAt: factAt},
	}
	state.Revision++
	state.ProjectionTime = &factAt
	if err := state.ValidateAt(factAt); err != nil {
		t.Fatalf("late audit fixture is not a valid persisted canonical state: %v", err)
	}
	bundle, err := ProjectPortable(state, portableTestAuthoredSource(state))
	if err != nil {
		t.Fatalf("project late landing audit: %v", err)
	}
	if got := bundle.Snapshot.Executions[0].State; got != "expired" {
		t.Fatalf("late landing audit changed expired execution to %q", got)
	}
	if len(bundle.Evidence) != 0 || len(bundle.Evaluations) != 0 || len(bundle.Definition.Requirements) != 0 {
		t.Fatalf("late host audit was promoted to portable qualification: %+v", bundle)
	}
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal late audit projection: %v", err)
	}
	if !strings.Contains(string(encoded), receipt.ID) || strings.Contains(string(encoded), `"deliveryReceiptId"`) {
		t.Fatalf("late fact must remain inspectable audit metadata without releasing the stable gate: %s", encoded)
	}
}

func portableCorrectionTaskIDs(t *testing.T, state State) (fixID, rereviewID string) {
	t.Helper()
	for id, task := range state.Tasks {
		switch task.Stage {
		case StageFix:
			fixID = id
		case StageRereview:
			rereviewID = id
		}
	}
	if fixID == "" || rereviewID == "" {
		t.Fatalf("canonical negative review did not create fix/re-review tasks: %+v", state.Tasks)
	}
	return fixID, rereviewID
}

func TestProjectPortableFailsClosedForMissingOrMismatchedAuthoredSource(t *testing.T) {
	state := portableTestState(t)
	authored := portableTestAuthoredSource(state)
	cases := []struct {
		name   string
		change func(*PortableAuthoredSource)
		want   error
	}{
		{name: "missing plan title", change: func(source *PortableAuthoredSource) { source.PlanTitle = "" }, want: ErrPortableMissingAuthoredSource},
		{name: "missing task title", change: func(source *PortableAuthoredSource) { source.TaskTitle = "" }, want: ErrPortableMissingAuthoredSource},
		{name: "missing acceptance", change: func(source *PortableAuthoredSource) { source.Acceptance = "" }, want: ErrPortableMissingAuthoredSource},
		{name: "missing source ref", change: func(source *PortableAuthoredSource) { source.SourceRef = "" }, want: ErrPortableMissingAuthoredSource},
		{name: "missing source revision", change: func(source *PortableAuthoredSource) { source.SourceRevision = "" }, want: ErrPortableMissingAuthoredSource},
		{name: "missing source bytes", change: func(source *PortableAuthoredSource) { source.SourceBytes = nil }, want: ErrPortableMissingAuthoredSource},
		{name: "revision mismatch", change: func(source *PortableAuthoredSource) { source.SourceRevision = portableTestBaseSHA }, want: ErrPortableInvalidSource},
		{name: "raw task not in source", change: func(source *PortableAuthoredSource) { source.TaskRaw = "a fabricated source row" }, want: ErrPortableInvalidSource},
		{name: "localhost trailing dot", change: func(source *PortableAuthoredSource) { source.SourceRef = "https://localhost./plan.md" }, want: ErrPortableInvalidSource},
		{name: "loopback IPv4 trailing dot", change: func(source *PortableAuthoredSource) { source.SourceRef = "https://127.0.0.1./plan.md" }, want: ErrPortableInvalidSource},
		{name: "IPv6 zone identifier", change: func(source *PortableAuthoredSource) { source.SourceRef = "https://[fe80::1%25en0]/plan.md" }, want: ErrPortableInvalidSource},
		{name: "multiple trailing dots", change: func(source *PortableAuthoredSource) { source.SourceRef = "https://example.com../plan.md" }, want: ErrPortableInvalidSource},
		{name: "empty DNS label", change: func(source *PortableAuthoredSource) { source.SourceRef = "https://example..com/plan.md" }, want: ErrPortableInvalidSource},
		{name: "abbreviated IPv4 loopback", change: func(source *PortableAuthoredSource) { source.SourceRef = "https://127.1/plan.md" }, want: ErrPortableInvalidSource},
		{name: "integer IPv4 loopback", change: func(source *PortableAuthoredSource) { source.SourceRef = "https://2130706433/plan.md" }, want: ErrPortableInvalidSource},
		{name: "hexadecimal IPv4 loopback", change: func(source *PortableAuthoredSource) { source.SourceRef = "https://0x7f000001/plan.md" }, want: ErrPortableInvalidSource},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := authored
			tc.change(&input)
			if _, err := ProjectPortable(state, input); !errors.Is(err, tc.want) {
				t.Fatalf("ProjectPortable error=%v, want errors.Is(%v)", err, tc.want)
			}
		})
	}
}
