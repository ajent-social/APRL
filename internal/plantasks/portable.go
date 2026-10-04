package plantasks

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// PortableContractVersion identifies the experimental Wazi interchange.
	PortableContractVersion = "0.0.1"
	// PortableContractDigest pins the exact Wazi plan/v0 package consumed here.
	PortableContractDigest = "sha256:7582512f122d2f2a9c4461facc7541c9887053f137260d6ebe9c6dea611d039d"
	// PortableAdapterVersion identifies this APRL native-state projection.
	PortableAdapterVersion = "aprl-native-lifecycle/0.1.0"
	portableAuthority      = "aprl"
)

var (
	// ErrPortableInvalidState indicates that the persisted lifecycle cannot be projected safely.
	ErrPortableInvalidState = errors.New("invalid portable lifecycle state")
	// ErrPortableMissingAuthoredSource indicates required source-authoritative fields are absent.
	ErrPortableMissingAuthoredSource = errors.New("portable authored source is incomplete")
	// ErrPortableInvalidSource indicates authored source conflicts with persisted bindings.
	ErrPortableInvalidSource = errors.New("portable authored source is invalid")
)

// PortableMappingError identifies a missing or inconsistent authored binding
// without copying untrusted source contents into the error.
type PortableMappingError struct {
	Field  string
	Reason string
	Cause  error
}

func (e *PortableMappingError) Error() string {
	if e == nil {
		return "portable lifecycle mapping failed"
	}
	return "portable lifecycle mapping " + e.Field + ": " + e.Reason
}

func (e *PortableMappingError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// PortableAuthoredSource is the caller-owned authored source that APRL's
// persisted lifecycle does not retain. SourceBytes are hashed exactly as
// supplied; TaskRaw must be a literal fragment of those bytes.
type PortableAuthoredSource struct {
	PlanTitle      string
	TaskTitle      string
	Stage          string
	AuthoredStatus string
	Acceptance     string
	SourceRef      string
	SourceRevision string
	SourceBytes    []byte
	TaskRaw        string
}

// PortableBundle is the experimental Wazi plan/v0 envelope. The adapter emits
// no evidence or requirement evaluations because APRL state is not sufficient
// to qualify those claims for portable consumers.
type PortableBundle struct {
	ContractVersion string                     `json:"contractVersion"`
	Definition      PortablePlanDefinition     `json:"definition"`
	Snapshot        *PortableExecutionSnapshot `json:"snapshot,omitempty"`
	Evidence        []PortableEvidence         `json:"evidence"`
	Evaluations     []PortableEvaluation       `json:"evaluations"`
}

// PortableSource mirrors the authored source binding in Wazi plan/v0.
type PortableSource struct {
	Authority      string `json:"authority"`
	AuthorityID    string `json:"authorityId"`
	Ref            string `json:"ref"`
	Revision       string `json:"revision"`
	Digest         string `json:"digest"`
	AdapterVersion string `json:"adapterVersion"`
}

// PortableTaskSource links the one parent task to its exact authored fragment.
type PortableTaskSource struct {
	Ref         string `json:"ref"`
	CanonicalID string `json:"canonicalId,omitempty"`
	Raw         string `json:"raw,omitempty"`
}

// PortableDependency is a Wazi dependency; this APRL parent emits none because
// the native child graph remains in its namespaced metadata.
type PortableDependency struct {
	TaskID        string `json:"taskId"`
	Predicate     string `json:"predicate"`
	RequirementID string `json:"requirementId,omitempty"`
}

// PortableTask represents the caller-authored parent, never a native child.
type PortableTask struct {
	ID              string               `json:"id"`
	Title           string               `json:"title"`
	Stage           string               `json:"stage"`
	AuthoredStatus  string               `json:"authoredStatus"`
	Acceptance      string               `json:"acceptance"`
	Source          PortableTaskSource   `json:"source"`
	Dependencies    []PortableDependency `json:"dependencies"`
	Metadata        map[string]any       `json:"metadata"`
	ExecutionUnitID string               `json:"executionUnitId,omitempty"`
}

// PortableExecutionUnit binds that parent to APRL's canonical lifecycle.
type PortableExecutionUnit struct {
	ID          string   `json:"id"`
	Authority   string   `json:"authority"`
	CanonicalID string   `json:"canonicalId"`
	TaskIDs     []string `json:"taskIds"`
	Stages      []string `json:"stages"`
}

// PortablePlanDefinition is the authored PlanDefinition subset emitted here.
type PortablePlanDefinition struct {
	ID             string                  `json:"id"`
	Revision       string                  `json:"revision"`
	Digest         string                  `json:"digest"`
	Title          string                  `json:"title"`
	Source         PortableSource          `json:"source"`
	Tasks          []PortableTask          `json:"tasks"`
	Requirements   []PortableRequirement   `json:"requirements"`
	ExecutionUnits []PortableExecutionUnit `json:"executionUnits"`
	Metadata       map[string]any          `json:"metadata"`
}

// PortableRequirement is an unused typed v0 slot; this projection emits none.
type PortableRequirement struct {
	ID               string          `json:"id"`
	TaskID           string          `json:"taskId"`
	Predicate        string          `json:"predicate"`
	PolicyRevision   string          `json:"policyRevision"`
	Subject          PortableSubject `json:"subject"`
	Domain           string          `json:"domain,omitempty"`
	AllowAlternative bool            `json:"allowLocalAlternative,omitempty"`
}

// PortableSubject contains the source head/base observed by a snapshot.
type PortableSubject struct {
	Head        string `json:"head,omitempty"`
	Base        string `json:"base,omitempty"`
	Artifact    string `json:"artifact,omitempty"`
	Environment string `json:"environment,omitempty"`
}

// PortableExecutionSnapshot reports one conservative lifecycle observation.
type PortableExecutionSnapshot struct {
	PlanID       string              `json:"planId"`
	PlanRevision string              `json:"planRevision"`
	PlanDigest   string              `json:"planDigest"`
	ObservedAt   string              `json:"observedAt"`
	Executions   []PortableExecution `json:"executions"`
	Metadata     map[string]any      `json:"metadata"`
	Subject      PortableSubject     `json:"subject"`
}

// PortableExecution describes the lifecycle's one stable compound attempt.
type PortableExecution struct {
	TaskID          string `json:"taskId"`
	AttemptID       string `json:"attemptId"`
	State           string `json:"state"`
	ExecutionUnitID string `json:"executionUnitId,omitempty"`
}

// PortableEvidence is a typed v0 slot; no evidence is qualified by this adapter.
type PortableEvidence struct{}

// PortableEvaluation is a typed v0 slot; no evaluations are issued here.
type PortableEvaluation struct{}

type portableAPRLExtension struct {
	LifecycleID                   string                    `json:"lifecycleId"`
	LifecycleRevision             int64                     `json:"lifecycleRevision"`
	NativeObservationDigest       string                    `json:"nativeObservationDigest"`
	DeliveryGateID                string                    `json:"deliveryGateId"`
	LifecycleAuthor               portableContributor       `json:"lifecycleAuthor"`
	LifecycleAuthorSourceRevision string                    `json:"lifecycleAuthorSourceRevision"`
	ContractDigest                string                    `json:"portableContractDigest"`
	AdapterVersion                string                    `json:"adapterVersion"`
	CallerSource                  portableCallerSource      `json:"callerSource"`
	Repository                    portableRepository        `json:"repository"`
	PolicyRevision                string                    `json:"policyRevision"`
	CurrentPR                     *portablePullRequest      `json:"currentPullRequest,omitempty"`
	Attempts                      []portableTaskAttempt     `json:"attempts"`
	Admissions                    []portableAdmission       `json:"admissions"`
	Cancelled                     bool                      `json:"cancelled"`
	Escalated                     bool                      `json:"escalated"`
	Children                      []portableNativeTask      `json:"children"`
	Receipts                      []portableNativeReceipt   `json:"receipts"`
	LateLandingFacts              []portableLateLandingFact `json:"lateLandingFacts"`
}

// portableCallerSource retains caller assertions without treating them as
// repository provenance or resolving them against a host checkout.
type portableCallerSource struct {
	Classification  string `json:"classification"`
	ClaimedRef      string `json:"claimedRef"`
	ClaimedRevision string `json:"claimedRevision"`
	Digest          string `json:"digest"`
}

type portableRepository struct {
	Owner  string `json:"owner"`
	Name   string `json:"name"`
	Target string `json:"target"`
}

type portableTaskAttempt struct {
	TaskID string `json:"taskId"`
	Count  int    `json:"count"`
}

type portableAdmission struct {
	TaskID    string `json:"taskId"`
	ActorID   string `json:"actorId"`
	Revision  int64  `json:"revision"`
	ExpiresAt string `json:"expiresAt"`
}

type portableNativeTask struct {
	ID           string                     `json:"id"`
	Stage        string                     `json:"stage"`
	Correction   int                        `json:"correction"`
	Dependencies []portableNativeDependency `json:"dependencies"`
	FindingIDs   []string                   `json:"findingIds"`
	PullRequest  *portablePullRequest       `json:"pullRequest,omitempty"`
	Authors      []portableContributor      `json:"authors"`
	Actor        *portableContributor       `json:"actor,omitempty"`
	Outcome      string                     `json:"outcome,omitempty"`
}

type portableNativeDependency struct {
	TaskID string `json:"taskId"`
	Kind   string `json:"kind"`
}

type portablePullRequest struct {
	Number         int64  `json:"number"`
	URL            string `json:"url"`
	HeadSHA        string `json:"headSha"`
	BaseSHA        string `json:"baseSha"`
	PolicyRevision string `json:"policyRevision"`
}

type portableContributor struct {
	ActorID        string `json:"actorId"`
	ActorKind      string `json:"actorKind"`
	AuthoredAt     string `json:"authoredAt,omitempty"`
	SourceRevision string `json:"sourceRevision,omitempty"`
}

type portableNativeReceipt struct {
	ID              string                `json:"id"`
	TaskID          string                `json:"taskId"`
	Outcome         string                `json:"outcome"`
	ObservedOutcome string                `json:"observedOutcome,omitempty"`
	Actor           portableContributor   `json:"actor"`
	PolicyRevision  string                `json:"policyRevision"`
	CreatedAt       string                `json:"createdAt"`
	FindingIDs      []string              `json:"findingIds"`
	PullRequest     *portablePullRequest  `json:"pullRequest,omitempty"`
	MergeCommit     string                `json:"mergeCommit,omitempty"`
	LandedCommit    string                `json:"landedCommit,omitempty"`
	LandedEvidence  *portableLandingProof `json:"landedEvidence,omitempty"`
}

type portableLandingProof struct {
	LifecycleID    string `json:"lifecycleId"`
	TaskID         string `json:"taskId"`
	ReceiptID      string `json:"receiptId"`
	Revision       int64  `json:"revision"`
	PRNumber       int64  `json:"prNumber"`
	MergeCommit    string `json:"mergeCommit"`
	Repository     string `json:"repository"`
	TargetBranch   string `json:"targetBranch"`
	PRURL          string `json:"prUrl"`
	ReviewedHead   string `json:"reviewedHead"`
	ReviewedBase   string `json:"reviewedBase"`
	PolicyRevision string `json:"policyRevision"`
	LandedCommit   string `json:"landedCommit"`
	SourceDigest   string `json:"sourceDigest"`
	Reviewer       string `json:"reviewer"`
	Author         string `json:"author"`
	Verifier       string `json:"verifier"`
	VerifiedAt     string `json:"verifiedAt"`
}

type portableLateLandingFact struct {
	ID         string                `json:"id"`
	HostActor  string                `json:"hostActor"`
	ObservedAt string                `json:"observedAt"`
	Receipt    portableNativeReceipt `json:"receipt"`
}

// ProjectPortable projects one canonical APRL lifecycle into the pinned Wazi
// v0 contract without creating authored child tasks, portable evidence or a
// second authority for admission, correction, merge, or landing.
func ProjectPortable(state State, authored PortableAuthoredSource) (PortableBundle, error) {
	if err := state.Validate(); err != nil {
		return PortableBundle{}, &PortableMappingError{Field: "state", Reason: "canonical lifecycle is invalid", Cause: ErrPortableInvalidState}
	}
	if err := validatePortableAuthoredSource(state, authored); err != nil {
		return PortableBundle{}, err
	}
	sourceDigest := portableDigest(authored.SourceBytes)
	sourceRef := portableCallerSourceRef(sourceDigest)
	planID := portableLifecycleID(state.Lifecycle.ID)
	canonicalID := portableCanonicalLifecycleID(state.Lifecycle.ID)
	taskID := portableTaskID(state.Lifecycle.ID)
	unitID := portableUnitID(state.Lifecycle.ID)
	stateDigest, err := portableStateDigest(state)
	if err != nil {
		return PortableBundle{}, &PortableMappingError{Field: "state_digest", Reason: "cannot encode canonical lifecycle state", Cause: ErrPortableInvalidState}
	}

	children, stages, receipts, facts, err := portableNativeMetadata(state)
	if err != nil {
		return PortableBundle{}, err
	}
	stages = append(stages, authored.Stage)
	sort.Strings(stages)
	stages = uniquePortableStrings(stages)
	metadata := portableAPRLExtension{
		LifecycleID: state.Lifecycle.ID, LifecycleRevision: state.Revision,
		NativeObservationDigest: stateDigest, DeliveryGateID: state.Lifecycle.DeliveryGateID,
		LifecycleAuthor: portableContributorFrom(state.Lifecycle.Authored), LifecycleAuthorSourceRevision: state.Lifecycle.Authored.SourceRevision,
		ContractDigest: PortableContractDigest, AdapterVersion: PortableAdapterVersion,
		CallerSource:   portableCallerSource{Classification: "caller-supplied-unverified", ClaimedRef: authored.SourceRef, ClaimedRevision: authored.SourceRevision, Digest: sourceDigest},
		Repository:     portableRepository{Owner: state.Lifecycle.Repository.Owner, Name: state.Lifecycle.Repository.Name, Target: state.Lifecycle.Repository.Target},
		PolicyRevision: state.Lifecycle.PolicyRevision,
		Cancelled:      state.Cancelled, Escalated: state.EscalationReason != "",
		Children: children, Receipts: receipts, LateLandingFacts: facts,
		Attempts: portableAttempts(state), Admissions: portableAdmissions(state, portableObservedAt(state)),
	}
	if state.CurrentPR != nil {
		pr := portablePR(*state.CurrentPR)
		metadata.CurrentPR = &pr
	}
	aprlMetadata := map[string]any{"aprl": metadata}
	unit := PortableExecutionUnit{ID: unitID, Authority: portableAuthority, CanonicalID: canonicalID, TaskIDs: []string{taskID}, Stages: stages}
	task := PortableTask{
		ID: taskID, Title: authored.TaskTitle, Stage: authored.Stage, AuthoredStatus: authored.AuthoredStatus,
		Acceptance: authored.Acceptance, Source: PortableTaskSource{Ref: sourceRef, CanonicalID: canonicalID, Raw: authored.TaskRaw},
		Dependencies: []PortableDependency{}, Metadata: map[string]any{"aprl": map[string]any{"lifecycleId": state.Lifecycle.ID, "deliveryGateId": state.Lifecycle.DeliveryGateID}},
		ExecutionUnitID: unitID,
	}
	definition := PortablePlanDefinition{
		ID: planID, Revision: sourceDigest, Digest: sourceDigest, Title: authored.PlanTitle,
		Source: PortableSource{Authority: "native", AuthorityID: "aprl:caller-source", Ref: sourceRef, Revision: sourceDigest, Digest: sourceDigest, AdapterVersion: PortableAdapterVersion},
		Tasks:  []PortableTask{task}, Requirements: []PortableRequirement{}, ExecutionUnits: []PortableExecutionUnit{unit}, Metadata: aprlMetadata,
	}
	observedAt := portableObservedAt(state)
	subject := PortableSubject{}
	if state.CurrentPR != nil {
		subject.Head = state.CurrentPR.HeadSHA
		subject.Base = state.CurrentPR.BaseSHA
	}
	snapshot := &PortableExecutionSnapshot{
		PlanID: planID, PlanRevision: definition.Revision, PlanDigest: definition.Digest,
		ObservedAt: observedAt.Format(time.RFC3339Nano), Subject: subject,
		Executions: []PortableExecution{{TaskID: taskID, AttemptID: portableAttemptID(state.Lifecycle.ID), State: portableExecutionState(state, observedAt), ExecutionUnitID: unitID}},
		Metadata:   map[string]any{"aprl": map[string]any{"lifecycleId": state.Lifecycle.ID, "lifecycleRevision": state.Revision, "lifecycleStateDigest": stateDigest, "observationTimeSource": portableObservationTimeSource(state)}},
	}
	return PortableBundle{ContractVersion: PortableContractVersion, Definition: definition, Snapshot: snapshot, Evidence: []PortableEvidence{}, Evaluations: []PortableEvaluation{}}, nil
}

func validatePortableAuthoredSource(state State, authored PortableAuthoredSource) error {
	missing := func(field string) error {
		return &PortableMappingError{Field: field, Reason: "authoritative authored source is required", Cause: ErrPortableMissingAuthoredSource}
	}
	if strings.TrimSpace(authored.PlanTitle) == "" {
		return missing("plan_title")
	}
	if strings.TrimSpace(authored.TaskTitle) == "" {
		return missing("task_title")
	}
	if strings.TrimSpace(authored.Stage) == "" {
		return missing("stage")
	}
	if strings.TrimSpace(authored.Acceptance) == "" {
		return missing("acceptance")
	}
	if strings.TrimSpace(authored.SourceRef) == "" {
		return missing("source_ref")
	}
	if strings.TrimSpace(authored.SourceRevision) == "" {
		return missing("source_revision")
	}
	if len(authored.SourceBytes) == 0 || strings.TrimSpace(authored.TaskRaw) == "" {
		return missing("source_bytes_or_task_raw")
	}
	for field, value := range map[string]string{"plan_title": authored.PlanTitle, "task_title": authored.TaskTitle, "stage": authored.Stage, "source_ref": authored.SourceRef, "source_revision": authored.SourceRevision} {
		if !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return &PortableMappingError{Field: field, Reason: "must be valid single-line UTF-8", Cause: ErrPortableInvalidSource}
		}
	}
	// Acceptance is opaque authored text and may span lines; only invalid UTF-8
	// or NUL would make the source fragment unsafe to preserve.
	if !utf8.ValidString(authored.Acceptance) || strings.ContainsRune(authored.Acceptance, '\x00') {
		return &PortableMappingError{Field: "acceptance", Reason: "must be valid UTF-8 without NUL", Cause: ErrPortableInvalidSource}
	}
	if !utf8.Valid(authored.SourceBytes) || !utf8.ValidString(authored.TaskRaw) || !bytes.Contains(authored.SourceBytes, []byte(authored.TaskRaw)) {
		return &PortableMappingError{Field: "source_bytes", Reason: "raw task fragment must be valid UTF-8 present in source bytes", Cause: ErrPortableInvalidSource}
	}
	if !bytes.Contains(authored.SourceBytes, []byte(authored.PlanTitle)) || !strings.Contains(authored.TaskRaw, authored.TaskTitle) || !strings.Contains(authored.TaskRaw, authored.Acceptance) {
		return &PortableMappingError{Field: "authored_fields", Reason: "title and acceptance must occur in the supplied authored source", Cause: ErrPortableInvalidSource}
	}
	if err := validatePortableTaskMarkers(authored.TaskRaw, authored.Stage, authored.AuthoredStatus); err != nil {
		return err
	}
	if authored.SourceRevision != state.Lifecycle.Authored.SourceRevision {
		return &PortableMappingError{Field: "source_revision", Reason: "does not match the canonical lifecycle author revision", Cause: ErrPortableInvalidSource}
	}
	if !portableSafeSourceRef(authored.SourceRef) {
		return &PortableMappingError{Field: "source_ref", Reason: "only safe URN, relative, or public HTTPS references are accepted", Cause: ErrPortableInvalidSource}
	}
	switch authored.AuthoredStatus {
	case "pending", "active", "blocked", "complete":
	default:
		return &PortableMappingError{Field: "authored_status", Reason: "must be explicit pending, active, blocked, or complete", Cause: ErrPortableInvalidSource}
	}
	return nil
}

var portableCheckboxRow = regexp.MustCompile(`(?m)^\s*-\s*\[([^\]\r\n])\]`)
var portableStageMarker = regexp.MustCompile(`\bstage:\s*([A-Za-z0-9][A-Za-z0-9_-]*)\b`)

func validatePortableTaskMarkers(taskRaw, stage, authoredStatus string) error {
	rows := portableCheckboxRow.FindAllStringSubmatch(taskRaw, -1)
	if len(rows) != 1 {
		return &PortableMappingError{Field: "task_raw", Reason: "must contain exactly one Markdown checkbox row", Cause: ErrPortableInvalidSource}
	}
	statusByMarker := map[string]string{" ": "pending", "x": "complete", "X": "complete", "~": "active", "!": "blocked"}
	rowStatus, ok := statusByMarker[rows[0][1]]
	if !ok || rowStatus != authoredStatus {
		return &PortableMappingError{Field: "authored_status", Reason: "does not match the authored Markdown checkbox", Cause: ErrPortableInvalidSource}
	}
	if strings.Count(taskRaw, "stage:") != 1 {
		return &PortableMappingError{Field: "task_raw", Reason: "must contain exactly one stage marker", Cause: ErrPortableInvalidSource}
	}
	markers := portableStageMarker.FindAllStringSubmatch(taskRaw, -1)
	if len(markers) != 1 || markers[0][1] != stage {
		return &PortableMappingError{Field: "stage", Reason: "does not match the authored stage marker", Cause: ErrPortableInvalidSource}
	}
	return nil
}

func portableNativeMetadata(state State) ([]portableNativeTask, []string, []portableNativeReceipt, []portableLateLandingFact, error) {
	taskIDs := make([]string, 0, len(state.Tasks))
	for id := range state.Tasks {
		taskIDs = append(taskIDs, id)
	}
	sort.Strings(taskIDs)
	children := make([]portableNativeTask, 0, len(taskIDs))
	stages := make([]string, 0, len(taskIDs))
	for _, id := range taskIDs {
		task := state.Tasks[id]
		child := portableNativeTask{ID: id, Stage: string(task.Stage), Correction: task.Correction, FindingIDs: append([]string{}, task.FindingIDs...), Authors: portableContributors(task.Authors), Outcome: string(state.Completed[id])}
		stages = append(stages, string(task.Stage))
		deps := append([]Dependency(nil), task.Dependencies...)
		sort.Slice(deps, func(i, j int) bool {
			if deps[i].TaskID == deps[j].TaskID {
				return deps[i].Kind < deps[j].Kind
			}
			return deps[i].TaskID < deps[j].TaskID
		})
		child.Dependencies = make([]portableNativeDependency, 0, len(deps))
		for _, dep := range deps {
			child.Dependencies = append(child.Dependencies, portableNativeDependency{TaskID: dep.TaskID, Kind: string(dep.Kind)})
		}
		if task.Actor.ActorID != "" {
			contributor := portableContributorFrom(task.Actor)
			child.Actor = &contributor
		}
		if task.PR != nil {
			pr := portablePR(*task.PR)
			child.PullRequest = &pr
		}
		children = append(children, child)
	}
	receiptIDs := make([]string, 0, len(state.Receipts))
	for id := range state.Receipts {
		receiptIDs = append(receiptIDs, id)
	}
	sort.Strings(receiptIDs)
	receipts := make([]portableNativeReceipt, 0, len(receiptIDs))
	for _, id := range receiptIDs {
		receipt := state.Receipts[id]
		projected := portableNativeReceipt{ID: receipt.ID, TaskID: receipt.TaskID, Outcome: string(receipt.Outcome), ObservedOutcome: string(receipt.ObservedOutcome), Actor: portableContributorFrom(receipt.Actor), PolicyRevision: receipt.PolicyRevision, CreatedAt: receipt.CreatedAt.UTC().Format(time.RFC3339Nano), FindingIDs: append([]string{}, receipt.FindingIDs...), MergeCommit: receipt.MergeCommit, LandedCommit: receipt.LandedCommit}
		if receipt.PR != nil {
			pr := portablePR(*receipt.PR)
			projected.PullRequest = &pr
		}
		if receipt.LandedEvidence != nil {
			proof := portableProof(*receipt.LandedEvidence)
			projected.LandedEvidence = &proof
		}
		receipts = append(receipts, projected)
	}
	factIDs := make([]string, 0, len(state.LateLandedFacts))
	for id := range state.LateLandedFacts {
		factIDs = append(factIDs, id)
	}
	sort.Strings(factIDs)
	facts := make([]portableLateLandingFact, 0, len(factIDs))
	for _, id := range factIDs {
		fact := state.LateLandedFacts[id]
		projectedReceipt := portableNativeReceipt{ID: fact.Receipt.ID, TaskID: fact.Receipt.TaskID, Outcome: string(fact.Receipt.Outcome), ObservedOutcome: string(fact.Receipt.ObservedOutcome), Actor: portableContributorFrom(fact.Receipt.Actor), PolicyRevision: fact.Receipt.PolicyRevision, CreatedAt: fact.Receipt.CreatedAt.UTC().Format(time.RFC3339Nano), FindingIDs: append([]string{}, fact.Receipt.FindingIDs...), MergeCommit: fact.Receipt.MergeCommit, LandedCommit: fact.Receipt.LandedCommit}
		if fact.Receipt.PR != nil {
			pr := portablePR(*fact.Receipt.PR)
			projectedReceipt.PullRequest = &pr
		}
		if fact.Receipt.LandedEvidence != nil {
			proof := portableProof(*fact.Receipt.LandedEvidence)
			projectedReceipt.LandedEvidence = &proof
		}
		facts = append(facts, portableLateLandingFact{ID: fact.ID, HostActor: fact.HostActor, ObservedAt: fact.ObservedAt.UTC().Format(time.RFC3339Nano), Receipt: projectedReceipt})
	}
	return children, stages, receipts, facts, nil
}

func portableAttempts(state State) []portableTaskAttempt {
	ids := make([]string, 0, len(state.Attempts))
	for id := range state.Attempts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	values := make([]portableTaskAttempt, 0, len(ids))
	for _, id := range ids {
		values = append(values, portableTaskAttempt{TaskID: id, Count: state.Attempts[id]})
	}
	return values
}

func portableAdmissions(state State, observedAt time.Time) []portableAdmission {
	ids := make([]string, 0, len(state.Claims))
	for id, admission := range state.Claims {
		if observedAt.Before(admission.ExpiresAt) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	values := make([]portableAdmission, 0, len(ids))
	for _, id := range ids {
		admission := state.Claims[id]
		values = append(values, portableAdmission{TaskID: admission.TaskID, ActorID: admission.ActorID, Revision: admission.Revision, ExpiresAt: admission.ExpiresAt.UTC().Format(time.RFC3339Nano)})
	}
	return values
}

func portableExecutionState(state State, observedAt time.Time) string {
	if state.Cancelled {
		return "canceled"
	}
	if portableHasVerifiedLanding(state) {
		return "complete"
	}
	deadline := state.StartedAt.Add(time.Duration(state.Lifecycle.Limits.MaxDurationSeconds) * time.Second)
	if !observedAt.Before(deadline) {
		return "expired"
	}
	if state.EscalationReason != "" || portableHasCurrentUnknown(state) {
		return "unknown"
	}
	for _, admission := range state.Claims {
		if observedAt.Before(admission.ExpiresAt) {
			return "active"
		}
	}
	if len(state.Claims) > 0 {
		return "unknown"
	}
	if len(state.Receipts) > 0 || len(state.Completed) > 0 {
		return "active"
	}
	return "waiting"
}

func portableHasVerifiedLanding(state State) bool {
	if state.DeliveryReceiptID == "" {
		return false
	}
	receipt, ok := state.Receipts[state.DeliveryReceiptID]
	if !ok || receipt.Outcome != OutcomeLanded || receipt.LandedEvidence == nil || receipt.LandedEvidence.ReceiptID != receipt.ID || receipt.LandedEvidence.LifecycleID != state.Lifecycle.ID || receipt.LandedEvidence.TaskID != receipt.TaskID {
		return false
	}
	return state.Completed[receipt.TaskID] == OutcomeLanded
}

func portableHasCurrentUnknown(state State) bool {
	latest := make(map[string]Receipt, len(state.Tasks))
	for _, receipt := range state.Receipts {
		prior, ok := latest[receipt.TaskID]
		if !ok || receipt.CreatedAt.After(prior.CreatedAt) || (receipt.CreatedAt.Equal(prior.CreatedAt) && receipt.ID > prior.ID) {
			latest[receipt.TaskID] = receipt
		}
	}
	for _, receipt := range latest {
		if receipt.Outcome == OutcomeUnknown {
			return true
		}
	}
	return false
}

func portableObservedAt(state State) time.Time {
	if state.ProjectionTime != nil {
		return state.ProjectionTime.UTC()
	}
	observed := state.StartedAt.UTC()
	for _, receipt := range state.Receipts {
		if receipt.CreatedAt.After(observed) {
			observed = receipt.CreatedAt.UTC()
		}
	}
	for _, fact := range state.LateLandedFacts {
		if fact.ObservedAt.After(observed) {
			observed = fact.ObservedAt.UTC()
		}
	}
	return observed
}

func portableObservationTimeSource(state State) string {
	if state.ProjectionTime != nil {
		return "persisted_projection_time"
	}
	if len(state.LateLandedFacts) > 0 {
		return "latest_persisted_late_fact"
	}
	if len(state.Receipts) > 0 {
		return "latest_persisted_receipt"
	}
	return "lifecycle_started_at_fallback"
}

func portableStateDigest(state State) (string, error) {
	// Hash only a redacted view. This is an observation fingerprint, never a
	// proof of host authority, and must not commit secret claim hashes or
	// arbitrary diagnostic detail into portable output.
	redacted := state
	redacted.Claims = make(map[string]Admission, len(state.Claims))
	for taskID, admission := range state.Claims {
		admission.ClaimSHA = ""
		redacted.Claims[taskID] = admission
	}
	redacted.Receipts = make(map[string]Receipt, len(state.Receipts))
	for receiptID, receipt := range state.Receipts {
		receipt.Detail = ""
		redacted.Receipts[receiptID] = receipt
	}
	redacted.LateLandedFacts = make(map[string]LateLandingFact, len(state.LateLandedFacts))
	for factID, fact := range state.LateLandedFacts {
		fact.Receipt.Detail = ""
		redacted.LateLandedFacts[factID] = fact
	}
	if redacted.EscalationReason != "" {
		redacted.EscalationReason = "escalated"
	}
	payload, err := json.Marshal(redacted)
	if err != nil {
		return "", err
	}
	return portableDigest(payload), nil
}

func portableDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func portableCallerSourceRef(digest string) string {
	return "urn:aprl:caller-source:" + digest
}

func portableLifecycleID(id string) string          { return "aprl:lifecycle:" + id }
func portableCanonicalLifecycleID(id string) string { return "aprl:lifecycle:" + id }
func portableTaskID(id string) string               { return "aprl:delivery:" + id }
func portableUnitID(id string) string               { return "aprl:execution:" + id }
func portableAttemptID(id string) string            { return "aprl:lifecycle:" + id + ":attempt:1" }

func portableContributors(values []Provenance) []portableContributor {
	out := make([]portableContributor, 0, len(values))
	for _, actor := range values {
		out = append(out, portableContributorFrom(actor))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ActorID == out[j].ActorID {
			return out[i].SourceRevision < out[j].SourceRevision
		}
		return out[i].ActorID < out[j].ActorID
	})
	return out
}

func portableContributorFrom(value Provenance) portableContributor {
	actor := portableContributor{ActorID: value.ActorID, ActorKind: value.ActorKind, SourceRevision: value.SourceRevision}
	if !value.AuthoredAt.IsZero() {
		actor.AuthoredAt = value.AuthoredAt.UTC().Format(time.RFC3339Nano)
	}
	return actor
}

func portablePR(value PRBinding) portablePullRequest {
	return portablePullRequest(value)
}

func portableProof(value LandedEvidence) portableLandingProof {
	proof := value.Receipt
	return portableLandingProof{LifecycleID: value.LifecycleID, TaskID: value.TaskID, ReceiptID: value.ReceiptID, Revision: value.Revision, PRNumber: value.PRNumber, MergeCommit: value.MergeCommit, Repository: proof.Repository, TargetBranch: proof.TargetBranch, PRURL: proof.PRURL, ReviewedHead: proof.ReviewedHead, ReviewedBase: proof.ReviewedBase, PolicyRevision: proof.PolicyRevision, LandedCommit: proof.LandedCommit, SourceDigest: proof.SourceDigest, Reviewer: proof.Reviewer, Author: proof.Author, Verifier: proof.Verifier, VerifiedAt: proof.VerifiedAt.UTC().Format(time.RFC3339Nano)}
}

func portableSafeSourceRef(reference string) bool {
	if reference == "" || strings.ContainsAny(reference, "\\\x00\r\n") {
		return false
	}
	parsed, err := url.Parse(reference)
	if err != nil || parsed.User != nil {
		return false
	}
	switch parsed.Scheme {
	case "urn":
		return parsed.Opaque != "" && !strings.Contains(reference, " ")
	case "https":
		if parsed.Hostname() == "" || parsed.Fragment != "" || parsed.RawQuery != "" {
			return false
		}
		host := strings.ToLower(parsed.Hostname())
		if strings.Contains(host, "%") {
			return false
		}
		// A single trailing dot is the DNS absolute-name spelling of the same
		// host. Normalize it before local-name and IP checks; multiple trailing
		// dots, empty labels, and numeric shorthand hosts remain ambiguous.
		host = strings.TrimSuffix(host, ".")
		if host == "" || strings.HasSuffix(host, ".") {
			return false
		}
		if host == "localhost" || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".lan") || strings.HasSuffix(host, ".home.arpa") || strings.HasSuffix(host, ".test") {
			return false
		}
		if ip := net.ParseIP(host); ip != nil && (ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()) {
			return false
		}
		if net.ParseIP(host) == nil {
			labels := strings.Split(host, ".")
			if len(labels) < 2 || portableNumericHostLabel(labels[len(labels)-1]) {
				return false
			}
			for _, label := range labels {
				if label == "" {
					return false
				}
			}
		}
		return true
	case "":
		// Escaped relative references have resolver-dependent path semantics.
		// Keep this bounded mapper to literal repository-relative paths.
		if strings.Contains(reference, "%") {
			return false
		}
		if parsed.IsAbs() || parsed.RawQuery != "" || parsed.Fragment != "" || strings.HasPrefix(reference, "~") || strings.HasPrefix(reference, "/") || strings.Contains(reference, ":") {
			return false
		}
		clean := path.Clean(reference)
		return clean != "." && clean != ".." && !strings.HasPrefix(clean, "../") && clean == reference
	default:
		return false
	}
}

func portableNumericHostLabel(label string) bool {
	if label == "" {
		return false
	}
	allDecimal := true
	for _, r := range label {
		if r < '0' || r > '9' {
			allDecimal = false
			break
		}
	}
	if allDecimal {
		return true
	}
	if len(label) > 2 && strings.HasPrefix(label, "0x") {
		for _, r := range label[2:] {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
				return false
			}
		}
		return true
	}
	if len(label) > 1 && label[0] == '0' {
		for _, r := range label[1:] {
			if r < '0' || r > '7' {
				return false
			}
		}
		return true
	}
	return false
}

func uniquePortableStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}
