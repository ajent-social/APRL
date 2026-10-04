// Package plantasks defines the neutral, versioned task and receipt contract
// used to represent a bounded code-change lifecycle in ordinary plan tooling.
package plantasks

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	// VersionV1 identifies the current internal task contract.
	VersionV1             = 1
	maxReceiptDetailBytes = 4096
)

// Stage identifies a lifecycle task role.
type Stage string

const (
	// StageAuthor is the initial code author task stage.
	StageAuthor Stage = "author"
	// StageReview is the initial independent delivery gate.
	StageReview Stage = "review"
	// StageFix addresses an immutable negative verdict.
	StageFix Stage = "fix"
	// StageRereview reviews a corrected revision independently.
	StageRereview Stage = "rereview"
)

// DependencyKind distinguishes author handoff from verified landing.
type DependencyKind string

const (
	// DependencyHandoff requires a completed coding or findings handoff.
	DependencyHandoff DependencyKind = "handoff"
	// DependencyLanded requires verified repository landing.
	DependencyLanded DependencyKind = "landed"
)

// Outcome records a host-qualified task observation.
type Outcome string

const (
	// OutcomeCodingHandoff publishes a coding revision for independent review.
	OutcomeCodingHandoff Outcome = "coding_handoff"
	// OutcomeChangesRequest records blockers and opens bounded corrections.
	OutcomeChangesRequest Outcome = "changes_requested"
	// OutcomeApproved records independent acceptance of an exact revision.
	OutcomeApproved Outcome = "approved"
	// OutcomeMerged records a host-confirmed merge.
	OutcomeMerged Outcome = "merged"
	// OutcomeLanded records verified repository landing.
	OutcomeLanded Outcome = "landed"
	// OutcomeFailed records a failed task attempt.
	OutcomeFailed Outcome = "failed"
	// OutcomeUnknown records an ambiguous observation without releasing work.
	OutcomeUnknown Outcome = "unknown"
	// OutcomeCancel fences the lifecycle while preserving late observations.
	OutcomeCancel Outcome = "cancel"
)

// Lifecycle is the immutable admission envelope for a sequence of visible
// plan tasks. Limits are policy values supplied by the caller, not budgets.
type Lifecycle struct {
	Version        int    `json:"version"`
	ID             string `json:"lifecycle_id"`
	DeliveryGateID string `json:"delivery_gate_id"`
	// Repository pins the target repository and branch.
	Repository     Repository       `json:"repository"`
	PolicyRevision string           `json:"policy_revision"`
	Authored       Provenance       `json:"authored"`
	Limits         CorrectionLimits `json:"limits"`
	CreatedAt      time.Time        `json:"created_at"`
}

// Repository pins the target repository and branch.
type Repository struct {
	Owner  string `json:"owner"`
	Name   string `json:"name"`
	Target string `json:"target"`
}

// CorrectionLimits bounds automatic correction work for one lifecycle.
// These are caller-approved policy limits and carry no funding semantics.
type CorrectionLimits struct {
	MaxCorrections     int `json:"max_corrections"`
	MaxAttempts        int `json:"max_attempts"`
	MaxDurationSeconds int `json:"max_duration_seconds"`
	MaxConcurrentTasks int `json:"max_concurrent_tasks"`
}

// Provenance identifies a real actor and the revision they authored or
// reviewed. ActorID is an opaque stable identity, not a credential.
type Provenance struct {
	ActorID        string    `json:"actor_id"`
	ActorKind      string    `json:"actor_kind"`
	AuthoredAt     time.Time `json:"authored_at"`
	SourceRevision string    `json:"source_revision,omitempty"`
}

// Task is a claimable lifecycle row with immutable dependencies.
type Task struct {
	Version     int    `json:"version"`
	ID          string `json:"task_id"`
	LifecycleID string `json:"lifecycle_id"`
	// Stage identifies a lifecycle task role.
	Stage        Stage        `json:"stage"`
	Dependencies []Dependency `json:"dependencies"`
	Actor        Provenance   `json:"actor,omitempty"`
	Authors      []Provenance `json:"authors,omitempty"`
	Correction   int          `json:"correction"`
	FindingIDs   []string     `json:"finding_ids,omitempty"`
	PR           *PRBinding   `json:"pull_request,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
}

// Dependency binds a task to a typed predecessor gate.
type Dependency struct {
	TaskID string         `json:"task_id"`
	Kind   DependencyKind `json:"kind"`
}

// PRBinding pins one pull request revision and policy.
type PRBinding struct {
	Number         int64  `json:"number"`
	URL            string `json:"url"`
	HeadSHA        string `json:"head_sha"`
	BaseSHA        string `json:"base_sha"`
	PolicyRevision string `json:"policy_revision"`
}

// Receipt is immutable evidence for a task transition.
type Receipt struct {
	Version     int    `json:"version"`
	ID          string `json:"receipt_id"`
	LifecycleID string `json:"lifecycle_id"`
	TaskID      string `json:"task_id"`
	// Outcome records a host-qualified task observation.
	Outcome         Outcome    `json:"outcome"`
	Actor           Provenance `json:"actor"`
	PolicyRevision  string     `json:"policy_revision"`
	PR              *PRBinding `json:"pull_request,omitempty"`
	FindingIDs      []string   `json:"finding_ids,omitempty"`
	MergeCommit     string     `json:"merge_commit,omitempty"`
	LandedCommit    string     `json:"landed_commit,omitempty"`
	ObservedOutcome Outcome    `json:"observed_outcome,omitempty"`
	Detail          string     `json:"detail,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	// LandedEvidence is supplied only by a trusted host verifier.
	LandedEvidence *LandedEvidence `json:"landed_evidence,omitempty"`
}

var (
	idPattern  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	shaPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

// Validate checks the immutable lifecycle envelope.
func (l Lifecycle) Validate() error {
	if l.Version != VersionV1 {
		return invalid("version", "unsupported lifecycle version")
	}
	if !validID(l.ID) || !validID(l.DeliveryGateID) {
		return invalid("lifecycle_id", "lifecycle and delivery gate IDs must be lowercase non-zero UUIDs")
	}
	if !component(l.Repository.Owner) || !component(l.Repository.Name) || !safeTarget(l.Repository.Target) {
		return invalid("repository", "owner, name, and target are required safe identifiers")
	}
	if !nonempty(l.PolicyRevision) {
		return invalid("policy_revision", "is required")
	}
	if err := l.Authored.Validate(); err != nil {
		return fmt.Errorf("authored: %w", err)
	}
	if l.Limits.MaxCorrections < 0 || l.Limits.MaxAttempts <= 0 || l.Limits.MaxDurationSeconds <= 0 || l.Limits.MaxConcurrentTasks <= 0 {
		return invalid("limits", "corrections must be non-negative; attempt, duration, and concurrency limits must be positive")
	}
	if uint64(l.Limits.MaxDurationSeconds) > uint64((1<<63-1)/int64(time.Second)) {
		return invalid("limits.max_duration_seconds", "exceeds safe duration representation")
	}
	if l.CreatedAt.IsZero() {
		return invalid("created_at", "is required")
	}
	return nil
}

// Validate checks actor identity and revision provenance.
func (p Provenance) Validate() error {
	if !nonempty(p.ActorID) || !nonempty(p.ActorKind) || p.AuthoredAt.IsZero() {
		return invalid("provenance", "actor identity, kind, and authored time are required")
	}
	if p.SourceRevision != "" && !shaPattern.MatchString(p.SourceRevision) {
		return invalid("source_revision", "must be a lowercase 40-character Git SHA")
	}
	return nil
}

// Validate checks task-local invariants. Use ValidateTask for lifecycle-bound
// limits, repository policy, and stage handoff requirements.
func (t Task) Validate() error {
	if t.Version != VersionV1 {
		return invalid("version", "unsupported task version")
	}
	if !validID(t.ID) || !validID(t.LifecycleID) {
		return invalid("task_id", "task and lifecycle IDs must be lowercase non-zero UUIDs")
	}
	switch t.Stage {
	case StageAuthor, StageReview, StageFix, StageRereview:
	default:
		return invalid("stage", "unknown stage")
	}
	if t.Correction < 0 {
		return invalid("correction", "cannot be negative")
	}
	if t.CreatedAt.IsZero() {
		return invalid("created_at", "is required")
	}
	if t.Actor != (Provenance{}) {
		if err := t.Actor.Validate(); err != nil {
			return fmt.Errorf("actor: %w", err)
		}
	}
	seenDeps := make(map[string]struct{}, len(t.Dependencies))
	for _, dep := range t.Dependencies {
		if !validID(dep.TaskID) || dep.TaskID == t.ID {
			return invalid("dependencies", "must reference another valid task ID")
		}
		if dep.Kind != DependencyHandoff && dep.Kind != DependencyLanded {
			return invalid("dependencies", "unknown dependency kind")
		}
		if _, exists := seenDeps[dep.TaskID]; exists {
			return invalid("dependencies", "duplicate task dependency")
		}
		seenDeps[dep.TaskID] = struct{}{}
	}
	seenFindings := make(map[string]struct{}, len(t.FindingIDs))
	for _, id := range t.FindingIDs {
		if !validID(id) {
			return invalid("finding_ids", "must contain valid UUIDs")
		}
		if _, exists := seenFindings[id]; exists {
			return invalid("finding_ids", "contains duplicate finding IDs")
		}
		seenFindings[id] = struct{}{}
	}
	for _, author := range t.Authors {
		if err := author.Validate(); err != nil {
			return fmt.Errorf("authors: %w", err)
		}
	}
	if t.PR != nil {
		if err := t.PR.Validate(); err != nil {
			return fmt.Errorf("pull_request: %w", err)
		}
	}
	return nil
}

// Validate checks the canonical pull request snapshot.
func (p PRBinding) Validate() error {
	if p.Number <= 0 || !shaPattern.MatchString(p.HeadSHA) || !shaPattern.MatchString(p.BaseSHA) || !nonempty(p.PolicyRevision) {
		return invalid("pull_request", "number, head/base SHAs, and policy revision are required")
	}
	u, err := url.Parse(p.URL)
	if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return invalid("pull_request.url", "must be a canonical GitHub pull request URL")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 4 || parts[2] != "pull" || parts[3] != fmt.Sprint(p.Number) {
		return invalid("pull_request.url", "must identify the bound owner, repository, and PR number")
	}
	for _, s := range parts {
		if !component(s) {
			return invalid("pull_request.url", "contains an invalid path component")
		}
	}
	return nil
}

// ValidateTask checks a task against its lifecycle envelope.
func ValidateTask(l Lifecycle, t Task) error {
	if err := l.Validate(); err != nil {
		return fmt.Errorf("lifecycle: %w", err)
	}
	if err := t.Validate(); err != nil {
		return fmt.Errorf("task: %w", err)
	}
	if t.LifecycleID != l.ID {
		return invalid("lifecycle_id", "does not match lifecycle")
	}
	if t.Stage == StageReview && (t.Correction != 0 || t.ID != l.DeliveryGateID) {
		return invalid("task_id", "review task must be the initial stable delivery gate")
	}
	if t.Correction > l.Limits.MaxCorrections {
		return invalid("correction", "exceeds lifecycle correction limit")
	}
	if t.PR != nil && t.PR.PolicyRevision != l.PolicyRevision {
		return invalid("pull_request.policy_revision", "does not match admitted lifecycle policy")
	}
	if t.PR != nil && !prMatchesRepository(*t.PR, l.Repository) {
		return invalid("pull_request.url", "does not match admitted repository")
	}
	switch t.Stage {
	case StageAuthor:
		if t.Correction != 0 || t.PR != nil {
			return invalid("stage", "author task starts without a PR or correction index")
		}
	case StageReview:
		if !hasDependency(t.Dependencies, DependencyHandoff) {
			return invalid("stage", "review requires a coding handoff dependency")
		}
	case StageFix:
		if t.PR == nil || t.Correction == 0 || len(t.FindingIDs) == 0 || !hasDependency(t.Dependencies, DependencyHandoff) {
			return invalid("stage", "fix requires PR binding, correction index, findings, and handoff dependency")
		}
	case StageRereview:
		if t.PR == nil || t.Correction == 0 || !hasDependency(t.Dependencies, DependencyHandoff) {
			return invalid("stage", "re-review requires PR binding, correction index, and handoff dependency")
		}
	}
	return nil
}

// Validate checks the observation shape without granting authority.
func (r Receipt) Validate() error {
	if r.Version != VersionV1 {
		return invalid("version", "unsupported receipt version")
	}
	if !validID(r.ID) || !validID(r.LifecycleID) || !validID(r.TaskID) {
		return invalid("receipt_id", "receipt, lifecycle, and task IDs must be lowercase non-zero UUIDs")
	}
	if err := r.Actor.Validate(); err != nil {
		return fmt.Errorf("actor: %w", err)
	}
	if !nonempty(r.PolicyRevision) || r.CreatedAt.IsZero() {
		return invalid("receipt", "policy revision and creation time are required")
	}
	if len(r.Detail) > maxReceiptDetailBytes {
		return invalid("detail", "exceeds 4096 bytes")
	}
	switch r.Outcome {
	case OutcomeCodingHandoff, OutcomeChangesRequest, OutcomeApproved, OutcomeMerged, OutcomeLanded, OutcomeFailed, OutcomeUnknown, OutcomeCancel:
	default:
		return invalid("outcome", "unknown receipt outcome")
	}
	if r.PR != nil {
		if err := r.PR.Validate(); err != nil {
			return fmt.Errorf("pull_request: %w", err)
		}
	}
	if err := validateFindingIDs(r.FindingIDs); err != nil {
		return err
	}
	if r.Outcome == OutcomeChangesRequest && len(r.FindingIDs) == 0 {
		return invalid("finding_ids", "changes requested requires at least one finding")
	}
	if r.Outcome != OutcomeChangesRequest && len(r.FindingIDs) != 0 {
		return invalid("finding_ids", "findings are only valid on changes-requested receipts")
	}
	if requiresSourceRevision(r.Outcome) && r.Actor.SourceRevision == "" {
		return invalid("actor.source_revision", "evidence outcome requires the authored revision")
	}
	if r.MergeCommit != "" && !shaPattern.MatchString(r.MergeCommit) {
		return invalid("merge_commit", "must be a lowercase 40-character Git SHA")
	}
	if r.LandedCommit != "" && !shaPattern.MatchString(r.LandedCommit) {
		return invalid("landed_commit", "must be a lowercase 40-character Git SHA")
	}
	if r.ObservedOutcome != "" && r.ObservedOutcome != OutcomeLanded && r.ObservedOutcome != OutcomeMerged && r.ObservedOutcome != OutcomeUnknown {
		return invalid("observed_outcome", "must be an observed merge, landing, or unknown")
	}
	if r.Outcome == OutcomeUnknown && (r.MergeCommit != "" || r.LandedCommit != "") {
		return invalid("outcome", "unknown cannot carry success evidence")
	}
	if r.Outcome == OutcomeMerged && r.MergeCommit == "" {
		return invalid("merge_commit", "merged receipt requires the confirmed merge commit")
	}
	if r.Outcome == OutcomeLanded && (r.MergeCommit == "" || r.LandedCommit == "") {
		return invalid("landed_commit", "landed receipt requires merge and landed commits")
	}
	if r.ObservedOutcome == OutcomeLanded && (r.MergeCommit == "" || r.LandedCommit == "") {
		return invalid("observed_outcome", "observed landing requires merge and landed commits")
	}
	if r.ObservedOutcome == OutcomeMerged && r.MergeCommit == "" {
		return invalid("observed_outcome", "observed merge requires merge commit")
	}
	if r.Outcome != OutcomeCancel && r.ObservedOutcome != "" {
		return invalid("observed_outcome", "only cancellation may carry late observational outcome")
	}
	if requiresPR(r.Outcome) && r.PR == nil {
		return invalid("pull_request", "this outcome requires an exact PR binding")
	}
	if requiresHeadEvidence(r) && r.PR != nil && r.Actor.SourceRevision != r.PR.HeadSHA {
		return invalid("actor.source_revision", "must equal the pull request head SHA for this evidence")
	}
	if r.ObservedOutcome != "" && r.PR == nil {
		return invalid("pull_request", "late observation requires an exact PR binding")
	}
	if r.LandedEvidence != nil {
		if r.Outcome != OutcomeLanded && r.ObservedOutcome != OutcomeLanded {
			return invalid("landed_evidence", "only a landing may carry host proof")
		}
		if err := validateLandedEvidenceShape(*r.LandedEvidence); err != nil {
			return err
		}
		if r.LandedEvidence.LifecycleID != r.LifecycleID || r.LandedEvidence.TaskID != r.TaskID || r.LandedEvidence.ReceiptID != r.ID {
			return invalid("landed_evidence", "host proof subject differs from receipt")
		}
	}
	return nil
}

// ValidateReceipt checks receipt identity, policy and revision binding.
func ValidateReceipt(l Lifecycle, t Task, r Receipt) error {
	if err := ValidateTask(l, t); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.LifecycleID != l.ID || r.TaskID != t.ID {
		return invalid("receipt", "does not match lifecycle and task")
	}
	if r.PolicyRevision != l.PolicyRevision {
		return invalid("policy_revision", "does not match admitted lifecycle policy")
	}
	if requiresPR(r.Outcome) && t.Stage != StageAuthor && t.PR == nil {
		return invalid("pull_request", "review outcome requires a populated task binding")
	}
	if r.PR != nil && t.PR != nil {
		if r.Outcome == OutcomeCodingHandoff && t.Stage == StageFix {
			if !samePRIdentity(*r.PR, *t.PR) {
				return invalid("pull_request", "fix handoff must retain the original PR identity and policy")
			}
		} else if *r.PR != *t.PR {
			return invalid("pull_request", "does not match task snapshot binding")
		}
	}
	if r.PR != nil && r.PR.PolicyRevision != l.PolicyRevision {
		return invalid("pull_request.policy_revision", "does not match admitted lifecycle policy")
	}
	if r.PR != nil && !prMatchesRepository(*r.PR, l.Repository) {
		return invalid("pull_request.url", "does not match admitted repository")
	}
	if r.Outcome == OutcomeCodingHandoff && t.Stage != StageAuthor && t.Stage != StageFix {
		return invalid("outcome", "coding handoff is only valid for author or fix tasks")
	}
	if (r.Outcome == OutcomeApproved || r.Outcome == OutcomeChangesRequest) && t.Stage != StageReview && t.Stage != StageRereview {
		return invalid("outcome", "review verdict requires review or re-review task")
	}
	if (r.Outcome == OutcomeMerged || r.Outcome == OutcomeLanded) && t.Stage != StageReview && t.Stage != StageRereview {
		return invalid("outcome", "delivery confirmation requires review or re-review task")
	}
	return nil
}

func hasDependency(deps []Dependency, kind DependencyKind) bool {
	for _, d := range deps {
		if d.Kind == kind {
			return true
		}
	}
	return false
}
func validateFindingIDs(ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if !validID(id) {
			return invalid("finding_ids", "must contain valid UUIDs")
		}
		if _, exists := seen[id]; exists {
			return invalid("finding_ids", "contains duplicate finding IDs")
		}
		seen[id] = struct{}{}
	}
	return nil
}
func samePRIdentity(a, b PRBinding) bool {
	return a.Number == b.Number && a.URL == b.URL && a.PolicyRevision == b.PolicyRevision
}
func prMatchesRepository(p PRBinding, repo Repository) bool {
	u, err := url.Parse(p.URL)
	if err != nil {
		return false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	return len(parts) == 4 && parts[0] == repo.Owner && parts[1] == repo.Name
}
func requiresPR(o Outcome) bool {
	return o == OutcomeCodingHandoff || o == OutcomeChangesRequest || o == OutcomeApproved || o == OutcomeMerged || o == OutcomeLanded
}
func requiresSourceRevision(o Outcome) bool {
	return o == OutcomeCodingHandoff || o == OutcomeChangesRequest || o == OutcomeApproved || o == OutcomeMerged || o == OutcomeLanded
}
func requiresHeadEvidence(r Receipt) bool {
	return requiresSourceRevision(r.Outcome) || r.ObservedOutcome == OutcomeMerged || r.ObservedOutcome == OutcomeLanded
}
func validID(s string) bool {
	return idPattern.MatchString(s) && s != "00000000-0000-0000-0000-000000000000"
}
func nonempty(s string) bool { return strings.TrimSpace(s) != "" }
func component(s string) bool {
	return nonempty(s) && s != "." && s != ".." && !strings.ContainsAny(s, "/\\?#%")
}
func safeTarget(s string) bool {
	return nonempty(s) && !strings.HasPrefix(s, "-") && !strings.ContainsAny(s, "\\?#%\x00\r\n")
}
func invalid(field, message string) error { return fmt.Errorf("invalid %s: %s", field, message) }
