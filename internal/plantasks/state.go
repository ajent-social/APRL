package plantasks

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// Admission binds an externally acquired claim to one authenticated actor and
// one revision. It is evidence of local admission, never GitHub authority.
type Admission struct {
	TaskID    string    `json:"task_id"`
	ClaimSHA  string    `json:"claim_sha"`
	ActorID   string    `json:"actor_id"`
	Revision  int64     `json:"revision"`
	ExpiresAt time.Time `json:"expires_at"`
}

// State is the canonical, durable progression of one bounded lifecycle.
// Only trusted adapters may supply actors, repository observations and receipts.
type State struct {
	Lifecycle         Lifecycle            `json:"lifecycle"`
	Tasks             map[string]Task      `json:"tasks"`
	Receipts          map[string]Receipt   `json:"receipts"`
	Completed         map[string]Outcome   `json:"completed"`
	Claims            map[string]Admission `json:"claims"`
	Revision          int64                `json:"revision"`
	Cancelled         bool                 `json:"cancelled"`
	EscalationReason  string               `json:"escalation_reason,omitempty"`
	CurrentPR         *PRBinding           `json:"current_pr,omitempty"`
	DeliveryReceiptID string               `json:"delivery_receipt_id,omitempty"`
	Attempts          map[string]int       `json:"attempts"`
	StartedAt         time.Time            `json:"started_at"`
	// ProjectionTime pins observation derivation to persisted facts, not reads.
	ProjectionTime *time.Time `json:"projection_time,omitempty"`
	// LateLandedFacts retain trusted audits without completing delivery tasks.
	LateLandedFacts map[string]LateLandingFact `json:"late_landed_facts,omitempty"`
}

// Validate rejects inconsistent persisted graphs before they can be admitted.
func (s State) Validate() error {
	if err := s.Lifecycle.Validate(); err != nil {
		return err
	}
	if s.Revision < 0 || s.StartedAt.IsZero() || len(s.Tasks) == 0 {
		return errors.New("invalid lifecycle state")
	}
	if s.ProjectionTime != nil && (s.ProjectionTime.IsZero() || s.ProjectionTime.Location() != time.UTC || s.ProjectionTime.Before(s.StartedAt)) {
		return errors.New("invalid persisted projection time")
	}
	gate, exists := s.Tasks[s.Lifecycle.DeliveryGateID]
	if !exists || gate.Stage != StageReview {
		return errors.New("delivery gate missing or invalid")
	}
	if len(gate.Dependencies) != 1 || gate.Dependencies[0].Kind != DependencyHandoff || s.Tasks[gate.Dependencies[0].TaskID].Stage != StageAuthor {
		return errors.New("delivery gate lacks author handoff")
	}
	for id, t := range s.Tasks {
		if id != t.ID {
			return errors.New("task key mismatch")
		}
		if err := ValidateTask(s.Lifecycle, t); err != nil {
			return err
		}
		if t.Stage == StageAuthor && id != gate.Dependencies[0].TaskID {
			return errors.New("unrelated author in lifecycle")
		}
		for _, d := range t.Dependencies {
			if _, ok := s.Tasks[d.TaskID]; !ok {
				return errors.New("unknown dependency")
			}
		}
		if t.Stage == StageFix {
			if len(t.Dependencies) != 1 {
				return errors.New("fix lacks findings dependency")
			}
			d := t.Dependencies[0]
			prior, ok := s.Tasks[d.TaskID]
			if !ok || t.ID != derivedTaskID(prior.ID+":fix") || d.Kind != DependencyHandoff || !s.isDeliveryReview(prior.ID) || prior.Correction+1 != t.Correction {
				return errors.New("invalid fix lineage")
			}
			found := false
			for _, r := range s.Receipts {
				if r.TaskID == prior.ID && r.Outcome == OutcomeChangesRequest {
					if found || !reflect.DeepEqual(r.FindingIDs, t.FindingIDs) {
						return errors.New("fix findings differ from review")
					}
					found = true
				}
			}
			if !found {
				return errors.New("fix lacks recorded findings")
			}
		}
		if t.Stage == StageRereview {
			if !s.isDeliveryReview(t.ID) {
				return errors.New("invalid re-review lineage")
			}
			fix := s.Tasks[t.Dependencies[0].TaskID]
			if len(fix.Dependencies) != 1 || t.ID != derivedTaskID(fix.Dependencies[0].TaskID+":rereview") {
				return errors.New("noncanonical re-review")
			}
		}
		if t.Stage == StageReview || t.Stage == StageRereview {
			for _, d := range t.Dependencies {
				for _, r := range s.Receipts {
					if r.TaskID == d.TaskID && r.Outcome == OutcomeCodingHandoff {
						for _, author := range append(append([]Provenance(nil), s.Tasks[r.TaskID].Authors...), r.Actor) {
							if !containsAuthor(t.Authors, author) {
								return errors.New("review omits coding contributor")
							}
						}
					}
					if r.TaskID == d.TaskID && r.Outcome == OutcomeCodingHandoff && !reflect.DeepEqual(t.PR, r.PR) {
						return errors.New("review differs from authoritative handoff")
					}
				}
			}
		}
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] {
			return errors.New("cyclic task dependencies")
		}
		if done[id] {
			return nil
		}
		visiting[id] = true
		for _, d := range s.Tasks[id].Dependencies {
			if err := visit(d.TaskID); err != nil {
				return err
			}
		}
		delete(visiting, id)
		done[id] = true
		return nil
	}
	for id := range s.Tasks {
		if err := visit(id); err != nil {
			return err
		}
	}
	negativeVerdicts := map[string]bool{}
	for id, r := range s.Receipts {
		t, ok := s.Tasks[r.TaskID]
		if !ok || id != r.ID {
			return errors.New("receipt key or task mismatch")
		}
		if t.Actor.ActorID != "" && t.Actor.ActorID != r.Actor.ActorID {
			return errors.New("receipt violates assigned actor")
		}
		if r.Outcome == OutcomeChangesRequest {
			if negativeVerdicts[t.ID] {
				return errors.New("duplicate negative verdict")
			}
			negativeVerdicts[t.ID] = true
			if s.Completed[t.ID] != OutcomeChangesRequest {
				return errors.New("negative review lacks completion")
			}
			if t.Correction < s.Lifecycle.Limits.MaxCorrections {
				fix, fixOK := s.Tasks[derivedTaskID(t.ID+":fix")]
				review, reviewOK := s.Tasks[derivedTaskID(t.ID+":rereview")]
				if !fixOK || !reviewOK || fix.Stage != StageFix || review.Stage != StageRereview {
					return errors.New("negative review lacks canonical correction")
				}
			} else if s.EscalationReason == "" {
				return errors.New("exhausted correction lacks escalation")
			}
		}
		if err := ValidateReceipt(s.Lifecycle, t, r); err != nil {
			return err
		}
		if r.Outcome == OutcomeApproved || r.Outcome == OutcomeChangesRequest {
			if s.authoredBy(r.Actor.ActorID, t.Correction) {
				return errors.New("persisted self approval")
			}
			for _, author := range t.Authors {
				if author.ActorID == r.Actor.ActorID {
					return errors.New("persisted author verdict")
				}
			}
			for _, other := range s.Receipts {
				if other.Outcome == OutcomeCodingHandoff && s.Tasks[other.TaskID].Correction <= t.Correction && other.Actor.ActorID == r.Actor.ActorID {
					return errors.New("persisted authored approval")
				}
			}
		}
		if r.Outcome == OutcomeMerged || r.Outcome == OutcomeLanded {
			predecessor := OutcomeApproved
			if r.Outcome == OutcomeLanded {
				predecessor = OutcomeMerged
			}
			supported := false
			for _, prior := range s.Receipts {
				if prior.TaskID == r.TaskID && prior.Outcome == predecessor && prior.Actor.ActorID == r.Actor.ActorID && reflect.DeepEqual(prior.PR, r.PR) && !prior.CreatedAt.After(r.CreatedAt) && (r.Outcome != OutcomeLanded || prior.MergeCommit == r.MergeCommit) {
					supported = true
				}
			}
			if !supported {
				return errors.New("persisted delivery lacks predecessor evidence")
			}
		}
	}
	for id := range s.Completed {
		if _, ok := s.Tasks[id]; !ok {
			return errors.New("unknown completed task")
		}
		outcome := s.Completed[id]
		if outcome != OutcomeCodingHandoff && outcome != OutcomeChangesRequest && outcome != OutcomeLanded {
			return errors.New("invalid completion outcome")
		}
		supported := false
		for _, r := range s.Receipts {
			if r.TaskID == id && r.Outcome == outcome {
				supported = true
			}
		}
		if !supported {
			return errors.New("completion lacks authoritative receipt")
		}
	}
	for id, count := range s.Attempts {
		if _, ok := s.Tasks[id]; !ok || count < 0 || count > s.Lifecycle.Limits.MaxAttempts {
			return errors.New("invalid attempt counter")
		}
	}
	latestCorrection := -1
	var latestPR *PRBinding
	handoffTasks := map[string]bool{}
	for _, r := range s.Receipts {
		if r.Outcome != OutcomeCodingHandoff {
			continue
		}
		if handoffTasks[r.TaskID] {
			return errors.New("duplicate task handoff")
		}
		handoffTasks[r.TaskID] = true
		correction := s.Tasks[r.TaskID].Correction
		if correction == latestCorrection && !reflect.DeepEqual(latestPR, r.PR) {
			return errors.New("conflicting current handoffs")
		}
		if correction > latestCorrection {
			latestCorrection = correction
			latestPR = r.PR
		}
	}
	if !reflect.DeepEqual(s.CurrentPR, latestPR) {
		return errors.New("current PR is not latest authoritative handoff")
	}
	for id, outcome := range s.Completed {
		if outcome == OutcomeLanded && s.isDeliveryReview(id) && s.DeliveryReceiptID == "" {
			return errors.New("delivered gate lacks receipt pointer")
		}
	}
	if s.DeliveryReceiptID != "" {
		r, ok := s.Receipts[s.DeliveryReceiptID]
		if !ok || r.Outcome != OutcomeLanded || !s.isDeliveryReview(r.TaskID) || s.Completed[r.TaskID] != OutcomeLanded || !reflect.DeepEqual(r.PR, s.CurrentPR) {
			return errors.New("delivery lacks verified corrective lineage")
		}
	}
	for id, c := range s.Claims {
		if id != c.TaskID || c.ActorID == "" || c.ClaimSHA == "" || c.ExpiresAt.IsZero() {
			return errors.New("invalid claim admission")
		}
		if task := s.Tasks[id]; task.Actor.ActorID != "" && task.Actor.ActorID != c.ActorID {
			return errors.New("claim violates assigned actor")
		}
		if c.Revision < 0 || c.Revision > s.Revision || s.Attempts[id] <= 0 {
			return errors.New("admission lacks valid attempt")
		}
		if c.ExpiresAt.After(s.StartedAt.Add(time.Duration(s.Lifecycle.Limits.MaxDurationSeconds) * time.Second)) {
			return errors.New("admission exceeds lifecycle deadline")
		}
		if _, ok := s.Tasks[id]; !ok {
			return errors.New("claim task missing")
		}
	}
	return validateLandingExtensions(s)
}

// ValidateAt adds clock-dependent invariants while leaving historical state
// readable after its deadline. Expired claims do not count as live execution.
func (s State) ValidateAt(now time.Time) error {
	if now.IsZero() {
		return errors.New("missing validation clock")
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if s.ProjectionTime != nil && s.ProjectionTime.After(now) {
		return errors.New("persisted projection time is in the future")
	}
	active := 0
	for _, claim := range s.Claims {
		if now.Before(claim.ExpiresAt) {
			active++
		}
	}
	if active > s.Lifecycle.Limits.MaxConcurrentTasks {
		return errors.New("active admissions exceed concurrency envelope")
	}
	return nil
}

// isDeliveryReview proves a bounded corrective chain reaches the stable gate.
func (s State) isDeliveryReview(id string) bool {
	seen := map[string]bool{}
	for {
		if seen[id] {
			return false
		}
		seen[id] = true
		t, ok := s.Tasks[id]
		if !ok {
			return false
		}
		if id == s.Lifecycle.DeliveryGateID {
			return t.Stage == StageReview && t.Correction == 0
		}
		if t.Stage != StageRereview || len(t.Dependencies) != 1 {
			return false
		}
		dep := t.Dependencies[0]
		fix, ok := s.Tasks[dep.TaskID]
		if !ok || dep.Kind != DependencyHandoff || fix.Stage != StageFix || fix.Correction != t.Correction || len(fix.Dependencies) != 1 {
			return false
		}
		parent := fix.Dependencies[0]
		prior, ok := s.Tasks[parent.TaskID]
		if !ok || parent.Kind != DependencyHandoff || prior.Correction+1 != fix.Correction || s.Completed[prior.ID] != OutcomeChangesRequest {
			return false
		}
		id = prior.ID
	}
}

// Eligible tests readiness using authoritative outcomes, not plan checkboxes.
func (s State) Eligible(taskID, actorID string, now time.Time) error {
	t, ok := s.Tasks[taskID]
	if !ok {
		return errors.New("unknown task")
	}
	if t.Actor.ActorID != "" && t.Actor.ActorID != actorID {
		return errors.New("task assigned to another actor")
	}
	if s.Cancelled || s.EscalationReason != "" || actorID == "" {
		return errors.New("lifecycle cancelled or actor absent")
	}
	if !now.Before(s.StartedAt.Add(time.Duration(s.Lifecycle.Limits.MaxDurationSeconds) * time.Second)) {
		return errors.New("lifecycle duration exhausted")
	}
	if _, done := s.Completed[taskID]; done {
		return errors.New("task already completed")
	}
	if c, ok := s.Claims[taskID]; ok && now.Before(c.ExpiresAt) {
		return errors.New("task already admitted")
	}
	for _, d := range t.Dependencies {
		o := s.Completed[d.TaskID]
		if string(d.Kind) == "landed" {
			if string(o) != "landed" && (d.TaskID != s.Lifecycle.DeliveryGateID || s.DeliveryReceiptID == "") {
				return errors.New("dependency not landed")
			}
		}
		if string(d.Kind) == "handoff" {
			if string(o) != "coding_handoff" && string(o) != "changes_requested" {
				return errors.New("dependency not handed off")
			}
		}
	}
	if string(t.Stage) == "review" || string(t.Stage) == "rereview" {
		if t.PR == nil {
			return errors.New("review lacks PR handoff")
		}
		if s.CurrentPR == nil || !reflect.DeepEqual(*t.PR, *s.CurrentPR) {
			return errors.New("review snapshot is stale")
		}
		for _, r := range s.Receipts {
			if r.Outcome == OutcomeCodingHandoff && s.Tasks[r.TaskID].Correction <= t.Correction && r.Actor.ActorID == actorID {
				return errors.New("authored revision cannot self review")
			}
		}
		if s.authoredBy(actorID, t.Correction) {
			return errors.New("self review denied")
		}
		for _, a := range t.Authors {
			if actorID == a.ActorID {
				return errors.New("self review denied")
			}
		}
	}
	return nil
}

// Admit binds the winning claim. The caller must verify claim ownership and
// actor authentication; a repository-provided actor string is insufficient.
func (s *State) Admit(a Admission, now time.Time) error {
	if a.Revision != s.Revision || !now.Before(a.ExpiresAt) || a.ClaimSHA == "" || a.ExpiresAt.After(s.StartedAt.Add(time.Duration(s.Lifecycle.Limits.MaxDurationSeconds)*time.Second)) {
		return errors.New("stale admission")
	}
	if err := s.Eligible(a.TaskID, a.ActorID, now); err != nil {
		return err
	}
	active := 0
	for _, c := range s.Claims {
		if now.Before(c.ExpiresAt) {
			active++
		}
	}
	if active >= s.Lifecycle.Limits.MaxConcurrentTasks {
		return errors.New("concurrency exhausted")
	}
	if s.Attempts[a.TaskID] >= s.Lifecycle.Limits.MaxAttempts {
		return errors.New("attempt limit exhausted")
	}
	if s.Attempts == nil {
		s.Attempts = map[string]int{}
	}
	s.Attempts[a.TaskID]++
	if s.Claims == nil {
		s.Claims = map[string]Admission{}
	}
	s.Claims[a.TaskID] = a
	return nil
}

// Record retains exact idempotent receipt replay and refuses stale actor/claim
// results. Approval and merge do not complete a delivery task; landing does.
func (s *State) Record(r Receipt, claimSHA string, now time.Time) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	var candidate State
	if err := json.Unmarshal(data, &candidate); err != nil {
		return err
	}
	if err := candidate.record(r, claimSHA, now); err != nil {
		return err
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	*s = candidate
	return nil
}

func (s *State) record(r Receipt, claimSHA string, now time.Time) error {
	if r.Outcome != OutcomeCancel && r.Outcome != OutcomeUnknown && (s.Cancelled || !now.Before(s.StartedAt.Add(time.Duration(s.Lifecycle.Limits.MaxDurationSeconds)*time.Second))) {
		return errors.New("result cannot advance cancelled or expired lifecycle")
	}
	if old, ok := s.Receipts[r.ID]; ok {
		if reflect.DeepEqual(old, r) {
			return nil
		}
		return errors.New("receipt replay conflict")
	}
	t, ok := s.Tasks[r.TaskID]
	if !ok {
		return errors.New("unknown receipt task")
	}
	if err := ValidateReceipt(s.Lifecycle, t, r); err != nil {
		return err
	}
	if r.CreatedAt.After(now) {
		return errors.New("receipt timestamp is in future")
	}
	if r.PR != nil {
		copyPR := *r.PR
		r.PR = &copyPR
	}
	c, ok := s.Claims[t.ID]
	if !ok || c.ClaimSHA != claimSHA || c.ActorID != r.Actor.ActorID || !now.Before(c.ExpiresAt) {
		return errors.New("receipt lacks live admitted claim")
	}
	if s.Cancelled && string(r.Outcome) != "cancel" && string(r.Outcome) != "unknown" {
		return errors.New("cancelled lifecycle cannot progress")
	}
	if !now.Before(s.StartedAt.Add(time.Duration(s.Lifecycle.Limits.MaxDurationSeconds)*time.Second)) && r.Outcome != OutcomeCancel && r.Outcome != OutcomeUnknown {
		return errors.New("lifecycle deadline exhausted")
	}
	if (r.Outcome == OutcomeChangesRequest || r.Outcome == OutcomeApproved || r.Outcome == OutcomeMerged || r.Outcome == OutcomeLanded) && !reflect.DeepEqual(r.PR, s.CurrentPR) {
		return errors.New("current snapshot differs")
	}
	if r.Outcome != OutcomeCodingHandoff && r.PR != nil && t.PR != nil && !reflect.DeepEqual(*r.PR, *t.PR) {
		return errors.New("stale PR snapshot")
	}
	has := func(outcome string) bool {
		for _, prior := range s.Receipts {
			if prior.TaskID == t.ID && string(prior.Outcome) == outcome && reflect.DeepEqual(prior.PR, r.PR) && prior.Actor.ActorID == r.Actor.ActorID {
				return true
			}
		}
		return false
	}
	switch string(r.Outcome) {
	case "cancel":
		s.Cancelled = true
	case "merged":
		if !has("approved") {
			return errors.New("merge without current approval")
		}
	case "landed":
		if !has("merged") {
			return errors.New("landing without confirmed merge")
		}
	case "approved":
		for _, prior := range s.Receipts {
			if prior.Outcome == OutcomeCodingHandoff && s.Tasks[prior.TaskID].Correction <= t.Correction && prior.Actor.ActorID == r.Actor.ActorID {
				return errors.New("authored revision cannot self review")
			}
		}
		if s.authoredBy(r.Actor.ActorID, t.Correction) {
			return errors.New("self review denied")
		}
		for _, a := range t.Authors {
			if r.Actor.ActorID == a.ActorID {
				return errors.New("self review denied")
			}
		}
	case "coding_handoff":
		copyPR := *r.PR
		s.CurrentPR = &copyPR
		// PR publication is committed together with visible review readiness.
		for id, next := range s.Tasks {
			for _, d := range next.Dependencies {
				if d.TaskID == t.ID && string(d.Kind) == "handoff" && (string(next.Stage) == "review" || string(next.Stage) == "rereview") {
					copyPR := *r.PR
					next.PR = &copyPR
					next.Authors = handoffAuthors(next.Authors, t.Authors, r.Actor)
					s.Tasks[id] = next
				}
			}
		}
	}
	if s.Receipts == nil {
		s.Receipts = map[string]Receipt{}
	}
	s.Receipts[r.ID] = r
	if s.Completed == nil {
		s.Completed = map[string]Outcome{}
	}
	if string(r.Outcome) == "coding_handoff" || string(r.Outcome) == "changes_requested" || string(r.Outcome) == "landed" {
		s.Completed[t.ID] = r.Outcome
		if r.Outcome == OutcomeLanded && s.isDeliveryReview(t.ID) {
			s.DeliveryReceiptID = r.ID
		}
		delete(s.Claims, t.ID)
	}
	if r.Outcome == OutcomeChangesRequest {
		if t.Correction >= s.Lifecycle.Limits.MaxCorrections {
			s.EscalationReason = "correction limit exhausted"
			return nil
		}
		fix, review := correctionTasks(t, r)
		if err := s.AddCorrection(t.ID, fix, review); err != nil {
			return err
		}
	}
	return nil
}

func correctionTasks(parent Task, r Receipt) (Task, Task) {
	fix := Task{Version: VersionV1, ID: derivedTaskID(parent.ID + ":fix"), LifecycleID: parent.LifecycleID, Stage: StageFix, Dependencies: []Dependency{{TaskID: parent.ID, Kind: DependencyHandoff}}, Authors: append([]Provenance(nil), parent.Authors...), Correction: parent.Correction + 1, FindingIDs: append([]string(nil), r.FindingIDs...), PR: r.PR, CreatedAt: r.CreatedAt}
	review := fix
	review.ID = derivedTaskID(parent.ID + ":rereview")
	review.Stage = StageRereview
	review.Dependencies = []Dependency{{TaskID: fix.ID, Kind: DependencyHandoff}}
	return fix, review
}

func derivedTaskID(key string) string {
	b := sha256.Sum256([]byte("aprl.plan.v1:" + key))
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// AddCorrection records visible bounded fix/re-review children. It never makes
// the negative review a successful delivery dependency.
func (s *State) AddCorrection(reviewID string, fix, review Task) error {
	if s.Cancelled || string(s.Completed[reviewID]) != "changes_requested" {
		return errors.New("correction lacks findings outcome")
	}
	original, ok := s.Tasks[reviewID]
	if !ok {
		return errors.New("review missing")
	}
	if !s.isDeliveryReview(reviewID) {
		return errors.New("correction not attached to delivery gate")
	}
	var recordedFindings []string
	for _, r := range s.Receipts {
		if r.TaskID == reviewID && r.Outcome == OutcomeChangesRequest {
			recordedFindings = r.FindingIDs
		}
	}
	if !reflect.DeepEqual(recordedFindings, fix.FindingIDs) {
		return errors.New("fix does not cover recorded findings")
	}
	if fix.Correction != original.Correction+1 || review.Correction != fix.Correction || fix.Correction > s.Lifecycle.Limits.MaxCorrections {
		return errors.New("correction envelope exhausted")
	}
	if string(fix.Stage) != "fix" || string(review.Stage) != "rereview" || len(fix.FindingIDs) == 0 || !reflect.DeepEqual(fix.FindingIDs, review.FindingIDs) {
		return errors.New("invalid corrective tasks")
	}
	if len(fix.Dependencies) != 1 || fix.Dependencies[0].TaskID != reviewID || fix.Dependencies[0].Kind != DependencyHandoff {
		return errors.New("fix must depend on findings handoff")
	}
	if !reflect.DeepEqual(fix.PR, original.PR) || !reflect.DeepEqual(review.PR, original.PR) {
		return errors.New("correction snapshot differs from findings")
	}
	if _, ok := s.Tasks[fix.ID]; ok {
		if reflect.DeepEqual(s.Tasks[fix.ID], fix) && reflect.DeepEqual(s.Tasks[review.ID], review) {
			return nil
		}
		return errors.New("duplicate fix")
	}
	for _, existing := range s.Tasks {
		if existing.Stage == StageFix && len(existing.Dependencies) == 1 && existing.Dependencies[0].TaskID == reviewID {
			return errors.New("review already has corrective tasks")
		}
	}
	if _, ok := s.Tasks[review.ID]; ok {
		return errors.New("duplicate review")
	}
	if err := ValidateTask(s.Lifecycle, fix); err != nil {
		return err
	}
	if err := ValidateTask(s.Lifecycle, review); err != nil {
		return err
	}
	for _, d := range fix.Dependencies {
		if d.TaskID == reviewID && string(d.Kind) == "landed" {
			return errors.New("fix cannot depend on blocked delivery")
		}
	}
	if len(review.Dependencies) != 1 || review.Dependencies[0].TaskID != fix.ID || string(review.Dependencies[0].Kind) != "handoff" {
		return fmt.Errorf("re-review must depend on fix handoff")
	}
	s.Tasks[fix.ID] = fix
	s.Tasks[review.ID] = review
	return nil
}

func containsAuthor(authors []Provenance, actor Provenance) bool {
	for _, author := range authors {
		if reflect.DeepEqual(author, actor) {
			return true
		}
	}
	return false
}

func handoffAuthors(previous, declared []Provenance, actor Provenance) []Provenance {
	authors := append([]Provenance(nil), previous...)
	for _, contributor := range declared {
		if !containsAuthor(authors, contributor) {
			authors = append(authors, contributor)
		}
	}
	return append(authors, actor)
}

func (s State) authoredBy(actorID string, correction int) bool {
	if actorID == s.Lifecycle.Authored.ActorID {
		return true
	}
	for _, task := range s.Tasks {
		if task.Correction > correction || (task.Stage != StageAuthor && task.Stage != StageFix) {
			continue
		}
		for _, author := range task.Authors {
			if author.ActorID == actorID {
				return true
			}
		}
	}
	for _, receipt := range s.Receipts {
		if receipt.Outcome == OutcomeCodingHandoff && s.Tasks[receipt.TaskID].Correction <= correction && receipt.Actor.ActorID == actorID {
			return true
		}
	}
	return false
}
