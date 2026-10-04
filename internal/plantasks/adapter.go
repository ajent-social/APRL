package plantasks

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"
)

var (
	// ErrInvalidAdapter indicates missing or invalid adapter dependencies.
	ErrInvalidAdapter = errors.New("invalid plan task adapter")
	// ErrActorDenied reports authentication or provenance mismatch.
	ErrActorDenied = errors.New("plan task actor authentication denied")
	// ErrClaimDenied reports fresh claim ownership denial.
	ErrClaimDenied = errors.New("plan task claim verification denied")
	// ErrRuntimeDenied reports current runtime authority denial.
	ErrRuntimeDenied = errors.New("plan task runtime policy denied")
	// ErrOutcomeDenied reports missing host-qualified outcome evidence.
	ErrOutcomeDenied = errors.New("plan task outcome verification denied")
)

// ClaimVerifier proves that the caller currently owns the exact external
// claim. Implementations must perform a fresh verification on every call.
type ClaimVerifier interface {
	// Verify must freshly look up the canonical ClaimTaskID(taskID) reference.
	// taskID remains the stable UUID in this in-process adapter contract.
	Verify(ctx context.Context, taskID, claimSHA, actorID string) error
}

// ActorAuthenticator returns the actual authenticated session principal. It
// must not derive identity from task, receipt, or request payload fields.
type ActorAuthenticator interface {
	Authenticate(ctx context.Context) (Provenance, error)
}

// RuntimePolicy binds authenticated actor identity to host-controlled runtime
// scope and grants. Implementations must not derive authority from task JSON.
type RuntimePolicy interface {
	Authorize(ctx context.Context, lifecycle Lifecycle, task Task, actor Provenance) error
}

// OutcomeVerifier proves submitted results using trusted host/broker receipts
// or current qualified remote facts. It must never treat worker JSON itself
// as authority. Callers should supply a bounded context for remote checks.
type OutcomeVerifier interface {
	Verify(ctx context.Context, lifecycle Lifecycle, task Task, receipt Receipt) error
}

// Adapter is the fenced seam between shared plan/apply code and lifecycle
// state. It performs no repository or runtime operations itself.
type Adapter struct {
	store    *Store
	claims   ClaimVerifier
	actors   ActorAuthenticator
	policy   RuntimePolicy
	outcomes OutcomeVerifier
}

// NewAdapter requires all host authority dependencies before exposing operations.
func NewAdapter(store *Store, claims ClaimVerifier, actors ActorAuthenticator, policy RuntimePolicy, outcomes OutcomeVerifier) (*Adapter, error) {
	if store == nil || store.pool == nil || store.clock == nil || isNilDependency(claims) || isNilDependency(actors) || isNilDependency(policy) || isNilDependency(outcomes) {
		return nil, fmt.Errorf("construct plan task adapter: %w", ErrInvalidAdapter)
	}
	return &Adapter{store: store, claims: claims, actors: actors, policy: policy, outcomes: outcomes}, nil
}

// Admit binds a currently authenticated actor to the exact externally-owned
// task claim. Claim ownership is checked just before the database update and
// again while the lifecycle row is locked. Denials never release a claim.
func (a *Adapter) Admit(ctx context.Context, lifecycleID, taskID, claimSHA string, expectedRevision int64, expiresAt time.Time) error {
	if err := a.validate(ctx, lifecycleID, taskID, claimSHA, expectedRevision); err != nil {
		return err
	}
	actor, err := a.authenticate(ctx)
	if err != nil {
		return err
	}
	if err := a.verifyClaim(ctx, taskID, claimSHA, actor.ActorID); err != nil {
		return err
	}
	expiresAt = expiresAt.UTC()
	return a.store.Update(ctx, lifecycleID, expectedRevision, func(state *State) error {
		lockedActor, err := a.authenticate(ctx)
		if err != nil {
			return err
		}
		if lockedActor != actor {
			return fmt.Errorf("authenticated actor changed during admission: %w", ErrActorDenied)
		}
		if err := a.verifyClaim(ctx, taskID, claimSHA, lockedActor.ActorID); err != nil {
			return err
		}
		if state.Lifecycle.ID != lifecycleID {
			return ErrNotFound
		}
		task, ok := state.Tasks[taskID]
		if !ok {
			return ErrNotFound
		}
		if err := a.authorize(ctx, state.Lifecycle, task, lockedActor); err != nil {
			return err
		}
		now := a.store.clock.Now().UTC()
		deadline := state.StartedAt.Add(time.Duration(state.Lifecycle.Limits.MaxDurationSeconds) * time.Second)
		if !expiresAt.After(now) || expiresAt.After(deadline) {
			return errors.New("claim expiry is outside lifecycle duration")
		}
		admission := Admission{TaskID: taskID, ClaimSHA: claimSHA, ActorID: lockedActor.ActorID, Revision: expectedRevision, ExpiresAt: expiresAt}
		return state.Admit(admission, now)
	})
}

// Result records an authenticated task receipt against the live claim. Actor
// authentication and claim ownership are refreshed before the transaction
// and while its row lock is held. This adapter never releases claims.
func (a *Adapter) Result(ctx context.Context, lifecycleID string, receipt Receipt, claimSHA string, expectedRevision int64) error {
	if err := a.validate(ctx, lifecycleID, receipt.TaskID, claimSHA, expectedRevision); err != nil {
		return err
	}
	if receipt.LifecycleID != lifecycleID {
		return errors.New("receipt lifecycle does not match requested lifecycle")
	}
	actor, err := a.authenticate(ctx)
	if err != nil {
		return err
	}
	if !sameAuthenticatedActor(actor, receipt.Actor) {
		return fmt.Errorf("receipt actor does not match authenticated session: %w", ErrActorDenied)
	}
	receipt.Actor = actor
	if receipt.LandedEvidence != nil {
		return fmt.Errorf("worker cannot supply host landing proof: %w", ErrOutcomeDenied)
	}
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf("validate plan task receipt: %w", err)
	}
	if err := a.verifyClaim(ctx, receipt.TaskID, claimSHA, actor.ActorID); err != nil {
		return err
	}
	verified, err := a.fetchLandingTransition(ctx, lifecycleID, claimSHA, expectedRevision, actor, &receipt)
	if err != nil {
		return err
	}
	return a.store.update(ctx, lifecycleID, expectedRevision, func(state *State) error {
		lockedActor, err := a.authenticate(ctx)
		if err != nil {
			return err
		}
		if lockedActor != actor || !sameAuthenticatedActor(lockedActor, receipt.Actor) {
			return fmt.Errorf("receipt actor does not match locked authenticated session: %w", ErrActorDenied)
		}
		if err := a.verifyClaim(ctx, receipt.TaskID, claimSHA, lockedActor.ActorID); err != nil {
			return err
		}
		if state.Lifecycle.ID != lifecycleID {
			return ErrNotFound
		}
		task, ok := state.Tasks[receipt.TaskID]
		if !ok {
			return ErrNotFound
		}
		if err := a.authorize(ctx, state.Lifecycle, task, lockedActor); err != nil {
			return err
		}
		if err := a.verifyOutcome(ctx, state.Lifecycle, task, receipt); err != nil {
			return err
		}
		now := a.store.clock.Now().UTC()
		if verified != nil {
			verifier, ok := a.outcomes.(LandingVerifier)
			if !ok || isNilDependency(verifier) || receipt.LandedEvidence == nil {
				return fmt.Errorf("trusted landing verifier disappeared: %w", ErrOutcomeDenied)
			}
			bounded, cancel := context.WithTimeout(ctx, landingOperationTimeout)
			defer cancel()
			if err := verifier.VerifyLanding(bounded, *state, task, receipt, verified.Evidence, a.store.clock.Now().UTC()); err != nil || bounded.Err() != nil {
				return fmt.Errorf("fresh local landing proof verification failed: %w", ErrOutcomeDenied)
			}
			now = a.store.clock.Now().UTC()
			if err := validateLandedEvidence(*state, receipt, verified.Evidence, now, false); err != nil {
				return fmt.Errorf("landing proof differs from current state: %w", ErrOutcomeDenied)
			}
		}
		return state.Record(receipt, claimSHA, now)
	}, verified)
}

// fetchLandingTransition obtains host proof outside database locks. Delegated
// landing requires typed evidence; legacy standalone verifiers remain compatible
// without claiming qualification for the external v1 delivery protocol.
func (a *Adapter) fetchLandingTransition(ctx context.Context, lifecycleID, claimSHA string, expectedRevision int64, actor Provenance, receipt *Receipt) (*verifiedLandingTransition, error) {
	if receipt.Outcome != OutcomeLanded {
		return nil, nil
	}
	bounded, cancel := context.WithTimeout(ctx, landingOperationTimeout)
	defer cancel()
	state, err := a.store.Load(bounded, lifecycleID)
	if err != nil {
		return nil, err
	}
	if state.Revision != expectedRevision {
		return nil, fmt.Errorf("landing snapshot revision changed: %w", ErrConflict)
	}
	now := a.store.clock.Now().UTC()
	if state.Cancelled || !now.Before(state.StartedAt.Add(time.Duration(state.Lifecycle.Limits.MaxDurationSeconds)*time.Second)) {
		return nil, fmt.Errorf("landing scope is terminal: %w", ErrClaimDenied)
	}
	claim, exists := state.Claims[receipt.TaskID]
	if !exists || claim.ClaimSHA != claimSHA || claim.ActorID != actor.ActorID || !now.Before(claim.ExpiresAt) {
		return nil, fmt.Errorf("landing lacks a live matching admission: %w", ErrClaimDenied)
	}
	task, exists := state.Tasks[receipt.TaskID]
	if !exists {
		return nil, ErrNotFound
	}
	if err := ValidateReceipt(state.Lifecycle, task, *receipt); err != nil {
		return nil, err
	}
	if err := a.authorize(bounded, state.Lifecycle, task, actor); err != nil {
		return nil, err
	}
	var delegated bool
	var exactExpiry string
	if err := a.store.pool.QueryRow(bounded, `SELECT EXISTS(SELECT 1 FROM plan_delegations WHERE lifecycle_id=$1::uuid),
		COALESCE((SELECT expires_at_exact FROM plan_delegations WHERE lifecycle_id=$1::uuid),'')`, lifecycleID).Scan(&delegated, &exactExpiry); err != nil {
		return nil, fmt.Errorf("resolve landing scope: %w", ErrOutcomeDenied)
	}
	if delegated {
		expires, parseErr := time.Parse(time.RFC3339Nano, exactExpiry)
		if parseErr != nil || !a.store.clock.Now().UTC().Before(expires) {
			return nil, fmt.Errorf("exact delegation scope expired: %w", ErrClaimDenied)
		}
	}
	verifier, qualified := a.outcomes.(LandingVerifier)
	if !qualified || isNilDependency(verifier) {
		if delegated {
			return nil, fmt.Errorf("delegated landing requires typed host proof: %w", ErrOutcomeDenied)
		}
		return nil, nil
	}
	evidence, err := verifier.FetchLanding(bounded, state, task, *receipt)
	if err != nil || bounded.Err() != nil {
		return nil, fmt.Errorf("fetch trusted landing proof: %w", ErrOutcomeDenied)
	}
	if err := validateLandedEvidence(state, *receipt, evidence, a.store.clock.Now().UTC(), false); err != nil {
		return nil, fmt.Errorf("validate trusted landing proof: %w", ErrOutcomeDenied)
	}
	receipt.LandedEvidence = &evidence
	return &verifiedLandingTransition{ReceiptID: receipt.ID, Evidence: evidence}, nil
}

// VisibleTask is a read-only projection for shared plan tooling. Ready is an
// advisory result from the durable state and current authenticated actor.
type VisibleTask struct {
	TaskID  string `json:"task_id"`
	ClaimID string `json:"claim_id"`
	// Stage identifies a lifecycle task role.
	Stage          Stage        `json:"stage"`
	Dependencies   []Dependency `json:"dependencies"`
	FindingIDs     []string     `json:"finding_ids,omitempty"`
	PRURL          string       `json:"pull_request_url,omitempty"`
	HeadSHA        string       `json:"head_sha,omitempty"`
	BaseSHA        string       `json:"base_sha,omitempty"`
	Ready          bool         `json:"ready"`
	DeliveryStatus string       `json:"delivery_status"`
	ReviewOutcome  string       `json:"review_outcome,omitempty"`
}

// ProviderSnapshot presents one consistent persisted revision to host
// provider/planning adapters. Every row is authorized against host runtime
// grants before any task metadata is returned.
type ProviderSnapshot struct {
	Version           int           `json:"version"`
	LifecycleID       string        `json:"lifecycle_id"`
	Revision          int64         `json:"revision"`
	PolicyRevision    string        `json:"policy_revision"`
	DeliveryGateID    string        `json:"delivery_gate_id"`
	DeliveryReceiptID string        `json:"delivery_receipt_id,omitempty"`
	GatePR            *PRBinding    `json:"gate_pr,omitempty"`
	Tasks             []VisibleTask `json:"tasks"`
}

// VisibleTasks lists stable task identities and immutable visible bindings.
// Readiness is advisory; Admit repeats all authorization and state checks.
func (a *Adapter) VisibleTasks(ctx context.Context, lifecycleID string) ([]VisibleTask, error) {
	snapshot, err := a.ProviderSnapshot(ctx, lifecycleID)
	if err != nil {
		return nil, err
	}
	return snapshot.Tasks, nil
}

// ProviderSnapshot loads lifecycle state once and returns an authorized projection
// from that exact revision. Any denied row fails the complete projection.
func (a *Adapter) ProviderSnapshot(ctx context.Context, lifecycleID string) (ProviderSnapshot, error) {
	if a == nil || a.store == nil || a.store.pool == nil || a.store.clock == nil || isNilDependency(a.actors) || isNilDependency(a.policy) || isNilDependency(a.outcomes) || ctx == nil || !validID(lifecycleID) {
		return ProviderSnapshot{}, fmt.Errorf("load provider plan snapshot: %w", ErrInvalidAdapter)
	}
	actor, err := a.authenticate(ctx)
	if err != nil {
		return ProviderSnapshot{}, err
	}
	state, err := a.store.Load(ctx, lifecycleID)
	if err != nil {
		return ProviderSnapshot{}, err
	}
	ids := make([]string, 0, len(state.Tasks))
	for id := range state.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := a.authorize(ctx, state.Lifecycle, state.Tasks[id], actor); err != nil {
			return ProviderSnapshot{}, err
		}
	}
	visible := make([]VisibleTask, 0, len(ids))
	now := a.store.clock.Now().UTC()
	delegatedReady, err := a.store.delegationReady(ctx, lifecycleID, state, now)
	if err != nil {
		return ProviderSnapshot{}, err
	}
	for _, id := range ids {
		task := state.Tasks[id]
		claimID, err := ClaimTaskID(id)
		if err != nil {
			return ProviderSnapshot{}, err
		}
		item := VisibleTask{TaskID: id, ClaimID: claimID, Stage: task.Stage, Dependencies: append([]Dependency{}, task.Dependencies...), FindingIDs: append([]string(nil), task.FindingIDs...),
			Ready: delegatedReady && state.Eligible(id, actor.ActorID, now) == nil, DeliveryStatus: deliveryStatus(state, id), ReviewOutcome: reviewOutcome(state, task)}
		if task.PR != nil {
			item.PRURL = task.PR.URL
			item.HeadSHA = task.PR.HeadSHA
			item.BaseSHA = task.PR.BaseSHA
		}
		visible = append(visible, item)
	}
	snapshot := ProviderSnapshot{Version: VersionV1, LifecycleID: state.Lifecycle.ID, Revision: state.Revision,
		PolicyRevision: state.Lifecycle.PolicyRevision, DeliveryGateID: state.Lifecycle.DeliveryGateID,
		DeliveryReceiptID: state.DeliveryReceiptID, Tasks: visible}
	if state.DeliveryReceiptID != "" && state.CurrentPR != nil {
		gatePR := *state.CurrentPR
		snapshot.GatePR = &gatePR
	}
	return snapshot, nil
}

// ClaimTaskID maps an internal task UUID to the claim-system namespace used
// by host claim registries. External verifiers must use this canonical key.
func ClaimTaskID(taskID string) (string, error) {
	if !validID(taskID) {
		return "", fmt.Errorf("invalid plan task claim ID: %w", ErrInvalidAdapter)
	}
	return "T-" + taskID, nil
}

func (a *Adapter) validate(ctx context.Context, lifecycleID, taskID, claimSHA string, expectedRevision int64) error {
	if a == nil || a.store == nil || a.store.pool == nil || a.store.clock == nil || isNilDependency(a.claims) || isNilDependency(a.actors) || isNilDependency(a.policy) || isNilDependency(a.outcomes) || ctx == nil || !validID(lifecycleID) || !validID(taskID) || claimSHA == "" || expectedRevision < 0 {
		return fmt.Errorf("validate plan task operation: %w", ErrInvalidAdapter)
	}
	return nil
}

func (a *Adapter) authorize(ctx context.Context, lifecycle Lifecycle, task Task, actor Provenance) error {
	if a == nil || isNilDependency(a.policy) {
		return fmt.Errorf("authorize plan task runtime scope: %w", ErrRuntimeDenied)
	}
	if err := a.policy.Authorize(ctx, lifecycle, task, actor); err != nil {
		return fmt.Errorf("authorize plan task runtime scope: %w: %v", ErrRuntimeDenied, err)
	}
	return nil
}

func (a *Adapter) verifyOutcome(ctx context.Context, lifecycle Lifecycle, task Task, receipt Receipt) error {
	if a == nil || isNilDependency(a.outcomes) {
		return fmt.Errorf("verify plan task outcome: %w", ErrOutcomeDenied)
	}
	if err := a.outcomes.Verify(ctx, lifecycle, task, receipt); err != nil {
		return fmt.Errorf("verify plan task outcome: %w: %v", ErrOutcomeDenied, err)
	}
	return nil
}

func (a *Adapter) authenticate(ctx context.Context) (Provenance, error) {
	if a == nil || isNilDependency(a.actors) {
		return Provenance{}, fmt.Errorf("authenticate plan task actor: %w", ErrActorDenied)
	}
	actor, err := a.actors.Authenticate(ctx)
	if err != nil {
		return Provenance{}, fmt.Errorf("authenticate plan task actor: %w: %v", ErrActorDenied, err)
	}
	if actor.Validate() != nil {
		return Provenance{}, fmt.Errorf("authenticated principal lacks valid provenance: %w", ErrActorDenied)
	}
	return actor, nil
}

func (a *Adapter) verifyClaim(ctx context.Context, taskID, claimSHA, actorID string) error {
	if a == nil || isNilDependency(a.claims) {
		return fmt.Errorf("verify plan task claim: %w", ErrClaimDenied)
	}
	if err := a.claims.Verify(ctx, taskID, claimSHA, actorID); err != nil {
		return fmt.Errorf("verify plan task claim: %w: %v", ErrClaimDenied, err)
	}
	return nil
}

func deliveryStatus(state State, taskID string) string {
	if taskID == state.Lifecycle.DeliveryGateID && state.DeliveryReceiptID != "" {
		return string(OutcomeLanded)
	}
	if outcome, ok := state.Completed[taskID]; ok {
		return string(outcome)
	}
	if state.Cancelled {
		return "cancelled"
	}
	if state.EscalationReason != "" {
		return "escalated"
	}
	return "pending"
}

func reviewOutcome(state State, task Task) string {
	if task.Stage != StageReview && task.Stage != StageRereview {
		return ""
	}
	if outcome, ok := state.Completed[task.ID]; ok {
		return string(outcome)
	}
	return string(latestReviewOutcome(state, task.ID))
}

func latestReviewOutcome(state State, taskID string) Outcome {
	var latest *Receipt
	for _, receipt := range state.Receipts {
		if receipt.TaskID != taskID || !visibleDeliveryOutcome(receipt.Outcome) {
			continue
		}
		if latest == nil || receipt.CreatedAt.After(latest.CreatedAt) || receipt.CreatedAt.Equal(latest.CreatedAt) && receipt.ID > latest.ID {
			receiptCopy := receipt
			latest = &receiptCopy
		}
	}
	if latest == nil {
		return ""
	}
	return latest.Outcome
}

func visibleDeliveryOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeChangesRequest, OutcomeApproved, OutcomeMerged, OutcomeLanded, OutcomeFailed, OutcomeUnknown, OutcomeCancel:
		return true
	default:
		return false
	}
}

func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func sameAuthenticatedActor(authenticated, submitted Provenance) bool {
	return authenticated.ActorID == submitted.ActorID && authenticated.ActorKind == submitted.ActorKind && authenticated.SourceRevision == submitted.SourceRevision
}
