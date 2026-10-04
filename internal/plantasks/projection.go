package plantasks

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// DeliveryV1Projector renders an authoritative binding and lifecycle snapshot
// into the bounded delivery/v1 observation without creating scheduler state.
type DeliveryV1Projector struct {
	store *Store
}

// NewDeliveryV1Projector constructs a projector over the canonical store.
func NewDeliveryV1Projector(store *Store) (*DeliveryV1Projector, error) {
	if store == nil || store.pool == nil || isNilDependency(store.clock) {
		return nil, ErrInvalidStore
	}
	return &DeliveryV1Projector{store: store}, nil
}

// Project reads one coherent delegated binding and projects only persisted
// task, receipt, accounting, and host-verified landing facts.
func (p *DeliveryV1Projector) Project(ctx context.Context, supplied DelegationBinding) (DeliveryV1Observation, error) {
	if p == nil || p.store == nil || ctx == nil || supplied.CallerID == "" || supplied.DelegationID == "" || supplied.LifecycleID == "" {
		return DeliveryV1Observation{}, ErrInvalidDelegation
	}
	bounded, cancel := context.WithTimeout(ctx, delegationOperationTimeout)
	defer cancel()
	tx, err := p.store.pool.BeginTx(bounded, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return DeliveryV1Observation{}, fmt.Errorf("begin delivery observation snapshot: %w", ErrDelegationUnavailable)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	current, err := scanDelegation(bounded, tx, supplied.CallerID, supplied.DelegationID, false)
	if err != nil {
		return DeliveryV1Observation{}, err
	}
	if err := tx.Commit(bounded); err != nil {
		return DeliveryV1Observation{}, fmt.Errorf("commit delivery observation snapshot: %w", ErrDelegationUnavailable)
	}
	if err := bounded.Err(); err != nil {
		return DeliveryV1Observation{}, fmt.Errorf("delivery observation exceeded deadline: %w", ErrDelegationUnavailable)
	}
	if !sameProjectionBinding(current, supplied) {
		return DeliveryV1Observation{}, ErrConflict
	}
	if current.State == nil || current.LifecycleID == "" || current.Sequence <= 0 || current.Status != DelegationAdmitted && current.Status != DelegationCancelled {
		return DeliveryV1Observation{}, ErrConflict
	}
	state := *current.State
	if state.Lifecycle.ID != current.LifecycleID || state.Revision != current.LifecycleRevision || state.Cancelled != current.Cancelled {
		return DeliveryV1Observation{}, ErrConflict
	}
	if err := state.Validate(); err != nil {
		return DeliveryV1Observation{}, fmt.Errorf("validate projection snapshot: %w", ErrDelegationConflict)
	}
	projectionTime := state.StartedAt.UTC()
	if state.ProjectionTime != nil {
		projectionTime = state.ProjectionTime.UTC()
	}
	request := current.Request
	observation := DeliveryV1Observation{
		Version: DeliveryV1ProtocolVersion, DelegationID: current.DelegationID,
		RequestDigest: current.RequestDigest, LifecycleID: current.LifecycleID,
		Sequence: uint64(current.Sequence), State: "admitted",
		Children:   make([]DeliveryV1ChildTask, 0, len(state.Tasks)),
		Accounting: DeliveryV1Accounting{},
	}
	ids := make([]string, 0, len(state.Tasks))
	for id := range state.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	children := make(map[string]DeliveryV1ChildTask, len(state.Tasks))
	for _, id := range ids {
		task := state.Tasks[id]
		child := projectDeliveryChild(state, task, projectionTime)
		children[id] = child
		observation.Children = append(observation.Children, child)
	}
	for id, child := range children {
		if _, exists := state.Completed[id]; exists || child.State == "canceled" {
			continue
		}
		if state.Cancelled {
			child.State = "canceled"
		} else if projectionTime.Before(request.Spec.Envelope.ExpiresAt) {
			if child.State == "pending" || child.State == "blocked" {
				if projectionDependenciesReady(state, state.Tasks[id], projectionTime) {
					child.State = "ready"
				} else {
					child.State = "blocked"
				}
			}
		}
		children[id] = child
	}
	observation.Children = observation.Children[:0]
	for _, id := range ids {
		observation.Children = append(observation.Children, children[id])
	}
	if state.Cancelled || current.Status == DelegationCancelled {
		observation.State = "canceled"
	}
	if state.EscalationReason != "" && observation.State != "canceled" {
		observation.State = "paused"
		observation.Reason = state.EscalationReason
	}
	if landed := deliveryProofForState(state); landed != nil {
		observation.State = "landed"
		observation.Landed = landed
		observation.Reason = ""
	}
	if observation.State != "landed" && observation.State != "canceled" {
		if !projectionTime.Before(request.Spec.Envelope.ExpiresAt) {
			observation.State = "unknown"
			observation.Reason = "scope_expired"
			for i := range observation.Children {
				if observation.Children[i].State != "completed" {
					observation.Children[i].State = "blocked"
				}
			}
		} else if hasUnknownReceipt(state) {
			observation.State = "unknown"
			observation.Reason = "outcome_unknown"
		} else if hasChangesRequested(state) {
			observation.State = "changes_requested"
		} else if hasActiveClaim(state, projectionTime) {
			observation.State = "running"
		}
	}
	if err := observation.Validate(request); err != nil {
		return DeliveryV1Observation{}, fmt.Errorf("validate delivery observation: %w: %w", ErrDelegationConflict, err)
	}
	if err := bounded.Err(); err != nil {
		return DeliveryV1Observation{}, fmt.Errorf("delivery observation exceeded deadline: %w", ErrDelegationUnavailable)
	}
	return observation, nil
}

func sameProjectionBinding(current, supplied DelegationBinding) bool {
	return current.CallerID == supplied.CallerID && current.DelegationID == supplied.DelegationID &&
		reflect.DeepEqual(current.Request, supplied.Request) && reflect.DeepEqual(current.RequestBytes, supplied.RequestBytes) &&
		current.RequestDigest == supplied.RequestDigest && current.Status == supplied.Status &&
		reflect.DeepEqual(current.Authorization, supplied.Authorization) && current.LifecycleID == supplied.LifecycleID &&
		current.LifecycleRevision == supplied.LifecycleRevision && current.Sequence == supplied.Sequence &&
		current.Cancelled == supplied.Cancelled && reflect.DeepEqual(current.State, supplied.State)
}

func projectDeliveryChild(state State, task Task, at time.Time) DeliveryV1ChildTask {
	kind := "author"
	switch task.Stage {
	case StageReview:
		kind = "review"
	case StageFix:
		kind = "fix"
	case StageRereview:
		kind = "re_review"
	}
	child := DeliveryV1ChildTask{ID: task.ID, Kind: kind, State: "pending", DependsOn: make([]string, 0, len(task.Dependencies)), FindingIDs: append([]string{}, task.FindingIDs...)}
	for _, dep := range task.Dependencies {
		child.DependsOn = append(child.DependsOn, dep.TaskID)
	}
	if task.PR != nil {
		child.PRURL = task.PR.URL
		child.HeadCommit = task.PR.HeadSHA
	}
	if _, complete := state.Completed[task.ID]; complete {
		if state.Completed[task.ID] == OutcomeChangesRequest {
			child.State = "blocked"
		} else {
			child.State = "completed"
		}
	} else if state.Cancelled {
		child.State = "canceled"
	} else if claim, active := state.Claims[task.ID]; active && at.Before(claim.ExpiresAt) {
		child.State = "running"
	} else if len(task.Dependencies) == 0 {
		child.State = "ready"
	}
	return child
}

func projectionDependenciesReady(state State, task Task, at time.Time) bool {
	attempts, err := delegationAttemptCount(state.Attempts)
	maxAttempts := int64(state.Lifecycle.Limits.MaxAttempts)
	if err != nil || attempts >= maxAttempts || state.Attempts[task.ID] >= state.Lifecycle.Limits.MaxAttempts {
		return false
	}
	active := 0
	for _, claim := range state.Claims {
		if at.Before(claim.ExpiresAt) {
			active++
		}
	}
	if active >= state.Lifecycle.Limits.MaxConcurrentTasks {
		return false
	}
	if len(task.Dependencies) == 0 {
		return true
	}
	for _, dependency := range task.Dependencies {
		outcome, complete := state.Completed[dependency.TaskID]
		if !complete {
			return false
		}
		if outcome == OutcomeChangesRequest && task.Stage != StageFix {
			return false
		}
		if outcome == OutcomeCancel || outcome == OutcomeUnknown || outcome == OutcomeFailed {
			return false
		}
	}
	return true
}

func hasUnknownReceipt(state State) bool {
	for _, receipt := range state.Receipts {
		if receipt.Outcome == OutcomeUnknown {
			if _, completed := state.Completed[receipt.TaskID]; completed {
				continue
			}
			return true
		}
	}
	return false
}

func hasChangesRequested(state State) bool {
	maxCorrection := -1
	var current Task
	for _, task := range state.Tasks {
		if task.Stage != StageReview && task.Stage != StageRereview || task.PR == nil || state.CurrentPR == nil || !reflect.DeepEqual(task.PR, state.CurrentPR) {
			continue
		}
		if task.Correction > maxCorrection {
			maxCorrection = task.Correction
			current = task
		}
	}
	return maxCorrection >= 0 && state.Completed[current.ID] == OutcomeChangesRequest
}

func hasActiveClaim(state State, at time.Time) bool {
	for _, claim := range state.Claims {
		if at.Before(claim.ExpiresAt) {
			return true
		}
	}
	return false
}

func deliveryProofForState(state State) *DeliveryV1LandedReceipt {
	if state.DeliveryReceiptID == "" {
		return nil
	}
	receipt, ok := state.Receipts[state.DeliveryReceiptID]
	if !ok || receipt.Outcome != OutcomeLanded || receipt.LandedEvidence == nil {
		return nil
	}
	proof := receipt.LandedEvidence.Receipt
	return &proof
}
