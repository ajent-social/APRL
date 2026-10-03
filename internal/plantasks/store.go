package plantasks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ajent-social/APRL/internal/clock"
)

var (
	// ErrInvalidStore indicates missing or invalid storage dependencies.
	ErrInvalidStore = errors.New("invalid plan task store")
	// ErrNotFound reports a missing durable lifecycle.
	ErrNotFound = errors.New("plan lifecycle not found")
	// ErrConflict reports stale revision or a transition without new evidence.
	ErrConflict = errors.New("plan lifecycle revision conflict")
	// ErrReceiptAudit reports append-only receipt identity conflicts.
	ErrReceiptAudit = errors.New("plan receipt audit conflict")
	// ErrImmutable reports alteration of immutable lifecycle history.
	ErrImmutable = errors.New("plan lifecycle or task definition is immutable")
)

// Store persists plan lifecycle state and an append-only receipt audit.
// Update callbacks are trusted in-process code, never worker-supplied input.
type Store struct {
	pool  *pgxpool.Pool
	clock clock.Clock
}

// NewStore constructs a store over the APRL PostgreSQL pool.
func NewStore(pool *pgxpool.Pool, clk clock.Clock) (*Store, error) {
	if pool == nil || isNilDependency(clk) {
		return nil, fmt.Errorf("construct plan task store: %w", ErrInvalidStore)
	}
	return &Store{pool: pool, clock: clk}, nil
}

// Create durably creates a lifecycle. Replaying the exact same state is
// idempotent; reusing its lifecycle ID for different state is a conflict.
func (s *Store) Create(ctx context.Context, state State) error {
	if s == nil || s.pool == nil || isNilDependency(s.clock) || ctx == nil {
		return fmt.Errorf("create plan lifecycle: %w", ErrInvalidStore)
	}
	if err := state.ValidateAt(s.clock.Now().UTC()); err != nil {
		return fmt.Errorf("validate plan lifecycle: %w", err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode plan lifecycle: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin plan lifecycle create: %w", err)
	}
	defer rollback(tx)

	tag, err := tx.Exec(ctx, `INSERT INTO plan_lifecycles (id, revision, state)
		VALUES ($1::uuid, $2, $3::jsonb) ON CONFLICT (id) DO NOTHING`, state.Lifecycle.ID, state.Revision, payload)
	if err != nil {
		return fmt.Errorf("insert plan lifecycle: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var storedRevision int64
		var storedPayload []byte
		if err := tx.QueryRow(ctx, `SELECT revision, state FROM plan_lifecycles WHERE id=$1::uuid FOR UPDATE`, state.Lifecycle.ID).Scan(&storedRevision, &storedPayload); err != nil {
			return fmt.Errorf("read existing plan lifecycle: %w", err)
		}
		var stored State
		if err := decodeState(storedPayload, &stored); err != nil {
			return fmt.Errorf("decode existing plan lifecycle: %w", err)
		}
		if stored.Revision != storedRevision {
			return fmt.Errorf("existing lifecycle revision disagrees with state: %w", ErrConflict)
		}
		if equalJSONState(state, stored) {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit idempotent plan lifecycle create: %w", err)
			}
			return nil
		}
		return fmt.Errorf("lifecycle ID already has different state: %w", ErrConflict)
	}
	if err := insertReceipts(ctx, tx, state); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit plan lifecycle create: %w", err)
	}
	return nil
}

// Load returns a validated snapshot and verifies its embedded revision against
// the indexed revision column before exposing it to callers.
func (s *Store) Load(ctx context.Context, lifecycleID string) (State, error) {
	if s == nil || s.pool == nil || isNilDependency(s.clock) || ctx == nil || lifecycleID == "" {
		return State{}, fmt.Errorf("load plan lifecycle: %w", ErrInvalidStore)
	}
	var revision int64
	var payload []byte
	err := s.pool.QueryRow(ctx, `SELECT revision, state FROM plan_lifecycles WHERE id=$1::uuid`, lifecycleID).Scan(&revision, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return State{}, fmt.Errorf("load lifecycle %s: %w", lifecycleID, ErrNotFound)
	}
	if err != nil {
		return State{}, fmt.Errorf("load lifecycle %s: %w", lifecycleID, err)
	}
	var state State
	if err := decodeState(payload, &state); err != nil {
		return State{}, fmt.Errorf("decode lifecycle %s: %w", lifecycleID, err)
	}
	if state.Lifecycle.ID != lifecycleID || state.Revision != revision {
		return State{}, fmt.Errorf("lifecycle identity or revision mismatch: %w", ErrConflict)
	}
	if err := state.ValidateAt(s.clock.Now().UTC()); err != nil {
		return State{}, fmt.Errorf("validate stored lifecycle %s: %w", lifecycleID, err)
	}
	return state, nil
}

// Update applies one trusted transition under a row lock and optimistic
// revision check. Existing receipt records cannot be changed or removed.
func (s *Store) Update(ctx context.Context, lifecycleID string, expectedRevision int64, fn func(*State) error) error {
	if s == nil || s.pool == nil || isNilDependency(s.clock) || ctx == nil || lifecycleID == "" || expectedRevision < 0 || fn == nil {
		return fmt.Errorf("update plan lifecycle: %w", ErrInvalidStore)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin plan lifecycle update: %w", err)
	}
	defer rollback(tx)

	// Delegated mutations always lock the binding before the lifecycle.
	binding, err := lockDelegationForLifecycle(ctx, tx, lifecycleID)
	if err != nil {
		return err
	}

	var revision int64
	var payload []byte
	err = tx.QueryRow(ctx, `SELECT revision, state FROM plan_lifecycles WHERE id=$1::uuid FOR UPDATE`, lifecycleID).Scan(&revision, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("update lifecycle %s: %w", lifecycleID, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("lock lifecycle %s: %w", lifecycleID, err)
	}
	if binding == nil {
		// A delegated creation may have committed between the first binding
		// lookup and this lifecycle read. Never acquire its binding out of
		// order; abort so the caller retries with binding-first locks.
		var delegated bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM plan_delegations WHERE lifecycle_id=$1::uuid)`, lifecycleID).Scan(&delegated); err != nil {
			return fmt.Errorf("recheck standalone lifecycle binding: %w", err)
		}
		if delegated {
			return fmt.Errorf("delegation committed during lifecycle lookup: %w", ErrConflict)
		}
	}
	if revision != expectedRevision {
		return fmt.Errorf("expected revision %d, found %d: %w", expectedRevision, revision, ErrConflict)
	}
	var state State
	if err := decodeState(payload, &state); err != nil {
		return fmt.Errorf("decode lifecycle %s: %w", lifecycleID, err)
	}
	if state.Lifecycle.ID != lifecycleID || state.Revision != revision {
		return fmt.Errorf("lifecycle identity or revision mismatch: %w", ErrConflict)
	}
	if err := state.ValidateAt(s.clock.Now().UTC()); err != nil {
		return fmt.Errorf("validate stored lifecycle before transition: %w", err)
	}
	originalPayload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	var originalState State
	if err := decodeState(originalPayload, &originalState); err != nil {
		return fmt.Errorf("snapshot delegated lifecycle before transition: %w", err)
	}
	oldLifecycle := state.Lifecycle
	oldStartedAt := state.StartedAt
	oldCancelled := state.Cancelled
	oldAttempts := cloneAttempts(state.Attempts)
	oldClaims := cloneAdmissions(state.Claims)
	oldTasks, err := cloneTasks(state.Tasks)
	if err != nil {
		return fmt.Errorf("snapshot tasks before transition: %w", err)
	}
	oldReceipts, err := cloneReceipts(state.Receipts)
	if err != nil {
		return fmt.Errorf("snapshot receipt audit before transition: %w", err)
	}
	if err := fn(&state); err != nil {
		return fmt.Errorf("apply lifecycle transition: %w", err)
	}
	changedPayload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if bytes.Equal(originalPayload, changedPayload) {
		return fmt.Errorf("transition has no new durable evidence: %w", ErrConflict)
	}
	if !equalJSON(oldLifecycle, state.Lifecycle) {
		return fmt.Errorf("transition changed immutable lifecycle definition: %w", ErrImmutable)
	}
	if !oldStartedAt.Equal(state.StartedAt) || oldCancelled && !state.Cancelled {
		return fmt.Errorf("transition rewrote lifecycle start or reversed cancellation: %w", ErrImmutable)
	}
	if state.Revision != revision {
		return fmt.Errorf("transition changed revision directly: %w", ErrConflict)
	}
	if err := preserveReceipts(oldReceipts, state.Receipts); err != nil {
		return err
	}
	if err := preserveTasks(oldTasks, state.Tasks, oldReceipts, state.Receipts); err != nil {
		return err
	}
	if err := preserveAdmissions(oldAttempts, state.Attempts, oldClaims, state.Claims, oldReceipts, state.Receipts, revision, s.clock.Now().UTC(), oldStartedAt, state.Lifecycle); err != nil {
		return err
	}
	if err := fenceDelegationTransition(binding, originalState, state, s.clock.Now().UTC()); err != nil {
		return err
	}
	if revision == math.MaxInt64 {
		return fmt.Errorf("lifecycle revision exhausted: %w", ErrConflict)
	}
	state.Revision = revision + 1
	if err := state.ValidateAt(s.clock.Now().UTC()); err != nil {
		return fmt.Errorf("validate lifecycle transition: %w", err)
	}
	newPayload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode lifecycle transition: %w", err)
	}
	if err := insertNewReceipts(ctx, tx, lifecycleID, oldReceipts, state.Receipts); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE plan_lifecycles SET revision=$2, state=$3::jsonb WHERE id=$1::uuid AND revision=$4`, lifecycleID, state.Revision, newPayload, revision)
	if err != nil {
		return fmt.Errorf("persist lifecycle transition: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("persist lifecycle revision %d: %w", revision, ErrConflict)
	}
	if err := persistDelegationTransition(ctx, tx, binding, state); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit lifecycle transition: %w", err)
	}
	return nil
}

func insertReceipts(ctx context.Context, tx pgx.Tx, state State) error {
	for id, receipt := range state.Receipts {
		payload, err := json.Marshal(receipt)
		if err != nil {
			return fmt.Errorf("encode plan receipt %s: %w", id, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO plan_task_receipts (id, lifecycle_id, task_id, payload)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4::jsonb)`, id, state.Lifecycle.ID, receipt.TaskID, payload); err != nil {
			return fmt.Errorf("append plan receipt %s: %w", id, err)
		}
	}
	return nil
}

func insertNewReceipts(ctx context.Context, tx pgx.Tx, lifecycleID string, old, current map[string]Receipt) error {
	for id, receipt := range current {
		if _, exists := old[id]; exists {
			continue
		}
		payload, err := json.Marshal(receipt)
		if err != nil {
			return fmt.Errorf("encode new plan receipt %s: %w", id, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO plan_task_receipts (id, lifecycle_id, task_id, payload)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4::jsonb)`, id, lifecycleID, receipt.TaskID, payload); err != nil {
			return fmt.Errorf("append new plan receipt %s: %w", id, err)
		}
	}
	return nil
}

func preserveReceipts(old, current map[string]Receipt) error {
	for id, receipt := range old {
		newReceipt, exists := current[id]
		if !exists || !equalJSON(receipt, newReceipt) {
			return fmt.Errorf("historical plan receipt %s was changed or removed: %w", id, ErrReceiptAudit)
		}
	}
	return nil
}

func cloneReceipts(receipts map[string]Receipt) (map[string]Receipt, error) {
	clone := make(map[string]Receipt, len(receipts))
	for id, receipt := range receipts {
		payload, err := json.Marshal(receipt)
		if err != nil {
			return nil, err
		}
		var copied Receipt
		if err := json.Unmarshal(payload, &copied); err != nil {
			return nil, err
		}
		clone[id] = copied
	}
	return clone, nil
}

func cloneTasks(tasks map[string]Task) (map[string]Task, error) {
	clone := make(map[string]Task, len(tasks))
	for id, task := range tasks {
		payload, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		var copied Task
		if err := json.Unmarshal(payload, &copied); err != nil {
			return nil, err
		}
		clone[id] = copied
	}
	return clone, nil
}

func preserveTasks(old, current map[string]Task, oldReceipts, currentReceipts map[string]Receipt) error {
	for id, task := range old {
		updated, exists := current[id]
		if !exists {
			return fmt.Errorf("transition removed task %s: %w", id, ErrImmutable)
		}
		if equalJSON(task, updated) {
			continue
		}
		if !validHandoffPublication(task, updated, old, oldReceipts, currentReceipts) {
			return fmt.Errorf("transition rewrote task definition %s: %w", id, ErrImmutable)
		}
	}
	return nil
}

func validHandoffPublication(old, updated Task, allTasks map[string]Task, oldReceipts, currentReceipts map[string]Receipt) bool {
	if (old.Stage != StageReview && old.Stage != StageRereview) || updated.PR == nil || len(updated.Authors) < len(old.Authors)+1 {
		return false
	}
	if old.Stage == StageReview {
		if old.PR != nil || old.Correction != 0 {
			return false
		}
	} else if old.PR == nil || old.Correction <= 0 || !samePRIdentity(*old.PR, *updated.PR) {
		return false
	}
	candidate := old
	candidate.PR = updated.PR
	candidate.Authors = updated.Authors
	if !equalJSON(candidate, updated) {
		return false
	}
	for i, author := range old.Authors {
		if !equalJSON(author, updated.Authors[i]) {
			return false
		}
	}
	newAuthor := updated.Authors[len(updated.Authors)-1]
	for receiptID, receipt := range currentReceipts {
		if _, existed := oldReceipts[receiptID]; existed || receipt.Outcome != OutcomeCodingHandoff || receipt.PR == nil || !equalJSON(*receipt.PR, *updated.PR) || !equalJSON(receipt.Actor, newAuthor) {
			continue
		}
		if !hasHandoffDependency(old.Dependencies, receipt.TaskID) {
			continue
		}
		source, ok := allTasks[receipt.TaskID]
		if !ok {
			continue
		}
		if !equalJSON(updated.Authors, handoffAuthors(old.Authors, source.Authors, receipt.Actor)) {
			continue
		}
		if old.Stage == StageReview {
			if source.Stage == StageAuthor && source.Correction == 0 {
				return true
			}
			continue
		}
		if source.Stage != StageFix || source.Correction != old.Correction || source.PR == nil || !samePRIdentity(*old.PR, *source.PR) || !samePRIdentity(*source.PR, *receipt.PR) {
			continue
		}
		if hasTaskReceipt(old.ID, currentReceipts) {
			continue
		}
		return true
	}
	return false
}

func hasTaskReceipt(taskID string, receipts map[string]Receipt) bool {
	for _, receipt := range receipts {
		if receipt.TaskID == taskID {
			return true
		}
	}
	return false
}

func hasHandoffDependency(dependencies []Dependency, taskID string) bool {
	for _, dependency := range dependencies {
		if dependency.TaskID == taskID && dependency.Kind == DependencyHandoff {
			return true
		}
	}
	return false
}

func preserveAdmissions(oldAttempts, currentAttempts map[string]int, oldClaims, currentClaims map[string]Admission,
	oldReceipts, currentReceipts map[string]Receipt, revision int64, now, startedAt time.Time, lifecycle Lifecycle) error {
	keys := make(map[string]struct{}, len(oldAttempts)+len(currentAttempts))
	for id := range oldAttempts {
		keys[id] = struct{}{}
	}
	for id := range currentAttempts {
		keys[id] = struct{}{}
	}
	for id := range oldClaims {
		keys[id] = struct{}{}
	}
	for id := range currentClaims {
		keys[id] = struct{}{}
	}
	deadline := startedAt.Add(time.Duration(lifecycle.Limits.MaxDurationSeconds) * time.Second)
	for id := range keys {
		oldCount, currentCount := oldAttempts[id], currentAttempts[id]
		if currentCount < oldCount || currentCount > oldCount+1 {
			return fmt.Errorf("attempt count for task %s is not monotonic by one admission: %w", id, ErrImmutable)
		}
		oldClaim, hadClaim := oldClaims[id]
		newClaim, hasClaim := currentClaims[id]
		claimAddedOrReplaced := hasClaim && (!hadClaim || !equalJSON(oldClaim, newClaim))
		if claimAddedOrReplaced {
			if currentCount != oldCount+1 || newClaim.TaskID != id || newClaim.Revision != revision || newClaim.ClaimSHA == "" || newClaim.ActorID == "" || !now.Before(newClaim.ExpiresAt) || newClaim.ExpiresAt.After(deadline) {
				return fmt.Errorf("claim transition for task %s lacks one fresh bounded admission: %w", id, ErrImmutable)
			}
			if hadClaim && now.Before(oldClaim.ExpiresAt) {
				return fmt.Errorf("active claim for task %s was replaced: %w", id, ErrImmutable)
			}
		} else if currentCount != oldCount {
			return fmt.Errorf("attempt count for task %s changed without a new claim: %w", id, ErrImmutable)
		}
		if hadClaim && !hasClaim && now.Before(oldClaim.ExpiresAt) && !hasCompletionReceipt(id, oldReceipts, currentReceipts) {
			return fmt.Errorf("active claim for task %s was removed without a completion receipt: %w", id, ErrImmutable)
		}
	}
	return nil
}

func hasCompletionReceipt(taskID string, old, current map[string]Receipt) bool {
	for id, receipt := range current {
		if _, existed := old[id]; existed || receipt.TaskID != taskID {
			continue
		}
		if receipt.Outcome == OutcomeCodingHandoff || receipt.Outcome == OutcomeChangesRequest || receipt.Outcome == OutcomeLanded {
			return true
		}
	}
	return false
}

func cloneAttempts(attempts map[string]int) map[string]int {
	clone := make(map[string]int, len(attempts))
	for id, count := range attempts {
		clone[id] = count
	}
	return clone
}

func cloneAdmissions(admissions map[string]Admission) map[string]Admission {
	clone := make(map[string]Admission, len(admissions))
	for id, admission := range admissions {
		clone[id] = admission
	}
	return clone
}

func decodeState(payload []byte, state *State) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(state); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func equalJSONState(a, b State) bool { return equalJSON(a, b) }

func equalJSON(a, b any) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	return err == nil && bytes.Equal(left, right)
}

func rollback(tx pgx.Tx) {
	_ = tx.Rollback(context.Background())
}
