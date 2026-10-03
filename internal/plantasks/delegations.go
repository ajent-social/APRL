package plantasks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrInvalidDelegation reports missing or invalid service dependencies.
	ErrInvalidDelegation = errors.New("invalid delegation service")
	// ErrDelegationUnauthenticated reports an absent or mismatched trusted caller.
	ErrDelegationUnauthenticated = errors.New("delegation caller is not authenticated")
	// ErrDelegationConflict reports a changed request or stale durable binding.
	ErrDelegationConflict = errors.New("delegation request conflicts with stored binding")
	// ErrDelegationUnavailable reports policy or storage uncertainty.
	ErrDelegationUnavailable = errors.New("delegation policy or storage unavailable")
)

// DeliveryV1CallerAuthenticator obtains caller identity from trusted request context.
type DeliveryV1CallerAuthenticator interface {
	AuthenticatedCaller(context.Context) (string, error)
}

// DeliveryV1Authorization is a caller- and request-bound policy decision.
type DeliveryV1Authorization struct {
	Allowed        bool   `json:"allowed"`
	CallerID       string `json:"caller_id"`
	RequestDigest  string `json:"request_digest"`
	PolicyRevision string `json:"policy_revision"`
	GrantRevision  string `json:"grant_revision"`
	Reason         string `json:"reason,omitempty"`
}

// DeliveryV1AdmissionPolicy performs external authorization and local revalidation.
type DeliveryV1AdmissionPolicy interface {
	Authorize(context.Context, string, DeliveryV1Request) (DeliveryV1Authorization, error)
	Verify(context.Context, DeliveryV1Request, DeliveryV1Authorization, time.Time) error
}

// DeliveryV1InitialStateFactory constructs the host-trusted initial lifecycle.
type DeliveryV1InitialStateFactory interface {
	InitialState(DeliveryV1Request, DeliveryV1Authorization, time.Time) (State, error)
}

// DelegationStatus is the durable admission decision state.
type DelegationStatus string

const (
	// DelegationIntent is a persisted request awaiting a terminal policy decision.
	DelegationIntent DelegationStatus = "intent"
	// DelegationDenied is a durable terminal policy denial.
	DelegationDenied DelegationStatus = "denied"
	// DelegationAdmitted has one bound internal lifecycle.
	DelegationAdmitted DelegationStatus = "admitted"
	// DelegationCancelled is a request cancelled before lifecycle creation.
	DelegationCancelled DelegationStatus = "cancelled"
)

// DelegationBinding is the authenticated durable request identity and latest lifecycle snapshot.
type DelegationBinding struct {
	CallerID          string
	DelegationID      string
	Request           DeliveryV1Request
	RequestBytes      []byte
	RequestDigest     string
	Status            DelegationStatus
	Authorization     DeliveryV1Authorization
	LifecycleID       string
	LifecycleRevision int64
	Sequence          int64
	Cancelled         bool
	State             *State
}

// Delegations binds authenticated caller requests to canonical task lifecycles.
type Delegations struct {
	store   *Store
	auth    DeliveryV1CallerAuthenticator
	policy  DeliveryV1AdmissionPolicy
	factory DeliveryV1InitialStateFactory
}

// NewDelegations constructs a fail-closed durable delegation service.
func NewDelegations(store *Store, auth DeliveryV1CallerAuthenticator, policy DeliveryV1AdmissionPolicy, factory DeliveryV1InitialStateFactory) (*Delegations, error) {
	if store == nil || store.pool == nil || isNilDependency(store.clock) || isNilDependency(auth) || isNilDependency(policy) || isNilDependency(factory) {
		return nil, fmt.Errorf("construct delivery delegation store: %w", ErrInvalidDelegation)
	}
	return &Delegations{store: store, auth: auth, policy: policy, factory: factory}, nil
}

const delegationOperationTimeout = 10 * time.Second

func (d *Delegations) boundedContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if d == nil || d.store == nil || ctx == nil {
		return nil, nil, fmt.Errorf("invalid delegation operation: %w", ErrInvalidDelegation)
	}
	bounded, cancel := context.WithTimeout(ctx, delegationOperationTimeout)
	return bounded, cancel, nil
}

// Submit persists an idempotent intent before policy I/O and atomically decides admission.
func (d *Delegations) Submit(ctx context.Context, raw []byte) (DelegationBinding, error) {
	bounded, cancel, err := d.boundedContext(ctx)
	if err != nil {
		return DelegationBinding{}, err
	}
	defer cancel()
	callerID, err := d.auth.AuthenticatedCaller(bounded)
	if err != nil || !deliveryV1ValidID(callerID) {
		return DelegationBinding{}, fmt.Errorf("authenticate delegation caller: %w", ErrDelegationUnauthenticated)
	}
	request, err := DecodeDeliveryV1Request(raw)
	if err != nil {
		return DelegationBinding{}, err
	}
	if request.CallerID != callerID {
		return DelegationBinding{}, fmt.Errorf("request caller differs from authenticated caller: %w", ErrDelegationUnauthenticated)
	}
	requestBytes, err := EncodeDeliveryV1Request(request)
	if err != nil {
		return DelegationBinding{}, err
	}
	digestBytes := sha256.Sum256(requestBytes)
	digest := hex.EncodeToString(digestBytes[:])
	binding, err := d.persistIntent(bounded, callerID, request, requestBytes, digest)
	if err != nil {
		return DelegationBinding{}, err
	}
	if binding.Status != DelegationIntent || binding.Cancelled {
		return binding, nil
	}
	if request.Spec.Envelope.MaxAttempts < 2 || !d.store.clock.Now().UTC().Before(request.Spec.Envelope.ExpiresAt) {
		reason := "host policy requires capacity for independent review"
		if !d.store.clock.Now().UTC().Before(request.Spec.Envelope.ExpiresAt) {
			reason = "request expired before authorization"
		}
		denial := DeliveryV1Authorization{CallerID: callerID, RequestDigest: digest, PolicyRevision: request.Spec.PolicyRevision, Reason: reason}
		return d.finishDecision(bounded, callerID, request, requestBytes, digest, denial, nil)
	}

	authorization, err := d.policy.Authorize(bounded, callerID, request)
	if err != nil {
		if current, loadErr := d.getForCaller(bounded, callerID, request.DelegationID); loadErr == nil && current.Status != DelegationIntent {
			return current, nil
		}
		return DelegationBinding{}, fmt.Errorf("authorize delegation: %w", ErrDelegationUnavailable)
	}
	if authorization.Allowed && !d.store.clock.Now().UTC().Before(request.Spec.Envelope.ExpiresAt) {
		authorization.Allowed = false
		authorization.GrantRevision = ""
		authorization.Reason = "request expired before lifecycle construction"
	}
	if err := validateDelegationAuthorization(callerID, digest, request.Spec.PolicyRevision, authorization); err != nil {
		return DelegationBinding{}, err
	}
	var initial *State
	if authorization.Allowed {
		now := d.store.clock.Now().UTC()
		created, createErr := d.factory.InitialState(request, authorization, now)
		if createErr != nil {
			return DelegationBinding{}, fmt.Errorf("construct trusted initial lifecycle: %w", ErrDelegationUnavailable)
		}
		if err := validateDelegationInitialState(request, authorization, created, now); err != nil {
			return DelegationBinding{}, fmt.Errorf("validate trusted initial lifecycle: %w", ErrDelegationUnavailable)
		}
		encoded, err := json.Marshal(created)
		if err != nil {
			return DelegationBinding{}, fmt.Errorf("clone trusted initial lifecycle: %w", ErrDelegationUnavailable)
		}
		var cloned State
		if err := json.Unmarshal(encoded, &cloned); err != nil {
			return DelegationBinding{}, fmt.Errorf("clone trusted initial lifecycle: %w", ErrDelegationUnavailable)
		}
		initial = &cloned
	}
	result, err := d.finishDecision(bounded, callerID, request, requestBytes, digest, authorization, initial)
	if err != nil {
		if current, loadErr := d.getForCaller(bounded, callerID, request.DelegationID); loadErr == nil && current.Status != DelegationIntent {
			return current, nil
		}
		return DelegationBinding{}, err
	}
	return result, nil
}

func validateDelegationAuthorization(callerID, digest, requestedPolicyRevision string, authorization DeliveryV1Authorization) error {
	if authorization.CallerID != callerID || authorization.RequestDigest != digest || authorization.PolicyRevision != requestedPolicyRevision || strings.TrimSpace(authorization.PolicyRevision) == "" || len(authorization.PolicyRevision) > deliveryV1MaxIDLength || len(authorization.GrantRevision) > deliveryV1MaxIDLength || len(authorization.Reason) > deliveryV1MaxReasonLength {
		return fmt.Errorf("policy decision is not bound to caller and request: %w", ErrDelegationUnavailable)
	}
	if authorization.Allowed && strings.TrimSpace(authorization.GrantRevision) == "" {
		return fmt.Errorf("allow decision lacks grant revision: %w", ErrDelegationUnavailable)
	}
	return nil
}

func validateDelegationInitialState(request DeliveryV1Request, authorization DeliveryV1Authorization, state State, now time.Time) error {
	if !authorization.Allowed || state.Revision != 0 || state.Cancelled || state.EscalationReason != "" || state.CurrentPR != nil || state.DeliveryReceiptID != "" || len(state.Tasks) != 2 || len(state.Receipts) != 0 || len(state.Completed) != 0 || len(state.Claims) != 0 || len(state.Attempts) != 0 {
		return errors.New("factory did not produce a pristine two-task lifecycle")
	}
	if state.StartedAt.IsZero() || state.StartedAt.After(now) || !state.Lifecycle.CreatedAt.Equal(state.StartedAt) || state.Lifecycle.Limits.MaxAttempts < 2 || state.Lifecycle.Limits.MaxAttempts > request.Spec.Envelope.MaxAttempts || state.Lifecycle.Limits.MaxConcurrentTasks > request.Spec.Envelope.MaxConcurrent || state.Lifecycle.Limits.MaxConcurrentTasks < 1 {
		return errors.New("factory lifecycle exceeds or differs from delegated limits")
	}
	parts := strings.Split(strings.TrimPrefix(request.Spec.Repository, "https://github.com/"), "/")
	if len(parts) != 2 || state.Lifecycle.Repository.Owner != parts[0] || state.Lifecycle.Repository.Name != parts[1] || state.Lifecycle.Repository.Target != request.Spec.TargetBranch || state.Lifecycle.PolicyRevision != request.Spec.PolicyRevision || authorization.PolicyRevision != request.Spec.PolicyRevision || state.Lifecycle.Authored.SourceRevision != request.Spec.SourceCommit {
		return errors.New("factory lifecycle does not match delegated repository and policy scope")
	}
	var author, review *Task
	for _, task := range state.Tasks {
		switch task.Stage {
		case StageAuthor:
			if author != nil || task.Correction != 0 {
				return errors.New("factory lifecycle has invalid author task")
			}
			taskCopy := task
			author = &taskCopy
		case StageReview:
			if review != nil || task.Correction != 0 || task.ID != state.Lifecycle.DeliveryGateID {
				return errors.New("factory lifecycle has invalid review gate")
			}
			taskCopy := task
			review = &taskCopy
		default:
			return errors.New("factory lifecycle starts beyond required review gate")
		}
	}
	if author == nil || review == nil || len(review.Dependencies) != 1 || review.Dependencies[0].TaskID != author.ID || review.Dependencies[0].Kind != DependencyHandoff {
		return errors.New("factory lifecycle lacks mandatory author and review graph")
	}
	if err := state.ValidateAt(now); err != nil {
		return err
	}
	remaining := request.Spec.Envelope.ExpiresAt.Sub(state.StartedAt)
	if remaining <= 0 {
		return errors.New("lifecycle starts after caller expiry")
	}
	maxDurationSeconds := int64(remaining / time.Second)
	if remaining%time.Second != 0 {
		maxDurationSeconds++
	}
	if int64(state.Lifecycle.Limits.MaxDurationSeconds) > maxDurationSeconds {
		return errors.New("lifecycle duration exceeds rounded caller duration")
	}
	return nil
}

func (d *Delegations) persistIntent(ctx context.Context, callerID string, request DeliveryV1Request, requestBytes []byte, digest string) (DelegationBinding, error) {
	scope, err := json.Marshal(request)
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("encode immutable delegation scope: %w", err)
	}
	envelope, err := json.Marshal(request.Spec.Envelope)
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("encode delegation envelope: %w", err)
	}
	acceptance, err := json.Marshal(request.Spec.Acceptance)
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("encode delegation acceptance: %w", err)
	}
	var tx pgx.Tx
	tx, err = d.store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("begin delegation intent: %w", ErrDelegationUnavailable)
	}
	defer rollback(tx)
	_, err = tx.Exec(ctx, `INSERT INTO plan_delegations (
		caller_id, delegation_id, request_bytes, request_digest, request_scope,
		project_id, job_id, task_id, plan_revision, plan_digest, repository, target_branch, source_commit,
		acceptance, policy_revision, profile_revision, execution_mode, budget_envelope,
		max_concurrent, max_attempts, expires_at_exact, status, observation_sequence)
		VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15,$16,$17,$18::jsonb,$19,$20,$21,'intent',1)
		ON CONFLICT (caller_id, delegation_id) DO NOTHING`,
		callerID, request.DelegationID, requestBytes, digest, scope, request.ProjectID, request.JobID, request.TaskID,
		int64(request.PlanRevision), request.PlanDigest, request.Spec.Repository, request.Spec.TargetBranch, request.Spec.SourceCommit,
		acceptance, request.Spec.PolicyRevision, request.Spec.ProfileRevision, request.Spec.ExecutionMode, envelope,
		request.Spec.Envelope.MaxConcurrent, int64(request.Spec.Envelope.MaxAttempts), request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano))
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("persist delegation intent: %w", ErrDelegationUnavailable)
	}
	binding, err := scanDelegation(ctx, tx, callerID, request.DelegationID, true)
	if err != nil {
		return DelegationBinding{}, err
	}
	if binding.RequestDigest != digest || !bytes.Equal(binding.RequestBytes, requestBytes) {
		return DelegationBinding{}, fmt.Errorf("delegation key has changed request: %w", ErrDelegationConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return DelegationBinding{}, fmt.Errorf("commit delegation intent: %w", ErrDelegationUnavailable)
	}
	return binding, nil
}

func scanDelegation(ctx context.Context, tx pgx.Tx, callerID, delegationID string, forUpdate bool) (DelegationBinding, error) {
	query := `SELECT request_bytes, request_digest, request_scope, max_attempts, max_concurrent, expires_at_exact, status, COALESCE(admission_decision, 'null'::jsonb), COALESCE(lifecycle_id::text,''), COALESCE(lifecycle_revision,-1), observation_sequence, cancelled
		FROM plan_delegations WHERE caller_id=$1 AND delegation_id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var binding DelegationBinding
	var rawAuthorization []byte
	var storedScope []byte
	var storedMaxAttempts int64
	var storedMaxConcurrent int
	var storedExpiry string
	var lifeRevision int64
	binding.CallerID, binding.DelegationID = callerID, delegationID
	if err := tx.QueryRow(ctx, query, callerID, delegationID).Scan(&binding.RequestBytes, &binding.RequestDigest, &storedScope, &storedMaxAttempts, &storedMaxConcurrent, &storedExpiry, &binding.Status, &rawAuthorization, &binding.LifecycleID, &lifeRevision, &binding.Sequence, &binding.Cancelled); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DelegationBinding{}, fmt.Errorf("read delegation binding: %w", ErrNotFound)
		}
		return DelegationBinding{}, fmt.Errorf("read delegation binding: %w", ErrDelegationUnavailable)
	}
	request, err := DecodeDeliveryV1Request(binding.RequestBytes)
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("stored delegation request is malformed: %w", ErrDelegationConflict)
	}
	canonical, err := EncodeDeliveryV1Request(request)
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("stored delegation request is invalid: %w", ErrDelegationConflict)
	}
	digest := sha256.Sum256(canonical)
	scopedRequest, scopeErr := DecodeDeliveryV1Request(storedScope)
	scopedCanonical, canonicalErr := EncodeDeliveryV1Request(scopedRequest)
	expectedExpiry := request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano)
	if request.CallerID != callerID || request.DelegationID != delegationID || !bytes.Equal(canonical, binding.RequestBytes) || hex.EncodeToString(digest[:]) != binding.RequestDigest || binding.Sequence <= 0 || scopeErr != nil || canonicalErr != nil || !bytes.Equal(scopedCanonical, canonical) || storedMaxAttempts != int64(request.Spec.Envelope.MaxAttempts) || storedMaxConcurrent != request.Spec.Envelope.MaxConcurrent || storedExpiry != expectedExpiry {
		return DelegationBinding{}, fmt.Errorf("stored delegation identity, scope, or digest mismatch: %w", ErrDelegationConflict)
	}
	binding.Request = request
	if !bytes.Equal(rawAuthorization, []byte("null")) {
		if err := json.Unmarshal(rawAuthorization, &binding.Authorization); err != nil {
			return DelegationBinding{}, fmt.Errorf("decode delegation authorization: %w", ErrDelegationConflict)
		}
	}
	if binding.Status == DelegationAdmitted {
		if binding.LifecycleID == "" || lifeRevision < 0 {
			return DelegationBinding{}, fmt.Errorf("admitted delegation lacks lifecycle binding: %w", ErrDelegationConflict)
		}
		var payload []byte
		var revision int64
		if err := tx.QueryRow(ctx, `SELECT revision,state FROM plan_lifecycles WHERE id=$1::uuid`, binding.LifecycleID).Scan(&revision, &payload); err != nil {
			return DelegationBinding{}, fmt.Errorf("read delegated lifecycle: %w", ErrDelegationUnavailable)
		}
		var state State
		if err := decodeState(payload, &state); err != nil || state.Lifecycle.ID != binding.LifecycleID || state.Revision != revision || revision != lifeRevision {
			return DelegationBinding{}, fmt.Errorf("delegation lifecycle revision is incoherent: %w", ErrDelegationConflict)
		}
		if err := state.Validate(); err != nil {
			return DelegationBinding{}, fmt.Errorf("stored delegated lifecycle is invalid: %w", ErrDelegationConflict)
		}
		if state.Cancelled != binding.Cancelled || !delegationStateMatchesRequest(request, binding.Authorization, state) {
			return DelegationBinding{}, fmt.Errorf("delegation and lifecycle scope or cancellation disagree: %w", ErrDelegationConflict)
		}
		binding.LifecycleRevision = revision
		binding.State = &state
	} else if binding.LifecycleID != "" || lifeRevision >= 0 {
		return DelegationBinding{}, fmt.Errorf("non-admitted delegation has lifecycle: %w", ErrDelegationConflict)
	}
	return binding, nil
}

// Get returns a caller-scoped durable binding and its coherent lifecycle state.
func (d *Delegations) Get(ctx context.Context, delegationID string) (DelegationBinding, error) {
	bounded, cancel, err := d.boundedContext(ctx)
	if err != nil {
		return DelegationBinding{}, err
	}
	defer cancel()
	callerID, err := d.auth.AuthenticatedCaller(bounded)
	if err != nil || !deliveryV1ValidID(callerID) {
		return DelegationBinding{}, fmt.Errorf("authenticate delegation caller: %w", ErrDelegationUnauthenticated)
	}
	return d.getForCaller(bounded, callerID, delegationID)
}

func (d *Delegations) getForCaller(ctx context.Context, callerID, delegationID string) (DelegationBinding, error) {
	if !deliveryV1ValidID(delegationID) {
		return DelegationBinding{}, fmt.Errorf("invalid delegation ID: %w", ErrDeliveryV1Invalid)
	}
	tx, err := d.store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("begin delegation read: %w", ErrDelegationUnavailable)
	}
	defer rollback(tx)
	binding, err := scanDelegation(ctx, tx, callerID, delegationID, false)
	if err != nil {
		return DelegationBinding{}, err
	}
	if binding.State != nil {
		if err := binding.State.ValidateAt(d.store.clock.Now().UTC()); err != nil {
			return DelegationBinding{}, fmt.Errorf("validate delegated lifecycle snapshot: %w", ErrDelegationConflict)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return DelegationBinding{}, fmt.Errorf("commit delegation read: %w", ErrDelegationUnavailable)
	}
	return binding, nil
}

func (d *Delegations) finishDecision(ctx context.Context, callerID string, request DeliveryV1Request, requestBytes []byte, digest string, authorization DeliveryV1Authorization, initial *State) (DelegationBinding, error) {
	tx, err := d.store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("begin delegation decision: %w", ErrDelegationUnavailable)
	}
	defer rollback(tx)
	binding, err := scanDelegation(ctx, tx, callerID, request.DelegationID, true)
	if err != nil {
		return DelegationBinding{}, err
	}
	if binding.RequestDigest != digest || !bytes.Equal(binding.RequestBytes, requestBytes) {
		return DelegationBinding{}, fmt.Errorf("delegation changed during policy evaluation: %w", ErrDelegationConflict)
	}
	if binding.Status != DelegationIntent || binding.Cancelled {
		if err := tx.Commit(ctx); err != nil {
			return DelegationBinding{}, fmt.Errorf("commit concurrent delegation decision: %w", ErrDelegationUnavailable)
		}
		return binding, nil
	}
	now := d.store.clock.Now().UTC()
	if !now.Before(request.Spec.Envelope.ExpiresAt) {
		authorization.Allowed = false
		authorization.GrantRevision = ""
		authorization.Reason = "request expired before admission"
	}
	if authorization.Allowed {
		if err := d.policy.Verify(ctx, request, authorization, now); err != nil {
			return DelegationBinding{}, fmt.Errorf("revalidate delegation authorization: %w", ErrDelegationUnavailable)
		}
		now = d.store.clock.Now().UTC()
		if !now.Before(request.Spec.Envelope.ExpiresAt) {
			authorization.Allowed = false
			authorization.GrantRevision = ""
			authorization.Reason = "request expired during admission verification"
		}
	}
	if !authorization.Allowed {
		encodedAuthorization, err := json.Marshal(authorization)
		if err != nil {
			return DelegationBinding{}, fmt.Errorf("encode denied delegation decision: %w", ErrDelegationUnavailable)
		}
		tag, err := tx.Exec(ctx, `UPDATE plan_delegations SET status='denied', admission_decision=$3::jsonb, observation_sequence=observation_sequence+1, updated_at=CURRENT_TIMESTAMP
			WHERE caller_id=$1 AND delegation_id=$2 AND status='intent' AND cancelled=FALSE`, callerID, request.DelegationID, encodedAuthorization)
		if err != nil || tag.RowsAffected() != 1 {
			return DelegationBinding{}, fmt.Errorf("persist denied delegation: %w", ErrDelegationUnavailable)
		}
	} else {
		if initial == nil {
			return DelegationBinding{}, fmt.Errorf("allow decision lacks initial state: %w", ErrDelegationUnavailable)
		}
		if err := validateDelegationInitialState(request, authorization, *initial, now); err != nil {
			return DelegationBinding{}, fmt.Errorf("revalidate initial lifecycle: %w", ErrDelegationUnavailable)
		}
		if err := initial.ValidateAt(now); err != nil {
			return DelegationBinding{}, fmt.Errorf("revalidate initial lifecycle at decision time: %w", ErrDelegationUnavailable)
		}
		if initial.Lifecycle.ID == "" {
			return DelegationBinding{}, fmt.Errorf("factory lifecycle ID is empty: %w", ErrDelegationUnavailable)
		}
		if _, err := tx.Exec(ctx, `SAVEPOINT delegated_lifecycle_creation`); err != nil {
			return DelegationBinding{}, fmt.Errorf("savepoint delegated lifecycle creation: %w", ErrDelegationUnavailable)
		}
		payload, err := json.Marshal(initial)
		if err != nil {
			return DelegationBinding{}, fmt.Errorf("encode initial lifecycle: %w", ErrDelegationUnavailable)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO plan_lifecycles(id,revision,state) VALUES($1::uuid,$2,$3::jsonb)`, initial.Lifecycle.ID, initial.Revision, payload); err != nil {
			return DelegationBinding{}, fmt.Errorf("insert delegated lifecycle: %w", ErrDelegationUnavailable)
		}
		if err := insertReceipts(ctx, tx, *initial); err != nil {
			return DelegationBinding{}, fmt.Errorf("insert initial delegated receipts: %w", ErrDelegationUnavailable)
		}
		if !d.store.clock.Now().UTC().Before(request.Spec.Envelope.ExpiresAt) {
			if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT delegated_lifecycle_creation`); err != nil {
				return DelegationBinding{}, fmt.Errorf("remove expired delegated lifecycle: %w", ErrDelegationUnavailable)
			}
			if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT delegated_lifecycle_creation`); err != nil {
				return DelegationBinding{}, fmt.Errorf("release expired lifecycle savepoint: %w", ErrDelegationUnavailable)
			}
			authorization.Allowed = false
			authorization.GrantRevision = ""
			authorization.Reason = "request expired during lifecycle persistence"
			encodedAuthorization, err := json.Marshal(authorization)
			if err != nil {
				return DelegationBinding{}, fmt.Errorf("encode expired delegation decision: %w", ErrDelegationUnavailable)
			}
			tag, err := tx.Exec(ctx, `UPDATE plan_delegations SET status='denied', admission_decision=$3::jsonb, observation_sequence=observation_sequence+1, updated_at=CURRENT_TIMESTAMP
				WHERE caller_id=$1 AND delegation_id=$2 AND status='intent' AND cancelled=FALSE`, callerID, request.DelegationID, encodedAuthorization)
			if err != nil || tag.RowsAffected() != 1 {
				return DelegationBinding{}, fmt.Errorf("persist expiry denial: %w", ErrDelegationUnavailable)
			}
		} else {
			if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT delegated_lifecycle_creation`); err != nil {
				return DelegationBinding{}, fmt.Errorf("release delegated lifecycle savepoint: %w", ErrDelegationUnavailable)
			}
			encodedAuthorization, err := json.Marshal(authorization)
			if err != nil {
				return DelegationBinding{}, fmt.Errorf("encode admitted delegation decision: %w", ErrDelegationUnavailable)
			}
			tag, err := tx.Exec(ctx, `UPDATE plan_delegations SET status='admitted', admission_decision=$3::jsonb, lifecycle_id=$4::uuid, lifecycle_revision=$5, observation_sequence=observation_sequence+1, updated_at=CURRENT_TIMESTAMP
				WHERE caller_id=$1 AND delegation_id=$2 AND status='intent' AND cancelled=FALSE`, callerID, request.DelegationID, encodedAuthorization, initial.Lifecycle.ID, initial.Revision)
			if err != nil || tag.RowsAffected() != 1 {
				return DelegationBinding{}, fmt.Errorf("persist admitted delegation: %w", ErrDelegationUnavailable)
			}
		}
	}
	result, err := scanDelegation(ctx, tx, callerID, request.DelegationID, false)
	if err != nil {
		return DelegationBinding{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DelegationBinding{}, fmt.Errorf("commit delegation decision: %w", ErrDelegationUnavailable)
	}
	return result, nil
}

// Cancel sets a monotonic caller-scoped cancellation fence.
func (d *Delegations) Cancel(ctx context.Context, delegationID string) (DelegationBinding, error) {
	bounded, cancel, err := d.boundedContext(ctx)
	if err != nil {
		return DelegationBinding{}, err
	}
	defer cancel()
	callerID, err := d.auth.AuthenticatedCaller(bounded)
	if err != nil || !deliveryV1ValidID(callerID) {
		return DelegationBinding{}, fmt.Errorf("authenticate delegation caller: %w", ErrDelegationUnauthenticated)
	}
	if !deliveryV1ValidID(delegationID) {
		return DelegationBinding{}, fmt.Errorf("invalid delegation ID: %w", ErrDeliveryV1Invalid)
	}
	tx, err := d.store.pool.BeginTx(bounded, pgx.TxOptions{})
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("begin delegation cancellation: %w", ErrDelegationUnavailable)
	}
	defer rollback(tx)
	binding, err := scanDelegation(bounded, tx, callerID, delegationID, true)
	if err != nil {
		return DelegationBinding{}, err
	}
	if binding.Status == DelegationDenied || binding.Status == DelegationCancelled || binding.Cancelled {
		if err := tx.Commit(bounded); err != nil {
			return DelegationBinding{}, fmt.Errorf("commit idempotent delegation cancellation: %w", ErrDelegationUnavailable)
		}
		return binding, nil
	}
	if binding.Status == DelegationIntent {
		tag, err := tx.Exec(bounded, `UPDATE plan_delegations SET status='cancelled', cancelled=TRUE, observation_sequence=observation_sequence+1, updated_at=CURRENT_TIMESTAMP
			WHERE caller_id=$1 AND delegation_id=$2 AND status='intent' AND cancelled=FALSE`, callerID, delegationID)
		if err != nil || tag.RowsAffected() != 1 {
			return DelegationBinding{}, fmt.Errorf("cancel pending delegation: %w", ErrDelegationUnavailable)
		}
	} else {
		if binding.State == nil {
			return DelegationBinding{}, fmt.Errorf("admitted delegation lacks lifecycle snapshot: %w", ErrDelegationConflict)
		}
		oldState := *binding.State
		newState := oldState
		newState.Cancelled = true
		if newState.Revision == int64(^uint64(0)>>1) {
			return DelegationBinding{}, fmt.Errorf("lifecycle revision exhausted: %w", ErrDelegationConflict)
		}
		now := d.store.clock.Now().UTC()
		if err := fenceDelegationTransition(&binding, oldState, newState, now); err != nil {
			return DelegationBinding{}, err
		}
		newState.Revision++
		payload, err := json.Marshal(newState)
		if err != nil {
			return DelegationBinding{}, fmt.Errorf("encode cancelled lifecycle: %w", ErrDelegationUnavailable)
		}
		tag, err := tx.Exec(bounded, `UPDATE plan_lifecycles SET revision=$2,state=$3::jsonb WHERE id=$1::uuid AND revision=$4`, binding.LifecycleID, newState.Revision, payload, oldState.Revision)
		if err != nil || tag.RowsAffected() != 1 {
			return DelegationBinding{}, fmt.Errorf("persist cancelled lifecycle: %w", ErrDelegationUnavailable)
		}
		if err := persistDelegationTransition(bounded, tx, &binding, newState); err != nil {
			return DelegationBinding{}, err
		}
	}
	result, err := scanDelegation(bounded, tx, callerID, delegationID, false)
	if err != nil {
		return DelegationBinding{}, err
	}
	if err := tx.Commit(bounded); err != nil {
		return DelegationBinding{}, fmt.Errorf("commit delegation cancellation: %w", ErrDelegationUnavailable)
	}
	return result, nil
}

// lockDelegationForLifecycle serializes bound transitions before locking their lifecycle row.
func lockDelegationForLifecycle(ctx context.Context, tx pgx.Tx, lifecycleID string) (*DelegationBinding, error) {
	var callerID, delegationID string
	err := tx.QueryRow(ctx, `SELECT caller_id,delegation_id FROM plan_delegations WHERE lifecycle_id=$1::uuid FOR UPDATE`, lifecycleID).Scan(&callerID, &delegationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock delegation binding: %w", ErrDelegationUnavailable)
	}
	binding, err := scanDelegation(ctx, tx, callerID, delegationID, false)
	if err != nil {
		return nil, err
	}
	return &binding, nil
}

func fenceDelegationTransition(binding *DelegationBinding, oldState, newState State, now time.Time) error {
	if binding == nil {
		return nil
	}
	if binding.Status != DelegationAdmitted || binding.LifecycleID != oldState.Lifecycle.ID || binding.LifecycleID != newState.Lifecycle.ID || oldState.Revision != newState.Revision || binding.LifecycleRevision != oldState.Revision {
		return fmt.Errorf("delegation lifecycle binding changed: %w", ErrDelegationConflict)
	}
	if binding.Cancelled && !delegationOnlyLateFacts(oldState, newState, true) {
		return fmt.Errorf("delegation is cancelled: %w", ErrDelegationConflict)
	}
	if binding.Cancelled != oldState.Cancelled || !delegationStateMatchesRequest(binding.Request, binding.Authorization, oldState) {
		return fmt.Errorf("delegation and lifecycle binding differ: %w", ErrDelegationConflict)
	}
	if oldState.Cancelled && !newState.Cancelled {
		return fmt.Errorf("delegation cancellation reversed: %w", ErrImmutable)
	}
	if newState.Lifecycle.Limits.MaxAttempts > binding.Request.Spec.Envelope.MaxAttempts || newState.Lifecycle.Limits.MaxConcurrentTasks > binding.Request.Spec.Envelope.MaxConcurrent {
		return fmt.Errorf("lifecycle limits exceed delegation envelope: %w", ErrDelegationConflict)
	}
	attempts, err := delegationAttemptCount(newState.Attempts)
	maxAttempts := binding.Request.Spec.Envelope.MaxAttempts
	if newState.Lifecycle.Limits.MaxAttempts < maxAttempts {
		maxAttempts = newState.Lifecycle.Limits.MaxAttempts
	}
	if err != nil || attempts > int64(maxAttempts) {
		return fmt.Errorf("aggregate attempt limit exceeded: %w", ErrDelegationConflict)
	}
	active := 0
	for _, claim := range newState.Claims {
		if claim.ExpiresAt.After(binding.Request.Spec.Envelope.ExpiresAt) {
			return fmt.Errorf("claim exceeds exact delegation expiry: %w", ErrDelegationConflict)
		}
		if now.Before(claim.ExpiresAt) {
			active++
		}
	}
	maxConcurrent := binding.Request.Spec.Envelope.MaxConcurrent
	if newState.Lifecycle.Limits.MaxConcurrentTasks < maxConcurrent {
		maxConcurrent = newState.Lifecycle.Limits.MaxConcurrentTasks
	}
	if active > maxConcurrent {
		return fmt.Errorf("delegation concurrency limit exceeded: %w", ErrDelegationConflict)
	}
	if !now.Before(binding.Request.Spec.Envelope.ExpiresAt) && !delegationOnlyLateFacts(oldState, newState, false) {
		return fmt.Errorf("delegation expired: %w", ErrDelegationConflict)
	}
	return nil
}

func delegationStateMatchesRequest(request DeliveryV1Request, authorization DeliveryV1Authorization, state State) bool {
	parts := strings.Split(strings.TrimPrefix(request.Spec.Repository, "https://github.com/"), "/")
	if len(parts) != 2 {
		return false
	}
	limits := state.Lifecycle.Limits
	return state.Lifecycle.Repository.Owner == parts[0] && state.Lifecycle.Repository.Name == parts[1] &&
		state.Lifecycle.Repository.Target == request.Spec.TargetBranch && state.Lifecycle.PolicyRevision == request.Spec.PolicyRevision &&
		authorization.PolicyRevision == request.Spec.PolicyRevision && state.Lifecycle.Authored.SourceRevision == request.Spec.SourceCommit &&
		limits.MaxAttempts >= 2 && limits.MaxAttempts <= request.Spec.Envelope.MaxAttempts &&
		limits.MaxConcurrentTasks > 0 && limits.MaxConcurrentTasks <= request.Spec.Envelope.MaxConcurrent
}

func delegationOnlyLateFacts(oldState, newState State, allowCancelOnly bool) bool {
	if !reflect.DeepEqual(oldState.Lifecycle, newState.Lifecycle) || oldState.StartedAt != newState.StartedAt ||
		!reflect.DeepEqual(oldState.Tasks, newState.Tasks) || !reflect.DeepEqual(oldState.Attempts, newState.Attempts) ||
		!reflect.DeepEqual(oldState.Claims, newState.Claims) || !reflect.DeepEqual(oldState.Completed, newState.Completed) ||
		!reflect.DeepEqual(oldState.CurrentPR, newState.CurrentPR) || oldState.DeliveryReceiptID != newState.DeliveryReceiptID ||
		oldState.EscalationReason != newState.EscalationReason || newState.Revision != oldState.Revision {
		return false
	}
	added := 0
	for id, oldReceipt := range oldState.Receipts {
		newReceipt, exists := newState.Receipts[id]
		if !exists || !reflect.DeepEqual(oldReceipt, newReceipt) {
			return false
		}
	}
	for id, receipt := range newState.Receipts {
		if _, exists := oldState.Receipts[id]; exists {
			continue
		}
		if receipt.Outcome != OutcomeUnknown && receipt.Outcome != OutcomeCancel {
			return false
		}
		added++
	}
	if added == 0 && oldState.Cancelled == newState.Cancelled {
		return false
	}
	if allowCancelOnly && added == 0 && !newState.Cancelled {
		return false
	}
	return true
}

func persistDelegationTransition(ctx context.Context, tx pgx.Tx, binding *DelegationBinding, newState State) error {
	if binding == nil {
		return nil
	}
	if binding.Sequence <= 0 || binding.Sequence == int64(^uint64(0)>>1) {
		return fmt.Errorf("delegation observation sequence exhausted: %w", ErrDelegationConflict)
	}
	var sequence int64
	err := tx.QueryRow(ctx, `UPDATE plan_delegations SET lifecycle_revision=$3, observation_sequence=observation_sequence+1, cancelled=cancelled OR $4, updated_at=CURRENT_TIMESTAMP
		WHERE caller_id=$1 AND delegation_id=$2 AND status='admitted' AND lifecycle_id=$5::uuid AND lifecycle_revision=$6 AND observation_sequence=$7
		RETURNING observation_sequence`, binding.CallerID, binding.DelegationID, newState.Revision, newState.Cancelled, binding.LifecycleID, binding.LifecycleRevision, binding.Sequence).Scan(&sequence)
	if err != nil || sequence != binding.Sequence+1 {
		return fmt.Errorf("persist delegation transition: %w", ErrDelegationConflict)
	}
	binding.Sequence = sequence
	binding.LifecycleRevision = newState.Revision
	binding.Cancelled = binding.Cancelled || newState.Cancelled
	return nil
}

func delegationAttemptCount(attempts map[string]int) (int64, error) {
	var total int64
	for _, count := range attempts {
		if count < 0 || total > int64(^uint64(0)>>1)-int64(count) {
			return 0, errors.New("invalid aggregate attempt count")
		}
		total += int64(count)
	}
	return total, nil
}

// delegationReady is a fail-closed, read-only admission fence for bound lifecycle snapshots.
func (s *Store) delegationReady(ctx context.Context, lifecycleID string, state State, now time.Time) (bool, error) {
	if s == nil || s.pool == nil || ctx == nil || lifecycleID == "" || now.IsZero() {
		return false, ErrInvalidStore
	}
	var callerID, delegationID string
	var digest string
	var requestBytes []byte
	var lifeRevision, maxAttempts int64
	var maxConcurrent int
	var expiresRaw string
	var status string
	var cancelled bool
	err := s.pool.QueryRow(ctx, `SELECT caller_id,delegation_id,request_bytes,request_digest,lifecycle_revision,max_attempts,max_concurrent,expires_at_exact,status,cancelled
		FROM plan_delegations WHERE lifecycle_id=$1::uuid`, lifecycleID).Scan(&callerID, &delegationID, &requestBytes, &digest, &lifeRevision, &maxAttempts, &maxConcurrent, &expiresRaw, &status, &cancelled)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read delegation readiness: %w", ErrDelegationUnavailable)
	}
	request, decodeErr := DecodeDeliveryV1Request(requestBytes)
	if decodeErr != nil {
		return false, fmt.Errorf("decode delegation readiness scope: %w", ErrDelegationConflict)
	}
	canonicalRequest, encodeErr := EncodeDeliveryV1Request(request)
	if encodeErr != nil {
		return false, fmt.Errorf("encode delegation readiness scope: %w", ErrDelegationConflict)
	}
	requestHash := sha256.Sum256(canonicalRequest)
	if request.CallerID != callerID || request.DelegationID != delegationID || status != string(DelegationAdmitted) || cancelled || state.Cancelled || state.Lifecycle.ID != lifecycleID || lifeRevision != state.Revision || hex.EncodeToString(requestHash[:]) != digest || maxAttempts != int64(request.Spec.Envelope.MaxAttempts) || maxConcurrent != request.Spec.Envelope.MaxConcurrent || expiresRaw != request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano) {
		return false, nil
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, expiresRaw)
	if err != nil {
		return false, fmt.Errorf("decode exact delegation expiry: %w", ErrDelegationConflict)
	}
	if !now.Before(expiresAt) || maxAttempts <= 0 || maxConcurrent <= 0 || maxConcurrent > deliveryV1MaxConcurrent {
		return false, nil
	}
	if err := state.ValidateAt(now); err != nil {
		return false, nil
	}
	attempts, err := delegationAttemptCount(state.Attempts)
	effectiveAttempts := maxAttempts
	if int64(state.Lifecycle.Limits.MaxAttempts) < effectiveAttempts {
		effectiveAttempts = int64(state.Lifecycle.Limits.MaxAttempts)
	}
	if err != nil || attempts >= effectiveAttempts {
		return false, nil
	}
	effectiveConcurrent := maxConcurrent
	if state.Lifecycle.Limits.MaxConcurrentTasks < effectiveConcurrent {
		effectiveConcurrent = state.Lifecycle.Limits.MaxConcurrentTasks
	}
	active := 0
	for _, claim := range state.Claims {
		if claim.ExpiresAt.After(expiresAt) {
			return false, nil
		}
		if now.Before(claim.ExpiresAt) {
			active++
		}
	}
	return active < effectiveConcurrent, nil
}
