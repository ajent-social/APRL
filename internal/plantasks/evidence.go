package plantasks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
)

const landingOperationTimeout = 10 * time.Second
const maxLateLandingFacts = 16

// LandedEvidence is host-verified proof that a reviewed source revision landed.
// It is trusted evidence and must never be accepted from an ordinary worker.
type LandedEvidence struct {
	Receipt     DeliveryV1LandedReceipt `json:"receipt"`
	LifecycleID string                  `json:"lifecycle_id"`
	TaskID      string                  `json:"task_id"`
	ReceiptID   string                  `json:"receipt_id"`
	Revision    int64                   `json:"revision"`
	PRNumber    int64                   `json:"pr_number"`
	MergeCommit string                  `json:"merge_commit"`
}

// LandingVerifier fetches external landing facts before storage locks and
// verifies the fetched facts locally while the lifecycle is locked.
type LandingVerifier interface {
	FetchLanding(context.Context, State, Task, Receipt) (LandedEvidence, error)
	VerifyLanding(context.Context, State, Task, Receipt, LandedEvidence, time.Time) error
}

// DeliveryV1HostFactAuthenticator authenticates a trusted host actor for
// reconciliation facts. Worker credentials must not implement this authority.
type DeliveryV1HostFactAuthenticator interface {
	AuthenticateHost(context.Context) (string, error)
}

// LateLandingFact records verified landing observed after lifecycle authority
// expired or was cancelled. It is audit-only and cannot complete work.
type LateLandingFact struct {
	ID         string    `json:"id"`
	HostActor  string    `json:"host_actor"`
	Receipt    Receipt   `json:"receipt"`
	ObservedAt time.Time `json:"observed_at"`
}

// LateLandingRecorder stores bounded, separately audited late landing facts.
type LateLandingRecorder struct {
	store    *Store
	auth     DeliveryV1HostFactAuthenticator
	verifier LandingVerifier
}

var errLateLandingReplay = errors.New("late landing fact already recorded")

// NewLateLandingRecorder constructs the host-only recorder over the canonical store.
func NewLateLandingRecorder(store *Store, auth DeliveryV1HostFactAuthenticator, verifier LandingVerifier) (*LateLandingRecorder, error) {
	if store == nil || store.pool == nil || isNilDependency(store.clock) || isNilDependency(auth) || isNilDependency(verifier) {
		return nil, fmt.Errorf("construct late landing recorder: %w", ErrInvalidStore)
	}
	return &LateLandingRecorder{store: store, auth: auth, verifier: verifier}, nil
}

// Record verifies and appends one host-observed landing fact after the exact
// delegated scope expired or was cancelled. It never changes workflow status.
func (r *LateLandingRecorder) Record(ctx context.Context, lifecycleID string, receipt Receipt, expectedRevision int64) error {
	if r == nil || r.store == nil || ctx == nil || !validID(lifecycleID) || expectedRevision < 0 {
		return ErrConflict
	}
	requestCtx, cancel := context.WithTimeout(ctx, landingOperationTimeout)
	defer cancel()
	state, err := r.store.Load(requestCtx, lifecycleID)
	if err != nil {
		return err
	}
	task, ok := state.Tasks[receipt.TaskID]
	if !ok || receipt.LifecycleID != lifecycleID || receipt.Outcome != OutcomeLanded || receipt.LandedEvidence != nil {
		return ErrReceiptAudit
	}
	binding, err := r.bindingForLifecycle(requestCtx, lifecycleID)
	if err != nil {
		return err
	}
	if binding.DelegationID != "" && (binding.LifecycleID != lifecycleID || binding.LifecycleRevision != state.Revision || binding.State == nil || !reflect.DeepEqual(*binding.State, state)) {
		return ErrConflict
	}
	if err := validateLateLandingEligibility(state, binding, r.store.clock.Now().UTC()); err != nil {
		return err
	}
	if err := ValidateReceipt(state.Lifecycle, task, receipt); err != nil {
		return fmt.Errorf("validate late landing receipt: %w", ErrReceiptAudit)
	}
	actor, err := r.auth.AuthenticateHost(requestCtx)
	if err != nil || actor == "" {
		return fmt.Errorf("authenticate late landing host actor: %w", ErrDelegationUnavailable)
	}
	if err := requestCtx.Err(); err != nil {
		return fmt.Errorf("host authentication exceeded deadline: %w", ErrDelegationUnavailable)
	}
	evidence, err := r.verifier.FetchLanding(requestCtx, state, task, receipt)
	if err != nil {
		return fmt.Errorf("fetch late landing evidence: %w", ErrDelegationUnavailable)
	}
	if err := requestCtx.Err(); err != nil {
		return fmt.Errorf("landing fetch exceeded deadline: %w", ErrDelegationUnavailable)
	}
	if err := validateLandedEvidenceShape(evidence); err != nil {
		return err
	}
	proofReceipt := receipt
	proofReceipt.LandedEvidence = &evidence
	return r.store.update(requestCtx, lifecycleID, expectedRevision, func(current *State) error {
		now := r.store.clock.Now().UTC()
		if err := validateLateLandingEligibility(*current, binding, now); err != nil {
			return err
		}
		currentTask, exists := current.Tasks[receipt.TaskID]
		if !exists || !reflect.DeepEqual(currentTask, task) {
			return ErrConflict
		}
		if err := r.authContextActor(requestCtx, actor); err != nil {
			return err
		}
		fact, replay := current.LateLandedFacts[receipt.ID]
		verifyState := *current
		if replay {
			oldInput := fact.Receipt
			oldInput.LandedEvidence = nil
			if fact.HostActor != actor || !reflect.DeepEqual(oldInput, receipt) || fact.Receipt.LandedEvidence == nil || !reflect.DeepEqual(*fact.Receipt.LandedEvidence, evidence) || evidence.Revision != expectedRevision || current.Revision != expectedRevision+1 {
				return ErrReceiptAudit
			}
			proofReceipt = fact.Receipt
			verifyState.Revision = expectedRevision
		} else if current.Revision != expectedRevision {
			return ErrConflict
		}
		if err := validateLandedEvidence(verifyState, receipt, evidence, now, true); err != nil {
			return err
		}
		verifyCtx, verifyCancel := context.WithTimeout(requestCtx, landingOperationTimeout)
		defer verifyCancel()
		if err := r.authContextActor(verifyCtx, actor); err != nil {
			return err
		}
		if err := r.verifier.VerifyLanding(verifyCtx, verifyState, currentTask, receipt, evidence, now); err != nil {
			return fmt.Errorf("verify late landing evidence: %w", ErrDelegationUnavailable)
		}
		if err := verifyCtx.Err(); err != nil {
			return fmt.Errorf("late landing verifier exceeded deadline: %w", ErrDelegationUnavailable)
		}
		if replay {
			return errLateLandingReplay
		}
		if current.LateLandedFacts == nil {
			current.LateLandedFacts = make(map[string]LateLandingFact)
		}
		fact = LateLandingFact{ID: receipt.ID, HostActor: actor, Receipt: proofReceipt, ObservedAt: now}
		if len(current.LateLandedFacts) >= maxLateLandingFacts {
			return ErrConflict
		}
		current.LateLandedFacts[fact.ID] = fact
		return nil
	}, &verifiedLandingTransition{FactID: receipt.ID, Evidence: evidence, HostActor: actor})
}

func (r *LateLandingRecorder) authContextActor(ctx context.Context, expected string) error {
	actor, err := r.auth.AuthenticateHost(ctx)
	if err != nil || actor == "" || actor != expected {
		return fmt.Errorf("late landing host actor changed: %w", ErrDelegationConflict)
	}
	return nil
}

func (r *LateLandingRecorder) bindingForLifecycle(ctx context.Context, lifecycleID string) (DelegationBinding, error) {
	tx, err := r.store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("begin late landing binding read: %w", ErrDelegationUnavailable)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var callerID, delegationID string
	err = tx.QueryRow(ctx, `SELECT caller_id,delegation_id FROM plan_delegations WHERE lifecycle_id=$1::uuid`, lifecycleID).Scan(&callerID, &delegationID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return DelegationBinding{}, fmt.Errorf("commit standalone lifecycle read: %w", ErrDelegationUnavailable)
		}
		return DelegationBinding{}, nil
	}
	if err != nil {
		return DelegationBinding{}, fmt.Errorf("read late landing binding: %w", ErrDelegationUnavailable)
	}
	binding, err := scanDelegation(ctx, tx, callerID, delegationID, false)
	if err != nil {
		return DelegationBinding{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DelegationBinding{}, fmt.Errorf("commit late landing binding read: %w", ErrDelegationUnavailable)
	}
	return binding, nil
}

func validateLateLandingEligibility(state State, binding DelegationBinding, now time.Time) error {
	if binding.LifecycleID != "" && binding.LifecycleID != state.Lifecycle.ID {
		return ErrConflict
	}
	if state.Cancelled {
		return nil
	}
	if binding.DelegationID != "" {
		if binding.Status != DelegationAdmitted || binding.Cancelled {
			return ErrConflict
		}
		if binding.Cancelled || now.Before(binding.Request.Spec.Envelope.ExpiresAt) {
			return ErrConflict
		}
		return nil
	}
	deadline := state.StartedAt.Add(time.Duration(state.Lifecycle.Limits.MaxDurationSeconds) * time.Second)
	if now.Before(deadline) {
		return ErrConflict
	}
	return nil
}

func validateLandedEvidenceShape(evidence LandedEvidence) error {
	r := evidence.Receipt
	_, verifiedOffset := r.VerifiedAt.Zone()
	if evidence.LifecycleID == "" || !validID(evidence.LifecycleID) || evidence.TaskID == "" || !validID(evidence.TaskID) ||
		evidence.ReceiptID == "" || !validID(evidence.ReceiptID) || evidence.Revision < 0 || evidence.PRNumber <= 0 ||
		evidence.MergeCommit == "" || r.Repository == "" || r.TargetBranch == "" || r.PRURL == "" ||
		!deliveryV1ValidObjectID(r.ReviewedHead) || !deliveryV1ValidObjectID(r.ReviewedBase) ||
		!deliveryV1ValidText(r.PolicyRevision, deliveryV1MaxIDLength) || !deliveryV1ObjectIDPattern.MatchString(r.LandedCommit) ||
		!deliveryV1DigestPattern.MatchString(r.SourceDigest) || !deliveryV1ValidText(r.Reviewer, deliveryV1MaxIDLength) || !deliveryV1ValidText(r.Author, deliveryV1MaxIDLength) || !deliveryV1ValidText(r.Verifier, deliveryV1MaxIDLength) || r.VerifiedAt.IsZero() || verifiedOffset != 0 ||
		!deliveryV1ObjectIDPattern.MatchString(evidence.MergeCommit) || evidence.MergeCommit != r.LandedCommit {
		return fmt.Errorf("malformed landed evidence: %w", ErrDeliveryV1Invalid)
	}
	return nil
}

func validateLandedEvidence(state State, receipt Receipt, evidence LandedEvidence, now time.Time, _ bool) error {
	if err := validateLandedEvidenceShape(evidence); err != nil {
		return err
	}
	task, ok := state.Tasks[receipt.TaskID]
	if !ok || !state.isDeliveryReview(task.ID) || receipt.Outcome != OutcomeLanded || receipt.LifecycleID != state.Lifecycle.ID ||
		evidence.LifecycleID != state.Lifecycle.ID || evidence.TaskID != task.ID || evidence.ReceiptID != receipt.ID ||
		evidence.Revision != state.Revision || task.PR == nil || receipt.PR == nil ||
		evidence.PRNumber != task.PR.Number || !reflect.DeepEqual(task.PR, receipt.PR) ||
		receipt.PolicyRevision != state.Lifecycle.PolicyRevision || evidence.Receipt.Repository != "https://github.com/"+state.Lifecycle.Repository.Owner+"/"+state.Lifecycle.Repository.Name ||
		evidence.Receipt.TargetBranch != state.Lifecycle.Repository.Target || evidence.Receipt.PRURL != task.PR.URL ||
		evidence.Receipt.ReviewedHead != task.PR.HeadSHA || evidence.Receipt.ReviewedBase != task.PR.BaseSHA ||
		evidence.Receipt.PolicyRevision != state.Lifecycle.PolicyRevision || evidence.MergeCommit != receipt.MergeCommit ||
		evidence.MergeCommit != receipt.LandedCommit ||
		evidence.Receipt.VerifiedAt.After(now) || evidence.Receipt.Reviewer != receipt.Actor.ActorID {
		return fmt.Errorf("landed evidence does not match reviewed lifecycle: %w", ErrReceiptAudit)
	}
	approved, merged := false, false
	for _, prior := range state.Receipts {
		if prior.TaskID != task.ID || !reflect.DeepEqual(prior.PR, receipt.PR) || prior.CreatedAt.After(receipt.CreatedAt) {
			continue
		}
		if prior.Outcome == OutcomeApproved && prior.Actor.ActorID == receipt.Actor.ActorID && !prior.CreatedAt.After(evidence.Receipt.VerifiedAt) {
			approved = true
		}
		if prior.Outcome == OutcomeMerged && prior.MergeCommit == receipt.MergeCommit && !prior.CreatedAt.After(evidence.Receipt.VerifiedAt) {
			merged = true
		}
	}
	if !approved || !merged || receipt.Actor.ActorID == "" || receipt.Actor.ActorID != evidence.Receipt.Reviewer ||
		evidence.Receipt.Author != state.Lifecycle.Authored.ActorID || evidence.Receipt.Author == evidence.Receipt.Reviewer {
		return fmt.Errorf("landing lacks independent approval and merge lineage: %w", ErrReceiptAudit)
	}
	if evidence.Receipt.Reviewer == state.Lifecycle.Authored.ActorID || evidence.Receipt.Verifier == state.Lifecycle.Authored.ActorID {
		return fmt.Errorf("landing reviewer or verifier is the lifecycle author: %w", ErrReceiptAudit)
	}
	for _, candidate := range state.Tasks {
		for _, author := range candidate.Authors {
			if author.ActorID == evidence.Receipt.Reviewer || author.ActorID == evidence.Receipt.Verifier {
				return fmt.Errorf("landing reviewer or verifier is a lifecycle contributor: %w", ErrReceiptAudit)
			}
		}
		if candidate.Actor.ActorID == evidence.Receipt.Reviewer && (candidate.Stage == StageAuthor || candidate.Stage == StageFix) {
			return fmt.Errorf("landing reviewer authored code: %w", ErrReceiptAudit)
		}
		if candidate.Actor.ActorID == evidence.Receipt.Verifier && (candidate.Stage == StageAuthor || candidate.Stage == StageFix) {
			return fmt.Errorf("landing verifier authored code: %w", ErrReceiptAudit)
		}
	}
	contributor := false
	for _, candidate := range state.Tasks {
		if candidate.Stage == StageAuthor && candidate.Correction == 0 {
			for _, author := range candidate.Authors {
				if author.ActorID == evidence.Receipt.Author {
					contributor = true
				}
			}
		}
	}
	for _, prior := range state.Receipts {
		if prior.Outcome != OutcomeCodingHandoff || prior.Actor.ActorID == "" {
			continue
		}
		priorTask := state.Tasks[prior.TaskID]
		if priorTask.Stage == StageAuthor && priorTask.Correction == 0 && prior.Actor.ActorID == evidence.Receipt.Author {
			contributor = true
		}
		if (priorTask.Stage == StageAuthor || priorTask.Stage == StageFix) && prior.Actor.ActorID == evidence.Receipt.Reviewer {
			return fmt.Errorf("landing reviewer contributed coding work: %w", ErrReceiptAudit)
		}
		if (priorTask.Stage == StageAuthor || priorTask.Stage == StageFix) && prior.Actor.ActorID == evidence.Receipt.Verifier {
			return fmt.Errorf("landing verifier contributed coding work: %w", ErrReceiptAudit)
		}
	}
	if !contributor || evidence.Receipt.Verifier == evidence.Receipt.Reviewer || evidence.Receipt.Verifier == evidence.Receipt.Author {
		return fmt.Errorf("landing actor lineage is incomplete: %w", ErrReceiptAudit)
	}
	return nil
}

func validateLandingEvidenceDigest(evidence LandedEvidence) bool {
	parsed, err := hex.DecodeString(evidence.Receipt.SourceDigest)
	if err != nil || len(parsed) != sha256.Size {
		return false
	}
	return hex.EncodeToString(parsed) == evidence.Receipt.SourceDigest
}

func validateLandingExtensions(state State) error {
	for id, receipt := range state.Receipts {
		if receipt.LandedEvidence == nil {
			continue
		}
		if receipt.Outcome != OutcomeLanded || id != receipt.ID || validateLandedEvidenceShape(*receipt.LandedEvidence) != nil || !validateLandingEvidenceDigest(*receipt.LandedEvidence) || receipt.LandedEvidence.ReceiptID != receipt.ID || receipt.LandedEvidence.TaskID != receipt.TaskID || receipt.LandedEvidence.LifecycleID != receipt.LifecycleID || receipt.LandedEvidence.Revision > state.Revision {
			return ErrReceiptAudit
		}
		proofState := state
		proofState.Revision = receipt.LandedEvidence.Revision
		at := receipt.LandedEvidence.Receipt.VerifiedAt
		if state.ProjectionTime != nil && state.ProjectionTime.After(at) {
			at = state.ProjectionTime.UTC()
		}
		if err := validateLandedEvidence(proofState, receipt, *receipt.LandedEvidence, at, false); err != nil {
			return ErrReceiptAudit
		}
	}
	for id, fact := range state.LateLandedFacts {
		if _, duplicate := state.Receipts[id]; duplicate {
			return ErrReceiptAudit
		}
		if id != fact.ID || id != fact.Receipt.ID || fact.Receipt.Outcome != OutcomeLanded || fact.HostActor == "" || fact.ObservedAt.IsZero() || fact.Receipt.LandedEvidence == nil || validateLandedEvidenceShape(*fact.Receipt.LandedEvidence) != nil || !validateLandingEvidenceDigest(*fact.Receipt.LandedEvidence) || fact.Receipt.LandedEvidence.ReceiptID != fact.Receipt.ID || fact.Receipt.LandedEvidence.TaskID != fact.Receipt.TaskID || fact.Receipt.LandedEvidence.LifecycleID != fact.Receipt.LifecycleID || fact.Receipt.LandedEvidence.Revision > state.Revision {
			return ErrReceiptAudit
		}
		proofState := state
		proofState.Revision = fact.Receipt.LandedEvidence.Revision
		if err := validateLandedEvidence(proofState, fact.Receipt, *fact.Receipt.LandedEvidence, fact.ObservedAt.UTC(), true); err != nil {
			return ErrReceiptAudit
		}
	}
	return nil
}

func preserveLandingExtensions(previous, next State, binding *DelegationBinding, verified *verifiedLandingTransition) error {
	if err := validateLandingExtensions(next); err != nil {
		return err
	}
	for id, old := range previous.Receipts {
		updated, exists := next.Receipts[id]
		if !exists {
			return ErrImmutable
		}
		if old.LandedEvidence != nil && !reflect.DeepEqual(old.LandedEvidence, updated.LandedEvidence) {
			return ErrImmutable
		}
	}
	newReceiptProof := false
	for id, receipt := range next.Receipts {
		if _, existed := previous.Receipts[id]; existed {
			continue
		}
		if receipt.Outcome == OutcomeLanded && binding != nil && receipt.LandedEvidence == nil {
			return ErrReceiptAudit
		}
		if receipt.LandedEvidence == nil {
			continue
		}
		if binding != nil && (binding.Status != DelegationAdmitted || binding.Cancelled || binding.LifecycleID != receipt.LifecycleID) ||
			verified == nil || verified.ReceiptID != id || !reflect.DeepEqual(*receipt.LandedEvidence, verified.Evidence) {
			return ErrReceiptAudit
		}
		newReceiptProof = true
	}
	for id, old := range previous.LateLandedFacts {
		updated, exists := next.LateLandedFacts[id]
		if !exists || !reflect.DeepEqual(old, updated) {
			return ErrImmutable
		}
	}
	newLateFact := false
	for id, fact := range next.LateLandedFacts {
		if _, existed := previous.LateLandedFacts[id]; existed {
			continue
		}
		if verified == nil || verified.FactID != id || verified.HostActor != fact.HostActor || fact.Receipt.LandedEvidence == nil || !reflect.DeepEqual(verified.Evidence, *fact.Receipt.LandedEvidence) {
			return ErrReceiptAudit
		}
		if binding != nil && (binding.LifecycleID != fact.Receipt.LifecycleID || binding.Status != DelegationAdmitted && binding.Status != DelegationCancelled || !binding.Cancelled && !binding.Request.Spec.Envelope.ExpiresAt.IsZero() && fact.ObservedAt.Before(binding.Request.Spec.Envelope.ExpiresAt)) {
			return ErrReceiptAudit
		}
		if len(next.LateLandedFacts) > len(previous.LateLandedFacts)+1 || !lateFactOnlyChanged(previous, next, id) {
			return ErrImmutable
		}
		newLateFact = true
	}
	if newReceiptProof && newLateFact {
		return ErrReceiptAudit
	}
	return nil
}

func lateFactOnlyChanged(previous, next State, factID string) bool {
	previous.LateLandedFacts = nil
	next.LateLandedFacts = nil
	previous.Revision = next.Revision
	previous.ProjectionTime = next.ProjectionTime
	return reflect.DeepEqual(previous, next) && factID != ""
}
