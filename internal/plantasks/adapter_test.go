package plantasks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ajent-social/APRL/internal/clock"
)

type adapterTestClaims struct{}

func (adapterTestClaims) Verify(context.Context, string, string, string) error { return nil }

type adapterTestActors struct{}

func (adapterTestActors) Authenticate(context.Context) (Provenance, error) {
	return Provenance{ActorID: "test-actor", ActorKind: "human", AuthoredAt: time.Unix(1, 0).UTC()}, nil
}

type adapterTestPolicy struct{}

func (adapterTestPolicy) Authorize(context.Context, Lifecycle, Task, Provenance) error { return nil }

type adapterTestOutcomeVerifier struct{ err error }

func (v adapterTestOutcomeVerifier) Verify(context.Context, Lifecycle, Task, Receipt) error {
	return v.err
}

func TestNewAdapterFailsClosedForMissingDependencies(t *testing.T) {
	validStore := &Store{pool: &pgxpool.Pool{}, clock: clock.NewManual(time.Unix(1, 0))}
	claims := adapterTestClaims{}
	actors := adapterTestActors{}
	policy := adapterTestPolicy{}
	outcomes := adapterTestOutcomeVerifier{}
	var nilClaims *adapterTestClaims
	var nilActors *adapterTestActors
	var nilPolicy *adapterTestPolicy
	var nilOutcomes *adapterTestOutcomeVerifier
	tests := []struct {
		name     string
		store    *Store
		claims   ClaimVerifier
		actors   ActorAuthenticator
		policy   RuntimePolicy
		outcomes OutcomeVerifier
	}{
		{name: "nil store", claims: claims, actors: actors, policy: policy, outcomes: outcomes},
		{name: "nil claim verifier", store: validStore, actors: actors, policy: policy, outcomes: outcomes},
		{name: "nil actor authenticator", store: validStore, claims: claims, policy: policy, outcomes: outcomes},
		{name: "nil runtime policy", store: validStore, claims: claims, actors: actors, outcomes: outcomes},
		{name: "nil outcome verifier", store: validStore, claims: claims, actors: actors, policy: policy},
		{name: "typed nil claim verifier", store: validStore, claims: nilClaims, actors: actors, policy: policy, outcomes: outcomes},
		{name: "typed nil actor authenticator", store: validStore, claims: claims, actors: nilActors, policy: policy, outcomes: outcomes},
		{name: "typed nil runtime policy", store: validStore, claims: claims, actors: actors, policy: nilPolicy, outcomes: outcomes},
		{name: "typed nil outcome verifier", store: validStore, claims: claims, actors: actors, policy: policy, outcomes: nilOutcomes},
		{name: "uninitialized store", store: &Store{}, claims: claims, actors: actors, policy: policy, outcomes: outcomes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, err := NewAdapter(tt.store, tt.claims, tt.actors, tt.policy, tt.outcomes)
			if err == nil || adapter != nil {
				t.Fatalf("NewAdapter() = (%v, %v), want nil adapter and error", adapter, err)
			}
		})
	}
}

func TestNewAdapterAcceptsCompleteDependencies(t *testing.T) {
	store := &Store{pool: &pgxpool.Pool{}, clock: clock.NewManual(time.Unix(1, 0))}
	adapter, err := NewAdapter(store, adapterTestClaims{}, adapterTestActors{}, adapterTestPolicy{}, adapterTestOutcomeVerifier{})
	if err != nil {
		t.Fatalf("NewAdapter() error = %v", err)
	}
	if adapter == nil {
		t.Fatal("NewAdapter() returned nil adapter")
	}
}

func TestOutcomeVerificationDenialIsFailClosed(t *testing.T) {
	adapter := &Adapter{outcomes: adapterTestOutcomeVerifier{err: errors.New("no trusted host fact")}}
	err := adapter.verifyOutcome(context.Background(), Lifecycle{}, Task{}, Receipt{})
	if !errors.Is(err, ErrOutcomeDenied) {
		t.Fatalf("verifyOutcome() error = %v, want ErrOutcomeDenied", err)
	}
}

func TestClaimTaskIDUsesCanonicalRegistryNamespace(t *testing.T) {
	got, err := ClaimTaskID(contractAuthorTaskID)
	if err != nil {
		t.Fatalf("ClaimTaskID() error = %v", err)
	}
	want := "T-" + contractAuthorTaskID
	if got != want || len(got) != 38 {
		t.Fatalf("ClaimTaskID() = %q (%d chars), want %q (38 chars)", got, len(got), want)
	}
	if _, err := ClaimTaskID("not-a-task-id"); err == nil {
		t.Fatal("invalid task UUID accepted as claim ID")
	}
}

func TestDeliveryStatusSeparatesWorkflowFromReviewHistory(t *testing.T) {
	state := State{Lifecycle: Lifecycle{DeliveryGateID: contractReviewTaskID},
		Completed: map[string]Outcome{contractAuthorTaskID: OutcomeCodingHandoff}, Receipts: map[string]Receipt{}}
	unfinishedTaskID := "60000000-0000-4000-8000-000000000001"
	if got := deliveryStatus(state, contractReviewTaskID); got != "pending" {
		t.Fatalf("pending delivery status = %q, want pending", got)
	}
	if got := deliveryStatus(state, contractAuthorTaskID); got != string(OutcomeCodingHandoff) {
		t.Fatalf("completed coding status = %q, want coding_handoff", got)
	}
	state.Completed[contractReviewTaskID] = OutcomeChangesRequest
	state.Receipts[contractReceiptID] = Receipt{TaskID: contractReviewTaskID, Outcome: OutcomeChangesRequest, CreatedAt: contractNow}
	task := Task{ID: contractReviewTaskID, Stage: StageReview}
	if got := deliveryStatus(state, contractReviewTaskID); got != string(OutcomeChangesRequest) {
		t.Fatalf("completed review delivery status = %q, want changes_requested", got)
	}
	if got := reviewOutcome(state, task); got != string(OutcomeChangesRequest) {
		t.Fatalf("review outcome = %q, want changes_requested", got)
	}
	state.EscalationReason = "correction limit exhausted"
	if got := deliveryStatus(state, contractReviewTaskID); got != string(OutcomeChangesRequest) {
		t.Fatalf("escalation rewrote completed review delivery status to %q", got)
	}
	if got := deliveryStatus(state, unfinishedTaskID); got != "escalated" {
		t.Fatalf("unfinished escalated delivery status = %q, want escalated", got)
	}
	if got := deliveryStatus(state, contractAuthorTaskID); got != string(OutcomeCodingHandoff) {
		t.Fatalf("escalation rewrote completed coding delivery status to %q", got)
	}
	if got := reviewOutcome(state, task); got != string(OutcomeChangesRequest) {
		t.Fatalf("escalation rewrote review history to %q", got)
	}
	state.DeliveryReceiptID = "50000000-0000-4000-8000-000000000001"
	state.CurrentPR = contractValidPR()
	if got := deliveryStatus(state, contractReviewTaskID); got != string(OutcomeLanded) {
		t.Fatalf("landed delivery status = %q, want landed", got)
	}
	if got := reviewOutcome(state, task); got != string(OutcomeChangesRequest) {
		t.Fatalf("landing rewrote original review outcome to %q", got)
	}
	state.Cancelled = true
	if got := deliveryStatus(state, contractReviewTaskID); got != string(OutcomeLanded) {
		t.Fatalf("cancellation rewrote verified delivery status to %q", got)
	}
	if got := reviewOutcome(state, task); got != string(OutcomeChangesRequest) {
		t.Fatalf("cancellation rewrote completed review history to %q", got)
	}
	if got := deliveryStatus(state, unfinishedTaskID); got != "cancelled" {
		t.Fatalf("cancelled unfinished delivery status = %q, want cancelled", got)
	}
}
