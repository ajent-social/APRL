package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/plantasks"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/internal/testutil"
)

const (
	plantasksDelegationTestIDPrefix = "f1000000-0000-4000-8000-"
	plantasksDelegationClaimSHA     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

var plantasksDelegationNextID atomic.Uint64

func TestPlanDelegationTemporaryDecisionPersistsIntentAndRetriesSameBinding(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{temporary: true}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	raw, request := plantasksDelegationRequest(t)

	_, err = service.Submit(ctx, raw)
	if !errors.Is(err, plantasks.ErrDelegationUnavailable) {
		t.Fatalf("temporary admission error = %v, want ErrDelegationUnavailable", err)
	}
	intent, err := service.Get(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("load persisted intent: %v", err)
	}
	if intent.Status != plantasks.DelegationIntent || intent.LifecycleID != "" || intent.State != nil {
		t.Fatalf("temporary decision created lifecycle or lost intent: %+v", intent)
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("temporary decision persisted counts binding/lifecycle/receipts=%d/%d/%d, want 1/0/0", intentCount, lifecycleCount, receiptCount)
	}

	policy.setTemporary(false)
	admitted, err := service.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("retry intent with identical request: %v", err)
	}
	if admitted.Status != plantasks.DelegationAdmitted || admitted.LifecycleID == "" || admitted.State == nil {
		t.Fatalf("successful retry did not atomically admit lifecycle: %+v", admitted)
	}
	if got := len(admitted.State.Tasks); got != 2 {
		t.Fatalf("initial admitted graph has %d tasks, want exactly author and review", got)
	}
	if admitted.RequestDigest == "" || string(admitted.RequestBytes) != string(raw) {
		t.Fatalf("persisted canonical request differs from submitted bytes: digest=%q bytes=%q", admitted.RequestDigest, admitted.RequestBytes)
	}
	if admitted.State.Lifecycle.Limits.MaxAttempts != request.Spec.Envelope.MaxAttempts || admitted.State.Lifecycle.Limits.MaxConcurrentTasks != request.Spec.Envelope.MaxConcurrent {
		t.Fatalf("initial limits do not preserve envelope: limits=%+v envelope=%+v", admitted.State.Lifecycle.Limits, request.Spec.Envelope)
	}
	intentCount, lifecycleCount, receiptCount = plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 1 || receiptCount != 0 {
		t.Fatalf("admitted counts binding/lifecycle/receipts=%d/%d/%d, want 1/1/0", intentCount, lifecycleCount, receiptCount)
	}

	reopenedStore := plantasksDelegationStore(t, db.Pool, manual)
	reopened, err := plantasks.NewDelegations(reopenedStore, auth, policy, factory)
	if err != nil {
		t.Fatalf("reopen delegations service: %v", err)
	}
	replayed, err := reopened.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("replay admitted request after reopening: %v", err)
	}
	if replayed.LifecycleID != admitted.LifecycleID || replayed.Sequence != admitted.Sequence || replayed.RequestDigest != admitted.RequestDigest || string(replayed.RequestBytes) != string(admitted.RequestBytes) {
		t.Fatalf("reopened retry changed durable identity/observation: admitted=%+v replayed=%+v", admitted, replayed)
	}
	if calls := policy.callCount(); calls != 2 {
		t.Fatalf("policy calls after temporary, admitted and replayed requests = %d, want 2 (replay must not reauthorize)", calls)
	}
}

func TestPlanDelegationDenialIsTerminalAndCreatesNoLifecycle(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{denyReason: "scope_unavailable"}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	raw, request := plantasksDelegationRequest(t)
	denied, err := service.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("definitive denial should be a terminal binding result: %v", err)
	}
	if denied.Status != plantasks.DelegationDenied || denied.LifecycleID != "" || denied.State != nil || denied.Authorization.Allowed {
		t.Fatalf("denial has lifecycle or success authority: %+v", denied)
	}
	calls := policy.callCount()
	replay, err := service.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("denied replay: %v", err)
	}
	if replay.Status != plantasks.DelegationDenied || replay.LifecycleID != "" || replay.Sequence != denied.Sequence || policy.callCount() != calls {
		t.Fatalf("denied replay changed terminal binding or re-ran policy: first=%+v replay=%+v calls=%d->%d", denied, replay, calls, policy.callCount())
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("denied counts binding/lifecycle/receipts=%d/%d/%d, want 1/0/0", intentCount, lifecycleCount, receiptCount)
	}
}

func TestPlanDelegationChangedRequestConflictsWithoutSecondLifecycle(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	raw, request := plantasksDelegationRequest(t)
	first, err := service.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("admit original request: %v", err)
	}
	changed := request
	changed.TaskID = "task-other"
	changedRaw, err := plantasks.EncodeDeliveryV1Request(changed)
	if err != nil {
		t.Fatalf("encode changed request: %v", err)
	}
	if _, err := service.Submit(ctx, changedRaw); !errors.Is(err, plantasks.ErrDelegationConflict) {
		t.Fatalf("changed request error = %v, want ErrDelegationConflict", err)
	}
	after, err := service.Get(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("read binding after changed request: %v", err)
	}
	if after.LifecycleID != first.LifecycleID || after.RequestDigest != first.RequestDigest || after.Sequence != first.Sequence || len(after.State.Tasks) != len(first.State.Tasks) {
		t.Fatalf("changed request mutated durable admission: before=%+v after=%+v", first, after)
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 1 || receiptCount != 0 {
		t.Fatalf("changed request counts binding/lifecycle/receipts=%d/%d/%d, want 1/1/0", intentCount, lifecycleCount, receiptCount)
	}
}

func TestPlanDelegationConcurrentIdenticalSubmitCreatesOneLifecycle(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	raw, request := plantasksDelegationRequest(t)

	const workers = 12
	start := make(chan struct{})
	results := make(chan plantasks.DelegationBinding, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			binding, err := service.Submit(ctx, raw)
			if err != nil {
				errs <- err
				return
			}
			results <- binding
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent identical submit: %v", err)
	}
	var first plantasks.DelegationBinding
	for binding := range results {
		if first.LifecycleID == "" {
			first = binding
			continue
		}
		if binding.LifecycleID != first.LifecycleID || binding.RequestDigest != first.RequestDigest || binding.Sequence != first.Sequence {
			t.Errorf("concurrent replay changed lifecycle or observation: first=%+v current=%+v", first, binding)
		}
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if first.LifecycleID == "" || intentCount != 1 || lifecycleCount != 1 || receiptCount != 0 {
		t.Fatalf("concurrent create result/counts binding/lifecycle/receipts=%+v/%d/%d/%d, want one lifecycle and binding", first, intentCount, lifecycleCount, receiptCount)
	}
}

func TestPlanDelegationAdmissionCommitFailureLeavesIntentAndNoLifecycle(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{temporary: true}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	raw, request := plantasksDelegationRequest(t)
	if _, err := service.Submit(ctx, raw); !errors.Is(err, plantasks.ErrDelegationUnavailable) {
		t.Fatalf("persist initial intent under temporary policy failure: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `CREATE FUNCTION reject_plan_delegation_admit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.status = 'admitted' THEN RAISE EXCEPTION 'forced final binding failure'; END IF; RETURN NEW; END; $$`); err != nil {
		t.Fatalf("create final-binding failure function: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `CREATE TRIGGER reject_plan_delegation_admit BEFORE UPDATE OF status ON plan_delegations
		FOR EACH ROW EXECUTE FUNCTION reject_plan_delegation_admit()`); err != nil {
		t.Fatalf("create final-binding failure trigger: %v", err)
	}
	policy.setTemporary(false)
	if _, err := service.Submit(ctx, raw); err == nil {
		t.Fatal("admission succeeded with forced final binding failure")
	}
	if _, err := db.Pool.Exec(ctx, `DROP TRIGGER reject_plan_delegation_admit ON plan_delegations`); err != nil {
		t.Fatalf("drop final-binding failure trigger: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `DROP FUNCTION reject_plan_delegation_admit()`); err != nil {
		t.Fatalf("drop final-binding failure function: %v", err)
	}
	binding, err := service.Get(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("load binding after rollback: %v", err)
	}
	if binding.Status != plantasks.DelegationIntent || binding.LifecycleID != "" || binding.State != nil {
		t.Fatalf("failed final bind left partial admission: %+v", binding)
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("failed final bind counts binding/lifecycle/receipts=%d/%d/%d, want 1/0/0", intentCount, lifecycleCount, receiptCount)
	}
}

type plantasksDelegationAuthenticator struct {
	caller string
	err    error
}

func (a *plantasksDelegationAuthenticator) AuthenticatedCaller(context.Context) (string, error) {
	return a.caller, a.err
}

type plantasksDelegationPolicy struct {
	mu                sync.Mutex
	calls             int
	temporary         bool
	denyReason        string
	authorizeEntered  chan struct{}
	authorizeContinue chan struct{}
	authorizeOnce     sync.Once
}

func (p *plantasksDelegationPolicy) Authorize(ctx context.Context, caller string, request plantasks.DeliveryV1Request) (plantasks.DeliveryV1Authorization, error) {
	p.mu.Lock()
	p.calls++
	temporary := p.temporary
	reason := p.denyReason
	entered, resume := p.authorizeEntered, p.authorizeContinue
	p.mu.Unlock()
	if entered != nil && resume != nil {
		p.authorizeOnce.Do(func() { close(entered) })
		select {
		case <-resume:
		case <-ctx.Done():
			return plantasks.DeliveryV1Authorization{}, ctx.Err()
		}
	}
	if temporary {
		return plantasks.DeliveryV1Authorization{}, plantasks.ErrDelegationUnavailable
	}
	authorization := plantasks.DeliveryV1Authorization{CallerID: caller, RequestDigest: plantasksDelegationDigest(request),
		PolicyRevision: request.Spec.PolicyRevision}
	if reason == "" && request.Spec.Envelope.MaxAttempts < 2 {
		reason = "insufficient_attempt_slots"
	}
	if reason == "" {
		authorization.Allowed = true
		authorization.GrantRevision = "grant-v1"
	} else {
		authorization.Reason = reason
	}
	return authorization, nil
}

func (p *plantasksDelegationPolicy) Verify(ctx context.Context, request plantasks.DeliveryV1Request, authorization plantasks.DeliveryV1Authorization, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if authorization.CallerID != request.CallerID || authorization.RequestDigest != plantasksDelegationDigest(request) || authorization.PolicyRevision != request.Spec.PolicyRevision || now.IsZero() {
		return plantasks.ErrDelegationConflict
	}
	if authorization.Allowed && authorization.GrantRevision == "" {
		return plantasks.ErrDelegationConflict
	}
	if authorization.Allowed && authorization.Reason != "" || !authorization.Allowed && authorization.Reason == "" {
		return plantasks.ErrDelegationConflict
	}
	return nil
}

func (p *plantasksDelegationPolicy) setTemporary(value bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.temporary = value
}

func (p *plantasksDelegationPolicy) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type plantasksDelegationInitialFactory struct {
	calls atomic.Uint64
}

func (f *plantasksDelegationInitialFactory) InitialState(request plantasks.DeliveryV1Request, authorization plantasks.DeliveryV1Authorization, now time.Time) (plantasks.State, error) {
	f.calls.Add(1)
	lifecycleID := fmt.Sprintf("%s%012x", plantasksDelegationTestIDPrefix, plantasksDelegationNextID.Add(1))
	state := plantasksStoreState(lifecycleID)
	state.StartedAt = now.UTC()
	state.Lifecycle.CreatedAt = now.UTC()
	state.Lifecycle.Authored.AuthoredAt = now.UTC()
	state.Lifecycle.Authored.SourceRevision = request.Spec.SourceCommit
	state.Lifecycle.Repository = plantasksDelegationRepository(request.Spec.Repository, request.Spec.TargetBranch)
	state.Lifecycle.PolicyRevision = authorization.PolicyRevision
	state.Lifecycle.Limits.MaxAttempts = request.Spec.Envelope.MaxAttempts
	state.Lifecycle.Limits.MaxConcurrentTasks = request.Spec.Envelope.MaxConcurrent
	remaining := request.Spec.Envelope.ExpiresAt.Sub(now)
	seconds := int64(remaining / time.Second)
	if remaining%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	state.Lifecycle.Limits.MaxDurationSeconds = seconds
	for id, task := range state.Tasks {
		task.CreatedAt = now.UTC()
		state.Tasks[id] = task
	}
	return state, nil
}

func plantasksDelegationStore(t *testing.T, pool *pgxpool.Pool, manual *clock.Manual) *plantasks.Store {
	t.Helper()
	store, err := plantasks.NewStore(pool, manual)
	if err != nil {
		t.Fatalf("construct plan task store: %v", err)
	}
	return store
}

func plantasksDelegationRequest(t *testing.T) ([]byte, plantasks.DeliveryV1Request) {
	t.Helper()
	fixture, err := os.ReadFile(filepath.Join("..", "..", "docs", "contracts", "fixtures", "code-delivery-v1-request.json"))
	if err != nil {
		t.Fatalf("read delivery v1 request fixture: %v", err)
	}
	request, err := plantasks.DecodeDeliveryV1Request(fixture)
	if err != nil {
		t.Fatalf("decode delivery v1 request fixture: %v", err)
	}
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode canonical delivery v1 request: %v", err)
	}
	return raw, request
}

func plantasksDelegationDigest(request plantasks.DeliveryV1Request) string {
	digest, _ := request.Digest()
	return digest
}

func plantasksDelegationRepository(repositoryURL, target string) plantasks.Repository {
	parsed, _ := url.Parse(repositoryURL)
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 2 {
		return plantasks.Repository{}
	}
	return plantasks.Repository{Owner: parts[0], Name: parts[1], Target: target}
}

func plantasksDelegationCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID, delegationID string) (int, int, int) {
	t.Helper()
	var bindingCount, lifecycleCount, receiptCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM plan_delegations WHERE caller_id=$1 AND delegation_id=$2`, callerID, delegationID).Scan(&bindingCount); err != nil {
		t.Fatalf("count delegation binding: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM plan_lifecycles`).Scan(&lifecycleCount); err != nil {
		t.Fatalf("count plan lifecycles: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM plan_task_receipts`).Scan(&receiptCount); err != nil {
		t.Fatalf("count plan receipts: %v", err)
	}
	return bindingCount, lifecycleCount, receiptCount
}

func TestPlanDelegationMalformedAndCrossCallerRequestsCreateNoBinding(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	raw, request := plantasksDelegationRequest(t)

	wrongCaller := &plantasksDelegationAuthenticator{caller: "different-caller"}
	wrongCallerService, err := plantasks.NewDelegations(store, wrongCaller, policy, factory)
	if err != nil {
		t.Fatalf("construct wrong-caller service: %v", err)
	}
	if _, err := wrongCallerService.Submit(ctx, raw); !errors.Is(err, plantasks.ErrDelegationUnauthenticated) {
		t.Fatalf("caller/body mismatch error = %v, want ErrDelegationUnauthenticated", err)
	}
	if _, err := wrongCallerService.Submit(ctx, []byte("{")); !errors.Is(err, plantasks.ErrDeliveryV1Malformed) {
		t.Fatalf("malformed body error = %v, want ErrDeliveryV1Malformed", err)
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 0 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("malformed or cross-caller request persisted binding/lifecycle/receipts=%d/%d/%d, want 0/0/0", intentCount, lifecycleCount, receiptCount)
	}

	ownerService, err := plantasks.NewDelegations(store, &plantasksDelegationAuthenticator{caller: request.CallerID}, policy, factory)
	if err != nil {
		t.Fatalf("construct owner service: %v", err)
	}
	if _, err := ownerService.Submit(ctx, raw); err != nil {
		t.Fatalf("admit owner request: %v", err)
	}
	if _, err := wrongCallerService.Get(ctx, request.DelegationID); !errors.Is(err, plantasks.ErrDelegationUnauthenticated) {
		t.Fatalf("cross-caller Get error = %v, want ErrDelegationUnauthenticated", err)
	}
	if _, err := wrongCallerService.Cancel(ctx, request.DelegationID); !errors.Is(err, plantasks.ErrDelegationUnauthenticated) {
		t.Fatalf("cross-caller Cancel error = %v, want ErrDelegationUnauthenticated", err)
	}
	binding, err := ownerService.Get(ctx, request.DelegationID)
	if err != nil || binding.Status != plantasks.DelegationAdmitted || binding.Cancelled {
		t.Fatalf("cross-caller read/cancel mutated owner binding: binding=%+v err=%v", binding, err)
	}
}

func TestPlanDelegationConstructorRejectsTypedNilDependencies(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	var nilAuth *plantasksDelegationAuthenticator
	if _, err := plantasks.NewDelegations(store, nilAuth, policy, factory); !errors.Is(err, plantasks.ErrInvalidDelegation) {
		t.Fatalf("typed-nil authenticator error = %v, want ErrInvalidDelegation", err)
	}
	if _, err := plantasks.NewDelegations(store, auth, nil, factory); !errors.Is(err, plantasks.ErrInvalidDelegation) {
		t.Fatalf("nil policy error = %v, want ErrInvalidDelegation", err)
	}
	if _, err := plantasks.NewDelegations(store, auth, policy, nil); !errors.Is(err, plantasks.ErrInvalidDelegation) {
		t.Fatalf("nil initial-state factory error = %v, want ErrInvalidDelegation", err)
	}
	if _, err := plantasks.NewDelegations(nil, auth, policy, factory); !errors.Is(err, plantasks.ErrInvalidDelegation) {
		t.Fatalf("nil store error = %v, want ErrInvalidDelegation", err)
	}
}

func TestPlanDelegationOneAttemptDenialDoesNotCreateLifecycleOrCallFactory(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	_, request := plantasksDelegationRequest(t)
	request.Spec.Envelope.MaxAttempts = 1
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode valid one-attempt request: %v", err)
	}
	denied, err := service.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("one-attempt policy denial: %v", err)
	}
	if denied.Status != plantasks.DelegationDenied || denied.LifecycleID != "" || denied.State != nil {
		t.Fatalf("one-attempt request was admitted without room for mandatory review: %+v", denied)
	}
	if factory.calls.Load() != 0 {
		t.Fatalf("initial-state factory called %d times for denied one-attempt request", factory.calls.Load())
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("one-attempt denial counts binding/lifecycle/receipts=%d/%d/%d, want 1/0/0", intentCount, lifecycleCount, receiptCount)
	}
}

func TestPlanDelegationConcurrentChangedDigestsHaveOneWinner(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	firstRaw, firstRequest := plantasksDelegationRequest(t)
	changedRequest := firstRequest
	changedRequest.PlanDigest = strings.Repeat("2", 64)
	changedRaw, err := plantasks.EncodeDeliveryV1Request(changedRequest)
	if err != nil {
		t.Fatalf("encode competing changed request: %v", err)
	}

	start := make(chan struct{})
	type response struct {
		binding plantasks.DelegationBinding
		err     error
	}
	responses := make(chan response, 2)
	var group sync.WaitGroup
	for _, raw := range [][]byte{firstRaw, changedRaw} {
		group.Add(1)
		go func(body []byte) {
			defer group.Done()
			<-start
			binding, err := service.Submit(ctx, body)
			responses <- response{binding: binding, err: err}
		}(raw)
	}
	close(start)
	group.Wait()
	close(responses)
	var successes, conflicts int
	var lifecycleID, digest string
	for result := range responses {
		switch {
		case result.err == nil:
			successes++
			lifecycleID = result.binding.LifecycleID
			digest = result.binding.RequestDigest
		case errors.Is(result.err, plantasks.ErrDelegationConflict):
			conflicts++
		default:
			t.Errorf("competing changed request error = %v, want success or ErrDelegationConflict", result.err)
		}
	}
	if successes != 1 || conflicts != 1 || lifecycleID == "" || digest == "" {
		t.Fatalf("changed-digest race outcomes success/conflict=%d/%d lifecycle=%q digest=%q", successes, conflicts, lifecycleID, digest)
	}
	binding, err := service.Get(ctx, firstRequest.DelegationID)
	if err != nil {
		t.Fatalf("load winning binding: %v", err)
	}
	if binding.LifecycleID != lifecycleID || binding.RequestDigest != digest {
		t.Fatalf("winner changed after competing request: got %+v", binding)
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, firstRequest.CallerID, firstRequest.DelegationID)
	if intentCount != 1 || lifecycleCount != 1 || receiptCount != 0 {
		t.Fatalf("changed-digest race persisted binding/lifecycle/receipts=%d/%d/%d, want 1/1/0", intentCount, lifecycleCount, receiptCount)
	}
}

func TestPlanDelegationPersistsExactNanosecondExpiryAndScope(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	_, request := plantasksDelegationRequest(t)
	request.Spec.Envelope.ExpiresAt = time.Date(2030, 1, 1, 0, 0, 0, 123456789, time.UTC)
	request.ProjectID = "project.release.7"
	request.JobID = "job.release.7"
	request.TaskID = "task.release.7"
	request.PlanRevision = 29
	request.PlanDigest = strings.Repeat("3", 64)
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode request with exact expiry: %v", err)
	}
	binding, err := service.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("submit request with exact expiry: %v", err)
	}
	if binding.Request.Spec.Envelope.ExpiresAt != request.Spec.Envelope.ExpiresAt || string(binding.RequestBytes) != string(raw) {
		t.Fatalf("binding changed request or nanosecond expiry: expires=%s request=%q", binding.Request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano), binding.RequestBytes)
	}
	var storedBytes []byte
	var storedDigest, storedExactExpiry string
	var storedScope []byte
	if err := db.Pool.QueryRow(ctx, `SELECT request_bytes, request_digest, expires_at_exact, request_scope::text
		FROM plan_delegations WHERE caller_id=$1 AND delegation_id=$2`, request.CallerID, request.DelegationID).
		Scan(&storedBytes, &storedDigest, &storedExactExpiry, &storedScope); err != nil {
		t.Fatalf("load immutable persisted request scope: %v", err)
	}
	if string(storedBytes) != string(raw) || storedDigest != binding.RequestDigest || storedExactExpiry != request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano) {
		t.Fatalf("durable request bytes/digest/expiry changed: digest=%q expiry=%q", storedDigest, storedExactExpiry)
	}
	var scope plantasks.DeliveryV1Request
	if err := json.Unmarshal(storedScope, &scope); err != nil {
		t.Fatalf("decode durable scope: %v", err)
	}
	if scope.CallerID != request.CallerID || scope.ProjectID != request.ProjectID || scope.JobID != request.JobID || scope.TaskID != request.TaskID ||
		scope.PlanRevision != request.PlanRevision || scope.PlanDigest != request.PlanDigest || scope.Spec.Repository != request.Spec.Repository ||
		scope.Spec.TargetBranch != request.Spec.TargetBranch || scope.Spec.SourceCommit != request.Spec.SourceCommit ||
		scope.Spec.PolicyRevision != request.Spec.PolicyRevision || scope.Spec.ProfileRevision != request.Spec.ProfileRevision ||
		!scope.Spec.Envelope.ExpiresAt.Equal(request.Spec.Envelope.ExpiresAt) || scope.Spec.Envelope.MaxAttempts != request.Spec.Envelope.MaxAttempts ||
		scope.Spec.Envelope.MaxConcurrent != request.Spec.Envelope.MaxConcurrent || scope.Spec.Envelope.MaxCostCents != request.Spec.Envelope.MaxCostCents {
		t.Fatalf("persisted JSON scope differs from canonical request: got %+v want %+v", scope, request)
	}
	if !reflect.DeepEqual(binding.Request, request) {
		t.Fatalf("binding request differs from caller scope: got %+v want %+v", binding.Request, request)
	}
}

func TestPlanDelegationCancelFencesInFlightIntentAuthorization(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{authorizeEntered: make(chan struct{}), authorizeContinue: make(chan struct{})}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	raw, request := plantasksDelegationRequest(t)
	type submitResult struct {
		binding plantasks.DelegationBinding
		err     error
	}
	submitted := make(chan submitResult, 1)
	go func() {
		binding, err := service.Submit(ctx, raw)
		submitted <- submitResult{binding: binding, err: err}
	}()
	select {
	case <-policy.authorizeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("admission policy was not reached after durable intent creation")
	}

	cancelled := make(chan struct {
		binding plantasks.DelegationBinding
		err     error
	}, 1)
	go func() {
		binding, err := service.Cancel(ctx, request.DelegationID)
		cancelled <- struct {
			binding plantasks.DelegationBinding
			err     error
		}{binding: binding, err: err}
	}()
	var cancelResult struct {
		binding plantasks.DelegationBinding
		err     error
	}
	select {
	case cancelResult = <-cancelled:
	case <-time.After(5 * time.Second):
		close(policy.authorizeContinue)
		t.Fatal("cancellation waited on external authorization while its durable decision was in flight")
	}
	if cancelResult.err != nil || cancelResult.binding.Status != plantasks.DelegationCancelled || cancelResult.binding.LifecycleID != "" {
		close(policy.authorizeContinue)
		t.Fatalf("cancelled pending intent = %+v, %v", cancelResult.binding, cancelResult.err)
	}
	close(policy.authorizeContinue)
	result := <-submitted
	if result.err == nil && result.binding.Status != plantasks.DelegationCancelled {
		t.Fatalf("in-flight authorization reopened cancelled binding: %+v", result.binding)
	}
	current, err := service.Get(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("load cancelled intent: %v", err)
	}
	if current.Status != plantasks.DelegationCancelled || current.LifecycleID != "" || current.State != nil {
		t.Fatalf("authorization race produced a lifecycle after cancel: %+v", current)
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 0 || receiptCount != 0 || factory.calls.Load() != 0 {
		t.Fatalf("cancel/auth race persisted binding/lifecycle/receipts=%d/%d/%d factoryCalls=%d", intentCount, lifecycleCount, receiptCount, factory.calls.Load())
	}
}
