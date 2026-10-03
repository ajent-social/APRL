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
	"github.com/ajent-social/APRL/tests/testutil"
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
	verifies          int
	temporary         bool
	denyReason        string
	authorizeEntered  chan struct{}
	authorizeContinue chan struct{}
	authorizeOnce     sync.Once
	manualClock       *clock.Manual
	advanceOnVerify   time.Duration
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
	p.mu.Lock()
	manualClock, advance := p.manualClock, p.advanceOnVerify
	p.advanceOnVerify = 0
	p.verifies++
	p.mu.Unlock()
	if manualClock != nil && advance != 0 {
		manualClock.Advance(advance)
	}
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

func (p *plantasksDelegationPolicy) verifyCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.verifies
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
	state.Lifecycle.Limits.MaxDurationSeconds = int(seconds)
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
	ownerService, err := plantasks.NewDelegations(store, &plantasksDelegationAuthenticator{caller: request.CallerID}, policy, factory)
	if err != nil {
		t.Fatalf("construct owner service: %v", err)
	}
	if _, err := ownerService.Submit(ctx, []byte("{")); !errors.Is(err, plantasks.ErrDeliveryV1Malformed) {
		t.Fatalf("malformed body error = %v, want ErrDeliveryV1Malformed", err)
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 0 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("malformed or cross-caller request persisted binding/lifecycle/receipts=%d/%d/%d, want 0/0/0", intentCount, lifecycleCount, receiptCount)
	}

	if _, err := ownerService.Submit(ctx, raw); err != nil {
		t.Fatalf("admit owner request: %v", err)
	}
	if _, err := wrongCallerService.Get(ctx, request.DelegationID); !errors.Is(err, plantasks.ErrNotFound) {
		t.Fatalf("cross-caller Get error = %v, want non-disclosing ErrNotFound", err)
	}
	if _, err := wrongCallerService.Cancel(ctx, request.DelegationID); !errors.Is(err, plantasks.ErrNotFound) {
		t.Fatalf("cross-caller Cancel error = %v, want non-disclosing ErrNotFound", err)
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
	var storedBytes, storedScope, storedAcceptance, storedEnvelope []byte
	var storedDigest, storedExactExpiry, storedProject, storedJob, storedTask, storedPlanDigest string
	var storedRepository, storedBranch, storedSourceCommit, storedPolicy, storedProfile, storedMode string
	var storedPlanRevision, storedMaxAttempts int64
	var storedMaxConcurrent int
	if err := db.Pool.QueryRow(ctx, `SELECT request_bytes, request_digest, expires_at_exact, request_scope::text,
		project_id, job_id, task_id, plan_revision, plan_digest, repository, target_branch, source_commit,
		acceptance::text, policy_revision, profile_revision, execution_mode, budget_envelope::text, max_concurrent, max_attempts
		FROM plan_delegations WHERE caller_id=$1 AND delegation_id=$2`, request.CallerID, request.DelegationID).
		Scan(&storedBytes, &storedDigest, &storedExactExpiry, &storedScope,
			&storedProject, &storedJob, &storedTask, &storedPlanRevision, &storedPlanDigest,
			&storedRepository, &storedBranch, &storedSourceCommit, &storedAcceptance,
			&storedPolicy, &storedProfile, &storedMode, &storedEnvelope, &storedMaxConcurrent, &storedMaxAttempts); err != nil {
		t.Fatalf("load immutable persisted request scope: %v", err)
	}
	if string(storedBytes) != string(raw) || storedDigest != binding.RequestDigest || storedExactExpiry != request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano) {
		t.Fatalf("durable request bytes/digest/expiry changed: digest=%q expiry=%q", storedDigest, storedExactExpiry)
	}
	var scope plantasks.DeliveryV1Request
	if err := json.Unmarshal(storedScope, &scope); err != nil {
		t.Fatalf("decode durable scope: %v", err)
	}
	var accepted []string
	if err := json.Unmarshal(storedAcceptance, &accepted); err != nil {
		t.Fatalf("decode durable acceptance scope: %v", err)
	}
	var envelope plantasks.DeliveryV1Envelope
	if err := json.Unmarshal(storedEnvelope, &envelope); err != nil {
		t.Fatalf("decode durable envelope scope: %v", err)
	}
	if storedProject != request.ProjectID || storedJob != request.JobID || storedTask != request.TaskID || storedPlanRevision != int64(request.PlanRevision) || storedPlanDigest != request.PlanDigest ||
		storedRepository != request.Spec.Repository || storedBranch != request.Spec.TargetBranch || storedSourceCommit != request.Spec.SourceCommit || storedPolicy != request.Spec.PolicyRevision ||
		storedProfile != request.Spec.ProfileRevision || storedMode != request.Spec.ExecutionMode || !reflect.DeepEqual(accepted, request.Spec.Acceptance) ||
		!reflect.DeepEqual(envelope, request.Spec.Envelope) || storedMaxConcurrent != request.Spec.Envelope.MaxConcurrent || storedMaxAttempts != int64(request.Spec.Envelope.MaxAttempts) ||
		!reflect.DeepEqual(scope, request) || scope.CallerID != request.CallerID || scope.ProjectID != request.ProjectID || scope.JobID != request.JobID || scope.TaskID != request.TaskID ||
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

func TestPlanDelegationAggregateAttemptsCoverAuthorReviewFixAndRereview(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	admissionPolicy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	delegations, err := plantasks.NewDelegations(store, auth, admissionPolicy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	_, request := plantasksDelegationRequest(t)
	request.Spec.Envelope.MaxAttempts = 3
	request.Spec.Envelope.MaxConcurrent = 1
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode three-attempt request: %v", err)
	}
	binding, err := delegations.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("submit request with three attempt slots: %v", err)
	}
	var authorTask, reviewTask plantasks.Task
	for _, task := range binding.State.Tasks {
		switch task.Stage {
		case plantasks.StageAuthor:
			authorTask = task
		case plantasks.StageReview:
			reviewTask = task
		}
	}
	if authorTask.ID == "" || reviewTask.ID == "" {
		t.Fatalf("factory did not create initial author/review tasks: %+v", binding.State.Tasks)
	}

	author := plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: manual.Now()}
	reviewer := plantasks.Provenance{ActorID: "reviewer-1", ActorKind: "human", AuthoredAt: manual.Now()}
	fixer := plantasks.Provenance{ActorID: "fixer-1", ActorKind: "agent", AuthoredAt: manual.Now()}
	actors := &plantasksTestAuthenticator{actor: author}
	claims := &plantasksTestClaimVerifier{allowed: map[string]plantasksTestClaim{}}
	runtime := &plantasksTestRuntimePolicy{lifecycleID: binding.LifecycleID,
		taskIDs:  map[string]bool{authorTask.ID: true, reviewTask.ID: true},
		actorIDs: map[string]bool{author.ActorID: true, reviewer.ActorID: true, fixer.ActorID: true}}
	outcomes := &plantasksTestOutcomeVerifier{allowed: map[string]plantasks.Receipt{}}
	adapter, err := plantasks.NewAdapter(store, claims, actors, runtime, outcomes)
	if err != nil {
		t.Fatalf("construct ordinary lifecycle adapter: %v", err)
	}
	admit := func(taskID, claimSHA string, actor plantasks.Provenance, revision int64) error {
		t.Helper()
		claims.allow(taskID, claimSHA, actor.ActorID)
		actors.actor = actor
		return adapter.Admit(ctx, binding.LifecycleID, taskID, claimSHA, revision, manual.Now().Add(time.Hour))
	}
	load := func() plantasks.State {
		t.Helper()
		state, err := store.Load(ctx, binding.LifecycleID)
		if err != nil {
			t.Fatalf("load delegated lifecycle: %v", err)
		}
		return state
	}
	pr := plantasks.PRBinding{Number: 12, URL: "https://github.com/example/project/pull/12",
		HeadSHA: request.Spec.SourceCommit, BaseSHA: strings.Repeat("b", 40), PolicyRevision: request.Spec.PolicyRevision}
	if err := admit(authorTask.ID, strings.Repeat("a", 40), author, 0); err != nil {
		t.Fatalf("admit author (attempt one): %v", err)
	}
	author.SourceRevision = pr.HeadSHA
	actors.actor = author
	authorHandoff := plantasks.Receipt{Version: plantasks.VersionV1, ID: "f2000000-0000-4000-8000-000000000001",
		LifecycleID: binding.LifecycleID, TaskID: authorTask.ID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor: author, PolicyRevision: request.Spec.PolicyRevision, PR: &pr, CreatedAt: manual.Now()}
	state := load()
	if err := adapter.Result(ctx, binding.LifecycleID, authorHandoff, strings.Repeat("a", 40), state.Revision); err != nil {
		t.Fatalf("record author handoff (attempt one): %v", err)
	}
	state = load()
	if err := admit(reviewTask.ID, strings.Repeat("b", 40), reviewer, state.Revision); err == nil {
		t.Fatal("review admission exceeded max_concurrent while author claim remained live")
	}
	if afterDenied := load(); afterDenied.Revision != state.Revision || !reflect.DeepEqual(afterDenied.Attempts, state.Attempts) || !reflect.DeepEqual(afterDenied.Claims, state.Claims) {
		t.Fatalf("concurrency denial mutated lifecycle: before=%+v after=%+v", state, afterDenied)
	}
	manual.Advance(time.Hour + time.Nanosecond)
	state = load()
	if err := admit(reviewTask.ID, strings.Repeat("b", 40), reviewer, state.Revision); err != nil {
		t.Fatalf("admit independent review after author claim expiry (attempt two): %v", err)
	}
	reviewer.SourceRevision = pr.HeadSHA
	actors.actor = reviewer
	negative := plantasks.Receipt{Version: plantasks.VersionV1, ID: "f2000000-0000-4000-8000-000000000002",
		LifecycleID: binding.LifecycleID, TaskID: reviewTask.ID, Outcome: plantasks.OutcomeChangesRequest,
		Actor: reviewer, PolicyRevision: request.Spec.PolicyRevision, PR: &pr,
		FindingIDs: []string{"f2000000-0000-4000-8000-000000000003"}, CreatedAt: manual.Now(), Detail: "one bounded correction"}
	state = load()
	if err := adapter.Result(ctx, binding.LifecycleID, negative, strings.Repeat("b", 40), state.Revision); err != nil {
		t.Fatalf("record negative review and open correction (attempt two): %v", err)
	}
	state = load()
	var fixTask, rereviewTask plantasks.Task
	for _, task := range state.Tasks {
		if task.Stage == plantasks.StageFix && task.Correction == 1 {
			fixTask = task
		}
		if task.Stage == plantasks.StageRereview && task.Correction == 1 {
			rereviewTask = task
		}
	}
	if fixTask.ID == "" || rereviewTask.ID == "" {
		t.Fatalf("negative review did not create bounded fix/re-review: tasks=%+v", state.Tasks)
	}
	runtime.taskIDs[fixTask.ID] = true
	runtime.taskIDs[rereviewTask.ID] = true
	if err := admit(fixTask.ID, strings.Repeat("c", 40), fixer, state.Revision); err != nil {
		t.Fatalf("admit fix (attempt three): %v", err)
	}
	newPR := pr
	newPR.HeadSHA = strings.Repeat("c", 40)
	fixer.SourceRevision = newPR.HeadSHA
	actors.actor = fixer
	fixHandoff := plantasks.Receipt{Version: plantasks.VersionV1, ID: "f2000000-0000-4000-8000-000000000004",
		LifecycleID: binding.LifecycleID, TaskID: fixTask.ID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor: fixer, PolicyRevision: request.Spec.PolicyRevision, PR: &newPR, CreatedAt: manual.Now()}
	state = load()
	if err := adapter.Result(ctx, binding.LifecycleID, fixHandoff, strings.Repeat("c", 40), state.Revision); err != nil {
		t.Fatalf("record fix handoff (attempt three): %v", err)
	}
	manual.Advance(time.Hour + time.Nanosecond)
	before := load()
	var totalAttempts int
	for _, count := range before.Attempts {
		totalAttempts += count
	}
	if totalAttempts != request.Spec.Envelope.MaxAttempts {
		t.Fatalf("admissions across author/review/fix=%d, want lifecycle cap %d", totalAttempts, request.Spec.Envelope.MaxAttempts)
	}
	if err := admit(rereviewTask.ID, strings.Repeat("d", 40), reviewer, before.Revision); err == nil {
		t.Fatal("re-review admission succeeded after author/review/fix exhausted lifecycle-wide attempts")
	}
	after := load()
	if after.Revision != before.Revision || !reflect.DeepEqual(after.Attempts, before.Attempts) || !reflect.DeepEqual(after.Claims, before.Claims) || len(after.Tasks) != len(before.Tasks) || len(after.Receipts) != len(before.Receipts) {
		t.Fatalf("denied fourth lifecycle admission mutated state: before=%+v after=%+v", before, after)
	}
}

func TestPlanDelegationCancelReplayAndUnknownReceiptDoNotReopenAuthority(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	manual := clock.NewManual(start)
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	admissionPolicy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	delegations, err := plantasks.NewDelegations(store, auth, admissionPolicy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	_, request := plantasksDelegationRequest(t)
	expiry := start.Add(30*time.Second + 123456789*time.Nanosecond)
	request.Spec.Envelope.ExpiresAt = expiry
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode exact-expiry request: %v", err)
	}
	binding, err := delegations.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("submit expiring delegation: %v", err)
	}
	var authorTask plantasks.Task
	for _, task := range binding.State.Tasks {
		if task.Stage == plantasks.StageAuthor {
			authorTask = task
		}
	}
	if authorTask.ID == "" {
		t.Fatalf("missing initial author task: %+v", binding.State.Tasks)
	}
	author := plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: manual.Now()}
	actors := &plantasksTestAuthenticator{actor: author}
	claim := strings.Repeat("a", 40)
	claims := &plantasksTestClaimVerifier{allowed: map[string]plantasksTestClaim{}}
	claims.allow(authorTask.ID, claim, author.ActorID)
	runtime := &plantasksTestRuntimePolicy{lifecycleID: binding.LifecycleID,
		taskIDs: map[string]bool{authorTask.ID: true}, actorIDs: map[string]bool{author.ActorID: true}}
	outcomes := &plantasksTestOutcomeVerifier{allowed: map[string]plantasks.Receipt{}}
	adapter, err := plantasks.NewAdapter(store, claims, actors, runtime, outcomes)
	if err != nil {
		t.Fatalf("construct ordinary lifecycle adapter: %v", err)
	}
	claimExpiry := expiry.Add(-time.Nanosecond)
	if err := adapter.Admit(ctx, binding.LifecycleID, authorTask.ID, claim, binding.State.Revision, claimExpiry); err != nil {
		t.Fatalf("admit author with strictly pre-expiry claim: %v", err)
	}

	firstCancel, err := delegations.Cancel(ctx, request.DelegationID)
	if err != nil || !firstCancel.Cancelled || firstCancel.Status != plantasks.DelegationAdmitted || firstCancel.LifecycleID != binding.LifecycleID {
		t.Fatalf("cancel admitted delegation: binding=%+v error=%v", firstCancel, err)
	}
	cancelledState, err := store.Load(ctx, binding.LifecycleID)
	if err != nil {
		t.Fatalf("load state after cancellation: %v", err)
	}
	if !cancelledState.Cancelled || len(cancelledState.Tasks) != 2 || len(cancelledState.Receipts) != 0 {
		t.Fatalf("cancellation rewrote task/receipt history: cancelled=%t tasks=%d receipts=%d", cancelledState.Cancelled, len(cancelledState.Tasks), len(cancelledState.Receipts))
	}
	cancelReplay, err := delegations.Cancel(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("replay cancellation: %v", err)
	}
	if cancelReplay.Sequence != firstCancel.Sequence || cancelReplay.State.Revision != firstCancel.State.Revision || len(cancelReplay.State.Tasks) != len(firstCancel.State.Tasks) || len(cancelReplay.State.Receipts) != len(firstCancel.State.Receipts) {
		t.Fatalf("cancel replay churned observation or lifecycle: first=%+v replay=%+v", firstCancel, cancelReplay)
	}

	unknown := plantasks.Receipt{Version: plantasks.VersionV1, ID: "f3000000-0000-4000-8000-000000000001",
		LifecycleID: binding.LifecycleID, TaskID: authorTask.ID, Outcome: plantasks.OutcomeUnknown,
		Actor: author, PolicyRevision: request.Spec.PolicyRevision, CreatedAt: manual.Now(), Detail: "execution outcome is unknown"}
	if err := adapter.Result(ctx, binding.LifecycleID, unknown, claim, firstCancel.State.Revision); err != nil {
		t.Fatalf("persist non-releasing unknown receipt on live admission after cancel: %v", err)
	}
	afterUnknown, err := delegations.Get(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("load binding after unknown receipt: %v", err)
	}
	if afterUnknown.Sequence != firstCancel.Sequence+1 || !afterUnknown.Cancelled || afterUnknown.State.Completed[authorTask.ID] != "" || afterUnknown.State.DeliveryReceiptID != "" || len(afterUnknown.State.Receipts) != 1 {
		t.Fatalf("unknown receipt reopened or failed to audit cancelled lifecycle: %+v", afterUnknown)
	}
	stable, err := delegations.Cancel(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("replay cancellation after unknown: %v", err)
	}
	if stable.Sequence != afterUnknown.Sequence || len(stable.State.Receipts) != 1 || stable.State.Completed[authorTask.ID] != "" {
		t.Fatalf("cancel replay altered unknown audit or positive completion: %+v", stable)
	}

	manual.Set(expiry)
	lateHandoff := plantasks.Receipt{Version: plantasks.VersionV1, ID: "f3000000-0000-4000-8000-000000000002",
		LifecycleID: binding.LifecycleID, TaskID: authorTask.ID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor: author, PolicyRevision: request.Spec.PolicyRevision, PR: &plantasks.PRBinding{
			Number: 12, URL: "https://github.com/example/project/pull/12", HeadSHA: request.Spec.SourceCommit,
			BaseSHA: strings.Repeat("b", 40), PolicyRevision: request.Spec.PolicyRevision}, CreatedAt: expiry}
	if err := adapter.Result(ctx, binding.LifecycleID, lateHandoff, claim, afterUnknown.State.Revision); err == nil {
		t.Fatal("positive coding handoff was accepted at the exact caller expiry")
	}
	final, err := delegations.Get(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("load binding after exact-expiry denial: %v", err)
	}
	if final.Sequence != afterUnknown.Sequence || !final.Cancelled || final.State.Completed[authorTask.ID] != "" || len(final.State.Receipts) != 1 {
		t.Fatalf("exact-expiry positive denial changed canceled unknown history: %+v", final)
	}
}

func TestPlanDelegationRechecksExactExpiryAfterLocalAuthorizationVerification(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	manual := clock.NewManual(start)
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{manualClock: manual, advanceOnVerify: 6 * time.Nanosecond}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	_, request := plantasksDelegationRequest(t)
	request.Spec.Envelope.ExpiresAt = start.Add(5 * time.Nanosecond)
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode near-expiry request: %v", err)
	}
	binding, err := service.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("late local verification should produce terminal denial: %v", err)
	}
	if binding.Status != plantasks.DelegationDenied || binding.LifecycleID != "" || binding.State != nil || binding.Authorization.Allowed {
		t.Fatalf("decision was admitted from pre-verification time sample: %+v", binding)
	}
	if !manual.Now().After(request.Spec.Envelope.ExpiresAt) || policy.verifyCount() != 1 {
		t.Fatalf("expiry was not rechecked after local verification: now=%s expiry=%s verifyCalls=%d", manual.Now().Format(time.RFC3339Nano), request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano), policy.verifyCount())
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("late local verification persisted binding/lifecycle/receipts=%d/%d/%d, want 1/0/0", intentCount, lifecycleCount, receiptCount)
	}
}

func TestPlanDelegationRechecksExactExpiryAfterLifecyclePersistence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	manual := clock.NewManual(start)
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	raw, request := plantasksDelegationRequest(t)
	expiry := start.Add(time.Hour)
	request.Spec.Envelope.ExpiresAt = expiry
	raw, err = plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode exact-expiry request: %v", err)
	}

	lockKey := int64(plantasksDelegationNextID.Add(1))
	lockConn, err := db.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire advisory-lock connection: %v", err)
	}
	defer lockConn.Release()
	if _, err := lockConn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey); err != nil {
		t.Fatalf("hold lifecycle-insert advisory lock: %v", err)
	}
	defer func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer unlockCancel()
		if _, err := lockConn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", lockKey); err != nil {
			t.Errorf("release lifecycle-insert advisory lock: %v", err)
		}
	}()
	if _, err := db.Pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION block_plan_lifecycle_insert() RETURNS trigger
		LANGUAGE plpgsql AS $body$ BEGIN PERFORM pg_advisory_xact_lock(%d); RETURN NEW; END $body$`, lockKey)); err != nil {
		t.Fatalf("create lifecycle insert blocker: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `CREATE TRIGGER block_plan_lifecycle_insert
		BEFORE INSERT ON plan_lifecycles FOR EACH ROW EXECUTE FUNCTION block_plan_lifecycle_insert()`); err != nil {
		t.Fatalf("install lifecycle insert blocker: %v", err)
	}

	type submitResult struct {
		binding plantasks.DelegationBinding
		err     error
	}
	completed := make(chan submitResult, 1)
	go func() {
		binding, err := service.Submit(ctx, raw)
		completed <- submitResult{binding: binding, err: err}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var blocked bool
		if err := db.Pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE query ILIKE '%INSERT INTO plan_lifecycles%'
			AND wait_event_type='Lock' AND wait_event='advisory'
		)`).Scan(&blocked); err != nil {
			t.Fatalf("observe blocked lifecycle insert: %v", err)
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delegation submit never reached the blocked lifecycle insert")
		}
		select {
		case result := <-completed:
			t.Fatalf("delegation submit completed before lifecycle insert block: binding=%+v err=%v", result.binding, result.err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	manual.Set(expiry)
	if _, err := lockConn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey); err != nil {
		t.Fatalf("release lifecycle-insert advisory lock: %v", err)
	}
	result := <-completed
	if result.err != nil {
		t.Fatalf("submit after expiry during lifecycle persistence: %v", result.err)
	}
	if result.binding.Status != plantasks.DelegationDenied || result.binding.State != nil || result.binding.LifecycleID != "" {
		t.Fatalf("post-persistence expiry did not return terminal denial without lifecycle: %+v", result.binding)
	}
	bindingCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if bindingCount != 1 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("post-persistence expiry retained lifecycle artifacts: binding=%d lifecycle=%d receipts=%d", bindingCount, lifecycleCount, receiptCount)
	}
	replay, err := service.Submit(ctx, raw)
	if err != nil || replay.Status != plantasks.DelegationDenied || replay.Sequence != result.binding.Sequence {
		t.Fatalf("expiry denial replay changed result: binding=%+v err=%v", replay, err)
	}
	if b, l, r := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID); b != 1 || l != 0 || r != 0 {
		t.Fatalf("expiry denial replay changed durable rows: binding=%d lifecycle=%d receipts=%d", b, l, r)
	}
}

func TestPlanDelegationClaimDeadlineCannotExceedExactRequestExpiry(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	manual := clock.NewManual(start)
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	service, err := plantasks.NewDelegations(store, auth, &plantasksDelegationPolicy{}, &plantasksDelegationInitialFactory{})
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	exactExpiry := start.Add(time.Hour + 123456789*time.Nanosecond)
	for _, test := range []struct {
		name       string
		delegation string
		claimUntil time.Time
		wantError  bool
	}{
		{name: "equal to exact external expiry", delegation: "deadline-equal", claimUntil: exactExpiry},
		{name: "one nanosecond beyond external expiry", delegation: "deadline-late", claimUntil: exactExpiry.Add(time.Nanosecond), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, request := plantasksDelegationRequest(t)
			request.DelegationID = test.delegation
			request.Spec.Envelope.ExpiresAt = exactExpiry
			raw, err := plantasks.EncodeDeliveryV1Request(request)
			if err != nil {
				t.Fatalf("encode exact-expiry request: %v", err)
			}
			binding, err := service.Submit(ctx, raw)
			if err != nil || binding.Status != plantasks.DelegationAdmitted || binding.State == nil {
				t.Fatalf("submit request: binding=%+v err=%v", binding, err)
			}
			var authorTask plantasks.Task
			for _, task := range binding.State.Tasks {
				if task.Stage == plantasks.StageAuthor {
					authorTask = task
				}
			}
			if authorTask.ID == "" {
				t.Fatalf("missing initial author task: %+v", binding.State.Tasks)
			}
			actor := plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: start}
			actors := &plantasksTestAuthenticator{actor: actor}
			claim := strings.Repeat("a", 40)
			claims := &plantasksTestClaimVerifier{allowed: map[string]plantasksTestClaim{}}
			claims.allow(authorTask.ID, claim, actor.ActorID)
			runtime := &plantasksTestRuntimePolicy{lifecycleID: binding.LifecycleID,
				taskIDs: map[string]bool{authorTask.ID: true}, actorIDs: map[string]bool{actor.ActorID: true}}
			outcomes := &plantasksTestOutcomeVerifier{allowed: map[string]plantasks.Receipt{}}
			adapter, err := plantasks.NewAdapter(store, claims, actors, runtime, outcomes)
			if err != nil {
				t.Fatalf("construct ordinary lifecycle adapter: %v", err)
			}
			err = adapter.Admit(ctx, binding.LifecycleID, authorTask.ID, claim, binding.State.Revision, test.claimUntil)
			if test.wantError {
				if err == nil {
					t.Fatal("claim extending one nanosecond past exact external deadline was admitted")
				}
				after, loadErr := store.Load(ctx, binding.LifecycleID)
				if loadErr != nil {
					t.Fatalf("load lifecycle after over-deadline denial: %v", loadErr)
				}
				if after.Revision != binding.State.Revision || len(after.Claims) != 0 || len(after.Attempts) != 0 {
					t.Fatalf("over-deadline denial mutated lifecycle: before=%+v after=%+v", binding.State, after)
				}
				return
			}
			if err != nil {
				t.Fatalf("claim at exact external deadline was denied: %v", err)
			}
			after, loadErr := store.Load(ctx, binding.LifecycleID)
			if loadErr != nil {
				t.Fatalf("load lifecycle after exact-deadline admission: %v", loadErr)
			}
			if got := after.Claims[authorTask.ID].ExpiresAt; !got.Equal(exactExpiry) {
				t.Fatalf("claim expiry = %s, want exact external expiry %s", got.Format(time.RFC3339Nano), exactExpiry.Format(time.RFC3339Nano))
			}
		})
	}
}

func TestPlanDelegationAlreadyExpiredRequestDeniesBeforeFactoryOrLocalVerify(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	manual := clock.NewManual(now)
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	policy := &plantasksDelegationPolicy{}
	factory := &plantasksDelegationInitialFactory{}
	service, err := plantasks.NewDelegations(store, auth, policy, factory)
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	_, request := plantasksDelegationRequest(t)
	request.Spec.Envelope.ExpiresAt = now
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode syntactically valid expired request: %v", err)
	}
	binding, err := service.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("expired request should become terminal denial: %v", err)
	}
	if binding.Status != plantasks.DelegationDenied || binding.LifecycleID != "" || binding.State != nil || binding.Authorization.Allowed {
		t.Fatalf("already-expired request was admitted: %+v", binding)
	}
	if factory.calls.Load() != 0 || policy.verifyCount() != 0 {
		t.Fatalf("expired request crossed trusted construction/local verification: factory=%d verify=%d", factory.calls.Load(), policy.verifyCount())
	}
	intentCount, lifecycleCount, receiptCount := plantasksDelegationCounts(t, ctx, db.Pool, request.CallerID, request.DelegationID)
	if intentCount != 1 || lifecycleCount != 0 || receiptCount != 0 {
		t.Fatalf("expired request counts binding/lifecycle/receipts=%d/%d/%d, want 1/0/0", intentCount, lifecycleCount, receiptCount)
	}
}

func TestPlanDelegationReceiptAndBindingSnapshotCommitAtomically(t *testing.T) {
	ctx := context.Background()
	db := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, db.Pool, manual)
	auth := &plantasksDelegationAuthenticator{caller: "caller-example"}
	delegations, err := plantasks.NewDelegations(store, auth, &plantasksDelegationPolicy{}, &plantasksDelegationInitialFactory{})
	if err != nil {
		t.Fatalf("construct delegations service: %v", err)
	}
	raw, request := plantasksDelegationRequest(t)
	binding, err := delegations.Submit(ctx, raw)
	if err != nil {
		t.Fatalf("admit request: %v", err)
	}
	var authorTask plantasks.Task
	for _, task := range binding.State.Tasks {
		if task.Stage == plantasks.StageAuthor {
			authorTask = task
		}
	}
	author := plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: manual.Now()}
	actors := &plantasksTestAuthenticator{actor: author}
	claim := strings.Repeat("a", 40)
	claims := &plantasksTestClaimVerifier{allowed: map[string]plantasksTestClaim{}}
	claims.allow(authorTask.ID, claim, author.ActorID)
	runtime := &plantasksTestRuntimePolicy{lifecycleID: binding.LifecycleID,
		taskIDs: map[string]bool{authorTask.ID: true}, actorIDs: map[string]bool{author.ActorID: true}}
	outcomes := &plantasksTestOutcomeVerifier{allowed: map[string]plantasks.Receipt{}}
	adapter, err := plantasks.NewAdapter(store, claims, actors, runtime, outcomes)
	if err != nil {
		t.Fatalf("construct ordinary lifecycle adapter: %v", err)
	}
	if err := adapter.Admit(ctx, binding.LifecycleID, authorTask.ID, claim, binding.State.Revision, manual.Now().Add(time.Hour)); err != nil {
		t.Fatalf("admit author: %v", err)
	}
	beforeBinding, err := delegations.Get(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("load binding before receipt attempt: %v", err)
	}
	beforeState, err := store.Load(ctx, binding.LifecycleID)
	if err != nil {
		t.Fatalf("load lifecycle before receipt attempt: %v", err)
	}

	if _, err := db.Pool.Exec(ctx, `CREATE FUNCTION reject_delegation_receipt_binding() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced observation-binding commit failure'; END; $$`); err != nil {
		t.Fatalf("create observation-binding failure function: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `CREATE TRIGGER reject_delegation_receipt_binding BEFORE UPDATE OF lifecycle_revision ON plan_delegations
		FOR EACH ROW EXECUTE FUNCTION reject_delegation_receipt_binding()`); err != nil {
		t.Fatalf("create observation-binding failure trigger: %v", err)
	}
	unknown := plantasks.Receipt{Version: plantasks.VersionV1, ID: "f4000000-0000-4000-8000-000000000001",
		LifecycleID: binding.LifecycleID, TaskID: authorTask.ID, Outcome: plantasks.OutcomeUnknown,
		Actor: author, PolicyRevision: request.Spec.PolicyRevision, CreatedAt: manual.Now(), Detail: "provider outcome is unknown"}
	if err := adapter.Result(ctx, binding.LifecycleID, unknown, claim, beforeState.Revision); err == nil {
		t.Fatal("unknown receipt committed despite forced observation-binding failure")
	}
	if _, err := db.Pool.Exec(ctx, `DROP TRIGGER reject_delegation_receipt_binding ON plan_delegations`); err != nil {
		t.Fatalf("drop observation-binding failure trigger: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `DROP FUNCTION reject_delegation_receipt_binding()`); err != nil {
		t.Fatalf("drop observation-binding failure function: %v", err)
	}
	afterBinding, err := delegations.Get(ctx, request.DelegationID)
	if err != nil {
		t.Fatalf("load binding after rolled-back receipt: %v", err)
	}
	afterState, err := store.Load(ctx, binding.LifecycleID)
	if err != nil {
		t.Fatalf("load lifecycle after rolled-back receipt: %v", err)
	}
	var auditCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM plan_task_receipts WHERE lifecycle_id=$1::uuid`, binding.LifecycleID).Scan(&auditCount); err != nil {
		t.Fatalf("count receipt audit after rollback: %v", err)
	}
	if afterBinding.Sequence != beforeBinding.Sequence || afterBinding.LifecycleRevision != beforeBinding.LifecycleRevision ||
		afterState.Revision != beforeState.Revision || len(afterState.Receipts) != len(beforeState.Receipts) || auditCount != 0 || afterState.Completed[authorTask.ID] != "" {
		t.Fatalf("failed binding commit left partial receipt/lifecycle state: bindingBefore=%+v bindingAfter=%+v stateBefore=%+v stateAfter=%+v audit=%d", beforeBinding, afterBinding, beforeState, afterState, auditCount)
	}
}
