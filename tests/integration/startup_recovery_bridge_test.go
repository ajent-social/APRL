package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/app"
	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/dispatch"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/ajent-social/APRL/internal/reconcile"
)

func TestStartupRecoveryBridgeBlocksControlEffectsUntilInventoryComplete(t *testing.T) {
	f := newDispatchTestFixture(t)
	seed := startupBridgeSeed(t, f, "startup-bridge-barrier")
	recovery := startupBridgeNewRecovery(seed.holds, false)
	service := startupBridgeNewControl(t, f, recovery)
	ctx, cancel := context.WithCancel(context.Background())
	done := serviceRoleRun(ctx, t, service)
	stopped := false
	defer func() {
		if !stopped {
			cancel()
			serviceRoleStopAllowError(t, service, done)
		}
	}()

	select {
	case <-recovery.started:
	case <-time.After(3 * time.Second):
		t.Fatal("control did not enter the startup inventory barrier")
	}
	startupBridgeRequireUnchanged(t, f, seed, "before inventory release")
	close(recovery.release)
	serviceRoleWaitUntil(t, "control publishes after hold inventory completes", func() (bool, error) {
		var published *time.Time
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, seed.outboxID).Scan(&published); err != nil {
			return false, err
		}
		return published != nil, nil
	})
	serviceRoleWaitUntil(t, "control routes the inbox after hold inventory completes", func() (bool, error) {
		var disposition string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT disposition FROM webhook_deliveries WHERE delivery_id=$1`, seed.deliveryID).Scan(&disposition); err != nil {
			return false, err
		}
		return disposition == "PROCESSED", nil
	})
	serviceRoleStop(t, service, done)
	stopped = true
	cancel()

	if len(recovery.observedHolds()) != 1 || recovery.observedHolds()[0].RunID != seed.lease.RunID || recovery.observedHolds()[0].State != processholds.StateUnknown {
		t.Fatalf("fixture inventory did not preserve the ambiguous hold: %+v", recovery.observedHolds())
	}
	current, err := leases.Claim(f.ctx, f.db.Pool, f.clock, startupBridgeClaimRequest(f, seed.jobID))
	if !errors.Is(err, leases.ErrBusy) {
		t.Fatalf("replacement lease after complete inventory=(%+v,%v), want process-capacity denial", current, err)
	}
	var jobState string
	var leaseToken *string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token::text FROM jobs WHERE id=$1::uuid`, seed.jobID).Scan(&jobState, &leaseToken); err != nil {
		t.Fatal(err)
	}
	var runs, reservations int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM agent_runs WHERE job_id=$1::uuid`, seed.jobID).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM budget_reservations WHERE run_id IN (SELECT id FROM agent_runs WHERE job_id=$1::uuid)`, seed.jobID).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if jobState != "PENDING" || leaseToken != nil || runs != 1 || reservations != 1 {
		t.Fatalf("replacement crossed unresolved hold fence: job=%s lease=%v runs=%d reservations=%d", jobState, leaseToken, runs, reservations)
	}
	startupBridgeRequireUnknownRetained(t, f, seed)
	if exists, err := f.redis.Client.Exists(f.ctx, f.streamKey()).Result(); err != nil || exists == 0 {
		t.Fatalf("control did not start dispatch after recovery: stream exists=%d err=%v", exists, err)
	}
}

func TestStartupRecoveryBridgeLateInventoryCannotStartControl(t *testing.T) {
	f := newDispatchTestFixture(t)
	seed := startupBridgeSeed(t, f, "startup-bridge-late")
	recovery := startupBridgeNewRecovery(seed.holds, true)
	config := serviceRoleConfig(app.RoleControl)
	config.RecoveryTimeout = 100 * time.Millisecond
	service := startupBridgeNewControlWithConfig(t, f, recovery, config)
	done := serviceRoleRun(context.Background(), t, service)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("late nil inventory result allowed control startup")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("control did not fail after the bounded recovery deadline")
	}
	startupBridgeRequireUnchanged(t, f, seed, "after late inventory")
	if len(recovery.observedHolds()) != 0 {
		t.Fatalf("late inventory unexpectedly completed: %+v", recovery.observedHolds())
	}
}

type startupBridgeSeedData struct {
	jobID       string
	outboxID    string
	deliveryID  string
	lease       leases.Lease
	hold        processholds.Hold
	reservation budget.Reservation
	holds       *processholds.Store
}

func startupBridgeSeed(t *testing.T, f *dispatchTestFixture, deliveryID string) startupBridgeSeedData {
	t.Helper()
	jobID := f.addJob(t, "author")
	outboxID := f.addOutbox(t, jobID, queue.DispatchKind, `{"job_id":"`+jobID+`"}`, f.clock.Now())
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, startupBridgeClaimRequest(f, jobID))
	if err != nil {
		t.Fatalf("claim old run before hold reservation: %v", err)
	}
	envelope := contracts.BudgetEnvelope{MaxCostMicroUSD: 1000, MaxInputTokens: 10, MaxOutputTokens: 10, MaxCalls: 1, PricingVersion: "startup-bridge-test-v1"}
	reservation, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, lease, envelope)
	if err != nil {
		t.Fatalf("reserve old run budget: %v", err)
	}
	holds, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "startup-bridge-test", MaxActive: 1, VerifierTimeout: time.Second}, unresolvedAdmissionVerifier{})
	if err != nil {
		t.Fatalf("construct real process-hold store: %v", err)
	}
	hold, err := holds.Reserve(f.ctx, lease, "/owned/startup-bridge-fixture", "startup-bridge-supervisor")
	if err != nil {
		t.Fatalf("reserve original host capacity: %v", err)
	}
	if hold, err = holds.BeginStart(f.ctx, lease); err != nil {
		t.Fatalf("persist original process launch intent: %v", err)
	}
	if hold, err = holds.Unknown(f.ctx, lease.RunID, processholds.ReasonSupervisorLost); err != nil {
		t.Fatalf("retain ambiguous process hold: %v", err)
	}
	if err := budget.MarkUnknown(f.ctx, f.db.Pool, f.clock, reservation.ID); err != nil {
		t.Fatalf("retain ambiguous budget usage: %v", err)
	}
	f.clock.Advance(2 * time.Minute)
	payload := `{"action":"labeled","repository":{"full_name":"owner/repo"},"sender":{"id":17,"type":"User"},"issue":{"id":1001,"number":31,"user":{"id":44,"type":"User"}},"label":{"name":"aprl:implement"}}`
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO webhook_deliveries(delivery_id,event_type,payload,disposition,received_at) VALUES($1,'issues',$2::jsonb,'INBOX',$3)`, deliveryID, payload, f.clock.Now().Add(-time.Second)); err != nil {
		t.Fatalf("seed routable inbox delivery: %v", err)
	}
	return startupBridgeSeedData{jobID: jobID, outboxID: outboxID, deliveryID: deliveryID, lease: lease, hold: hold, reservation: reservation, holds: holds}
}

func startupBridgeClaimRequest(f *dispatchTestFixture, jobID string) leases.ClaimRequest {
	return leases.ClaimRequest{TaskID: f.taskID, JobID: jobID, TTL: time.Minute, AgentType: "A", PromptHash: dispatchTestPromptHash, SupervisorIdentity: "startup-bridge-supervisor", CredentialID: "startup-bridge-credential"}
}

type startupBridgeRecovery struct {
	holds   *processholds.Store
	late    bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.RWMutex
	ready   bool
	seen    map[string]startupBridgeHoldState
}

type startupBridgeHoldState struct {
	state    string
	revision int64
}

func startupBridgeNewRecovery(holds *processholds.Store, late bool) *startupBridgeRecovery {
	return &startupBridgeRecovery{holds: holds, late: late, started: make(chan struct{}), release: make(chan struct{}), seen: make(map[string]startupBridgeHoldState)}
}

func (r *startupBridgeRecovery) Recover(ctx context.Context) error {
	r.once.Do(func() { close(r.started) })
	if r.late {
		<-ctx.Done()
		return nil
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	unresolved, err := r.holds.ListUnresolved(ctx)
	if err != nil {
		return err
	}
	seen := make(map[string]startupBridgeHoldState, len(unresolved))
	for _, hold := range unresolved {
		// The fixture can inventory an unresolved hold but has no host proof
		// capable of turning ambiguity into release authority.
		seen[hold.RunID] = startupBridgeHoldState{state: hold.State, revision: hold.Revision}
	}
	r.mu.Lock()
	r.seen = seen
	r.ready = true
	r.mu.Unlock()
	return nil
}

func (r *startupBridgeRecovery) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	ready := r.ready
	seen := make(map[string]startupBridgeHoldState, len(r.seen))
	for runID, state := range r.seen {
		seen[runID] = state
	}
	r.mu.RUnlock()
	if !ready {
		return app.ErrUnavailable
	}
	current, err := r.holds.ListUnresolved(ctx)
	if err != nil {
		return err
	}
	if len(current) != len(seen) {
		return app.ErrUnavailable
	}
	for _, hold := range current {
		prior, ok := seen[hold.RunID]
		if !ok || prior.state != hold.State || prior.revision != hold.Revision {
			return app.ErrUnavailable
		}
	}
	return ctx.Err()
}

func (r *startupBridgeRecovery) observedHolds() []startupBridgeHold {
	r.mu.RLock()
	defer r.mu.RUnlock()
	holds := make([]startupBridgeHold, 0, len(r.seen))
	for runID, state := range r.seen {
		holds = append(holds, startupBridgeHold{RunID: runID, State: state.state})
	}
	return holds
}

type startupBridgeHold struct {
	RunID string
	State string
}

func startupBridgeNewControl(t *testing.T, f *dispatchTestFixture, recovery *startupBridgeRecovery) *app.App {
	t.Helper()
	return startupBridgeNewControlWithConfig(t, f, recovery, serviceRoleConfig(app.RoleControl))
}

func startupBridgeNewControlWithConfig(t *testing.T, f *dispatchTestFixture, recovery *startupBridgeRecovery, config app.Config) *app.App {
	t.Helper()
	service, err := app.NewControl(config, app.ControlDependencies{
		Pool: f.db.Pool, Redis: f.redis.Client, Clock: f.clock, Streams: f.streams,
		RouterConfig: serviceRoleRouterConfig(), Operations: serviceRoleOperations{}, Labels: serviceRoleLabels{},
		ReconcileConfig: reconcile.Config{BatchSize: 1, RPCTimeout: 100 * time.Millisecond, MaxRetryDelay: time.Second},
		CancelAck:       func(context.Context, dispatch.OutboxItem) error { return nil }, NotifyAck: func(context.Context, dispatch.OutboxItem) error { return nil },
		StartupRecovery: recovery,
	})
	if err != nil {
		t.Fatalf("construct composed control role: %v", err)
	}
	return service
}

func startupBridgeRequireUnchanged(t *testing.T, f *dispatchTestFixture, seed startupBridgeSeedData, when string) {
	t.Helper()
	var jobStatus, runStatus, holdState, budgetStatus, disposition string
	var leaseToken *string
	var holdRevision int64
	var published *time.Time
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token::text FROM jobs WHERE id=$1::uuid`, seed.jobID).Scan(&jobStatus, &leaseToken); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT execution_status FROM agent_runs WHERE id=$1::uuid`, seed.lease.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,revision FROM process_holds WHERE run_id=$1::uuid`, seed.lease.RunID).Scan(&holdState, &holdRevision); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM budget_reservations WHERE id=$1::uuid`, seed.reservation.ID).Scan(&budgetStatus); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, seed.outboxID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT disposition FROM webhook_deliveries WHERE delivery_id=$1`, seed.deliveryID).Scan(&disposition); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "LEASED" || leaseToken == nil || *leaseToken != seed.lease.Token || runStatus != "RUNNING" || holdState != processholds.StateUnknown || holdRevision != seed.hold.Revision || budgetStatus != "UNKNOWN" || published != nil || disposition != "INBOX" {
		t.Fatalf("startup side effect %s: job=%s lease=%v run=%s hold=%s/%d budget=%s published=%v inbox=%s", when, jobStatus, leaseToken, runStatus, holdState, holdRevision, budgetStatus, published, disposition)
	}
	if exists, err := f.redis.Client.Exists(f.ctx, f.streamKey()).Result(); err != nil || exists != 0 {
		t.Fatalf("Redis stream/group exists %s: exists=%d err=%v", when, exists, err)
	}
	var rows int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM agent_runs WHERE task_id=$1::uuid`, f.taskID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("agent run count %s = %d, want only original run", when, rows)
	}
}

func startupBridgeRequireUnknownRetained(t *testing.T, f *dispatchTestFixture, seed startupBridgeSeedData) {
	t.Helper()
	var state string
	var revision int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,revision FROM process_holds WHERE run_id=$1::uuid`, seed.lease.RunID).Scan(&state, &revision); err != nil {
		t.Fatal(err)
	}
	var budgetStatus string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM budget_reservations WHERE id=$1::uuid`, seed.reservation.ID).Scan(&budgetStatus); err != nil {
		t.Fatal(err)
	}
	if state != processholds.StateUnknown || revision != seed.hold.Revision || budgetStatus != "UNKNOWN" {
		t.Fatalf("startup recovery changed ambiguous evidence: hold=%s/%d budget=%s; want UNKNOWN/%d/UNKNOWN", state, revision, budgetStatus, seed.hold.Revision)
	}
}

var _ app.StartupRecovery = (*startupBridgeRecovery)(nil)
