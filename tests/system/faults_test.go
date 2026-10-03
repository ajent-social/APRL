package system

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ajent-social/APRL/internal/broker"
	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/ci"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/control"
	"github.com/ajent-social/APRL/internal/dispatch"
	"github.com/ajent-social/APRL/internal/httpapi"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/ajent-social/APRL/internal/reconcile"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

const (
	faultsRepository = "faults-owner/repo"
	faultsOrgID      = "faults-system-org"
	faultsWebhookKey = "faults-test-webhook-secret-32-bytes"
)

type faultsFixture struct {
	db      testutil.DatabaseFixture
	redis   testutil.RedisFixture
	ctx     context.Context
	cancel  context.CancelFunc
	clock   *clock.Manual
	router  *router.Router
	webhook *httpapi.WebhookHandler
	streams *queue.Streams
}

func faultsRequireFixture(t *testing.T) *faultsFixture {
	t.Helper()
	db, redisFixture := testutil.RequireServices(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate fault fixture: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id,rolling_limit_micro_usd) VALUES($1,2000000)`, faultsOrgID); err != nil {
		t.Fatal(err)
	}
	clk := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	policy := router.RepositoryPolicy{OrgID: faultsOrgID, PolicyVersion: "faults-v1", TaskBudgetLimitMicroUSD: 2_000_000,
		IssueEnrollmentLabel: "aprl:implement", AuthorizedIssueLabelerIDs: map[int64]struct{}{17: {}}}
	r, err := router.New(db.Pool, clk, router.Config{Repositories: map[string]router.RepositoryPolicy{faultsRepository: policy}, MaxTaskBudgetMicroUSD: 2_000_000})
	if err != nil {
		t.Fatal(err)
	}
	wh, err := httpapi.NewWebhookHandler([]byte(faultsWebhookKey), db.Pool, clk)
	if err != nil {
		t.Fatal(err)
	}
	return &faultsFixture{db: db, redis: redisFixture, ctx: ctx, cancel: cancel, clock: clk, router: r, webhook: wh}
}

func (f *faultsFixture) newStreams(t *testing.T, suffix string) *queue.Streams {
	t.Helper()
	options := *f.redis.Client.Options()
	options.ContextTimeoutEnabled = true
	client := redis.NewClient(&options)
	streams, err := queue.NewStreams(client, f.redis.KeyPrefix+"foundation-"+suffix, "foundation-"+suffix)
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := streams.Close(); err != nil {
			t.Errorf("close fault stream: %v", err)
		}
	})
	return streams
}

func (f *faultsFixture) signedDelivery(t *testing.T, deliveryID string) int {
	return f.signedIssueDelivery(t, deliveryID, 31)
}

func (f *faultsFixture) signedIssueDelivery(t *testing.T, deliveryID string, issueNumber int) int {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"action":"labeled","repository":{"full_name":%q},"sender":{"id":17,"type":"User"},"issue":{"id":1001,"number":%d,"user":{"id":44,"type":"User"}},"label":{"name":"aprl:implement"}}`, faultsRepository, issueNumber))
	mac := hmac.New(sha256.New, []byte(faultsWebhookKey))
	_, _ = mac.Write(body)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Delivery", deliveryID)
	req.Header.Set("X-GitHub-Event", "issues")
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	rec := httptest.NewRecorder()
	f.webhook.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted && rec.Code != http.StatusOK {
		t.Fatalf("signed webhook status=%d body=%s", rec.Code, rec.Body.String())
	}
	return rec.Code
}

func (f *faultsFixture) route(t *testing.T, deliveryID string) string {
	return f.routeIssue(t, deliveryID, 31)
}

func (f *faultsFixture) routeIssue(t *testing.T, deliveryID string, issueNumber int) string {
	t.Helper()
	if err := f.router.RouteDelivery(f.ctx, deliveryID); err != nil {
		t.Fatalf("route delivery: %v", err)
	}
	var taskID string
	source := fmt.Sprintf("issue:%d", issueNumber)
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text FROM tasks WHERE repo_full_name=$1 AND source_key=$2`, faultsRepository, source).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	return taskID
}

type faultsOperations struct{}

func (faultsOperations) Reconcile(context.Context, string) (broker.Operation, error) {
	return broker.Operation{}, broker.ErrUnknown
}

type faultsLabels struct{}

func (faultsLabels) ApplyLifecycleLabel(context.Context, reconcile.LabelIntent) error { return nil }

type faultsHostVerifier struct{}

func (faultsHostVerifier) VerifyNeverStarted(context.Context, processholds.Hold) (processholds.ReapEvidence, error) {
	return processholds.ReapEvidence{}, fmt.Errorf("fault fixture has no trusted never-started proof")
}

func (faultsHostVerifier) VerifyGroupDrained(context.Context, processholds.Hold) (processholds.ReapEvidence, error) {
	return processholds.ReapEvidence{}, fmt.Errorf("fault fixture has no trusted process-group proof")
}

func faultsRecoveryGate(pool *pgxpool.Pool, clk clock.Clock) func(context.Context) error {
	store, err := processholds.NewStore(pool, clk, processholds.Config{ResourceScope: "faults-test-host", MaxActive: 1, VerifierTimeout: time.Second}, faultsHostVerifier{})
	if err != nil {
		return func(context.Context) error { return err }
	}
	return func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		holds, err := store.ListUnresolved(ctx)
		if err != nil {
			return err
		}
		if len(holds) != 0 {
			return fmt.Errorf("trusted fixture verifier cannot prove %d unresolved process hold(s)", len(holds))
		}
		return nil
	}
}

func TestFoundationFaultsSignedReplaySurvivesRedisLossAndRestart(t *testing.T) {
	f := faultsRequireFixture(t)
	if got := f.signedDelivery(t, "fault-delivery-replay"); got != http.StatusAccepted {
		t.Fatalf("first signed delivery status=%d, want 202", got)
	}
	taskID := f.route(t, "fault-delivery-replay")
	var jobID string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text FROM jobs WHERE task_id=$1::uuid`, taskID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	var dueAt time.Time
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT min(next_attempt_at) FROM outbox WHERE job_id=$1::uuid`, jobID).Scan(&dueAt); err != nil {
		t.Fatal(err)
	}
	// RouteDelivery may use the database wall clock for its initial due time;
	// move the manual clock to the persisted due instant before dispatch.
	if f.clock.Now().Before(dueAt) {
		f.clock.Set(dueAt)
	}
	original := f.newStreams(t, "jobs")
	dispatcher, err := dispatch.NewDispatcher(f.db.Pool, f.clock, original, nil)
	if err != nil {
		t.Fatal(err)
	}
	if published, err := dispatcher.DispatchDue(f.ctx, 1); err != nil || published != 1 {
		t.Fatalf("initial durable dispatch published=%d err=%v, want 1", published, err)
	}
	// Simulate loss of only this test's Redis queue keys. The durable job and
	// outbox remain untouched in Postgres.
	pattern := f.redis.KeyPrefix + "foundation-jobs*"
	var cursor uint64
	for {
		keys, next, err := f.redis.Client.Scan(f.ctx, cursor, pattern, 100).Result()
		if err != nil {
			t.Fatalf("scan owned Redis prefix: %v", err)
		}
		if len(keys) != 0 {
			if err := f.redis.Client.Del(f.ctx, keys...).Err(); err != nil {
				t.Fatalf("delete owned Redis keys: %v", err)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	f.streams = f.newStreams(t, "jobs") // Recreated service object after queue loss.
	if err := f.streams.EnsureGroup(f.ctx); err != nil {
		t.Fatal(err)
	}
	repair, err := reconcile.New(f.db.Pool, f.clock, f.streams, faultsOperations{},
		faultsRecoveryGate(f.db.Pool, f.clock),
		leases.FenceExpiredTask, faultsLabels{}, reconcile.Config{BatchSize: 10, RPCTimeout: time.Second, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repair.RunOnce(f.ctx); err != nil {
		t.Fatalf("repair after Redis loss: %v", err)
	}
	entries, err := f.streams.Read(f.ctx, "faults-consumer", 20*time.Millisecond, 10)
	if err != nil || len(entries) != 1 || entries[0].JobID != jobID {
		t.Fatalf("recovered stream entries=%+v err=%v; want one original job %s", entries, err, jobID)
	}
	if got := f.signedDelivery(t, "fault-delivery-replay"); got != http.StatusOK {
		t.Fatalf("duplicate signed delivery status=%d, want 200", got)
	}
	if err := f.router.RouteDelivery(f.ctx, "fault-delivery-replay"); err != nil {
		t.Fatal(err)
	}
	var jobCount, deliveryCount int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid`, taskID).Scan(&jobCount); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM webhook_deliveries WHERE delivery_id=$1`, "fault-delivery-replay").Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if jobCount != 1 || deliveryCount != 1 {
		t.Fatalf("durable replay counts jobs=%d deliveries=%d, want 1/1", jobCount, deliveryCount)
	}
	var status string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM jobs WHERE id=$1::uuid`, jobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" {
		t.Fatalf("job status after lost Redis hint=%s, want PENDING", status)
	}
}

type faultsRun struct {
	job      contracts.Job
	lease    leases.Lease
	envelope contracts.BudgetEnvelope
}

func (f *faultsFixture) claimRun(t *testing.T, taskID string) faultsRun {
	t.Helper()
	run := f.claimRunWithoutReservation(t, taskID)
	if _, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope); err != nil {
		t.Fatalf("reserve bounded test inference: %v", err)
	}
	return run
}

func (f *faultsFixture) claimRunWithoutReservation(t *testing.T, taskID string) faultsRun {
	t.Helper()
	var jobID string
	var raw []byte
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text,payload FROM jobs WHERE task_id=$1::uuid`, taskID).Scan(&jobID, &raw); err != nil {
		t.Fatal(err)
	}
	job, err := contracts.DecodeJob(raw)
	if err != nil {
		t.Fatalf("decode routed job: %v", err)
	}
	agentType, ok := leases.AgentTypeForOperation(job.Operation)
	if !ok {
		t.Fatalf("routed operation %q has no trusted agent mapping", job.Operation)
	}
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: taskID, JobID: jobID,
		TTL: time.Minute, AgentType: agentType, PromptHash: strings.Repeat("a", 64),
		SupervisorIdentity: "faults-test-supervisor", CredentialID: "faults-test-credential"})
	if err != nil {
		t.Fatalf("claim routed job: %v", err)
	}
	envelope := contracts.BudgetEnvelope{MaxCostMicroUSD: 1000, MaxInputTokens: 100, MaxOutputTokens: 50, MaxCalls: 2, PricingVersion: "faults-rate-v1"}
	return faultsRun{job: job, lease: lease, envelope: envelope}
}

type faultsAuthenticator struct{ principal results.Principal }

func (a faultsAuthenticator) AuthenticateSupervisor(context.Context, *http.Request) (results.Principal, error) {
	return a.principal, nil
}

func (f *faultsFixture) submitResult(t *testing.T, run faultsRun, result contracts.Result) *httptest.ResponseRecorder {
	t.Helper()
	service, err := results.New(f.db.Pool, f.clock, f.router)
	if err != nil {
		t.Fatal(err)
	}
	principal := results.Principal{TaskID: run.lease.TaskID, RunID: run.lease.RunID,
		Identity: "faults-test-supervisor", CredentialID: "faults-test-credential"}
	handler, err := httpapi.NewResultsHandler(service, faultsAuthenticator{principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/results", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func faultsResult(run faultsRun, summary string) contracts.Result {
	return contracts.Result{Version: 1, TaskID: run.job.TaskID, JobID: run.job.JobID, RunID: run.lease.RunID,
		Generation: run.lease.Generation, LeaseToken: run.lease.Token, Snapshot: run.job.Snapshot, Attempt: run.job.Attempt,
		OperationID: run.job.OperationID, CorrelationID: run.job.CorrelationID, Status: "failed", Summary: summary}
}

func TestFoundationFaultsDuplicateAndStaleHTTPResults(t *testing.T) {
	f := faultsRequireFixture(t)
	f.signedDelivery(t, "fault-result-replay")
	firstTask := f.route(t, "fault-result-replay")
	first := f.claimRun(t, firstTask)
	result := faultsResult(first, "bounded fixture failure")
	if rec := f.submitResult(t, first, result); rec.Code != http.StatusOK {
		t.Fatalf("first results POST status=%d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if rec := f.submitResult(t, first, result); rec.Code != http.StatusOK {
		t.Fatalf("exact duplicate results POST status=%d body=%s, want idempotent 200", rec.Code, rec.Body.String())
	}
	conflict := result
	conflict.Summary = "changed meaning under same operation id"
	if rec := f.submitResult(t, first, conflict); rec.Code != http.StatusConflict {
		t.Fatalf("changed duplicate results POST status=%d body=%s, want 409", rec.Code, rec.Body.String())
	}
	var receiptCount int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM worker_result_receipts WHERE operation_id=$1::uuid`, result.OperationID).Scan(&receiptCount); err != nil {
		t.Fatal(err)
	}
	if receiptCount != 1 {
		t.Fatalf("receipt count after duplicate/conflict=%d, want 1", receiptCount)
	}

	f.signedIssueDelivery(t, "fault-result-stale", 32)
	staleTask := f.routeIssue(t, "fault-result-stale", 32)
	staleRun := f.claimRun(t, staleTask)
	staleResult := faultsResult(staleRun, "must remain stale")
	if err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		locked, err := repos.LockTask(ctx, staleTask)
		if err != nil {
			return err
		}
		_, err = control.PauseLocked(ctx, repos, locked, f.clock, control.PauseRequest{
			Authority: control.Authority{ActorID: "17", Repository: faultsRepository, Permission: control.PermissionWrite, VerifiedAt: f.clock.Now()},
			Reason:    "system fault test pause",
		})
		return err
	}); err != nil {
		t.Fatalf("pause task through durable control boundary: %v", err)
	}
	var reservationStatusBefore, reservationStatusAfter string
	var remainingBefore, remainingAfter int64
	var chargesBefore, chargesAfter int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,remaining_micro_usd FROM budget_reservations WHERE run_id=$1::uuid`, staleRun.lease.RunID).Scan(&reservationStatusBefore, &remainingBefore); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM cost_entries WHERE reservation_id=(SELECT id FROM budget_reservations WHERE run_id=$1::uuid)`, staleRun.lease.RunID).Scan(&chargesBefore); err != nil {
		t.Fatal(err)
	}
	if rec := f.submitResult(t, staleRun, staleResult); rec.Code != http.StatusConflict {
		t.Fatalf("revoked generation results POST status=%d body=%s, want stale 409", rec.Code, rec.Body.String())
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM worker_result_receipts WHERE operation_id=$1::uuid`, staleResult.OperationID).Scan(&receiptCount); err != nil {
		t.Fatal(err)
	}
	if receiptCount != 0 {
		t.Fatalf("stale result created %d receipts, want none", receiptCount)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,remaining_micro_usd FROM budget_reservations WHERE run_id=$1::uuid`, staleRun.lease.RunID).Scan(&reservationStatusAfter, &remainingAfter); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM cost_entries WHERE reservation_id=(SELECT id FROM budget_reservations WHERE run_id=$1::uuid)`, staleRun.lease.RunID).Scan(&chargesAfter); err != nil {
		t.Fatal(err)
	}
	if reservationStatusAfter != reservationStatusBefore || remainingAfter != remainingBefore || chargesAfter != chargesBefore {
		t.Fatalf("stale result altered budget evidence status=%s/%s remaining=%d/%d charges=%d/%d", reservationStatusBefore, reservationStatusAfter, remainingBefore, remainingAfter, chargesBefore, chargesAfter)
	}
	var cancelPayload []byte
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT payload FROM outbox WHERE task_id=$1::uuid AND job_id=$2::uuid AND kind='CANCEL' ORDER BY created_at DESC LIMIT 1`, staleTask, staleRun.job.JobID).Scan(&cancelPayload); err != nil {
		t.Fatalf("read durable pause cancellation target: %v", err)
	}
	var cancelTarget struct {
		TaskID string `json:"task_id"`
		JobID  string `json:"job_id"`
		RunID  string `json:"run_id"`
	}
	if err := json.Unmarshal(cancelPayload, &cancelTarget); err != nil {
		t.Fatalf("decode durable cancellation target: %v", err)
	}
	if cancelTarget.TaskID != staleTask || cancelTarget.JobID != staleRun.job.JobID || cancelTarget.RunID != staleRun.lease.RunID {
		t.Fatalf("pause cancellation target=%+v, want exact original task/job/run tuple", cancelTarget)
	}
	var state string
	var generation int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,generation FROM tasks WHERE id=$1::uuid`, staleTask).Scan(&state, &generation); err != nil {
		t.Fatal(err)
	}
	if state != "PAUSED" || generation != staleRun.lease.Generation+1 {
		t.Fatalf("task after stale result=(%s,%d), want PAUSED/%d", state, generation, staleRun.lease.Generation+1)
	}
}

func TestFoundationFaultsConcurrentBudgetAdmissionsStayWithinOrgLimit(t *testing.T) {
	f := faultsRequireFixture(t)
	if _, err := f.db.Pool.Exec(f.ctx, `UPDATE org_budgets SET rolling_limit_micro_usd=1000 WHERE org_id=$1`, faultsOrgID); err != nil {
		t.Fatal(err)
	}
	f.signedDelivery(t, "fault-budget-one")
	oneTask := f.route(t, "fault-budget-one")
	one := f.claimRunWithoutReservation(t, oneTask)
	f.signedIssueDelivery(t, "fault-budget-two", 32)
	twoTask := f.routeIssue(t, "fault-budget-two", 32)
	two := f.claimRunWithoutReservation(t, twoTask)
	start := make(chan struct{})
	type admission struct{ err error }
	results := make(chan admission, 2)
	var wg sync.WaitGroup
	for _, run := range []faultsRun{one, two} {
		wg.Add(1)
		go func(run faultsRun) {
			defer wg.Done()
			<-start
			_, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope)
			results <- admission{err: err}
		}(run)
	}
	close(start)
	wg.Wait()
	close(results)
	admitted, denied := 0, 0
	for result := range results {
		if result.err == nil {
			admitted++
		} else if errors.Is(result.err, budget.ErrOrganizationBudget) {
			denied++
		} else {
			t.Fatalf("budget admission failed unexpectedly: %v", result.err)
		}
	}
	if admitted != 1 || denied != 1 {
		t.Fatalf("concurrent budget admissions=(%d admitted,%d denied), want 1/1", admitted, denied)
	}
	var reserved int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT coalesce(sum(remaining_micro_usd),0) FROM budget_reservations WHERE org_id=$1 AND status IN ('RESERVED','SETTLING','UNKNOWN')`, faultsOrgID).Scan(&reserved); err != nil {
		t.Fatal(err)
	}
	if reserved > 1000 {
		t.Fatalf("outstanding org reservation=%d, exceeds 1000 micro-USD", reserved)
	}
}

func TestFoundationFaultsUnknownProcessHoldBlocksRestartRepair(t *testing.T) {
	f := faultsRequireFixture(t)
	f.signedIssueDelivery(t, "fault-hold-gate", 33)
	taskID := f.routeIssue(t, "fault-hold-gate", 33)
	run := f.claimRun(t, taskID)
	store, err := processholds.NewStore(f.db.Pool, f.clock,
		processholds.Config{ResourceScope: "faults-test-host", MaxActive: 1, VerifierTimeout: time.Second}, faultsHostVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reserve(f.ctx, run.lease, "/faults/system/issue-33", "faults-test-supervisor"); err != nil {
		t.Fatalf("reserve fixture process capacity: %v", err)
	}
	if _, err := store.BeginStart(f.ctx, run.lease); err != nil {
		t.Fatalf("commit one-time launch intent before ambiguous start: %v", err)
	}
	var reservationID string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text FROM budget_reservations WHERE run_id=$1::uuid`, run.lease.RunID).Scan(&reservationID); err != nil {
		t.Fatal(err)
	}
	if err := budget.MarkUnknown(f.ctx, f.db.Pool, f.clock, reservationID); err != nil {
		t.Fatalf("retain ambiguous provider usage as UNKNOWN: %v", err)
	}
	f.clock.Advance(2 * time.Minute) // Expire the durable lease without releasing the hold.
	streams := f.newStreams(t, "held-job")
	repair, err := reconcile.New(f.db.Pool, f.clock, streams, faultsOperations{},
		faultsRecoveryGate(f.db.Pool, f.clock), leases.FenceExpiredTask, faultsLabels{},
		reconcile.Config{BatchSize: 10, RPCTimeout: time.Second, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repair.RunOnce(f.ctx); !errors.Is(err, reconcile.ErrNotReady) {
		t.Fatalf("restart repair with unresolved host hold=%v, want ErrNotReady", err)
	}
	var taskState, jobState, holdState, budgetState, workspace string
	var generation int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,generation FROM tasks WHERE id=$1::uuid`, taskID).Scan(&taskState, &generation); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM jobs WHERE id=$1::uuid`, run.job.JobID).Scan(&jobState); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,workspace FROM process_holds WHERE run_id=$1::uuid`, run.lease.RunID).Scan(&holdState, &workspace); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM budget_reservations WHERE run_id=$1::uuid`, run.lease.RunID).Scan(&budgetState); err != nil {
		t.Fatal(err)
	}
	if taskState != "AUTHORING" || generation != 0 || jobState != "LEASED" || holdState != processholds.StateUnknown || budgetState != "UNKNOWN" || workspace != "/faults/system/issue-33" {
		t.Fatalf("unknown-hold restart state task=%s/%d job=%s hold=%s workspace=%s budget=%s; want original generation/lease, UNKNOWN hold, owned workspace, and retained UNKNOWN reservation", taskState, generation, jobState, holdState, workspace, budgetState)
	}
	var streamLen int64
	streamKey := f.redis.KeyPrefix + "foundation-held-job"
	streamLen, err = f.redis.Client.XLen(f.ctx, streamKey).Result()
	if err != nil && err != redis.Nil {
		t.Fatalf("inspect isolated Redis stream: %v", err)
	}
	if streamLen != 0 {
		t.Fatalf("restart repair published %d stream entries despite unresolved hold", streamLen)
	}
	unresolved, err := store.ListUnresolved(f.ctx)
	if err != nil || len(unresolved) != 1 || unresolved[0].State != processholds.StateUnknown {
		t.Fatalf("durable unresolved host inventory=%+v err=%v, want one UNKNOWN hold", unresolved, err)
	}
	if _, err := store.Reaped(f.ctx, run.lease.RunID); err == nil {
		t.Fatal("fixture host without exact process proof unexpectedly reaped UNKNOWN hold")
	}
	f.signedIssueDelivery(t, "fault-hold-capacity", 35)
	otherTask := f.routeIssue(t, "fault-hold-capacity", 35)
	otherRun := f.claimRunWithoutReservation(t, otherTask)
	if _, err := store.Reserve(f.ctx, otherRun.lease, "/faults/system/issue-35", "faults-test-supervisor"); !errors.Is(err, processholds.ErrCapacity) {
		t.Fatalf("replacement process reservation=%v, want capacity denial while old UNKNOWN hold remains", err)
	}
}

type faultsBrokerTransport struct {
	mu          sync.Mutex
	executes    int
	lookups     int
	observation broker.RemoteObservation
}

func (f *faultsBrokerTransport) PullRequest(context.Context, string, int64) (broker.RemotePullRequest, error) {
	return broker.RemotePullRequest{}, fmt.Errorf("unexpected pull request read for pre-PR publication")
}

func (f *faultsBrokerTransport) Checks(context.Context, string, int64, contracts.Snapshot) ([]ci.Observation, error) {
	return nil, nil
}

func (f *faultsBrokerTransport) ChangedFiles(context.Context, string, string, string) ([]string, error) {
	return []string{}, nil
}

func (f *faultsBrokerTransport) Execute(context.Context, broker.RemoteOperation) (broker.RemoteReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.executes++
	// Model a host that applied the publication but dropped its response.
	return broker.RemoteReceipt{}, fmt.Errorf("injected response loss after remote application")
}

func (f *faultsBrokerTransport) Lookup(context.Context, string, string) (broker.RemoteObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	return f.observation, nil
}

func (f *faultsBrokerTransport) setObservation(observation broker.RemoteObservation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observation = observation
}

func (f *faultsBrokerTransport) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executes, f.lookups
}

func TestFoundationFaultsAmbiguousHostWriteReconcilesByLookupOnly(t *testing.T) {
	f := faultsRequireFixture(t)
	f.signedIssueDelivery(t, "fault-remote-write", 34)
	taskID := f.routeIssue(t, "fault-remote-write", 34)
	run := f.claimRunWithoutReservation(t, taskID)
	transport := &faultsBrokerTransport{}
	brokerConfig := broker.Config{Repositories: map[string]broker.RepositoryPolicy{
		faultsRepository: {AllowedTargetBranches: []string{"main"}},
	}, RPCTimeout: time.Second}
	service, err := broker.New(f.db.Pool, f.clock, transport, broker.TaskUUIDBranchResolver{}, brokerConfig)
	if err != nil {
		t.Fatal(err)
	}
	newHead := strings.Repeat("b", 40)
	request := broker.Request{OperationID: run.job.OperationID, Action: broker.ActionPublish, NewHeadSHA: newHead}
	if _, err := service.Execute(f.ctx, run.lease, request); !errors.Is(err, broker.ErrUnknown) {
		t.Fatalf("ambiguous remote publication error=%v, want UNKNOWN", err)
	}
	var status string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM github_operations WHERE id=$1::uuid`, run.job.OperationID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "UNKNOWN" {
		t.Fatalf("durable ambiguous host operation status=%s, want UNKNOWN", status)
	}

	// Construct a fresh broker/reconciler as after restart. Recovery receives
	// the immutable operation ID and may only ask the host for its outcome.
	restarted, err := broker.New(f.db.Pool, f.clock, transport, broker.TaskUUIDBranchResolver{}, brokerConfig)
	if err != nil {
		t.Fatal(err)
	}
	streams := f.newStreams(t, "ambiguous-write")
	repair, err := reconcile.New(f.db.Pool, f.clock, streams, restarted,
		faultsRecoveryGate(f.db.Pool, f.clock), leases.FenceExpiredTask, faultsLabels{},
		reconcile.Config{BatchSize: 10, RPCTimeout: time.Second, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repair.RunOnce(f.ctx); err != nil {
		t.Fatalf("first lookup-only recovery pass: %v", err)
	}
	writes, lookups := transport.counts()
	if writes != 1 || lookups != 1 {
		t.Fatalf("after unresolved recovery writes=%d lookups=%d, want one original write and one lookup", writes, lookups)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM github_operations WHERE id=$1::uuid`, run.job.OperationID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "UNKNOWN" {
		t.Fatalf("unresolved lookup changed operation status to %s, want UNKNOWN", status)
	}

	transport.setObservation(broker.RemoteObservation{Applied: true, Receipt: broker.RemoteReceipt{RemoteID: "faults-remote-operation", HeadSHA: newHead}})
	if _, err := repair.RunOnce(f.ctx); err != nil {
		t.Fatalf("confirmed lookup-only recovery pass: %v", err)
	}
	writes, lookups = transport.counts()
	if writes != 1 || lookups != 2 {
		t.Fatalf("after host confirmation writes=%d lookups=%d, want one write and two lookups", writes, lookups)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM github_operations WHERE id=$1::uuid`, run.job.OperationID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "CONFIRMED" {
		t.Fatalf("durable host operation status=%s after matching receipt, want CONFIRMED", status)
	}
}
