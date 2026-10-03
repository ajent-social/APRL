package system

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/dispatch"
	"github.com/ajent-social/APRL/internal/httpapi"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/internal/supervisor"
	"github.com/ajent-social/APRL/tests/testutil"
	"github.com/redis/go-redis/v9"
)

const foundationPromptHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestFoundationSignedReceiptRoutesDispatchesAndReturnsAuthenticatedResult(t *testing.T) {
	f := foundationRequireFixture(t)
	secret := []byte("foundation-test-webhook-key")
	webhook, err := httpapi.NewWebhookHandler(secret, f.db.Pool, f.clock)
	if err != nil {
		t.Fatalf("construct webhook handler: %v", err)
	}
	webhookServer := httptest.NewServer(webhook)
	t.Cleanup(webhookServer.Close)

	deliveryID := "foundation-issue-" + f.suffix
	payload := []byte(fmt.Sprintf(`{"action":"labeled","repository":{"full_name":%q},"sender":{"id":17,"type":"User"},"issue":{"id":1001,"number":31,"user":{"id":44,"type":"User"}},"label":{"name":"aprl:implement"}}`, f.repo))
	status, response := foundationPostSignedWebhook(t, webhookServer.Client(), webhookServer.URL, secret, deliveryID, "issues", payload)
	if status != http.StatusAccepted || !response.Accepted || response.Duplicate || response.DeliveryID != deliveryID {
		t.Fatalf("signed webhook=(%d,%+v), want accepted durable receipt", status, response)
	}
	status, response = foundationPostSignedWebhook(t, webhookServer.Client(), webhookServer.URL, secret, deliveryID, "issues", payload)
	if status != http.StatusOK || !response.Accepted || !response.Duplicate {
		t.Fatalf("identical signed webhook replay=(%d,%+v), want duplicate acknowledgement", status, response)
	}
	var receivedAt time.Time
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT received_at FROM webhook_deliveries WHERE delivery_id=$1`, deliveryID).Scan(&receivedAt); err != nil {
		t.Fatalf("read durable receipt timestamp: %v", err)
	}
	if !receivedAt.Equal(f.clock.Now()) {
		t.Fatalf("durable received_at=%s, want injected clock %s", receivedAt, f.clock.Now())
	}

	// Reconstruct the router after webhook persistence to prove the receipt is
	// sufficient durable input for enrollment and author job creation.
	f.router = foundationNewRouter(t, f)
	if err := f.router.RouteDelivery(f.ctx, deliveryID); err != nil {
		t.Fatalf("route durable webhook delivery: %v", err)
	}
	f.router = foundationNewRouter(t, f)
	if err := f.router.RouteDelivery(f.ctx, deliveryID); err != nil {
		t.Fatalf("replay already-routed delivery after router reconstruction: %v", err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text FROM tasks WHERE repo_full_name=$1 AND source_key='issue:31'`, f.repo).Scan(&f.taskID); err != nil {
		t.Fatalf("read routed task: %v", err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text FROM jobs WHERE task_id=$1::uuid AND operation_type='author'`, f.taskID).Scan(&f.jobID); err != nil {
		t.Fatalf("read routed author job: %v", err)
	}
	var jobCount int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid`, f.taskID).Scan(&jobCount); err != nil {
		t.Fatalf("count routed jobs after replay: %v", err)
	}
	if jobCount != 1 {
		t.Fatalf("routed jobs after duplicate delivery=%d, want 1", jobCount)
	}

	if err := f.streams.EnsureGroup(f.ctx); err != nil {
		t.Fatalf("create durable dispatch consumer group: %v", err)
	}
	var dispatchDueAt time.Time
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT next_attempt_at FROM outbox WHERE job_id=$1::uuid AND kind='DISPATCH'`, f.jobID).Scan(&dispatchDueAt); err != nil {
		t.Fatalf("read durable dispatch due time: %v", err)
	}
	if dispatchDueAt.After(f.clock.Now()) {
		f.clock.Set(dispatchDueAt)
	}
	dispatcher, err := dispatch.NewDispatcher(f.db.Pool, f.clock, f.streams, nil)
	if err != nil {
		t.Fatalf("construct outbox dispatcher: %v", err)
	}
	if count, err := dispatcher.DispatchDue(f.ctx, 1); err != nil || count != 1 {
		t.Fatalf("dispatch routed outbox=(%d,%v), want one publication", count, err)
	}
	deliveries, err := f.streams.Read(f.ctx, "foundation-consumer", 25*time.Millisecond, 1)
	if err != nil || len(deliveries) != 1 || deliveries[0].JobID != f.jobID || deliveries[0].Kind != queue.DispatchKind {
		t.Fatalf("read dispatched stream entry=(%+v,%v), want routed job %s", deliveries, err, f.jobID)
	}

	const identity = "foundation-test-supervisor"
	const credential = "foundation-test-credential-ref"
	// This explicit fixture envelope is test approval for the synthetic child;
	// it does not imply production task/profile admission policy.
	envelope := contracts.BudgetEnvelope{MaxCostMicroUSD: 1000, MaxInputTokens: 1000, MaxOutputTokens: 100,
		MaxCalls: 2, PricingVersion: "foundation-test-v1"}
	resultService, err := results.New(f.db.Pool, f.clock, f.router)
	if err != nil {
		t.Fatalf("construct results service: %v", err)
	}
	auth := &foundationAuthenticator{}
	resultHandler, err := httpapi.NewResultsHandler(resultService, auth)
	if err != nil {
		t.Fatalf("construct authenticated result handler: %v", err)
	}
	resultServer := httptest.NewServer(resultHandler)
	t.Cleanup(resultServer.Close)
	resultSubmitter := &foundationHTTPResultSubmitter{client: resultServer.Client(), url: resultServer.URL + "/internal/results", auth: auth}

	evidence := &foundationProcessEvidence{clock: f.clock, processes: make(map[string]processholds.ProcessIdentity)}
	processRunner := foundationBuildRunner(t)
	processRunner.evidence = evidence
	t.Cleanup(func() { processRunner.cleanup(t) })
	holds, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{
		ResourceScope: "foundation-resource-" + f.suffix, MaxActive: 1, VerifierTimeout: 2 * time.Second,
	}, evidence)
	if err != nil {
		t.Fatalf("construct process hold store: %v", err)
	}
	supervised, err := supervisor.New(f.db.Pool, f.clock, processRunner, resultSubmitter, holds, supervisor.Config{
		SupervisorIdentity: identity, CredentialID: credential, WorkspaceRoot: t.TempDir(), LeaseTTL: time.Minute,
		HeartbeatInterval: 100 * time.Millisecond, MaxExecution: 10 * time.Second, StartTimeout: 2 * time.Second,
		TermGrace: 100 * time.Millisecond, KillWait: time.Second,
	})
	if err != nil {
		t.Fatalf("construct native fake-child supervisor: %v", err)
	}
	var supervisorRunErr string
	consumer, err := queue.NewConsumer(f.db.Pool, f.clock, f.streams, queue.ConsumerConfig{
		LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: foundationPromptHash,
		SupervisorIdentity: identity, CredentialID: credential, BatchSize: 1, Block: time.Millisecond,
		OperationTimeout: 20 * time.Second,
		BudgetAdmission: func(ctx context.Context, lease leases.Lease, _ contracts.Job) error {
			_, err := budget.Reserve(ctx, f.db.Pool, f.clock, lease, envelope)
			return err
		},
	}, func(ctx context.Context, lease leases.Lease) error {
		err := supervised.Run(ctx, lease, envelope)
		if err != nil {
			supervisorRunErr = foundationDiagnosticText(err.Error(), 1024)
		}
		return err
	})
	if err != nil {
		t.Fatalf("construct durable stream consumer: %v", err)
	}
	if err := consumer.Handle(f.ctx, "foundation-consumer", deliveries[0]); err != nil {
		t.Fatalf("consume, execute and submit result: %v", err)
	}

	if got := resultSubmitter.status(); got != http.StatusOK {
		var jobStatus, leaseToken, leaseExpiry, runStatus, reservationStatus, holdStatus string
		var generation int64
		var runCount, reservationCount int
		var currentOutboxDue string
		diagnosticErr := f.db.Pool.QueryRow(f.ctx, `SELECT j.status,j.generation,COALESCE(j.lease_token::text,''),
			COALESCE(j.lease_expires_at::text,''),COALESCE((SELECT execution_status FROM agent_runs WHERE job_id=j.id ORDER BY attempt_number DESC LIMIT 1),''),
			(SELECT count(*) FROM agent_runs WHERE job_id=j.id),
			COALESCE((SELECT status FROM budget_reservations WHERE run_id=(SELECT id FROM agent_runs WHERE job_id=j.id ORDER BY attempt_number DESC LIMIT 1)),''),
			COALESCE((SELECT state FROM process_holds WHERE run_id=(SELECT id FROM agent_runs WHERE job_id=j.id ORDER BY attempt_number DESC LIMIT 1)),'')
			FROM jobs j WHERE j.id=$1::uuid`, f.jobID).
			Scan(&jobStatus, &generation, &leaseToken, &leaseExpiry, &runStatus, &runCount, &reservationStatus, &holdStatus)
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM budget_reservations WHERE task_id=$1::uuid AND run_id IN (SELECT id FROM agent_runs WHERE job_id=$2::uuid)`, f.taskID, f.jobID).Scan(&reservationCount); err != nil {
			diagnosticErr = errors.Join(diagnosticErr, fmt.Errorf("reservation count: %w", err))
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT COALESCE(max(next_attempt_at)::text,'') FROM outbox WHERE job_id=$1::uuid AND kind='DISPATCH'`, f.jobID).Scan(&currentOutboxDue); err != nil {
			diagnosticErr = errors.Join(diagnosticErr, fmt.Errorf("current dispatch due time: %w", err))
		}
		starts, childOutput, childExit := processRunner.diagnostic()
		t.Fatalf("authenticated result HTTP status=%d, want 200; diagnostic query err=%v job_status=%s generation=%d lease_token_present=%t lease_expiry=%s outbox_due_initial=%s outbox_due_current=%s clock_now=%s runs=%d latest_run=%s reservations=%d reservation=%s hold=%s runner_starts=%d supervisor_error=%q child_exit=%s child_stderr=%q",
			got, diagnosticErr, jobStatus, generation, leaseToken != "", leaseExpiry, dispatchDueAt.UTC().Format(time.RFC3339Nano),
			currentOutboxDue, f.clock.Now().UTC().Format(time.RFC3339Nano), runCount, runStatus, reservationCount, reservationStatus, holdStatus, starts, supervisorRunErr, childExit, childOutput)
	}
	var taskState, jobState, runState, reservationState string
	var reservationRemaining int64
	var costEntryCount int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT t.state,j.status,r.execution_status,b.status,b.remaining_micro_usd
		FROM tasks t JOIN jobs j ON j.task_id=t.id JOIN agent_runs r ON r.job_id=j.id
		JOIN budget_reservations b ON b.run_id=r.id WHERE t.id=$1::uuid AND j.id=$2::uuid`, f.taskID, f.jobID).
		Scan(&taskState, &jobState, &runState, &reservationState, &reservationRemaining); err != nil {
		t.Fatalf("read final durable lifecycle: %v", err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM cost_entries c
		JOIN budget_reservations b ON b.id=c.reservation_id JOIN agent_runs r ON r.id=b.run_id WHERE r.job_id=$1::uuid`, f.jobID).
		Scan(&costEntryCount); err != nil {
		t.Fatalf("count trusted provider usage receipts: %v", err)
	}
	// The bounded fake child proves execution, not provider usage. Without
	// trusted usage receipts the successful run must retain its full reservation.
	if taskState != "AUTHORING" || jobState != "COMPLETED" || runState != "SUCCESS" || reservationState != "UNKNOWN" || reservationRemaining != 1000 || costEntryCount != 0 {
		t.Fatalf("durable final state task/job/run/reservation=(%s,%s,%s,%s remaining=%d cost_entries=%d), want AUTHORING/COMPLETED/SUCCESS/UNKNOWN with full fixture envelope retained and no usage receipt",
			taskState, jobState, runState, reservationState, reservationRemaining, costEntryCount)
	}
	var holdState string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state FROM process_holds WHERE run_id=(SELECT id FROM agent_runs WHERE job_id=$1::uuid)`, f.jobID).Scan(&holdState); err != nil {
		t.Fatalf("read durable process hold: %v", err)
	}
	if holdState != processholds.StateReaped {
		t.Fatalf("durable host process hold=%s, want REAPED", holdState)
	}
	var receiptCount int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM worker_result_receipts WHERE task_id=$1::uuid AND job_id=$2::uuid`, f.taskID, f.jobID).Scan(&receiptCount); err != nil {
		t.Fatalf("count durable result receipts: %v", err)
	}
	if receiptCount != 1 {
		t.Fatalf("durable result receipts=%d, want exactly one", receiptCount)
	}
	var durableJobOperationID, receiptOperationID string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT j.payload->>'operation_id',r.operation_id::text
		FROM jobs j JOIN worker_result_receipts r ON r.job_id=j.id WHERE j.id=$1::uuid`, f.jobID).
		Scan(&durableJobOperationID, &receiptOperationID); err != nil {
		t.Fatalf("read durable result operation identity: %v", err)
	}
	if durableJobOperationID == "" || durableJobOperationID != receiptOperationID {
		t.Fatalf("receipt operation=%s, job operation=%s", receiptOperationID, durableJobOperationID)
	}
	pending, err := f.redisRW.XPending(f.ctx, f.streamKey, f.group).Result()
	if err != nil {
		t.Fatalf("read dispatch pending state: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("dispatch pending entries=%d after durable completion, want 0", pending.Count)
	}
}

type foundationFixture struct {
	ctx       context.Context
	db        testutil.DatabaseFixture
	redis     testutil.RedisFixture
	redisRW   *redis.Client
	streams   *queue.Streams
	clock     *clock.Manual
	router    *router.Router
	suffix    string
	repo      string
	orgID     string
	taskID    string
	jobID     string
	streamKey string
	group     string
}

func foundationRequireFixture(t *testing.T) *foundationFixture {
	t.Helper()
	db, redisFixture := testutil.RequireServices(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate system test schema: %v", err)
	}
	suffix := foundationRandomSuffix(t)
	orgID := "foundation-" + suffix
	repo := "owner/foundation-" + suffix
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, orgID); err != nil {
		t.Fatalf("create organization budget: %v", err)
	}
	manual := clock.NewManual(time.Now().UTC().Truncate(time.Second))
	f := &foundationFixture{ctx: ctx, db: db, redis: redisFixture, clock: manual,
		suffix: suffix, repo: repo, orgID: orgID, streamKey: redisFixture.KeyPrefix + "foundation:" + suffix,
		group: "foundation-group-" + suffix}
	f.router = foundationNewRouter(t, f)
	options := *redisFixture.Client.Options()
	options.ContextTimeoutEnabled = true
	f.redisRW = redis.NewClient(&options)
	streams, err := queue.NewStreams(f.redisRW, f.streamKey, f.group)
	if err != nil {
		t.Fatalf("construct isolated Redis stream: %v", err)
	}
	f.streams = streams
	t.Cleanup(func() {
		if err := streams.Close(); err != nil {
			t.Errorf("close system test stream client: %v", err)
		}
	})
	return f
}

func foundationNewRouter(t *testing.T, f *foundationFixture) *router.Router {
	t.Helper()
	policy := router.RepositoryPolicy{OrgID: f.orgID, PolicyVersion: "foundation-policy-v1", IssueEnrollmentLabel: "aprl:implement",
		AuthorizedIssueLabelerIDs: map[int64]struct{}{17: {}}, TaskBudgetLimitMicroUSD: 5000}
	instance, err := router.New(f.db.Pool, f.clock, router.Config{
		Repositories: map[string]router.RepositoryPolicy{f.repo: policy}, MaxTaskBudgetMicroUSD: 5000,
		TrustedAPRLActorIDs: map[int64]struct{}{}, TrustedAPRLAppIDs: map[int64]struct{}{},
		TrustedCIActorIDs: map[int64]struct{}{}, TrustedCIAppIDs: map[int64]struct{}{}, TrustedCISenderLogins: map[string]struct{}{},
	})
	if err != nil {
		t.Fatalf("construct enrolled fake-GitHub router: %v", err)
	}
	return instance
}

type foundationWebhookResponse struct {
	Accepted   bool   `json:"accepted"`
	Duplicate  bool   `json:"duplicate"`
	DeliveryID string `json:"delivery_id"`
}

func foundationPostSignedWebhook(t *testing.T, client *http.Client, endpoint string, secret []byte, id, event string, payload []byte) (int, foundationWebhookResponse) {
	t.Helper()
	requestCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint+"/webhooks/github", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("construct signed webhook request: %v", err)
	}
	mac := hmac.New(sha256.New, secret)
	if _, err := mac.Write(payload); err != nil {
		t.Fatalf("compute fixture webhook signature: %v", err)
	}
	request.Header.Set("X-GitHub-Delivery", id)
	request.Header.Set("X-GitHub-Event", event)
	request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("POST signed GitHub delivery: %v", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close webhook response body: %v", err)
		}
	}()
	var parsed foundationWebhookResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&parsed); err != nil {
		t.Fatalf("decode webhook acknowledgement: %v", err)
	}
	return response.StatusCode, parsed
}

type foundationAuthenticator struct {
	mu        sync.Mutex
	principal results.Principal
}

func (a *foundationAuthenticator) AuthenticateSupervisor(_ context.Context, r *http.Request) (results.Principal, error) {
	if r.Header.Get("Authorization") != "Bearer foundation-test-host-token" {
		return results.Principal{}, errors.New("test host authentication denied")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.principal.TaskID == "" || a.principal.RunID == "" {
		return results.Principal{}, errors.New("test host principal is not bound")
	}
	return a.principal, nil
}

type foundationHTTPResultSubmitter struct {
	client     *http.Client
	url        string
	auth       *foundationAuthenticator
	mu         sync.Mutex
	statusCode int
}

func (s *foundationHTTPResultSubmitter) Submit(ctx context.Context, principal results.Principal, result contracts.Result) (results.Accepted, error) {
	s.auth.mu.Lock()
	s.auth.principal = principal
	s.auth.mu.Unlock()
	defer func() {
		s.auth.mu.Lock()
		s.auth.principal = results.Principal{}
		s.auth.mu.Unlock()
	}()
	body, err := json.Marshal(result)
	if err != nil {
		return results.Accepted{}, fmt.Errorf("marshal fake child result: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return results.Accepted{}, fmt.Errorf("construct authenticated result request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer foundation-test-host-token")
	response, err := s.client.Do(request)
	if err != nil {
		return results.Accepted{}, fmt.Errorf("POST fake child result to real HTTP handler: %w", err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	closeErr := response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return results.Accepted{}, fmt.Errorf("read authenticated result acknowledgement: %w", err)
	}
	s.mu.Lock()
	s.statusCode = response.StatusCode
	s.mu.Unlock()
	var parsed struct {
		Accepted    bool   `json:"accepted"`
		OperationID string `json:"operation_id"`
		Error       string `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &parsed); err != nil {
		return results.Accepted{}, fmt.Errorf("decode result acknowledgement: %w", err)
	}
	if response.StatusCode != http.StatusOK || !parsed.Accepted || parsed.OperationID == "" {
		return results.Accepted{}, fmt.Errorf("result HTTP acknowledgement status=%d accepted=%t error=%s", response.StatusCode, parsed.Accepted, parsed.Error)
	}
	return results.Accepted{OperationID: parsed.OperationID}, nil
}

func (s *foundationHTTPResultSubmitter) status() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statusCode
}

type foundationProcessEvidence struct {
	clock     clock.Clock
	mu        sync.Mutex
	processes map[string]processholds.ProcessIdentity
}

func (e *foundationProcessEvidence) record(runID string, identity processholds.ProcessIdentity) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.processes[runID] = identity
}

func (e *foundationProcessEvidence) VerifyNeverStarted(context.Context, processholds.Hold) (processholds.ReapEvidence, error) {
	return processholds.ReapEvidence{}, errors.New("foundation runner does not assert unobserved launches were never started")
}

func (e *foundationProcessEvidence) VerifyGroupDrained(ctx context.Context, hold processholds.Hold) (processholds.ReapEvidence, error) {
	if err := ctx.Err(); err != nil {
		return processholds.ReapEvidence{}, err
	}
	if hold.Process == nil {
		return processholds.ReapEvidence{}, errors.New("process identity is absent")
	}
	e.mu.Lock()
	identity, ok := e.processes[hold.RunID]
	e.mu.Unlock()
	if !ok || identity != *hold.Process {
		return processholds.ReapEvidence{}, errors.New("host launch ledger identity mismatch")
	}
	err := syscall.Kill(-int(identity.PGID), 0)
	if !errors.Is(err, syscall.ESRCH) {
		return processholds.ReapEvidence{}, errors.New("owned process group is still present")
	}
	process := identity
	return processholds.ReapEvidence{Kind: processholds.ProofGroupDrained, RunID: hold.RunID, TaskID: hold.TaskID,
		JobID: hold.JobID, Generation: hold.Generation, ResourceScope: hold.ResourceScope, LeaseTokenSHA: hold.LeaseTokenSHA,
		Workspace: hold.Workspace, SupervisorID: hold.SupervisorID, Process: &process, VerifiedAt: e.clock.Now().UTC(),
		VerifierID: "foundation-test-host-ledger"}, nil
}

type foundationRunner struct {
	path       string
	evidence   *foundationProcessEvidence
	mu         sync.Mutex
	starts     int
	lastExit   string
	lastOutput string
	process    *foundationProcess
}

func foundationBuildRunner(t *testing.T) *foundationRunner {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate system test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
	path := filepath.Join(t.TempDir(), "foundation-fakeagent")
	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(buildCtx, "go", "build", "-o", path, "./tests/testutil/fakeagent")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build local fakeagent child: %v\n%s", err, output)
	}
	return &foundationRunner{path: path}
}

func (r *foundationRunner) Start(_ context.Context, spec supervisor.ProcessSpec) (supervisor.Process, error) {
	cmd := exec.Command(r.path)
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = foundationSortedEnvironment(spec.Environment)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, stderr := &foundationCapture{}, &foundationCapture{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.starts++
	r.mu.Unlock()
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("read fakeagent process group: %w", err)
	}
	startIdentity, err := foundationProcessStartIdentity(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("read native fixture process identity: %w", err)
	}
	identity := processholds.ProcessIdentity{PID: int64(cmd.Process.Pid), PGID: int64(pgid), StartIdentity: startIdentity}
	r.evidence.record(spec.RunID, identity)
	process := &foundationProcess{cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr, identity: identity, runner: r, waitDone: make(chan struct{})}
	r.mu.Lock()
	r.process = process
	r.mu.Unlock()
	return process, nil
}

func (r *foundationRunner) diagnostic() (int, string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts, r.lastOutput, r.lastExit
}

func (r *foundationRunner) cleanup(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	process := r.process
	r.mu.Unlock()
	if process == nil {
		return
	}
	select {
	case <-process.waitDone:
		return
	default:
	}
	currentIdentity, identityErr := foundationProcessStartIdentity(int(process.identity.PID))
	if identityErr == nil && currentIdentity == process.identity.StartIdentity {
		if err := process.SignalGroup(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("kill remaining foundation child group: %v", err)
		}
	} else if err := syscall.Kill(-int(process.identity.PGID), 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("cannot verify remaining foundation child group before cleanup: %v", identityErr)
	}
	process.startWait()
	select {
	case <-process.waitDone:
	case <-time.After(3 * time.Second):
		t.Errorf("timed out joining foundation child process")
		return
	}
	if err := process.waitErr; err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Errorf("join foundation child: %v", err)
		}
	}
}

func foundationSortedEnvironment(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	environment := make([]string, 0, len(keys))
	for _, key := range keys {
		environment = append(environment, key+"="+values[key])
	}
	return environment
}

type foundationProcess struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	stdout   *foundationCapture
	stderr   *foundationCapture
	identity processholds.ProcessIdentity
	runner   *foundationRunner
	mu       sync.Mutex
	waitOnce sync.Once
	waitDone chan struct{}
	waitErr  error
}

type foundationCapture struct{ bytes.Buffer }

func (c *foundationCapture) Write(p []byte) (int, error) {
	const limit = 4 << 10
	remaining := limit - c.Len()
	if remaining > 0 {
		if len(p) > remaining {
			_, _ = c.Buffer.Write(p[:remaining])
		} else {
			_, _ = c.Buffer.Write(p)
		}
	}
	return len(p), nil
}

func (p *foundationProcess) PID() int              { return int(p.identity.PID) }
func (p *foundationProcess) ProcessGroupID() int   { return int(p.identity.PGID) }
func (p *foundationProcess) StartIdentity() string { return p.identity.StartIdentity }

func (p *foundationProcess) Activate(ctx context.Context, result contracts.Result) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := json.NewEncoder(p.stdin).Encode(result); err != nil {
		return err
	}
	return p.stdin.Close()
}

func (p *foundationProcess) Wait() (contracts.Result, error) {
	p.startWait()
	<-p.waitDone
	if err := p.waitErr; err != nil {
		return contracts.Result{}, fmt.Errorf("wait fakeagent: %w: %s", err, p.stderr.String())
	}
	var result contracts.Result
	if err := json.Unmarshal(p.stdout.Bytes(), &result); err != nil {
		return contracts.Result{}, fmt.Errorf("decode fakeagent result: %w", err)
	}
	return result, nil
}

func (p *foundationProcess) startWait() {
	p.waitOnce.Do(func() {
		go func() {
			err := p.cmd.Wait()
			p.waitErr = err
			p.runner.mu.Lock()
			p.runner.lastOutput = p.stderr.String()
			if err != nil {
				p.runner.lastExit = err.Error()
			} else {
				p.runner.lastExit = "exit 0"
			}
			p.runner.mu.Unlock()
			close(p.waitDone)
		}()
	})
}

func (p *foundationProcess) SignalGroup(signal syscall.Signal) error {
	if p.identity.PID <= 0 || p.identity.PGID <= 0 || p.identity.StartIdentity == "" {
		return errors.New("fixture process handle is not fully identified")
	}
	// A signal zero is only a group-absence probe. It must remain usable after
	// the leader exits so the supervisor can prove an already-drained group.
	if signal == 0 {
		return syscall.Kill(-int(p.identity.PGID), 0)
	}
	currentIdentity, err := foundationProcessStartIdentity(int(p.identity.PID))
	if err != nil || currentIdentity != p.identity.StartIdentity {
		return errors.Join(errors.New("fixture process identity changed before signal"), err)
	}
	err = syscall.Kill(-int(p.identity.PGID), signal)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

func foundationRandomSuffix(t *testing.T) string {
	t.Helper()
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("allocate unique fixture identity: %v", err)
	}
	return hex.EncodeToString(value[:])
}

func foundationDiagnosticText(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	return value[:maxBytes]
}
