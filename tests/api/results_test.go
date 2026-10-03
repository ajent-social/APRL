package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/httpapi"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

type resultsTestAuthenticator struct {
	principal results.Principal
	token     string
}

func (a resultsTestAuthenticator) AuthenticateSupervisor(_ context.Context, request *http.Request) (results.Principal, error) {
	if a.token == "" || request.Header.Get("X-Test-Supervisor") != a.token {
		return results.Principal{}, fmt.Errorf("supervisor authentication failed")
	}
	return a.principal, nil
}

type resultsTestResponse struct {
	Accepted    bool   `json:"accepted"`
	OperationID string `json:"operation_id"`
	Error       string `json:"error"`
}

type resultsPushFixture struct {
	ctx                                                              context.Context
	db                                                               testutil.DatabaseFixture
	clock                                                            *clock.Manual
	router                                                           *router.Router
	handler                                                          http.Handler
	taskID, jobID, operationID, correlationID, findingID, deliveryID string
	lease                                                            leases.Lease
	result                                                           contracts.Result
	seed                                                             int64
	prNumber                                                         int
}

func TestResults(t *testing.T) {
	database := testutil.RequireDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := storage.Migrate(ctx, database.Pool); err != nil {
		t.Fatalf("migrate result fixture: %v", err)
	}
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	serviceClock := clock.NewManual(now)
	seed := time.Now().UnixNano()
	orgID := fmt.Sprintf("results-test-%d", seed)
	if _, err := database.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, orgID); err != nil {
		t.Fatalf("insert result organization: %v", err)
	}
	var taskID string
	if err := database.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version)
		VALUES($1,'owner/results-test',$2,'host','AUTHORING','results-test-v1') RETURNING id::text`, orgID, orgID).Scan(&taskID); err != nil {
		t.Fatalf("insert result task: %v", err)
	}
	jobID := fmt.Sprintf("%08x-2222-4222-8222-%012x", seed>>32, seed&0xffffffffffff)
	operationID := fmt.Sprintf("%08x-3333-4333-8333-%012x", seed>>32, seed&0xffffffffffff)
	correlationID := fmt.Sprintf("%08x-4444-4444-8444-%012x", seed>>32, seed&0xffffffffffff)
	job := contracts.Job{Version: 1, TaskID: taskID, JobID: jobID, Generation: 0, Attempt: 1,
		OperationID: operationID, CorrelationID: correlationID, Operation: "ci_reconcile"}
	jobJSON, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO jobs(id,task_id,logical_key,operation_type,generation,payload)
		VALUES($1::uuid,$2::uuid,$3,'ci_reconcile',0,$4::jsonb)`, jobID, taskID, "results-test-job:"+jobID, jobJSON); err != nil {
		t.Fatalf("insert result job: %v", err)
	}
	lease, err := leases.Claim(ctx, database.Pool, serviceClock, leases.ClaimRequest{TaskID: taskID, JobID: jobID,
		TTL: time.Minute, AgentType: "A", PromptHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SupervisorIdentity: "result-host", CredentialID: "credential-ref-1"})
	if err != nil {
		t.Fatalf("claim result run: %v", err)
	}
	principal := results.Principal{TaskID: taskID, RunID: lease.RunID, Identity: "result-host", CredentialID: "credential-ref-1"}
	service, err := results.New(database.Pool, serviceClock, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewResultsHandler(service, resultsTestAuthenticator{principal: principal, token: "host-test-auth"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	result := contracts.Result{Version: 1, TaskID: taskID, JobID: jobID, RunID: lease.RunID, Generation: lease.Generation,
		LeaseToken: lease.Token, Snapshot: lease.Snapshot, Attempt: lease.Attempt, OperationID: operationID,
		CorrelationID: correlationID, Status: "succeeded", Summary: "CI reconciliation completed"}
	post := func(value contracts.Result) (int, resultsTestResponse) {
		t.Helper()
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/internal/results", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Test-Supervisor", "host-test-auth")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := response.Body.Close(); err != nil {
				t.Errorf("close result response body: %v", err)
			}
		}()
		var body resultsTestResponse
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, body
	}
	t.Run("accepts_current_completion", func(t *testing.T) {
		status, body := post(result)
		if status != http.StatusOK || !body.Accepted || body.OperationID != operationID || body.Error != "" {
			t.Fatalf("first result response=(%d,%+v), want accepted 200", status, body)
		}
		var receiptCount int
		if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM worker_result_receipts WHERE operation_id=$1::uuid`, operationID).Scan(&receiptCount); err != nil {
			t.Fatal(err)
		}
		if receiptCount != 1 {
			t.Fatalf("result receipt count=%d, want 1", receiptCount)
		}
	})
	if _, err := database.Pool.Exec(ctx, `UPDATE tasks SET generation=1 WHERE id=$1::uuid`, taskID); err != nil {
		t.Fatalf("advance generation after accepted result: %v", err)
	}
	t.Run("replays_exact_receipt_after_generation_advance", func(t *testing.T) {
		status, body := post(result)
		if status != http.StatusOK || !body.Accepted || body.OperationID != operationID {
			t.Fatalf("exact replay response=(%d,%+v), want accepted 200", status, body)
		}
	})
	t.Run("rejects_conflicting_replay", func(t *testing.T) {
		conflict := result
		conflict.Summary = "different semantic result"
		status, body := post(conflict)
		if status != http.StatusConflict || body.Error != "stale_result" {
			t.Fatalf("conflicting replay response=(%d,%+v), want stale_result 409", status, body)
		}
	})
	t.Run("rejects_stale_fence_tuple", func(t *testing.T) {
		stale := result
		stale.LeaseToken = "22222222-2222-4222-8222-222222222222"
		status, body := post(stale)
		if status != http.StatusConflict || body.Error != "stale_result" {
			t.Fatalf("stale lease tuple response=(%d,%+v), want stale_result 409", status, body)
		}
	})
	wrongHandler, err := httpapi.NewResultsHandler(service, resultsTestAuthenticator{token: "host-test-auth", principal: results.Principal{
		TaskID: taskID, RunID: lease.RunID, Identity: "different-host", CredentialID: "credential-ref-1"}})
	if err != nil {
		t.Fatal(err)
	}
	wrongRequestBody, _ := json.Marshal(result)
	wrongRequest := httptest.NewRequest(http.MethodPost, "/internal/results", bytes.NewReader(wrongRequestBody))
	wrongRequest.Header.Set("X-Test-Supervisor", "host-test-auth")
	wrongResponse := httptest.NewRecorder()
	wrongHandler.ServeHTTP(wrongResponse, wrongRequest)
	t.Run("rejects_wrong_supervisor_before_replay", func(t *testing.T) {
		if wrongResponse.Code != http.StatusForbidden {
			t.Fatalf("wrong supervisor replay status=%d body=%s, want 403", wrongResponse.Code, wrongResponse.Body.String())
		}
	})
	t.Run("rejects_valid_other_run_before_replay", func(t *testing.T) {
		otherJobID := fmt.Sprintf("%08x-5555-4555-8555-%012x", seed>>32, (seed+1)&0xffffffffffff)
		otherOperationID := fmt.Sprintf("%08x-6666-4666-8666-%012x", seed>>32, (seed+1)&0xffffffffffff)
		otherCorrelationID := fmt.Sprintf("%08x-7777-4777-8777-%012x", seed>>32, (seed+1)&0xffffffffffff)
		var otherTaskID string
		if err := database.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version)
			VALUES($1,'owner/results-test','other-run','host','AUTHORING','results-test-v1') RETURNING id::text`, orgID).Scan(&otherTaskID); err != nil {
			t.Fatal(err)
		}
		otherJob := contracts.Job{Version: 1, TaskID: otherTaskID, JobID: otherJobID, Generation: 0, Attempt: 1,
			OperationID: otherOperationID, CorrelationID: otherCorrelationID, Operation: "ci_reconcile"}
		otherPayload, err := json.Marshal(otherJob)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Pool.Exec(ctx, `INSERT INTO jobs(id,task_id,logical_key,operation_type,generation,payload)
			VALUES($1::uuid,$2::uuid,$3,'ci_reconcile',0,$4::jsonb)`, otherJobID, otherTaskID, "results-test-other-job:"+otherJobID, otherPayload); err != nil {
			t.Fatal(err)
		}
		otherLease, err := leases.Claim(ctx, database.Pool, serviceClock, leases.ClaimRequest{TaskID: otherTaskID, JobID: otherJobID,
			TTL: time.Minute, AgentType: "A", PromptHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SupervisorIdentity: "other-result-host", CredentialID: "credential-ref-other"})
		if err != nil {
			t.Fatal(err)
		}
		otherHandler, err := httpapi.NewResultsHandler(service, resultsTestAuthenticator{token: "other-host-auth", principal: results.Principal{
			TaskID: otherTaskID, RunID: otherLease.RunID, Identity: "other-result-host", CredentialID: "credential-ref-other"}})
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(result)
		request := httptest.NewRequest(http.MethodPost, "/internal/results", bytes.NewReader(payload))
		request.Header.Set("X-Test-Supervisor", "other-host-auth")
		response := httptest.NewRecorder()
		otherHandler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("other valid run replay status=%d body=%s, want 403", response.Code, response.Body.String())
		}
	})
	t.Run("retains_unknown_inference_usage_coverage", func(t *testing.T) {
		unknownTaskID, unknownJobID := "", fmt.Sprintf("%08x-8888-4888-8888-%012x", seed>>32, (seed+2)&0xffffffffffff)
		unknownOperationID := fmt.Sprintf("%08x-9999-4999-8999-%012x", seed>>32, (seed+2)&0xffffffffffff)
		unknownCorrelationID := fmt.Sprintf("%08x-aaaa-4aaa-8aaa-%012x", seed>>32, (seed+2)&0xffffffffffff)
		if err := database.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version)
			VALUES($1,'owner/results-test','unknown-run','host','AUTHORING','results-test-v1') RETURNING id::text`, orgID).Scan(&unknownTaskID); err != nil {
			t.Fatal(err)
		}
		envelope := contracts.BudgetEnvelope{MaxCostMicroUSD: 50, MaxInputTokens: 20, MaxOutputTokens: 20, MaxCalls: 2, PricingVersion: "results-test-rates-v1"}
		unknownJob := contracts.Job{Version: 1, TaskID: unknownTaskID, JobID: unknownJobID, Generation: 0, Attempt: 1,
			OperationID: unknownOperationID, CorrelationID: unknownCorrelationID, Operation: "author", Envelope: envelope}
		unknownPayload, err := json.Marshal(unknownJob)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Pool.Exec(ctx, `INSERT INTO jobs(id,task_id,logical_key,operation_type,generation,payload)
			VALUES($1::uuid,$2::uuid,$3,'author',0,$4::jsonb)`, unknownJobID, unknownTaskID, "results-test-unknown-job:"+unknownJobID, unknownPayload); err != nil {
			t.Fatal(err)
		}
		unknownLease, err := leases.Claim(ctx, database.Pool, serviceClock, leases.ClaimRequest{TaskID: unknownTaskID, JobID: unknownJobID,
			TTL: time.Minute, AgentType: "A", PromptHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SupervisorIdentity: "unknown-result-host", CredentialID: "credential-ref-unknown"})
		if err != nil {
			t.Fatal(err)
		}
		reservation, err := budget.Reserve(ctx, database.Pool, serviceClock, unknownLease, envelope)
		if err != nil {
			t.Fatalf("reserve unknown-usage test: %v", err)
		}
		unknownPrincipal := results.Principal{TaskID: unknownTaskID, RunID: unknownLease.RunID, Identity: "unknown-result-host", CredentialID: "credential-ref-unknown"}
		unknownHandler, err := httpapi.NewResultsHandler(service, resultsTestAuthenticator{token: "unknown-host-auth", principal: unknownPrincipal})
		if err != nil {
			t.Fatal(err)
		}
		unknownResult := contracts.Result{Version: 1, TaskID: unknownTaskID, JobID: unknownJobID, RunID: unknownLease.RunID, Generation: 0,
			LeaseToken: unknownLease.Token, Snapshot: unknownLease.Snapshot, Attempt: unknownLease.Attempt, OperationID: unknownOperationID,
			CorrelationID: unknownCorrelationID, Status: "succeeded", Summary: "authoring completed"}
		unknownJSON, err := json.Marshal(unknownResult)
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/internal/results", bytes.NewReader(unknownJSON))
		request.Header.Set("X-Test-Supervisor", "unknown-host-auth")
		response := httptest.NewRecorder()
		unknownHandler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("inference result status=%d body=%s", response.Code, response.Body.String())
		}
		var status string
		var remaining int64
		if err := database.Pool.QueryRow(ctx, `SELECT status,remaining_micro_usd FROM budget_reservations WHERE id=$1::uuid`, reservation.ID).Scan(&status, &remaining); err != nil {
			t.Fatal(err)
		}
		if status != "UNKNOWN" || remaining != envelope.MaxCostMicroUSD {
			t.Fatalf("reservation status/remaining=(%s,%d), want UNKNOWN/%d", status, remaining, envelope.MaxCostMicroUSD)
		}
	})
	unauthenticated := httptest.NewRecorder()
	unauthenticatedRequest := httptest.NewRequest(http.MethodPost, "/internal/results", bytes.NewReader(wrongRequestBody))
	handler.ServeHTTP(unauthenticated, unauthenticatedRequest)
	t.Run("rejects_missing_authentication", func(t *testing.T) {
		if unauthenticated.Code != http.StatusUnauthorized {
			t.Fatalf("missing supervisor auth status=%d, want 401", unauthenticated.Code)
		}
	})
	t.Run("confirmed_push_result_first_and_webhook_first_are_atomic", func(t *testing.T) {
		for _, order := range []string{"result_first", "webhook_first"} {
			t.Run(order, func(t *testing.T) {
				f := resultsNewPushFixture(t, "CONFIRMED")
				if order == "webhook_first" {
					resultsPushWebhook(t, f)
				}
				status, response := resultsSubmitPush(t, f)
				if status != http.StatusOK || !response.Accepted {
					t.Fatalf("push result response=(%d,%+v)", status, response)
				}
				if order == "result_first" {
					resultsPushWebhook(t, f)
				}
				resultsAssertPushHandoff(t, f)
			})
		}
	})
	t.Run("push_webhook_and_result_race_once", func(t *testing.T) {
		f := resultsNewPushFixture(t, "CONFIRMED")
		resultsStorePushWebhook(t, f)
		start := make(chan struct{})
		var wait sync.WaitGroup
		var routeErr error
		var resultStatus int
		var response resultsTestResponse
		wait.Add(2)
		go func() { defer wait.Done(); <-start; routeErr = f.router.RouteDelivery(f.ctx, f.deliveryID) }()
		go func() { defer wait.Done(); <-start; resultStatus, response = resultsSubmitPush(t, f) }()
		close(start)
		wait.Wait()
		if routeErr != nil {
			t.Fatalf("route concurrent push: %v", routeErr)
		}
		if resultStatus != http.StatusOK || !response.Accepted {
			t.Fatalf("concurrent result response=(%d,%+v)", resultStatus, response)
		}
		resultsAssertPushHandoff(t, f)
	})
	t.Run("unconfirmed_or_wrong_run_push_intent_is_denied", func(t *testing.T) {
		f := resultsNewPushFixture(t, "IN_FLIGHT")
		status, response := resultsSubmitPush(t, f)
		if status != http.StatusConflict || response.Error != "stale_result" {
			t.Fatalf("unconfirmed push=(%d,%+v), want stale", status, response)
		}
		wrongRun := resultsNewPushFixture(t, "CONFIRMED", true)
		status, response = resultsSubmitPush(t, wrongRun)
		if status != http.StatusConflict || response.Error != "stale_result" {
			t.Fatalf("wrong original run proof=(%d,%+v), want stale", status, response)
		}
	})
	t.Run("explicit_control_pause_revokes_original_run", func(t *testing.T) {
		f := resultsNewPushFixture(t, "CONFIRMED")
		if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET state='PAUSED' WHERE id=$1::uuid`, f.taskID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool.Exec(f.ctx, `UPDATE jobs SET status='CANCELLED' WHERE id=$1::uuid`, f.jobID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool.Exec(f.ctx, `UPDATE agent_runs SET execution_status='TERMINATED',finished_at=$1 WHERE id=$2::uuid`, f.clock.Now(), f.lease.RunID); err != nil {
			t.Fatal(err)
		}
		status, response := resultsSubmitPush(t, f)
		if status != http.StatusConflict || response.Error != "stale_result" {
			t.Fatalf("control-paused result=(%d,%+v), want stale", status, response)
		}
	})
	t.Run("recognized_paused_webhook_records_without_replies", func(t *testing.T) {
		f := resultsNewPushFixture(t, "CONFIRMED")
		if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET state='PAUSED' WHERE id=$1::uuid`, f.taskID); err != nil {
			t.Fatal(err)
		}
		resultsPushWebhook(t, f)
		status, response := resultsSubmitPush(t, f)
		if status != http.StatusConflict || response.Error != "stale_result" {
			t.Fatalf("paused webhook-first result=(%d,%+v), want stale", status, response)
		}
		var replies int
		var marker bool
		var runStatus, jobStatus, reservationStatus string
		var leaseToken, leaseExpiresAt *string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT execution_status FROM agent_runs WHERE id=$1::uuid`, f.lease.RunID).Scan(&runStatus); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token,lease_expires_at::text FROM jobs WHERE id=$1::uuid`, f.jobID).Scan(&jobStatus, &leaseToken, &leaseExpiresAt); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM budget_reservations WHERE run_id=$1::uuid`, f.lease.RunID).Scan(&reservationStatus); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid AND operation_type='reply'`, f.taskID).Scan(&replies); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT COALESCE((result->>'reply_jobs_created')::boolean,false) FROM github_operations WHERE id=$1::uuid`, f.operationID).Scan(&marker); err != nil {
			t.Fatal(err)
		}
		if runStatus != "TERMINATED" || jobStatus != "CANCELLED" || leaseToken != nil || leaseExpiresAt != nil || reservationStatus != "UNKNOWN" {
			t.Fatalf("paused run/job/budget=(%s,%s,%v,%v,%s), want TERMINATED/CANCELLED/nil/nil/UNKNOWN", runStatus, jobStatus, leaseToken, leaseExpiresAt, reservationStatus)
		}
		if replies != 0 || marker {
			t.Fatalf("paused reply disposition jobs=%d marker=%t, want 0/false", replies, marker)
		}
	})
	t.Run("commit_failure_rolls_back_handoff_and_receipt", func(t *testing.T) {
		f := resultsNewPushFixture(t, "CONFIRMED")
		trigger := fmt.Sprintf("aprl_result_fail_%x", uint64(f.seed))
		function := trigger + "_fn"
		ddl := fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'injected result commit failure'; END; $body$;
CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON worker_result_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %s();`, function, trigger, function)
		if _, err := f.db.Pool.Exec(f.ctx, ddl); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, _ = f.db.Pool.Exec(cleanupCtx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON worker_result_receipts", trigger))
			_, _ = f.db.Pool.Exec(cleanupCtx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", function))
		})
		status, response := resultsSubmitPush(t, f)
		if status != http.StatusServiceUnavailable || response.Error != "storage_unavailable" {
			t.Fatalf("commit fault response=(%d,%+v), want 503", status, response)
		}
		if _, err := f.db.Pool.Exec(f.ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON worker_result_receipts", trigger)); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool.Exec(f.ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", function)); err != nil {
			t.Fatal(err)
		}
		resultsAssertPushUnchanged(t, f)
	})
}

func resultsNewPushFixture(t *testing.T, operationStatus string, wrongRunProof ...bool) *resultsPushFixture {
	t.Helper()
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate push fixture: %v", err)
	}
	seed := time.Now().UnixNano()
	clockNow := clock.NewManual(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	orgID := fmt.Sprintf("results-push-%d", seed)
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id,rolling_limit_micro_usd) VALUES($1,100000)`, orgID); err != nil {
		t.Fatal(err)
	}
	var taskID string
	if err := db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,budget_limit_micro_usd,policy_version)
		VALUES($1,'owner/results-push',$2,'host','FIXING',5000,'results-push-test-v1') RETURNING id::text`, orgID, orgID).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	var prID int64
	prNumber := int(seed%2_000_000_000) + 1
	if err := db.Pool.QueryRow(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha,integration_sha)
		VALUES($1,'owner/results-push',$2,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','main','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','dddddddddddddddddddddddddddddddddddddddd') RETURNING id`, taskID, prNumber).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	var reviewID int64
	if err := db.Pool.QueryRow(ctx, `INSERT INTO review_cycles(pr_id,generation,head_sha,base_sha,verdict,normalized_version,blocker_fingerprints,payload)
		VALUES($1,0,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','request_changes','results-push-test-v1','[]'::jsonb,'{}'::jsonb) RETURNING id`, prID).Scan(&reviewID); err != nil {
		t.Fatal(err)
	}
	findingID := fmt.Sprintf("%08x-bbbb-4bbb-8bbb-%012x", seed>>32, seed&0xffffffffffff)
	if _, err := db.Pool.Exec(ctx, `INSERT INTO findings(id,review_id,severity,body) VALUES($1::uuid,$2,'warning','fixture finding')`, findingID, reviewID); err != nil {
		t.Fatal(err)
	}
	jobID := fmt.Sprintf("%08x-cccc-4ccc-8ccc-%012x", seed>>32, seed&0xffffffffffff)
	operationID := fmt.Sprintf("%08x-dddd-4ddd-8ddd-%012x", seed>>32, seed&0xffffffffffff)
	correlationID := fmt.Sprintf("%08x-eeee-4eee-8eee-%012x", seed>>32, seed&0xffffffffffff)
	envelope := contracts.BudgetEnvelope{MaxCostMicroUSD: 100, MaxInputTokens: 50, MaxOutputTokens: 50, MaxCalls: 3, PricingVersion: "results-push-rate-v1"}
	job := contracts.Job{Version: 1, TaskID: taskID, JobID: jobID, Generation: 0, Snapshot: contracts.Snapshot{HeadSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BaseSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", IntegrationSHA: "dddddddddddddddddddddddddddddddddddddddd"}, Attempt: 1,
		OperationID: operationID, CorrelationID: correlationID, Operation: "fix", Envelope: envelope}
	jobJSON, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO jobs(id,task_id,pr_id,logical_key,operation_type,generation,expected_head_sha,expected_base_sha,remediation_attempt,payload)
		VALUES($1::uuid,$2::uuid,$3,$4,'fix',0,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',1,$5::jsonb)`, jobID, taskID, prID, "results-push-job:"+jobID, jobJSON); err != nil {
		t.Fatal(err)
	}
	lease, err := leases.Claim(ctx, db.Pool, clockNow, leases.ClaimRequest{TaskID: taskID, JobID: jobID, TTL: 5 * time.Minute, AgentType: "C",
		PromptHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SupervisorIdentity: "push-supervisor", CredentialID: "push-credential-ref"})
	if err != nil {
		t.Fatalf("claim push run: %v", err)
	}
	if _, err := budget.Reserve(ctx, db.Pool, clockNow, lease, envelope); err != nil {
		t.Fatalf("reserve push run: %v", err)
	}
	newHead := "cccccccccccccccccccccccccccccccccccccccc"
	requestRunID, requestLeaseToken := lease.RunID, lease.Token
	if len(wrongRunProof) > 0 && wrongRunProof[0] {
		requestRunID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	}
	requestJSON, err := json.Marshal(struct {
		RunID               string   `json:"run_id"`
		LeaseToken          string   `json:"lease_token"`
		Branch              string   `json:"branch"`
		NewHeadSHA          string   `json:"new_head_sha"`
		AddressedFindingIDs []string `json:"addressed_finding_ids"`
		ReplyIntents        []struct {
			FindingID string `json:"finding_id"`
			Body      string `json:"body"`
		} `json:"reply_intents"`
	}{RunID: requestRunID, LeaseToken: requestLeaseToken, Branch: "feature/aprl-42", NewHeadSHA: newHead,
		AddressedFindingIDs: []string{findingID}, ReplyIntents: []struct {
			FindingID string `json:"finding_id"`
			Body      string `json:"body"`
		}{{FindingID: findingID, Body: "The finding is addressed."}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO github_operations(id,job_id,task_id,generation,operation_type,identity,expected_head_sha,expected_base_sha,request,status)
		VALUES($1::uuid,$2::uuid,$3::uuid,0,'push','C','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',$4::jsonb,$5)`, operationID, jobID, taskID, requestJSON, operationStatus); err != nil {
		t.Fatal(err)
	}
	policy := router.RepositoryPolicy{OrgID: orgID, PolicyVersion: "results-push-test-v1", TaskBudgetLimitMicroUSD: 5000}
	pushRouter, err := router.New(db.Pool, clockNow, router.Config{
		Repositories:        map[string]router.RepositoryPolicy{"owner/results-push": policy},
		TrustedAPRLActorIDs: map[int64]struct{}{81: {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := results.New(db.Pool, clockNow, pushRouter)
	if err != nil {
		t.Fatal(err)
	}
	principal := results.Principal{TaskID: taskID, RunID: lease.RunID, Identity: "push-supervisor", CredentialID: "push-credential-ref"}
	handler, err := httpapi.NewResultsHandler(service, resultsTestAuthenticator{token: "push-auth", principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	result := contracts.Result{Version: 1, TaskID: taskID, JobID: jobID, RunID: lease.RunID, Generation: 0, LeaseToken: lease.Token, Snapshot: job.Snapshot, Attempt: 1,
		OperationID: operationID, CorrelationID: correlationID, Status: "succeeded", Summary: "push completed"}
	return &resultsPushFixture{ctx: ctx, db: db, clock: clockNow, router: pushRouter, handler: handler, taskID: taskID, jobID: jobID, operationID: operationID,
		correlationID: correlationID, findingID: findingID, deliveryID: fmt.Sprintf("results-push-delivery-%d", seed), lease: lease, result: result, seed: seed, prNumber: prNumber}
}

func resultsSubmitPush(t *testing.T, f *resultsPushFixture) (int, resultsTestResponse) {
	t.Helper()
	payload, err := json.Marshal(f.result)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/internal/results", bytes.NewReader(payload))
	request.Header.Set("X-Test-Supervisor", "push-auth")
	response := httptest.NewRecorder()
	f.handler.ServeHTTP(response, request)
	var body resultsTestResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return response.Code, body
}

func resultsPushWebhook(t *testing.T, f *resultsPushFixture) {
	t.Helper()
	resultsStorePushWebhook(t, f)
	if err := f.router.RouteDelivery(f.ctx, f.deliveryID); err != nil {
		t.Fatalf("route push confirmation: %v", err)
	}
}

func resultsStorePushWebhook(t *testing.T, f *resultsPushFixture) {
	t.Helper()
	payload := json.RawMessage(fmt.Sprintf(`{"action":"synchronize","repository":{"full_name":"owner/results-push"},"sender":{"id":81},"pull_request":{"number":%d,"head":{"ref":"feature/aprl-42","sha":"cccccccccccccccccccccccccccccccccccccccc"},"base":{"ref":"main","sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}}`, f.prNumber))
	err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		_, err := repos.StoreWebhookDelivery(ctx, storage.DeliveryReceipt{DeliveryID: f.deliveryID, EventType: "pull_request", Payload: payload})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func resultsAssertPushHandoff(t *testing.T, f *resultsPushFixture) {
	t.Helper()
	var state, head, integration, runStatus, reservationStatus string
	var generation int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT t.state,t.generation,p.head_sha,COALESCE(p.integration_sha,'') FROM tasks t JOIN prs p ON p.task_id=t.id WHERE t.id=$1::uuid`, f.taskID).Scan(&state, &generation, &head, &integration); err != nil {
		t.Fatal(err)
	}
	if state != "WAITING_CI" || generation != 1 || head != "cccccccccccccccccccccccccccccccccccccccc" || integration != "" {
		t.Fatalf("push handoff task=(%s,%d,%s,integration=%q)", state, generation, head, integration)
	}
	var replies, dispatches, receipts int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid AND operation_type='reply' AND generation=1`, f.taskID).Scan(&replies); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND kind='DISPATCH'`, f.taskID).Scan(&dispatches); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM worker_result_receipts WHERE operation_id=$1::uuid`, f.operationID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if replies != 1 || dispatches != 2 || receipts != 1 {
		t.Fatalf("push effects replies=%d dispatches=%d receipts=%d, want 1/2/1", replies, dispatches, receipts)
	}
	var marker bool
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT (result->>'reply_jobs_created')::boolean FROM github_operations WHERE id=$1::uuid`, f.operationID).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if !marker {
		t.Fatal("confirmed push reply marker is false")
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT execution_status FROM agent_runs WHERE id=$1::uuid`, f.lease.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM budget_reservations WHERE run_id=$1::uuid`, f.lease.RunID).Scan(&reservationStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "SUCCESS" || reservationStatus != "UNKNOWN" {
		t.Fatalf("push run/budget statuses=(%s,%s), want SUCCESS/UNKNOWN", runStatus, reservationStatus)
	}
}

func resultsAssertPushUnchanged(t *testing.T, f *resultsPushFixture) {
	t.Helper()
	var generation int64
	var state, head, jobStatus, runStatus, reservationStatus string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT t.state,t.generation,p.head_sha FROM tasks t JOIN prs p ON p.task_id=t.id WHERE t.id=$1::uuid`, f.taskID).Scan(&state, &generation, &head); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM jobs WHERE id=$1::uuid`, f.jobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT execution_status FROM agent_runs WHERE id=$1::uuid`, f.lease.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM budget_reservations WHERE run_id=$1::uuid`, f.lease.RunID).Scan(&reservationStatus); err != nil {
		t.Fatal(err)
	}
	var replies, receipts int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid AND operation_type='reply'`, f.taskID).Scan(&replies); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM worker_result_receipts WHERE operation_id=$1::uuid`, f.operationID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if generation != 0 || state != "FIXING" || head != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || jobStatus != "LEASED" || runStatus != "RUNNING" || reservationStatus != "RESERVED" || replies != 0 || receipts != 0 {
		t.Fatalf("failed transaction left state=%s gen=%d head=%s job=%s run=%s reservation=%s replies=%d receipts=%d", state, generation, head, jobStatus, runStatus, reservationStatus, replies, receipts)
	}
}
