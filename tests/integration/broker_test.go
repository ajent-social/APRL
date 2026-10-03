package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/broker"
	"github.com/ajent-social/APRL/internal/ci"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/policy"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

func TestBroker(t *testing.T) {
	database := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, database.Pool); err != nil {
		t.Fatalf("migrate broker integration schema: %v", err)
	}

	t.Run("valid_fix_can_push_and_identity_is_logical_role", func(t *testing.T) {
		f := brokerNewFixture(t, database, "fix", "C", "FIXING", true)
		result, err := f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionPush,
			Branch: "feature/owned", NewHeadSHA: brokerTestNewHead})
		if err != nil || result.Status != "CONFIRMED" || result.Identity != "C" {
			t.Fatalf("authorized fixer push=(%+v,%v)", result, err)
		}
		if f.transport.countExecutions() != 1 {
			t.Fatalf("remote executions=%d, want 1", f.transport.countExecutions())
		}
		encoded, err := json.Marshal(f.transport.lastOperation())
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), f.lease.Token) || strings.Contains(string(encoded), f.lease.RunID) {
			t.Fatalf("lease authority leaked into remote DTO: %s", encoded)
		}
		var jobPayload []byte
		if err := database.Pool.QueryRow(ctx, `SELECT payload FROM jobs WHERE id=$1::uuid`, f.jobID).Scan(&jobPayload); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(jobPayload), f.lease.Token) || strings.Contains(string(jobPayload), f.lease.RunID) || strings.Contains(string(jobPayload), "opaque-credential-id") {
			t.Fatalf("lease or supervisor authority leaked into worker job payload: %s", jobPayload)
		}
		var jobOperation, identity string
		if err := database.Pool.QueryRow(ctx, `SELECT op.identity,j.operation_type FROM github_operations op JOIN jobs j ON j.id=op.job_id WHERE op.id=$1::uuid`, f.operationID).
			Scan(&identity, &jobOperation); err != nil {
			t.Fatal(err)
		}
		if identity != "C" || jobOperation != "fix" {
			t.Fatalf("persisted application identity/job=(%s,%s), want C/fix", identity, jobOperation)
		}
		conflict := broker.Request{OperationID: f.operationID, Action: broker.ActionPush, Branch: "feature/owned", NewHeadSHA: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
		if _, err := f.service.Execute(ctx, f.lease, conflict); !errors.Is(err, broker.ErrConflict) {
			t.Fatalf("same ID with changed new head error=%v, want conflict", err)
		}
		if f.transport.countExecutions() != 1 {
			t.Fatalf("conflicting replay reached transport, executions=%d", f.transport.countExecutions())
		}
		rebound := f.lease
		rebound.RunID = "22222222-2222-4222-8222-222222222222"
		rebound.Token = "33333333-3333-4333-8333-333333333333"
		if _, err := f.service.Execute(ctx, rebound, broker.Request{OperationID: f.operationID, Action: broker.ActionPush,
			Branch: "feature/owned", NewHeadSHA: brokerTestNewHead}); !errors.Is(err, broker.ErrConflict) {
			t.Fatalf("same intent rebound to a new run error=%v, want conflict", err)
		}
		if f.transport.countExecutions() != 1 {
			t.Fatalf("rebound intent reached transport, executions=%d", f.transport.countExecutions())
		}
	})

	t.Run("nil_transport_fails_closed", func(t *testing.T) {
		if _, err := broker.New(database.Pool, clock.NewManual(time.Now().UTC()), nil, broker.TaskUUIDBranchResolver{}, broker.Config{
			Repositories: map[string]broker.RepositoryPolicy{"owner/broker-test": {AllowedTargetBranches: []string{"main"}}}, RPCTimeout: time.Second}); !errors.Is(err, broker.ErrInvalid) {
			t.Fatalf("nil transport constructor error=%v, want invalid", err)
		}
	})

	t.Run("unenrolled_repository_is_denied_before_any_transport_call", func(t *testing.T) {
		f := brokerNewFixture(t, database, "fix", "C", "FIXING", true)
		service, err := broker.New(database.Pool, clock.NewManual(time.Now().UTC()), f.transport, broker.TaskUUIDBranchResolver{}, broker.Config{
			Repositories: map[string]broker.RepositoryPolicy{"owner/other-repo": {AllowedTargetBranches: []string{"main"}}}, RPCTimeout: 5 * time.Second})
		if err != nil {
			t.Fatalf("construct broker with another enrolled repository: %v", err)
		}
		f.service = service
		_, err = f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionPush,
			Branch: "feature/owned", NewHeadSHA: brokerTestNewHead})
		if !errors.Is(err, broker.ErrForbidden) {
			t.Fatalf("unenrolled repository error=%v, want forbidden", err)
		}
		if calls := f.transport.countCalls(); calls != 0 {
			t.Fatalf("unenrolled repository reached transport %d times", calls)
		}
	})

	t.Run("author_can_create_pr_on_host_assigned_task_branch", func(t *testing.T) {
		f := brokerNewFixture(t, database, "author", "A", "AUTHORING", false)
		result, err := f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionCreatePR, Title: "Broker fixture"})
		if err != nil || result.Status != "CONFIRMED" {
			t.Fatalf("author PR creation=(%+v,%v)", result, err)
		}
		if got := f.transport.lastOperation().Branch; got != "aprl/"+f.taskID {
			t.Fatalf("assigned pre-PR branch=%q, want task UUID branch", got)
		}
	})

	t.Run("author_can_publish_only_to_host_assigned_task_branch", func(t *testing.T) {
		f := brokerNewFixture(t, database, "author", "A", "AUTHORING", false)
		result, err := f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionPublish,
			NewHeadSHA: brokerTestNewHead})
		if err != nil || result.Status != "CONFIRMED" {
			t.Fatalf("author publish=(%+v,%v)", result, err)
		}
		if got := f.transport.lastOperation().Branch; got != "aprl/"+f.taskID {
			t.Fatalf("publish branch=%q, want host-assigned task UUID branch", got)
		}
	})

	t.Run("reviewer_can_review_without_sending_lease_authority", func(t *testing.T) {
		f := brokerNewFixture(t, database, "review", "B", "IN_REVIEW", true)
		review := brokerTestReview(t, database, f)
		result, err := f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionReview, Review: &review})
		if err != nil || result.Status != "CONFIRMED" {
			t.Fatalf("reviewer operation=(%+v,%v)", result, err)
		}
		remoteReview := f.transport.lastOperation().Review
		if remoteReview == nil || remoteReview.Verdict != contracts.VerdictApprove {
			t.Fatalf("remote review content=%+v", remoteReview)
		}
		encoded, err := json.Marshal(remoteReview)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), f.lease.Token) || strings.Contains(string(encoded), f.lease.RunID) {
			t.Fatalf("review authority leaked to remote content: %s", encoded)
		}
	})

	t.Run("reviewer_can_resolve_only_a_verified_current_correction", func(t *testing.T) {
		f := brokerNewFixture(t, database, "review", "B", "IN_REVIEW", true)
		var prID int64
		if err := database.Pool.QueryRow(ctx, `SELECT id FROM prs WHERE task_id=$1::uuid`, f.taskID).Scan(&prID); err != nil {
			t.Fatal(err)
		}
		cyclePayload := json.RawMessage(`{"verdict":"approve"}`)
		if _, err := database.Pool.Exec(ctx, `INSERT INTO review_cycles(pr_id,generation,head_sha,base_sha,verdict,normalized_version,blocker_fingerprints,payload)
			VALUES($1,0,$2,$3,'approve','broker-test-v1','[]'::jsonb,$4::jsonb)`, prID, brokerTestHead, brokerTestBase, cyclePayload); err != nil {
			t.Fatal(err)
		}
		if _, err := database.Pool.Exec(ctx, `INSERT INTO findings(id,review_id,severity,body,disposition,resolved_head_sha)
			SELECT $1::uuid,id,'warning','addressed finding','ADDRESSED',$2 FROM review_cycles WHERE pr_id=$3 AND generation=0`,
			brokerTestFindingID, brokerTestHead, prID); err != nil {
			t.Fatal(err)
		}
		result, err := f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionResolve, FindingID: brokerTestFindingID})
		if err != nil || result.Status != "CONFIRMED" {
			t.Fatalf("verified resolution=(%+v,%v)", result, err)
		}
		if f.transport.lastOperation().FindingID != brokerTestFindingID {
			t.Fatalf("resolution target=%s, want %s", f.transport.lastOperation().FindingID, brokerTestFindingID)
		}
	})

	t.Run("denies_valid_fix_lease_approval_merge_resolution_and_unsafe_push", func(t *testing.T) {
		f := brokerNewFixture(t, database, "fix", "C", "FIXING", true)
		cases := []struct {
			name    string
			request broker.Request
		}{
			{name: "review", request: broker.Request{OperationID: f.operationID, Action: broker.ActionReview, Review: &contracts.Review{Version: 1}}},
			{name: "merge", request: broker.Request{OperationID: f.operationID, Action: broker.ActionMerge}},
			{name: "resolve", request: broker.Request{OperationID: f.operationID, Action: broker.ActionResolve, FindingID: brokerTestFindingID}},
			{name: "out_of_branch", request: broker.Request{OperationID: f.operationID, Action: broker.ActionPush, Branch: "main", NewHeadSHA: brokerTestNewHead}},
			{name: "force", request: broker.Request{OperationID: f.operationID, Action: broker.ActionPush, Branch: "feature/owned", NewHeadSHA: brokerTestNewHead, Force: true}},
			{name: "workflow_flag", request: broker.Request{OperationID: f.operationID, Action: broker.ActionPush, Branch: "feature/owned", NewHeadSHA: brokerTestNewHead, EditWorkflow: true}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := f.service.Execute(ctx, f.lease, tc.request)
				if !errors.Is(err, broker.ErrForbidden) && !errors.Is(err, broker.ErrInvalid) {
					t.Fatalf("unsafe capability error=%v, want forbidden/invalid", err)
				}
			})
		}
		if f.transport.countExecutions() != 0 {
			t.Fatalf("unsafe requests reached transport %d times", f.transport.countExecutions())
		}
	})

	t.Run("rejects_workflow_commit_from_remote_diff", func(t *testing.T) {
		f := brokerNewFixture(t, database, "fix", "C", "FIXING", true)
		f.transport.setChangedFiles([]string{"src/main.go", ".github/workflows/ci.yml"})
		_, err := f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionPush,
			Branch: "feature/owned", NewHeadSHA: brokerTestNewHead})
		if !errors.Is(err, broker.ErrForbidden) {
			t.Fatalf("workflow commit error=%v, want forbidden", err)
		}
		if f.transport.countExecutions() != 0 {
			t.Fatalf("workflow mutation reached transport")
		}
	})

	t.Run("unknown_push_is_queried_before_retry", func(t *testing.T) {
		f := brokerNewFixture(t, database, "fix", "C", "FIXING", true)
		f.transport.failNextExecution()
		request := broker.Request{OperationID: f.operationID, Action: broker.ActionPush, Branch: "feature/owned", NewHeadSHA: brokerTestNewHead}
		_, err := f.service.Execute(ctx, f.lease, request)
		if !errors.Is(err, broker.ErrUnknown) {
			t.Fatalf("ambiguous write error=%v, want unknown", err)
		}
		_, err = f.service.Execute(ctx, f.lease, request)
		if err != nil {
			t.Fatalf("retried push after remote lookup: %v", err)
		}
		events := f.transport.eventSnapshot()
		if len(events) != 4 || !strings.HasPrefix(events[0], "execute:") || !strings.HasPrefix(events[1], "lookup:") || !strings.HasPrefix(events[2], "lookup:") || !strings.HasPrefix(events[3], "execute:") {
			t.Fatalf("remote operation order=%v, want execute, lookup, lookup, execute", events)
		}
		var status string
		if err := database.Pool.QueryRow(ctx, `SELECT status FROM github_operations WHERE id=$1::uuid`, f.operationID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "CONFIRMED" {
			t.Fatalf("durable push status=%s, want CONFIRMED", status)
		}
	})

	t.Run("pause_first_denies_mutation_and_admitted_rpc_runs_without_locks", func(t *testing.T) {
		t.Run("pause_first", func(t *testing.T) {
			f := brokerNewFixture(t, database, "fix", "C", "FIXING", true)
			if _, err := database.Pool.Exec(ctx, `UPDATE tasks SET state='PAUSED',generation=generation+1 WHERE id=$1::uuid`, f.taskID); err != nil {
				t.Fatal(err)
			}
			_, err := f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionPush,
				Branch: "feature/owned", NewHeadSHA: brokerTestNewHead})
			if !errors.Is(err, broker.ErrStale) || f.transport.countExecutions() != 0 {
				t.Fatalf("pause-first admission=(%v, writes=%d), want stale/0", err, f.transport.countExecutions())
			}
		})
		t.Run("pause_after_intent_commit", func(t *testing.T) {
			f := brokerNewFixture(t, database, "fix", "C", "FIXING", true)
			started, release := make(chan struct{}), make(chan struct{})
			f.transport.blockNextExecution(started, release)
			type outcome struct {
				op  broker.Operation
				err error
			}
			finished := make(chan outcome, 1)
			go func() {
				op, err := f.service.Execute(ctx, f.lease, broker.Request{OperationID: f.operationID, Action: broker.ActionPush,
					Branch: "feature/owned", NewHeadSHA: brokerTestNewHead})
				finished <- outcome{op: op, err: err}
			}()
			<-started
			var status string
			if err := database.Pool.QueryRow(ctx, `SELECT status FROM github_operations WHERE id=$1::uuid`, f.operationID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "IN_FLIGHT" {
				t.Fatalf("intent status while RPC blocked=%s, want IN_FLIGHT", status)
			}
			if _, err := database.Pool.Exec(ctx, `UPDATE tasks SET state='PAUSED',generation=generation+1 WHERE id=$1::uuid`, f.taskID); err != nil {
				t.Fatal(err)
			}
			close(release)
			result := <-finished
			if result.err != nil || result.op.Status != "CONFIRMED" || f.transport.countExecutions() != 1 {
				t.Fatalf("admitted operation after pause=(%+v,%v), writes=%d", result.op, result.err, f.transport.countExecutions())
			}
		})
	})

	t.Run("protected_merge_requires_current_review_ci_and_human_approval", func(t *testing.T) {
		f := brokerNewFixture(t, database, "ci_reconcile", "A", "READY_TO_MERGE", true)
		f.configureMerge(t)
		if _, err := f.service.Merge(ctx, f.taskID, f.operationID); !errors.Is(err, broker.ErrForbidden) {
			t.Fatalf("missing protected approval error=%v, want forbidden", err)
		}
		if f.transport.countExecutions() != 0 {
			t.Fatalf("unapproved protected merge reached transport")
		}
		if _, err := database.Pool.Exec(ctx, `UPDATE prs SET approved_head_sha=$2,approved_base_sha=$3,human_approval_id='approval-42' WHERE task_id=$1::uuid`,
			f.taskID, brokerTestHead, brokerTestBase); err != nil {
			t.Fatal(err)
		}
		result, err := f.service.Merge(ctx, f.taskID, f.operationID)
		if err != nil || result.Status != "CONFIRMED" {
			t.Fatalf("approved merge=(%+v,%v)", result, err)
		}
		remote := f.transport.lastOperation()
		if remote.ExpectedHeadSHA != brokerTestHead {
			t.Fatalf("merge expected head=%s, want %s", remote.ExpectedHeadSHA, brokerTestHead)
		}
	})

	t.Run("merge_requires_receipt_for_exact_reviewed_source_head", func(t *testing.T) {
		for _, phase := range []string{"execute", "lookup"} {
			for _, head := range []string{"mismatched", "empty"} {
				t.Run(phase+"_"+head, func(t *testing.T) {
					f := brokerNewFixture(t, database, "ci_reconcile", "A", "READY_TO_MERGE", true)
					f.configureMerge(t)
					if _, err := database.Pool.Exec(ctx, `UPDATE prs SET approved_head_sha=$2,approved_base_sha=$3,human_approval_id='approval-head-bound' WHERE task_id=$1::uuid`,
						f.taskID, brokerTestHead, brokerTestBase); err != nil {
						t.Fatal(err)
					}
					receiptHead := "different-reviewed-head"
					if head == "empty" {
						receiptHead = ""
					}
					badReceipt := broker.RemoteReceipt{RemoteID: "remote-merge", HeadSHA: receiptHead, Merged: true}
					if phase == "execute" {
						f.transport.setNextReceipt(badReceipt)
						operation, err := f.service.Merge(ctx, f.taskID, f.operationID)
						if !errors.Is(err, broker.ErrUnknown) || operation.Status != "UNKNOWN" {
							t.Fatalf("mismatched execute receipt=(%+v,%v), want UNKNOWN", operation, err)
						}
					} else {
						f.transport.failNextExecution()
						operation, err := f.service.Merge(ctx, f.taskID, f.operationID)
						if !errors.Is(err, broker.ErrUnknown) || operation.Status != "UNKNOWN" {
							t.Fatalf("ambiguous merge execute=(%+v,%v), want UNKNOWN", operation, err)
						}
						f.transport.setLookupObservation(broker.RemoteObservation{Applied: true, Receipt: badReceipt})
						operation, err = f.service.Merge(ctx, f.taskID, f.operationID)
						if !errors.Is(err, broker.ErrUnknown) {
							t.Fatalf("mismatched lookup receipt=(%+v,%v), want UNKNOWN", operation, err)
						}
					}
					var status string
					if err := database.Pool.QueryRow(ctx, `SELECT status FROM github_operations WHERE id=$1::uuid`, f.operationID).Scan(&status); err != nil {
						t.Fatal(err)
					}
					if status != "UNKNOWN" || f.transport.countExecutions() != 1 {
						t.Fatalf("durable merge status=%q, executions=%d; want UNKNOWN and one mutation attempt", status, f.transport.countExecutions())
					}
				})
			}
		}
	})

	t.Run("merge_admission_serializes_with_pause", func(t *testing.T) {
		f := brokerNewFixture(t, database, "ci_reconcile", "A", "READY_TO_MERGE", true)
		f.configureMerge(t)
		if _, err := database.Pool.Exec(ctx, `UPDATE prs SET approved_head_sha=$2,approved_base_sha=$3,human_approval_id='approval-race' WHERE task_id=$1::uuid`,
			f.taskID, brokerTestHead, brokerTestBase); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		type mergeResult struct {
			operation broker.Operation
			err       error
		}
		mergeDone := make(chan mergeResult, 1)
		pauseDone := make(chan error, 1)
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			<-start
			operation, err := f.service.Merge(ctx, f.taskID, f.operationID)
			mergeDone <- mergeResult{operation: operation, err: err}
		}()
		go func() {
			defer group.Done()
			<-start
			_, err := database.Pool.Exec(ctx, `UPDATE tasks SET state='PAUSED',generation=generation+1 WHERE id=$1::uuid`, f.taskID)
			pauseDone <- err
		}()
		close(start)
		group.Wait()
		merged := <-mergeDone
		if err := <-pauseDone; err != nil {
			t.Fatalf("serialize pause state update: %v", err)
		}
		writes := f.transport.countExecutions()
		if writes > 1 {
			t.Fatalf("concurrent pause/merge issued %d writes", writes)
		}
		if writes == 0 && !errors.Is(merged.err, broker.ErrForbidden) && !errors.Is(merged.err, broker.ErrStale) {
			t.Fatalf("pause-first merge outcome=(%+v,%v), want denied", merged.operation, merged.err)
		}
		if writes == 1 && (merged.err != nil || merged.operation.Status != "CONFIRMED") {
			t.Fatalf("merge-first admitted operation=(%+v,%v)", merged.operation, merged.err)
		}
		var generation int64
		var state string
		if err := database.Pool.QueryRow(ctx, `SELECT state,generation FROM tasks WHERE id=$1::uuid`, f.taskID).Scan(&state, &generation); err != nil {
			t.Fatal(err)
		}
		if state != "PAUSED" || generation != 1 {
			t.Fatalf("post-race task=(%s,%d), want PAUSED/1", state, generation)
		}
	})
}

const (
	brokerTestHead        = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	brokerTestBase        = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	brokerTestIntegration = "cccccccccccccccccccccccccccccccccccccccc"
	brokerTestNewHead     = "dddddddddddddddddddddddddddddddddddddddd"
	brokerTestFindingID   = "11111111-1111-4111-8111-111111111111"
)

type brokerTestFixture struct {
	taskID, jobID, operationID, orgID string
	lease                             leases.Lease
	service                           *broker.Service
	transport                         *brokerTestTransport
	pool                              testutil.DatabaseFixture
}

func brokerNewFixture(t *testing.T, database testutil.DatabaseFixture, jobOperation, role, state string, attached bool) *brokerTestFixture {
	t.Helper()
	ctx := context.Background()
	if err := storage.Migrate(ctx, database.Pool); err != nil {
		t.Fatalf("migrate broker fixture: %v", err)
	}
	seed := uint64(time.Now().UnixNano())
	orgID := fmt.Sprintf("broker-%x", seed)
	if _, err := database.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id,rolling_limit_micro_usd) VALUES($1,100000)`, orgID); err != nil {
		t.Fatal(err)
	}
	var taskID, jobID, operationID string
	if err := database.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version)
		VALUES($1,'owner/broker-test',$2,'broker-test',$3,'broker-test-v1') RETURNING id::text`, orgID, fmt.Sprintf("source-%x", seed), state).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	var prNumber int64
	var prID int64
	snapshot := contracts.Snapshot{}
	if attached {
		snapshot = contracts.Snapshot{HeadSHA: brokerTestHead, BaseSHA: brokerTestBase, IntegrationSHA: brokerTestIntegration}
		prNumber = int64(seed%2_000_000_000) + 1
		if err := database.Pool.QueryRow(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha,integration_sha,ci_status)
			VALUES($1,'owner/broker-test',$2,$3,'main',$4,$5,'SUCCESS') RETURNING id,pr_number`, taskID, prNumber,
			brokerTestHead, brokerTestBase, brokerTestIntegration).Scan(&prID, &prNumber); err != nil {
			t.Fatal(err)
		}
	}
	jobID = brokerTestUUID(t, database)
	operationID = brokerTestUUID(t, database)
	job := contracts.Job{Version: 1, TaskID: taskID, JobID: jobID, Generation: 0, Snapshot: snapshot, Attempt: 1,
		OperationID: operationID, CorrelationID: brokerTestUUID(t, database), Operation: jobOperation}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var prRef any
	if attached {
		prRef = prID
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO jobs(id,task_id,pr_id,logical_key,operation_type,generation,expected_head_sha,expected_base_sha,payload)
		VALUES($1::uuid,$2::uuid,$3,$4,$5,0,$6,$7,$8::jsonb)`, jobID, taskID, prRef, "broker-test:"+jobID, jobOperation,
		optionalSHAValue(snapshot.HeadSHA), optionalSHAValue(snapshot.BaseSHA), string(payload)); err != nil {
		t.Fatal(err)
	}
	clockNow := clock.NewManual(time.Now().UTC())
	lease, err := leases.Claim(ctx, database.Pool, clockNow, leases.ClaimRequest{TaskID: taskID, JobID: jobID, TTL: time.Minute,
		AgentType: role, PromptHash: strings.Repeat("a", 64), SupervisorIdentity: "host-supervisor", CredentialID: "opaque-credential-id"})
	if err != nil {
		t.Fatalf("claim broker fixture lease: %v", err)
	}
	remote := broker.RemotePullRequest{Number: prNumber, HeadRef: "feature/owned", BaseRef: "main", HeadSHA: snapshot.HeadSHA,
		BaseSHA: snapshot.BaseSHA, Open: attached}
	transport := &brokerTestTransport{remote: remote, applied: make(map[string]broker.RemoteReceipt)}
	service, err := broker.New(database.Pool, clockNow, transport, broker.TaskUUIDBranchResolver{}, broker.Config{
		Repositories: map[string]broker.RepositoryPolicy{"owner/broker-test": {AllowedTargetBranches: []string{"main"}}}, RPCTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("construct broker service: %v", err)
	}
	return &brokerTestFixture{taskID: taskID, jobID: jobID, operationID: operationID, orgID: orgID, lease: lease, service: service,
		transport: transport, pool: database}
}

func (f *brokerTestFixture) configureMerge(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	producer := policy.Producer{Kind: policy.ProducerActor, ID: "7"}
	config := policy.CIConfig{TrustedProducers: []policy.Producer{producer}, RequiredChecks: []policy.RequiredCheck{{Name: "Build", Producer: producer}},
		WaitTimeout: 10 * time.Minute, PollInterval: time.Minute}
	var err error
	f.service, err = broker.New(f.pool.Pool, clock.NewManual(time.Now().UTC()), f.transport, broker.TaskUUIDBranchResolver{}, broker.Config{
		Repositories: map[string]broker.RepositoryPolicy{"owner/broker-test": {AllowedTargetBranches: []string{"main"},
			ProtectedTargetBranches: []string{"main"}, AutonomousMergeEnabled: true, CI: config}}, RPCTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("construct merge broker: %v", err)
	}
	cyclePayload, _ := json.Marshal(map[string]string{"verdict": "approve"})
	var prID int64
	if err := f.pool.Pool.QueryRow(ctx, `SELECT id FROM prs WHERE task_id=$1::uuid`, f.taskID).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Pool.Exec(ctx, `INSERT INTO review_cycles(pr_id,generation,head_sha,base_sha,verdict,normalized_version,blocker_fingerprints,payload)
		VALUES($1,0,$2,$3,'approve','broker-test-v1','[]'::jsonb,$4::jsonb)`, prID, brokerTestHead, brokerTestBase, string(cyclePayload)); err != nil {
		t.Fatal(err)
	}
	f.transport.setChecks([]ci.Observation{{Name: "Build", Producer: producer, HeadSHA: brokerTestHead, BaseSHA: brokerTestBase,
		IntegrationSHA: brokerTestIntegration, Status: "completed", Conclusion: "success", Attempt: 1, UpdatedAt: time.Now().UTC()}})
}

func brokerTestUUID(t *testing.T, database testutil.DatabaseFixture) string {
	t.Helper()
	var id string
	if err := database.Pool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func brokerTestReview(t *testing.T, database testutil.DatabaseFixture, fixture *brokerTestFixture) contracts.Review {
	t.Helper()
	var payload []byte
	if err := database.Pool.QueryRow(context.Background(), `SELECT payload FROM jobs WHERE id=$1::uuid`, fixture.jobID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	job, err := contracts.DecodeJob(payload)
	if err != nil {
		t.Fatal(err)
	}
	return contracts.Review{Version: 1, TaskID: fixture.taskID, JobID: fixture.jobID, RunID: fixture.lease.RunID,
		Generation: fixture.lease.Generation, LeaseToken: fixture.lease.Token, Snapshot: fixture.lease.Snapshot,
		Attempt: fixture.lease.Attempt, OperationID: job.OperationID, CorrelationID: job.CorrelationID,
		Verdict: contracts.VerdictApprove, Summary: "Review is clear", Findings: []contracts.Finding{}}
}

func optionalSHAValue(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type brokerTestTransport struct {
	mu                sync.Mutex
	remote            broker.RemotePullRequest
	checks            []ci.Observation
	files             []string
	events            []string
	calls             int
	operations        []broker.RemoteOperation
	applied           map[string]broker.RemoteReceipt
	nextReceipt       *broker.RemoteReceipt
	lookupObservation *broker.RemoteObservation
	fail              bool
	started           chan struct{}
	release           chan struct{}
}

func (f *brokerTestTransport) PullRequest(_ context.Context, _ string, _ int64) (broker.RemotePullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.remote, nil
}
func (f *brokerTestTransport) Checks(_ context.Context, _ string, _ int64, _ contracts.Snapshot) ([]ci.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return append([]ci.Observation(nil), f.checks...), nil
}
func (f *brokerTestTransport) ChangedFiles(_ context.Context, _, _, _ string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return append([]string(nil), f.files...), nil
}
func (f *brokerTestTransport) Execute(_ context.Context, operation broker.RemoteOperation) (broker.RemoteReceipt, error) {
	f.mu.Lock()
	f.calls++
	f.events = append(f.events, "execute:"+operation.ID)
	f.operations = append(f.operations, operation)
	started, release, shouldFail := f.started, f.release, f.fail
	nextReceipt := f.nextReceipt
	f.nextReceipt = nil
	f.fail = false
	if !brokerTestCapability(operation.ApplicationRole, operation.Action) || operation.Force {
		f.mu.Unlock()
		return broker.RemoteReceipt{}, broker.ErrForbidden
	}
	if operation.PRNumber != 0 && (operation.Snapshot.HeadSHA != f.remote.HeadSHA || operation.Snapshot.BaseSHA != f.remote.BaseSHA ||
		operation.ExpectedHeadSHA != f.remote.HeadSHA || operation.ExpectedBaseSHA != f.remote.BaseSHA) {
		f.mu.Unlock()
		return broker.RemoteReceipt{}, broker.ErrStale
	}
	if operation.Action == broker.ActionPush && (operation.Branch != f.remote.HeadRef || operation.ExpectedHeadSHA != f.remote.HeadSHA ||
		operation.ExpectedBaseSHA != f.remote.BaseSHA || operation.Snapshot.IntegrationSHA != brokerTestIntegration) {
		f.mu.Unlock()
		return broker.RemoteReceipt{}, broker.ErrStale
	}
	if operation.Action == broker.ActionMerge && (!f.remote.Open || f.remote.Draft || f.remote.Conflicted ||
		operation.TargetBranch != f.remote.BaseRef || operation.ExpectedHeadSHA != f.remote.HeadSHA || operation.ExpectedBaseSHA != f.remote.BaseSHA) {
		f.mu.Unlock()
		return broker.RemoteReceipt{}, broker.ErrStale
	}
	for _, path := range f.files {
		path = strings.TrimLeft(strings.ReplaceAll(path, "\\", "/"), "/")
		if path == ".github/workflows" || strings.HasPrefix(path, ".github/workflows/") {
			f.mu.Unlock()
			return broker.RemoteReceipt{}, broker.ErrForbidden
		}
	}
	f.mu.Unlock()
	if started != nil {
		close(started)
		<-release
	}
	if shouldFail {
		return broker.RemoteReceipt{}, errors.New("ambiguous fixture timeout")
	}
	receiptHead := operation.HeadSHA
	if operation.Action == broker.ActionMerge {
		receiptHead = operation.ExpectedHeadSHA
	}
	receipt := broker.RemoteReceipt{RemoteID: "remote-" + operation.ID, HeadSHA: receiptHead, Merged: operation.Action == broker.ActionMerge}
	if nextReceipt != nil {
		receipt = *nextReceipt
	}
	f.mu.Lock()
	f.applied[operation.ID] = receipt
	f.mu.Unlock()
	return receipt, nil
}
func (f *brokerTestTransport) Lookup(_ context.Context, operationID, _ string) (broker.RemoteObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.events = append(f.events, "lookup:"+operationID)
	if f.lookupObservation != nil {
		observation := *f.lookupObservation
		f.lookupObservation = nil
		return observation, nil
	}
	receipt, ok := f.applied[operationID]
	return broker.RemoteObservation{Applied: ok, Receipt: receipt}, nil
}
func (f *brokerTestTransport) failNextExecution() { f.mu.Lock(); defer f.mu.Unlock(); f.fail = true }
func (f *brokerTestTransport) setNextReceipt(receipt broker.RemoteReceipt) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextReceipt = &receipt
}
func (f *brokerTestTransport) setLookupObservation(observation broker.RemoteObservation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookupObservation = &observation
}
func (f *brokerTestTransport) setChangedFiles(files []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = files
}
func (f *brokerTestTransport) setChecks(checks []ci.Observation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks = checks
}
func (f *brokerTestTransport) blockNextExecution(started, release chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started, f.release = started, release
}
func (f *brokerTestTransport) eventSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}
func (f *brokerTestTransport) countCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}
func (f *brokerTestTransport) countExecutions() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.operations)
}
func (f *brokerTestTransport) lastOperation() broker.RemoteOperation {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.operations[len(f.operations)-1]
}

var _ broker.Transport = (*brokerTestTransport)(nil)

func brokerTestCapability(role string, action broker.Action) bool {
	switch role {
	case "A":
		return action == broker.ActionCreatePR || action == broker.ActionPublish || action == broker.ActionMerge
	case "B":
		return action == broker.ActionReview || action == broker.ActionResolve
	case "C":
		return action == broker.ActionPush || action == broker.ActionReply
	default:
		return false
	}
}
