package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

const (
	leasesHeadSHA    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	leasesBaseSHA    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	leasesPromptHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

type leasesFixture struct {
	ctx      context.Context
	db       testutil.DatabaseFixture
	clock    *clock.Manual
	taskID   string
	jobs     []string
	snapshot contracts.Snapshot
	nextID   int
}

func leasesNewFixture(t *testing.T, jobCount int) *leasesFixture {
	t.Helper()
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate lease fixture: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	orgID := "lease-org"
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, orgID); err != nil {
		t.Fatalf("insert lease org: %v", err)
	}
	var taskID string
	if err := db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,policy_version) VALUES($1,'owner/repo',$2,'owner','v1') RETURNING id::text`, orgID, orgID).Scan(&taskID); err != nil {
		t.Fatalf("insert lease task: %v", err)
	}
	snapshot := contracts.Snapshot{HeadSHA: leasesHeadSHA, BaseSHA: leasesBaseSHA}
	var prID int64
	if err := db.Pool.QueryRow(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha) VALUES($1,'owner/repo',1,$2,'main',$3) RETURNING id`, taskID, leasesHeadSHA, leasesBaseSHA).Scan(&prID); err != nil {
		t.Fatalf("insert lease PR: %v", err)
	}
	fixture := &leasesFixture{ctx: ctx, db: db, clock: manual, taskID: taskID, snapshot: snapshot}
	for i := 0; i < jobCount; i++ {
		fixture.jobs = append(fixture.jobs, fixture.addJob(t, prID))
	}
	return fixture
}

func (f *leasesFixture) addJob(t *testing.T, prID int64) string {
	t.Helper()
	f.nextID++
	jobID := fmt.Sprintf("%08x-1111-4111-8111-%012x", f.nextID, f.nextID)
	opID := fmt.Sprintf("%08x-2222-4222-8222-%012x", f.nextID, f.nextID)
	corrID := fmt.Sprintf("%08x-3333-4333-8333-%012x", f.nextID, f.nextID)
	payload, err := json.Marshal(contracts.Job{Version: 1, TaskID: f.taskID, JobID: jobID, Generation: 0, Snapshot: f.snapshot, Attempt: 1, OperationID: opID, CorrelationID: corrID, Operation: "author"})
	if err != nil {
		t.Fatalf("marshal lease job: %v", err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		t.Fatalf("decode lease job object: %v", err)
	}
	delete(object, "budget_envelope")
	payload, err = json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal lease job payload: %v", err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO jobs(id,task_id,pr_id,logical_key,operation_type,generation,expected_head_sha,expected_base_sha,remediation_attempt,payload)
		VALUES($1,$2,$3,$4,'author',0,$5,$6,1,$7::jsonb)`, jobID, f.taskID, prID, "lease:"+jobID, leasesHeadSHA, leasesBaseSHA, payload); err != nil {
		t.Fatalf("insert lease job: %v", err)
	}
	return jobID
}

func leasesClaimRequest(f *leasesFixture, jobID string) leases.ClaimRequest {
	return leases.ClaimRequest{TaskID: f.taskID, JobID: jobID, TTL: time.Minute, AgentType: "A", PromptHash: leasesPromptHash, SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test"}
}

func TestLeasesExpiryReplacementFencesOldToken(t *testing.T) {
	f := leasesNewFixture(t, 1)
	old, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatalf("claim old lease: %v", err)
	}
	if old.RunAttempt != 1 || old.Attempt != 1 {
		t.Fatalf("lease attempts=(logical %d, run %d), want 1,1", old.Attempt, old.RunAttempt)
	}
	f.clock.Advance(time.Minute) // Exact expiry boundary is expired (expiry > now is required).
	newLease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatalf("replace expired lease: %v", err)
	}
	if old.Token == newLease.Token || old.RunID == newLease.RunID || newLease.RunAttempt != 2 {
		t.Fatalf("replacement reused identity or attempt: old=%+v new=%+v", old, newLease)
	}
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, old); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("Validate(old)=%v, want ErrStale", err)
	}
	if _, err := leases.Heartbeat(f.ctx, f.db.Pool, f.clock, old, time.Minute); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("Heartbeat(old)=%v, want ErrStale", err)
	}
	if err := leases.Complete(f.ctx, f.db.Pool, f.clock, old, "SUCCESS"); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("Complete(old)=%v, want ErrStale", err)
	}
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, newLease); err != nil {
		t.Fatalf("Validate(new)=%v", err)
	}
	if err := leases.Complete(f.ctx, f.db.Pool, f.clock, newLease, "SUCCESS"); err != nil {
		t.Fatalf("Complete(new)=%v", err)
	}
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, newLease); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("Validate(completed)=%v, want ErrStale", err)
	}
}

func TestLeasesConcurrentClaimsOneOwnerPerTask(t *testing.T) {
	f := leasesNewFixture(t, 2)
	start := make(chan struct{})
	type result struct {
		lease leases.Lease
		err   error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, jobID := range f.jobs {
		wg.Add(1)
		go func(jobID string) {
			defer wg.Done()
			<-start
			l, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, jobID))
			results <- result{l, err}
		}(jobID)
	}
	close(start)
	wg.Wait()
	close(results)
	claimed, denied := 0, 0
	for r := range results {
		if r.err == nil {
			claimed++
		} else if errors.Is(r.err, leases.ErrBusy) {
			denied++
		} else {
			t.Fatalf("concurrent claim returned unexpected error: %v", r.err)
		}
	}
	if claimed != 1 || denied != 1 {
		t.Fatalf("simultaneous claims: claimed=%d busy=%d, want 1,1", claimed, denied)
	}
	var active int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1 AND status='LEASED' AND lease_expires_at>$2`, f.taskID, f.clock.Now()).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("active leases=%d, want one", active)
	}
}

func TestLeasesHeartbeatRenewsStrictTTL(t *testing.T) {
	f := leasesNewFixture(t, 1)
	claim, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Minute)
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, claim); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("validate at exact expiry=%v, want stale", err)
	}
	// Claim a fresh lease and renew well before its boundary; the previous owner
	// was expired by the exact-boundary check above and is therefore fenced.
	claim, err = leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(30 * time.Second)
	renewed, err := leases.Heartbeat(f.ctx, f.db.Pool, f.clock, claim, 2*time.Minute)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !renewed.ExpiresAt.Equal(f.clock.Now().Add(2 * time.Minute)) {
		t.Fatalf("heartbeat expiry=%s", renewed.ExpiresAt)
	}
	f.clock.Set(renewed.ExpiresAt.Add(-time.Nanosecond))
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, renewed); err != nil {
		t.Fatalf("validate before renewed expiry: %v", err)
	}
	f.clock.Set(renewed.ExpiresAt)
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, renewed); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("validate at renewed expiry=%v, want stale", err)
	}
}

func TestLeasesWrongJobGenerationSnapshotAndStateDenied(t *testing.T) {
	f := leasesNewFixture(t, 2)
	wrongRole := leasesClaimRequest(f, f.jobs[0])
	wrongRole.AgentType = "B"
	if _, err := leases.Claim(f.ctx, f.db.Pool, f.clock, wrongRole); !errors.Is(err, leases.ErrInvalid) {
		t.Fatalf("review role claimed author job: %v, want ErrInvalid", err)
	}
	claim, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatal(err)
	}
	wrongJob := claim
	wrongJob.JobID = f.jobs[1]
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, wrongJob); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("valid token against different job=%v, want stale", err)
	}
	t.Run("generation", func(t *testing.T) {
		if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET generation=1 WHERE id=$1`, f.taskID); err != nil {
			t.Fatal(err)
		}
		if err := leases.Validate(f.ctx, f.db.Pool, f.clock, claim); !errors.Is(err, leases.ErrStale) {
			t.Fatalf("changed generation=%v, want stale", err)
		}
	})
	// The generation-mutated fixture is already fenced; change the attached
	// snapshot in a fresh independent test fixture for its own boundary.
}

func TestLeasesControlOperationsDoNotRequireInference(t *testing.T) {
	for _, item := range []struct {
		operation string
		agentType string
	}{{"ci_reconcile", "A"}, {"reply", "C"}} {
		t.Run(item.operation, func(t *testing.T) {
			f := leasesNewFixture(t, 1)
			if _, err := f.db.Pool.Exec(f.ctx, `UPDATE jobs SET operation_type=$2,payload=jsonb_set(payload,'{operation}',to_jsonb($2::text)) WHERE id=$1::uuid`, f.jobs[0], item.operation); err != nil {
				t.Fatalf("update control operation: %v", err)
			}
			req := leasesClaimRequest(f, f.jobs[0])
			req.AgentType = item.agentType
			lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, req)
			if err != nil {
				t.Fatalf("claim control operation: %v", err)
			}
			if lease.InferenceRequired {
				t.Fatalf("control operation %q requires inference", item.operation)
			}
			if err := leases.Validate(f.ctx, f.db.Pool, f.clock, lease); err != nil {
				t.Fatalf("validate control lease: %v", err)
			}
		})
	}
}

func TestLeasesOperationAgentMapping(t *testing.T) {
	for _, item := range []struct {
		operation string
		role      string
		inference bool
	}{{"author", "A", true}, {"review", "B", true}, {"fix", "C", true}, {"ci_reconcile", "A", false}, {"reply", "C", false}} {
		role, ok := leases.AgentTypeForOperation(item.operation)
		if !ok || role != item.role || leases.RequiresInference(item.operation) != item.inference {
			t.Errorf("operation %q mapped to %q (%t), inference=%t; want %q, true, %t", item.operation, role, ok, leases.RequiresInference(item.operation), item.role, item.inference)
		}
	}
	if role, ok := leases.AgentTypeForOperation("unknown"); ok || role != "" || leases.RequiresInference("unknown") {
		t.Fatalf("unknown operation mapped to %q (%t) or requires inference", role, ok)
	}
}

func TestLeasesSnapshotAndForbiddenTaskStatesDenied(t *testing.T) {
	t.Run("snapshot", func(t *testing.T) {
		f := leasesNewFixture(t, 1)
		claim, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool.Exec(f.ctx, `UPDATE prs SET head_sha=$2 WHERE task_id=$1`, f.taskID, "dddddddddddddddddddddddddddddddddddddddd"); err != nil {
			t.Fatal(err)
		}
		if err := leases.Validate(f.ctx, f.db.Pool, f.clock, claim); !errors.Is(err, leases.ErrStale) {
			t.Fatalf("changed snapshot=%v, want stale", err)
		}
	})
	for _, state := range []string{"PAUSED", "ESCALATED", "MERGED", "CLOSED"} {
		t.Run(state, func(t *testing.T) {
			f := leasesNewFixture(t, 1)
			claim, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET state=$2 WHERE id=$1`, f.taskID, state); err != nil {
				t.Fatal(err)
			}
			if err := leases.Validate(f.ctx, f.db.Pool, f.clock, claim); !errors.Is(err, leases.ErrStale) {
				t.Fatalf("state %s=%v, want stale", state, err)
			}
		})
	}
}

func TestLeasesRetryHasDurableDueFence(t *testing.T) {
	f := leasesNewFixture(t, 1)
	original, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatal(err)
	}
	if err := leases.Retry(f.ctx, f.db.Pool, f.clock, original, f.clock.Now(), "SUCCESS"); !errors.Is(err, leases.ErrInvalid) {
		t.Fatalf("non-future retry=%v, want invalid", err)
	}
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, original); err != nil {
		t.Fatalf("invalid retry changed ownership: %v", err)
	}
	due := f.clock.Now().Add(time.Minute)
	if err := leases.Retry(f.ctx, f.db.Pool, f.clock, original, due, "SUCCESS"); err != nil {
		t.Fatal(err)
	}
	var status string
	var finished, evidence bool
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT j.status,r.finished_at IS NOT NULL AND r.execution_status='SUCCESS',EXISTS(
		SELECT 1 FROM outbox WHERE job_id=j.id AND kind='DISPATCH' AND next_attempt_at=$3 AND payload->>'retry_from_run_id'=r.id::text
	) FROM jobs j JOIN agent_runs r ON r.job_id=j.id WHERE j.id=$1::uuid AND r.id=$2::uuid`, original.JobID, original.RunID, due).Scan(&status, &finished, &evidence); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || !finished || !evidence {
		t.Fatalf("retry disposition=%s finished=%t evidence=%t", status, finished, evidence)
	}
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, original); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("retry kept old authority=%v", err)
	}
	if _, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, original.JobID)); !errors.Is(err, leases.ErrNotDue) {
		t.Fatalf("early duplicate delivery bypassed durable delay: %v", err)
	}
	if err := leases.Retry(f.ctx, f.db.Pool, f.clock, original, due, "SUCCESS"); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("old run scheduled twice=%v", err)
	}
	f.clock.Advance(time.Minute)
	next, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, original.JobID))
	if err != nil {
		t.Fatalf("exact due boundary denied: %v", err)
	}
	if next.JobID != original.JobID || next.RunID == original.RunID || next.Token == original.Token || next.RunAttempt != 2 || next.Attempt != original.Attempt {
		t.Fatalf("retry changed logical identity or reused authority: original=%+v next=%+v", original, next)
	}
}
