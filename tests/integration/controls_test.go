package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/control"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/lifecycle"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

const (
	controlHead     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	controlBase     = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	controlNextHead = "cccccccccccccccccccccccccccccccccccccccc"
	controlNextBase = "dddddddddddddddddddddddddddddddddddddddd"
)

type controlFixture struct {
	db     *testutil.DatabaseFixture
	redis  testutil.RedisFixture
	ctx    context.Context
	clock  *clock.Manual
	orgID  string
	taskID string
	prID   *int64
}

func controlRequireFixture(t *testing.T, state string, attached bool) controlFixture {
	t.Helper()
	db, redisFixture := testutil.RequireServices(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatal(err)
	}
	clockNow := clock.NewManual(time.Date(2026, 10, 1, 18, 0, 0, 0, time.UTC))
	orgID := "control-test-org"
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, orgID); err != nil {
		t.Fatal(err)
	}
	var taskID string
	if err := db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version) VALUES($1,'owner/control','issue:71','44',$2,'control-v1') RETURNING id::text`, orgID, state).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	fixture := controlFixture{db: &db, redis: redisFixture, ctx: ctx, clock: clockNow, orgID: orgID, taskID: taskID}
	if attached {
		var prID int64
		if err := db.Pool.QueryRow(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha,integration_sha,approved_head_sha,approved_base_sha,human_approval_id,ci_status) VALUES($1::uuid,'owner/control',71,$2,'main',$3,$4,$2,$3,'control-approval','SUCCESS') RETURNING id`, taskID, controlHead, controlBase, controlIntegration()).Scan(&prID); err != nil {
			t.Fatal(err)
		}
		fixture.prID = &prID
	}
	return fixture
}

func controlIntegration() string { return "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" }

func controlAuthority(f controlFixture) control.Authority {
	return control.Authority{ActorID: "44", Repository: "owner/control", Permission: control.PermissionMaintain, VerifiedAt: f.clock.Now()}
}

func controlRemote(f controlFixture) control.RemoteObservation {
	if f.prID == nil {
		return control.RemoteObservation{Repository: "owner/control", State: control.RemotePRAbsent, IssueState: "open", ObservedAt: f.clock.Now()}
	}
	return control.RemoteObservation{Repository: "owner/control", State: control.RemotePROpen, PRNumber: 71, Snapshot: contracts.Snapshot{HeadSHA: controlNextHead, BaseSHA: controlNextBase}, BaseRef: "main", ObservedAt: f.clock.Now()}
}

func controlPolicy() control.ResumePolicy {
	return control.ResumePolicy{Version: "control-v2", TaskBudgetLimitMicroUSD: 4_000_000, GlobalBudgetLimitMicroUSD: 5_000_000, MaxReviewCycles: 4, GlobalMaxReviewCycles: 5, AllowedTargetBranches: []string{"main", "release/*"}}
}

func controlWithTask(t *testing.T, f controlFixture, callback func(context.Context, *storage.Repositories, storage.LockedOrgBudget, storage.LockedTask) error) {
	t.Helper()
	err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		budget, locked, err := repos.LockOrgBudgetAndTask(ctx, f.orgID, f.taskID)
		if err != nil {
			return err
		}
		return callback(ctx, repos, budget, locked)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func controlInsertJob(t *testing.T, f controlFixture, operation string) string {
	t.Helper()
	var jobID string
	controlWithTask(t, f, func(ctx context.Context, repos *storage.Repositories, _ storage.LockedOrgBudget, locked storage.LockedTask) error {
		task := locked.Record()
		jobID = "d1000000-0000-4000-8000-000000000071"
		snapshot := contracts.Snapshot{}
		if f.prID != nil {
			snapshot = contracts.Snapshot{HeadSHA: controlHead, BaseSHA: controlBase, IntegrationSHA: controlIntegration()}
		}
		job := contracts.Job{Version: contracts.VersionV1, TaskID: task.ID, JobID: jobID, Generation: task.Generation, Snapshot: snapshot, Attempt: 1, OperationID: "d2000000-0000-4000-8000-000000000071", CorrelationID: "d3000000-0000-4000-8000-000000000071", Operation: operation}
		payload, err := json.Marshal(job)
		if err != nil {
			return err
		}
		_, _, err = repos.InsertJobAndOutbox(ctx, locked, storage.JobInput{ID: jobID, LogicalKey: fmt.Sprintf("task:%s:generation:%d:%s", task.ID, task.Generation, operation), OperationType: operation, PRID: task.PRID, Payload: payload})
		return err
	})
	return jobID
}

func controlPause(f controlFixture, authority control.Authority) error {
	return storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		_, locked, err := repos.LockOrgBudgetAndTask(ctx, f.orgID, f.taskID)
		if err != nil {
			return err
		}
		_, err = control.PauseLocked(ctx, repos, locked, f.clock, control.PauseRequest{Authority: authority, Reason: "operator stop"})
		return err
	})
}

func TestControlsPauseAuthorizationOutageAndLabelRemoval(t *testing.T) {
	f := controlRequireFixture(t, string(lifecycle.WaitingCI), true)
	if err := f.redis.Client.Ping(f.ctx).Err(); err != nil {
		t.Fatalf("owned Redis fixture is not available before outage-independent control test: %v", err)
	}
	jobID := controlInsertJob(t, f, control.OperationCIReconcile)
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: f.taskID, JobID: jobID, TTL: time.Hour, AgentType: "A", PromptHash: fmt.Sprintf("%064x", 1), SupervisorIdentity: "control-test-supervisor", CredentialID: "control-test-credential"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET reserved_micro_usd=250000 WHERE id=$1::uuid`, f.taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO budget_reservations(task_id,org_id,run_id,envelope_micro_usd,remaining_micro_usd,pricing_version,admission_envelope,status,created_at) VALUES($1::uuid,$2,$3::uuid,250000,250000,'control-v1','{"max_cost_micro_usd":250000,"max_input_tokens":1000,"max_output_tokens":100,"max_calls":1,"pricing_version":"control-v1"}'::jsonb,'RESERVED',$4)`, f.taskID, f.orgID, lease.RunID, f.clock.Now()); err != nil {
		t.Fatal(err)
	}

	unauthorized := control.Authority{ActorID: "99", Repository: "owner/control", Permission: "read", VerifiedAt: f.clock.Now()}
	if err := controlPause(f, unauthorized); !errors.Is(err, control.ErrUnauthorized) {
		t.Fatalf("unauthorized stop error=%v", err)
	}
	var changed bool
	controlWithTask(t, f, func(ctx context.Context, repos *storage.Repositories, _ storage.LockedOrgBudget, locked storage.LockedTask) error {
		updated, didChange, err := control.ApplyPauseLabelLocked(ctx, repos, locked, f.clock, control.PauseLabel, false, control.Authority{}, "label removed")
		changed = didChange || updated.Record().State != string(lifecycle.WaitingCI)
		return err
	})
	if changed {
		t.Fatal("removing bots:paused changed task state")
	}

	// Control writes need PostgreSQL only after the owned Redis fixture was verified above.
	if err := controlPause(f, controlAuthority(f)); err != nil {
		t.Fatalf("pause while Redis is unavailable to control path: %v", err)
	}
	var state string
	var generation int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,generation FROM tasks WHERE id=$1::uuid`, f.taskID).Scan(&state, &generation); err != nil {
		t.Fatal(err)
	}
	if state != string(lifecycle.Paused) || generation != 1 {
		t.Fatalf("pause state=%s generation=%d", state, generation)
	}
	controlWithTask(t, f, func(ctx context.Context, repos *storage.Repositories, _ storage.LockedOrgBudget, locked storage.LockedTask) error {
		updated, didChange, err := control.ApplyPauseLabelLocked(ctx, repos, locked, f.clock, control.PauseLabel, false, control.Authority{}, "label removed")
		if err == nil && (didChange || updated.Record().State != string(lifecycle.Paused)) {
			return errors.New("pause-label removal resumed or changed paused task")
		}
		return err
	})
	var jobStatus string
	var token *string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&jobStatus, &token); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "CANCELLED" || token != nil {
		t.Fatalf("pause did not revoke job authority: status=%s token=%v", jobStatus, token)
	}
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, lease); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("pre-pause lease remained valid: %v", err)
	}
	var runStatus string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT execution_status FROM agent_runs WHERE job_id=$1::uuid`, jobID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "TERMINATED" {
		t.Fatalf("pause failed to revoke durable run admission: run=%s", runStatus)
	}
	var reservationStatus string
	var remaining int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,remaining_micro_usd FROM budget_reservations WHERE run_id=$1::uuid`, lease.RunID).Scan(&reservationStatus, &remaining); err != nil {
		t.Fatal(err)
	}
	if reservationStatus != "UNKNOWN" || remaining != 250000 {
		t.Fatalf("pause released unsettled coverage: status=%s remaining=%d", reservationStatus, remaining)
	}
	var cancels, labels, actions int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND kind='CANCEL'`, f.taskID).Scan(&cancels); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND kind='LABEL_SYNC'`, f.taskID).Scan(&labels); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM control_actions WHERE task_id=$1::uuid AND action='pause'`, f.taskID).Scan(&actions); err != nil {
		t.Fatal(err)
	}
	if cancels != 1 || labels != 2 || actions != 1 {
		t.Fatalf("durable pause evidence cancel=%d label=%d action=%d", cancels, labels, actions)
	}
}

func TestControlsResumeRefreshesSnapshotAndPreservesAccounting(t *testing.T) {
	f := controlRequireFixture(t, string(lifecycle.WaitingCI), true)
	if err := controlPause(f, controlAuthority(f)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET cycle_count=1,spent_micro_usd=12345,reserved_micro_usd=2345 WHERE id=$1::uuid`, f.taskID); err != nil {
		t.Fatal(err)
	}
	var resumed storage.LockedTask
	controlWithTask(t, f, func(ctx context.Context, repos *storage.Repositories, budget storage.LockedOrgBudget, locked storage.LockedTask) error {
		var err error
		resumed, err = control.ResumeLocked(ctx, repos, budget, locked, f.clock, control.ResumeRequest{Authority: controlAuthority(f), Remote: controlRemote(f), Policy: controlPolicy(), Reason: "operator resume"})
		return err
	})
	task := resumed.Record()
	if task.State != string(lifecycle.WaitingCI) || task.Generation != 2 || task.Snapshot.HeadSHA != controlNextHead || task.Snapshot.BaseSHA != controlNextBase || task.Snapshot.IntegrationSHA != "" || task.CycleCount != 1 || task.BudgetLimitMicroUSD != 4_000_000 || task.MaxReviewCycles != 4 || task.PolicyVersion != "control-v2" {
		t.Fatalf("resume returned task %+v", task)
	}
	var spent, reserved int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT spent_micro_usd,reserved_micro_usd FROM tasks WHERE id=$1::uuid`, f.taskID).Scan(&spent, &reserved); err != nil {
		t.Fatal(err)
	}
	if spent != 12345 || reserved != 2345 {
		t.Fatalf("resume reset accounting: spent=%d reserved=%d", spent, reserved)
	}
	var ciStatus string
	var approved *string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT ci_status,approved_head_sha FROM prs WHERE id=$1`, *f.prID).Scan(&ciStatus, &approved); err != nil {
		t.Fatal(err)
	}
	if ciStatus != "PENDING" || approved != nil {
		t.Fatalf("resume failed to invalidate CI/review gates: ci=%s approved=%v", ciStatus, approved)
	}
	var operation, logicalKey string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT operation_type,logical_key FROM jobs WHERE task_id=$1::uuid AND generation=2`, f.taskID).Scan(&operation, &logicalKey); err != nil {
		t.Fatal(err)
	}
	if operation != control.OperationCIReconcile || logicalKey != fmt.Sprintf("task:%s:generation:2:%s", f.taskID, control.OperationCIReconcile) {
		t.Fatalf("resume job identity operation=%s key=%s", operation, logicalKey)
	}
}

func TestControlsResumeUnattachedIssueReturnsToAuthoring(t *testing.T) {
	f := controlRequireFixture(t, string(lifecycle.Paused), false)
	var resumed storage.LockedTask
	controlWithTask(t, f, func(ctx context.Context, repos *storage.Repositories, budget storage.LockedOrgBudget, locked storage.LockedTask) error {
		var err error
		resumed, err = control.ResumeLocked(ctx, repos, budget, locked, f.clock, control.ResumeRequest{Authority: controlAuthority(f), Remote: controlRemote(f), Policy: controlPolicy(), Reason: "resume authoring"})
		return err
	})
	if resumed.Record().State != string(lifecycle.Authoring) || resumed.Record().Generation != 1 || resumed.Record().PRID != nil || resumed.Record().Snapshot != (contracts.Snapshot{}) {
		t.Fatalf("unattached resume returned task %+v", resumed.Record())
	}
	var operation string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT operation_type FROM jobs WHERE task_id=$1::uuid AND generation=1`, f.taskID).Scan(&operation); err != nil {
		t.Fatal(err)
	}
	if operation != control.OperationAuthor {
		t.Fatalf("unattached resume queued %q", operation)
	}
}

func TestControlsResumeRequiresFreshAuthorityRemoteAndKillSwitchClear(t *testing.T) {
	for _, tc := range []struct {
		name              string
		mutate            func(*control.ResumeRequest, *controlFixture)
		kill              bool
		killInTransaction bool
		want              error
	}{
		{name: "unauthorized", mutate: func(req *control.ResumeRequest, _ *controlFixture) { req.Authority.Permission = "read" }, want: control.ErrUnauthorized},
		{name: "stale_remote", mutate: func(req *control.ResumeRequest, f *controlFixture) {
			req.Remote.ObservedAt = f.clock.Now().Add(-6 * time.Minute)
		}, want: control.ErrRemoteObservation},
		{name: "unapproved_target", mutate: func(req *control.ResumeRequest, _ *controlFixture) { req.Remote.BaseRef = "untrusted" }, want: control.ErrRemoteObservation},
		{name: "global_kill_switch", kill: true, want: control.ErrKillSwitch},
		{name: "kill_switch_changed_after_lock", killInTransaction: true, want: control.ErrKillSwitch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := controlRequireFixture(t, string(lifecycle.WaitingCI), true)
			if err := controlPause(f, controlAuthority(f)); err != nil {
				t.Fatal(err)
			}
			if tc.kill {
				if _, err := f.db.Pool.Exec(f.ctx, `UPDATE org_budgets SET emergency_mode=TRUE WHERE org_id=$1`, f.orgID); err != nil {
					t.Fatal(err)
				}
			}
			req := control.ResumeRequest{Authority: controlAuthority(f), Remote: controlRemote(f), Policy: controlPolicy(), Reason: "resume test"}
			if tc.mutate != nil {
				tc.mutate(&req, &f)
			}
			var observedResumeErr error
			err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
				budget, locked, err := repos.LockOrgBudgetAndTask(ctx, f.orgID, f.taskID)
				if err != nil {
					return err
				}
				if tc.killInTransaction {
					if _, err := repos.Queries().Exec(ctx, `UPDATE org_budgets SET emergency_mode=TRUE WHERE org_id=$1`, f.orgID); err != nil {
						return err
					}
				}
				_, observedResumeErr = control.ResumeLocked(ctx, repos, budget, locked, f.clock, req)
				if tc.killInTransaction && errors.Is(observedResumeErr, tc.want) {
					return nil
				}
				return observedResumeErr
			})
			if tc.killInTransaction {
				if err != nil || !errors.Is(observedResumeErr, tc.want) {
					t.Fatalf("transaction error=%v resume error=%v, want committed transaction and resume error %v", err, observedResumeErr, tc.want)
				}
				var emergency bool
				if err := f.db.Pool.QueryRow(f.ctx, `SELECT emergency_mode FROM org_budgets WHERE org_id=$1`, f.orgID).Scan(&emergency); err != nil {
					t.Fatal(err)
				}
				if !emergency {
					t.Fatal("same-transaction emergency-mode change was not committed")
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("resume error=%v, want %v", err, tc.want)
			}
			var state string
			var generation int64
			var jobs int
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,generation,(SELECT count(*) FROM jobs WHERE task_id=tasks.id) FROM tasks WHERE id=$1::uuid`, f.taskID).Scan(&state, &generation, &jobs); err != nil {
				t.Fatal(err)
			}
			if state != string(lifecycle.Paused) || generation != 1 || jobs != 0 {
				t.Fatalf("rejected resume changed task state=%s generation=%d jobs=%d", state, generation, jobs)
			}
		})
	}
}

func TestControlsSharedCycledRemediationLimit(t *testing.T) {
	for _, tc := range []struct{ name, state string }{{"ci_only_failure", string(lifecycle.WaitingCI)}, {"review_request_changes", string(lifecycle.InReview)}} {
		t.Run(tc.name, func(t *testing.T) {
			f := controlRequireFixture(t, tc.state, true)
			if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET cycle_count=3,max_review_cycles=3 WHERE id=$1::uuid`, f.taskID); err != nil {
				t.Fatal(err)
			}
			controlWithTask(t, f, func(ctx context.Context, repos *storage.Repositories, _ storage.LockedOrgBudget, locked storage.LockedTask) error {
				updated, attempt, admitted, err := control.AdmitFixLocked(ctx, repos, locked, f.clock)
				if err != nil {
					return err
				}
				if admitted || attempt != 3 || updated.Record().State != string(lifecycle.Escalated) || updated.Record().Generation != 1 {
					return fmt.Errorf("limit result admitted=%t attempt=%d task=%+v", admitted, attempt, updated.Record())
				}
				if _, _, admitted, err := control.AdmitFixLocked(ctx, repos, updated, f.clock); !errors.Is(err, control.ErrInvalid) || admitted {
					return fmt.Errorf("replayed C admission admitted=%t err=%v", admitted, err)
				}
				return nil
			})
			var fixes, state string
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*)::text FROM jobs WHERE task_id=$1::uuid AND operation_type='fix'`, f.taskID).Scan(&fixes); err != nil {
				t.Fatal(err)
			}
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT state FROM tasks WHERE id=$1::uuid`, f.taskID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if fixes != "0" || state != string(lifecycle.Escalated) {
				t.Fatalf("attempt-limit fence fixes=%s state=%s", fixes, state)
			}
		})
	}
}

func TestControlsAdmitAndQueueStableFixAttempt(t *testing.T) {
	f := controlRequireFixture(t, string(lifecycle.ChangesRequested), true)
	if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET cycle_count=2,max_review_cycles=3 WHERE id=$1::uuid`, f.taskID); err != nil {
		t.Fatal(err)
	}
	controlWithTask(t, f, func(ctx context.Context, repos *storage.Repositories, _ storage.LockedOrgBudget, locked storage.LockedTask) error {
		updated, attempt, admitted, err := control.AdmitFixLocked(ctx, repos, locked, f.clock)
		if err != nil {
			return err
		}
		if !admitted || attempt != 3 || updated.Record().CycleCount != 3 {
			return fmt.Errorf("attempt admitted=%t n=%d task=%+v", admitted, attempt, updated.Record())
		}
		_, _, err = control.QueueFixLocked(ctx, repos, updated, attempt)
		return err
	})
	var logicalKey string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT logical_key FROM jobs WHERE task_id=$1::uuid AND operation_type='fix'`, f.taskID).Scan(&logicalKey); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("task:%s:generation:0:cycle:3:fix", f.taskID)
	if logicalKey != want {
		t.Fatalf("fix logical key=%s want=%s", logicalKey, want)
	}
}
