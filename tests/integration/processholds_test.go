package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/control"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/lifecycle"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/storage"
)

type processholdsFixtureVerifier struct{ now func() time.Time }

type lateProcessholdsVerifier struct{}

func (lateProcessholdsVerifier) VerifyNeverStarted(ctx context.Context, h processholds.Hold) (processholds.ReapEvidence, error) {
	<-ctx.Done()
	return processholds.ReapEvidence{Kind: processholds.ProofNeverStarted, RunID: h.RunID, TaskID: h.TaskID, JobID: h.JobID,
		Generation: h.Generation, ResourceScope: h.ResourceScope, LeaseTokenSHA: h.LeaseTokenSHA, Workspace: h.Workspace,
		SupervisorID: h.SupervisorID, VerifiedAt: h.UpdatedAt, VerifierID: "late-test-host-ledger"}, nil
}
func (lateProcessholdsVerifier) VerifyGroupDrained(ctx context.Context, h processholds.Hold) (processholds.ReapEvidence, error) {
	<-ctx.Done()
	p := *h.Process
	return processholds.ReapEvidence{Kind: processholds.ProofGroupDrained, RunID: h.RunID, TaskID: h.TaskID, JobID: h.JobID,
		Generation: h.Generation, ResourceScope: h.ResourceScope, LeaseTokenSHA: h.LeaseTokenSHA, Workspace: h.Workspace,
		SupervisorID: h.SupervisorID, Process: &p, VerifiedAt: h.UpdatedAt, VerifierID: "late-test-host-ledger"}, nil
}

func (v processholdsFixtureVerifier) VerifyNeverStarted(_ context.Context, h processholds.Hold) (processholds.ReapEvidence, error) {
	return processholds.ReapEvidence{Kind: processholds.ProofNeverStarted, RunID: h.RunID, TaskID: h.TaskID, JobID: h.JobID,
		Generation: h.Generation, ResourceScope: h.ResourceScope, LeaseTokenSHA: h.LeaseTokenSHA, Workspace: h.Workspace,
		SupervisorID: h.SupervisorID, VerifiedAt: v.now(), VerifierID: "test-host-ledger"}, nil
}
func (v processholdsFixtureVerifier) VerifyGroupDrained(_ context.Context, h processholds.Hold) (processholds.ReapEvidence, error) {
	p := *h.Process
	return processholds.ReapEvidence{Kind: processholds.ProofGroupDrained, RunID: h.RunID, TaskID: h.TaskID, JobID: h.JobID,
		Generation: h.Generation, ResourceScope: h.ResourceScope, LeaseTokenSHA: h.LeaseTokenSHA, Workspace: h.Workspace,
		SupervisorID: h.SupervisorID, Process: &p, VerifiedAt: v.now(), VerifierID: "test-host-ledger"}, nil
}

func TestUnresolvedProcessHoldBlocksReplacementUntilTrustedDrain(t *testing.T) {
	for _, mode := range []string{"expired", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			f := leasesNewFixture(t, 2)
			lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
			if err != nil {
				t.Fatalf("claim original run: %v", err)
			}
			store, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "test-host-pool", MaxActive: 1, VerifierTimeout: time.Second}, processholdsFixtureVerifier{now: f.clock.Now})
			if err != nil {
				t.Fatal(err)
			}
			hold, err := store.Reserve(f.ctx, lease, "/private/workspaces/one", "host-supervisor-test")
			if err != nil {
				t.Fatalf("persist hold before start: %v", err)
			}
			process := processholds.ProcessIdentity{PID: 42, PGID: 40, StartIdentity: "boot-1/process-start-42"}
			if _, err := store.Started(f.ctx, hold.RunID, process); err != nil {
				t.Fatalf("record host start identity: %v", err)
			}
			if _, err := store.Unknown(f.ctx, hold.RunID, processholds.ReasonSupervisorLost); err != nil {
				t.Fatalf("retain uncertain hold: %v", err)
			}
			if mode == "expired" {
				f.clock.Advance(time.Minute)
			} else if err := leases.Complete(f.ctx, f.db.Pool, f.clock, lease, "TERMINATED"); err != nil {
				t.Fatalf("mark old run cancelled: %v", err)
			}
			if _, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[1])); !errors.Is(err, leases.ErrBusy) {
				t.Fatalf("replacement claim with unresolved old process=%v, want busy", err)
			}
			unresolved, err := store.ListUnresolved(f.ctx)
			if err != nil || len(unresolved) != 1 || unresolved[0].State != processholds.StateUnknown {
				t.Fatalf("uncertain hold inventory=(%+v,%v), want one UNKNOWN", unresolved, err)
			}
			reaped, err := store.Reaped(f.ctx, hold.RunID)
			if err != nil || reaped.State != processholds.StateReaped {
				t.Fatalf("verified process-group reaping=(%+v,%v)", reaped, err)
			}
			replacement, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[1]))
			if err != nil {
				t.Fatalf("replacement claim after host-confirmed drain: %v", err)
			}
			if replacement.RunID == lease.RunID {
				t.Fatal("replacement reused old run identity")
			}
		})
	}
}

func TestProcessHoldCapacityCountsAllUnresolvedRunsInScope(t *testing.T) {
	f := leasesNewFixture(t, 1)
	firstLease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatal(err)
	}
	store, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "test-host-pool", MaxActive: 1, VerifierTimeout: time.Second}, processholdsFixtureVerifier{now: f.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Reserve(f.ctx, firstLease, "/private/workspaces/first", "host-supervisor-test")
	if err != nil {
		t.Fatalf("reserve first run: %v", err)
	}

	secondTaskID, secondJobID := processholdsCreateSecondTask(t, f)
	secondLease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: secondTaskID, JobID: secondJobID, TTL: time.Minute,
		AgentType: "A", PromptHash: leasesPromptHash, SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test"})
	if err != nil {
		t.Fatalf("claim second task: %v", err)
	}
	if _, err := store.Reserve(f.ctx, secondLease, "/private/workspaces/second", "host-supervisor-test"); !errors.Is(err, processholds.ErrCapacity) {
		t.Fatalf("second scope reservation=%v, want capacity exhausted", err)
	}
	if _, err := store.Unknown(f.ctx, first.RunID, processholds.ReasonSupervisorLost); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reserve(f.ctx, secondLease, "/private/workspaces/second", "host-supervisor-test"); !errors.Is(err, processholds.ErrCapacity) {
		t.Fatalf("UNKNOWN hold stopped consuming capacity: %v", err)
	}
	if _, err := store.Reaped(f.ctx, first.RunID); err != nil {
		t.Fatalf("trusted never-started reconciliation: %v", err)
	}
	if _, err := store.Reserve(f.ctx, secondLease, "/private/workspaces/second", "host-supervisor-test"); err != nil {
		t.Fatalf("scope capacity not released after verified reaping: %v", err)
	}
}

func TestReserveReplayCannotReopenStartedUnknownOrReapedHold(t *testing.T) {
	for _, mode := range []string{"reserved", "started", "unknown", "reaped"} {
		t.Run(mode, func(t *testing.T) {
			f := leasesNewFixture(t, 1)
			lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
			if err != nil {
				t.Fatal(err)
			}
			store, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "test-host-pool", MaxActive: 1, VerifierTimeout: time.Second}, processholdsFixtureVerifier{now: f.clock.Now})
			if err != nil {
				t.Fatal(err)
			}
			hold, err := store.Reserve(f.ctx, lease, "/private/workspaces/replay", "host-supervisor-test")
			if err != nil {
				t.Fatalf("initial durable reservation: %v", err)
			}
			switch mode {
			case "started":
				process := processholds.ProcessIdentity{PID: 73, PGID: 70, StartIdentity: "boot-replay/process-start-73"}
				if _, err := store.Started(f.ctx, hold.RunID, process); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				process := processholds.ProcessIdentity{PID: 73, PGID: 70, StartIdentity: "boot-replay/process-start-73"}
				if _, err := store.Started(f.ctx, hold.RunID, process); err != nil {
					t.Fatal(err)
				}
				if _, err := store.Unknown(f.ctx, hold.RunID, processholds.ReasonSupervisorLost); err != nil {
					t.Fatal(err)
				}
			case "reaped":
				if _, err := store.Reaped(f.ctx, hold.RunID); err != nil {
					t.Fatalf("trusted never-started reaping: %v", err)
				}
			}
			before, err := store.Get(f.ctx, hold.RunID)
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.Reserve(f.ctx, lease, "/private/workspaces/replay", "host-supervisor-test")
			if mode == "reserved" {
				if err != nil || got.Revision != before.Revision || got.State != processholds.StateReserved {
					t.Fatalf("exact RESERVED replay=(%+v,%v), want unchanged reservation", got, err)
				}
			} else if !errors.Is(err, processholds.ErrConflict) {
				t.Fatalf("replay of %s hold=%v, want conflict", mode, err)
			}
			after, err := store.Get(f.ctx, hold.RunID)
			if err != nil || after.State != before.State || after.Revision != before.Revision {
				t.Fatalf("denied replay changed durable hold: before=%+v after=%+v err=%v", before, after, err)
			}
			var active int
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM process_holds WHERE resource_scope=$1 AND state <> 'REAPED'`, hold.ResourceScope).Scan(&active); err != nil {
				t.Fatal(err)
			}
			wantActive := 0
			if mode != "reaped" {
				wantActive = 1
			}
			if active != wantActive {
				t.Fatalf("replay changed held capacity: active=%d want=%d", active, wantActive)
			}
		})
	}
}

func TestBeginStartIsSingleDurableLaunchPermission(t *testing.T) {
	f := leasesNewFixture(t, 1)
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatal(err)
	}
	store, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "test-host-pool", MaxActive: 1, VerifierTimeout: time.Second}, processholdsFixtureVerifier{now: f.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	hold, err := store.Reserve(f.ctx, lease, "/private/workspaces/begin-start", "host-supervisor-test")
	if err != nil {
		t.Fatal(err)
	}
	startedIntent, err := store.BeginStart(f.ctx, lease)
	if err != nil || startedIntent.State != processholds.StateUnknown || startedIntent.UnknownReason != processholds.ReasonStartAmbiguous {
		t.Fatalf("durable begin-start intent=(%+v,%v)", startedIntent, err)
	}
	if _, err := store.BeginStart(f.ctx, lease); !errors.Is(err, processholds.ErrConflict) {
		t.Fatalf("replayed begin-start=%v, want conflict", err)
	}
	// Simulate process death after the durable intent commit but before its
	// response/Runner.Start. The intent remains charged until host evidence.
	loaded, err := store.Get(f.ctx, hold.RunID)
	if err != nil || loaded.State != processholds.StateUnknown || loaded.Revision != startedIntent.Revision {
		t.Fatalf("crash-window intent=(%+v,%v)", loaded, err)
	}
	unresolved, err := store.ListUnresolved(f.ctx)
	if err != nil || len(unresolved) != 1 || unresolved[0].RunID != hold.RunID {
		t.Fatalf("crash-window capacity hold=(%+v,%v)", unresolved, err)
	}
}

func TestBeginStartRejectsExpiredOrCancelledLease(t *testing.T) {
	for _, mode := range []string{"expired", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			f := leasesNewFixture(t, 1)
			lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
			if err != nil {
				t.Fatal(err)
			}
			store, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "test-host-pool", MaxActive: 1, VerifierTimeout: time.Second}, processholdsFixtureVerifier{now: f.clock.Now})
			if err != nil {
				t.Fatal(err)
			}
			hold, err := store.Reserve(f.ctx, lease, "/private/workspaces/stale-start", "host-supervisor-test")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "expired" {
				f.clock.Advance(time.Minute)
			} else if err := leases.Complete(f.ctx, f.db.Pool, f.clock, lease, "TERMINATED"); err != nil {
				t.Fatalf("cancel old lease before launch intent: %v", err)
			}
			if _, err := store.BeginStart(f.ctx, lease); !errors.Is(err, leases.ErrStale) {
				t.Fatalf("begin start for %s lease=%v, want stale", mode, err)
			}
			loaded, err := store.Get(f.ctx, hold.RunID)
			if err != nil || loaded.State != processholds.StateReserved || loaded.Revision != hold.Revision {
				t.Fatalf("stale begin-start changed reservation=(%+v,%v)", loaded, err)
			}
			unresolved, err := store.ListUnresolved(f.ctx)
			if err != nil || len(unresolved) != 1 {
				t.Fatalf("stale begin-start released capacity: holds=%+v err=%v", unresolved, err)
			}
		})
	}
}

func TestLateProcessVerifierCannotReleaseHoldAfterDeadline(t *testing.T) {
	f := leasesNewFixture(t, 1)
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leasesClaimRequest(f, f.jobs[0]))
	if err != nil {
		t.Fatal(err)
	}
	store, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "test-host-pool", MaxActive: 1, VerifierTimeout: 10 * time.Millisecond}, lateProcessholdsVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	hold, err := store.Reserve(f.ctx, lease, "/private/workspaces/late-proof", "host-supervisor-test")
	if err != nil {
		t.Fatal(err)
	}
	process := processholds.ProcessIdentity{PID: 88, PGID: 84, StartIdentity: "boot-late/process-start-88"}
	if _, err := store.Started(f.ctx, hold.RunID, process); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Unknown(f.ctx, hold.RunID, processholds.ReasonSupervisorLost); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reaped(f.ctx, hold.RunID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late exact verifier proof=%v, want verifier deadline rejection", err)
	}
	loaded, err := store.Get(f.ctx, hold.RunID)
	if err != nil || loaded.State != processholds.StateUnknown {
		t.Fatalf("late verifier proof changed durable hold=(%+v,%v)", loaded, err)
	}
}

func processholdsCreateSecondTask(t *testing.T, f *leasesFixture) (string, string) {
	t.Helper()
	orgID := "lease-org-processholds-second"
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, orgID); err != nil {
		t.Fatal(err)
	}
	var taskID string
	if err := f.db.Pool.QueryRow(f.ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,policy_version)
		VALUES($1,'owner/second-repo','processholds-second','owner','v1') RETURNING id::text`, orgID).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	var prID int64
	if err := f.db.Pool.QueryRow(f.ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha)
		VALUES($1,'owner/second-repo',1,$2,'main',$3) RETURNING id`, taskID, leasesHeadSHA, leasesBaseSHA).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	other := &leasesFixture{ctx: f.ctx, db: f.db, clock: f.clock, taskID: taskID, snapshot: f.snapshot, nextID: 300}
	return taskID, other.addJob(t, prID)
}

func TestControlPauseResumeKeepsUnknownProcessHoldFenced(t *testing.T) {
	f := controlRequireFixture(t, string(lifecycle.Authoring), false)
	jobID := controlInsertJob(t, f, "author")
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: f.taskID, JobID: jobID, TTL: time.Minute,
		AgentType: "A", PromptHash: leasesPromptHash, SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test"})
	if err != nil {
		t.Fatalf("claim original author run: %v", err)
	}
	store, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "control-host-pool", MaxActive: 1, VerifierTimeout: time.Second}, processholdsFixtureVerifier{now: f.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	hold, err := store.Reserve(f.ctx, lease, "/private/workspaces/control", "host-supervisor-test")
	if err != nil {
		t.Fatalf("reserve before runner start: %v", err)
	}
	process := processholds.ProcessIdentity{PID: 53, PGID: 51, StartIdentity: "boot-control/process-start-53"}
	if _, err := store.Started(f.ctx, hold.RunID, process); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Unknown(f.ctx, hold.RunID, processholds.ReasonSupervisorLost); err != nil {
		t.Fatal(err)
	}

	if err := controlPause(f, controlAuthority(f)); err != nil {
		t.Fatalf("real control pause UOW: %v", err)
	}
	controlWithTask(t, f, func(ctx context.Context, repos *storage.Repositories, budget storage.LockedOrgBudget, locked storage.LockedTask) error {
		_, err := control.ResumeLocked(ctx, repos, budget, locked, f.clock, control.ResumeRequest{Authority: controlAuthority(f), Remote: controlRemote(f), Policy: controlPolicy(), Reason: "operator resume"})
		return err
	})
	var nextJobID string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text FROM jobs WHERE task_id=$1::uuid AND generation=(SELECT generation FROM tasks WHERE id=$1::uuid) AND status='PENDING'`, f.taskID).Scan(&nextJobID); err != nil {
		t.Fatalf("read resumed generation job: %v", err)
	}
	req := leases.ClaimRequest{TaskID: f.taskID, JobID: nextJobID, TTL: time.Minute, AgentType: "A", PromptHash: leasesPromptHash,
		SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test"}
	if _, err := leases.Claim(f.ctx, f.db.Pool, f.clock, req); !errors.Is(err, leases.ErrBusy) {
		t.Fatalf("resumed generation claim with unresolved prior hold=%v, want busy", err)
	}
	if _, err := store.Reaped(f.ctx, hold.RunID); err != nil {
		t.Fatalf("trusted old-group drain: %v", err)
	}
	if _, err := leases.Claim(f.ctx, f.db.Pool, f.clock, req); err != nil {
		t.Fatalf("claim resumed generation after host drain: %v", err)
	}
}
