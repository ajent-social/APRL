package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/control"
	"github.com/ajent-social/APRL/internal/httpapi"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/internal/supervisor"
	"github.com/ajent-social/APRL/tests/testutil"
)

const supervisorPromptHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type supervisorFixture struct {
	db       testutil.DatabaseFixture
	ctx      context.Context
	clock    *clock.Manual
	orgID    string
	repo     string
	taskID   string
	jobID    string
	lease    leases.Lease
	envelope contracts.BudgetEnvelope
	job      contracts.Job
	workRoot string
	holds    *processholds.Store
	evidence *supervisorProcessEvidence
}

type supervisorProcessEvidence struct {
	mu      sync.Mutex
	now     func() time.Time
	started map[string]bool
	process map[string]processholds.ProcessIdentity
}

func newSupervisorProcessEvidence(now func() time.Time) *supervisorProcessEvidence {
	return &supervisorProcessEvidence{now: now, started: make(map[string]bool), process: make(map[string]processholds.ProcessIdentity)}
}

func (v *supervisorProcessEvidence) noteReserved(runID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, exists := v.started[runID]; !exists {
		v.started[runID] = false
	}
}

func (v *supervisorProcessEvidence) noteStartAttempt(runID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.started[runID] = true
}

func (v *supervisorProcessEvidence) noteProcess(runID string, identity processholds.ProcessIdentity) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.process[runID] = identity
}

func (v *supervisorProcessEvidence) VerifyNeverStarted(_ context.Context, h processholds.Hold) (processholds.ReapEvidence, error) {
	v.mu.Lock()
	started, known := v.started[h.RunID]
	v.mu.Unlock()
	if !known || started {
		return processholds.ReapEvidence{}, errors.New("test host launch ledger cannot prove process start was never attempted")
	}
	return v.reapEvidence(h, processholds.ProofNeverStarted), nil
}

func (v *supervisorProcessEvidence) VerifyGroupDrained(_ context.Context, h processholds.Hold) (processholds.ReapEvidence, error) {
	if h.Process == nil {
		return processholds.ReapEvidence{}, errors.New("process identity absent")
	}
	v.mu.Lock()
	identity, known := v.process[h.RunID]
	v.mu.Unlock()
	if !known || identity != *h.Process {
		return processholds.ReapEvidence{}, errors.New("test host launch ledger process identity mismatch")
	}
	err := syscall.Kill(-int(h.Process.PGID), 0)
	if !errors.Is(err, syscall.ESRCH) {
		return processholds.ReapEvidence{}, errors.New("owned process group is still visible to the host")
	}
	proof := v.reapEvidence(h, processholds.ProofGroupDrained)
	copyIdentity := *h.Process
	proof.Process = &copyIdentity
	return proof, nil
}

func (v *supervisorProcessEvidence) reapEvidence(h processholds.Hold, kind string) processholds.ReapEvidence {
	return processholds.ReapEvidence{Kind: kind, RunID: h.RunID, TaskID: h.TaskID, JobID: h.JobID,
		Generation: h.Generation, ResourceScope: h.ResourceScope, LeaseTokenSHA: h.LeaseTokenSHA,
		Workspace: h.Workspace, SupervisorID: h.SupervisorID,
		VerifiedAt: v.now().UTC(), VerifierID: "supervisor-test-host"}
}

func TestSupervisor(t *testing.T) {
	t.Run("nil_runner_fails_closed", func(t *testing.T) {
		f := supervisorRequireFixture(t)
		sink := supervisorNewHTTPSink(t, f)
		if _, err := supervisor.New(f.db.Pool, f.clock, nil, sink, f.holds, supervisorTestConfig(f.workRoot)); !errors.Is(err, supervisor.ErrInvalid) {
			t.Fatalf("New with nil runner = %v, want ErrInvalid", err)
		}
		if _, err := supervisor.New(f.db.Pool, f.clock, &supervisorProcessRunner{}, nil, f.holds, supervisorTestConfig(f.workRoot)); !errors.Is(err, supervisor.ErrInvalid) {
			t.Fatalf("New with nil result submitter = %v, want ErrInvalid", err)
		}
		if _, err := supervisor.New(f.db.Pool, f.clock, &supervisorProcessRunner{}, sink, nil, supervisorTestConfig(f.workRoot)); !errors.Is(err, supervisor.ErrInvalid) {
			t.Fatalf("New with nil process hold store = %v, want ErrInvalid", err)
		}
		var typedNilClock *clock.Manual
		var typedNilRunner *supervisorProcessRunner
		var typedNilSink *supervisorHTTPSink
		var typedNilHolds *processholds.Store
		for name, create := range map[string]func() error{
			"clock": func() error {
				_, err := supervisor.New(f.db.Pool, typedNilClock, &supervisorProcessRunner{}, sink, f.holds, supervisorTestConfig(f.workRoot))
				return err
			},
			"runner": func() error {
				_, err := supervisor.New(f.db.Pool, f.clock, typedNilRunner, sink, f.holds, supervisorTestConfig(f.workRoot))
				return err
			},
			"submitter": func() error {
				_, err := supervisor.New(f.db.Pool, f.clock, &supervisorProcessRunner{}, typedNilSink, f.holds, supervisorTestConfig(f.workRoot))
				return err
			},
			"process hold store": func() error {
				_, err := supervisor.New(f.db.Pool, f.clock, &supervisorProcessRunner{}, sink, typedNilHolds, supervisorTestConfig(f.workRoot))
				return err
			},
		} {
			if err := create(); !errors.Is(err, supervisor.ErrInvalid) {
				t.Errorf("New with typed-nil %s = %v, want ErrInvalid", name, err)
			}
		}
	})

	t.Run("mismatched_reserved_envelope_never_starts_process", func(t *testing.T) {
		f := supervisorRequireFixture(t)
		runner := &supervisorProcessRunner{started: make(chan struct{})}
		sink := supervisorNewHTTPSink(t, f)
		sup, err := supervisor.New(f.db.Pool, f.clock, runner, sink, f.holds, supervisorTestConfig(f.workRoot))
		if err != nil {
			t.Fatal(err)
		}
		wrong := f.envelope
		wrong.MaxInputTokens++
		if err := sup.Run(f.ctx, f.lease, wrong); !errors.Is(err, budget.ErrReservationConflict) {
			t.Fatalf("mismatched envelope error=%v, want ErrReservationConflict", err)
		}
		select {
		case <-runner.started:
			t.Fatal("worker process started with a mismatched reservation envelope")
		default:
		}
	})

	t.Run("ambiguous_start_error_fences_run_and_marks_usage_unknown", func(t *testing.T) {
		f := supervisorRequireFixture(t)
		sink := supervisorNewHTTPSink(t, f)
		sup, err := supervisor.New(f.db.Pool, f.clock, supervisorStartErrorRunner{evidence: f.evidence}, sink, f.holds, supervisorTestConfig(f.workRoot))
		if err != nil {
			t.Fatal(err)
		}
		if err := sup.Run(f.ctx, f.lease, f.envelope); err == nil {
			t.Fatal("ambiguous runner start unexpectedly succeeded")
		}
		var runStatus, budgetStatus string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT r.execution_status,b.status FROM agent_runs r
			JOIN budget_reservations b ON b.run_id=r.id WHERE r.id=$1::uuid`, f.lease.RunID).Scan(&runStatus, &budgetStatus); err != nil {
			t.Fatal(err)
		}
		if runStatus != "TERMINATED" || budgetStatus != "UNKNOWN" {
			t.Fatalf("ambiguous start disposition=(%s,%s), want (TERMINATED,UNKNOWN)", runStatus, budgetStatus)
		}
		hold, err := f.holds.Get(f.ctx, f.lease.RunID)
		if err != nil || hold.State != processholds.StateUnknown || hold.ReapEvidence != nil {
			t.Fatalf("ambiguous start process hold=(%+v,%v), want unresolved UNKNOWN", hold, err)
		}
	})

	t.Run("prestart_emergency_mode_denies_runner_launch", func(t *testing.T) {
		f := supervisorRequireFixture(t)
		if err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
			_, _, err := repos.LockOrgBudgetAndTask(ctx, f.orgID, f.taskID)
			if err != nil {
				return err
			}
			_, err = repos.Queries().Exec(ctx, `UPDATE org_budgets SET emergency_mode=true WHERE org_id=$1`, f.orgID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		runner := supervisorNewProcessRunner(t, nil, f.evidence)
		sink := supervisorNewHTTPSink(t, f)
		sup, err := supervisor.New(f.db.Pool, f.clock, runner, sink, f.holds, supervisorTestConfig(f.workRoot))
		if err != nil {
			t.Fatal(err)
		}
		if err := sup.Run(f.ctx, f.lease, f.envelope); !errors.Is(err, budget.ErrEmergency) {
			t.Fatalf("pre-start emergency mode result = %v, want ErrEmergency", err)
		}
		select {
		case <-runner.started:
			t.Fatal("runner started despite organization emergency mode")
		default:
		}
		hold, err := f.holds.Get(f.ctx, f.lease.RunID)
		if err != nil || hold.State != processholds.StateReaped || hold.ReapEvidence == nil || hold.ReapEvidence.Kind != processholds.ProofNeverStarted {
			t.Fatalf("pre-start denied hold=(%+v,%v), want trusted NEVER_STARTED/REAPED", hold, err)
		}
		var runStatus, jobStatus, jobToken, reservationStatus string
		var remaining int64
		var retryOutbox int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT r.execution_status,j.status,COALESCE(j.lease_token::text,''),
			b.status,b.remaining_micro_usd,
			(SELECT count(*) FROM outbox o WHERE o.job_id=j.id AND o.kind='DISPATCH' AND o.payload->>'retry_from_run_id'=r.id::text)
			FROM agent_runs r JOIN jobs j ON j.id=r.job_id JOIN budget_reservations b ON b.run_id=r.id
			WHERE r.id=$1::uuid`, f.lease.RunID).Scan(&runStatus, &jobStatus, &jobToken, &reservationStatus, &remaining, &retryOutbox); err != nil {
			t.Fatal(err)
		}
		if runStatus != "TERMINATED" || jobStatus != "PENDING" || jobToken != "" || reservationStatus != "SETTLED" || remaining != 0 || retryOutbox != 1 {
			t.Fatalf("proven never-started disposition run=%s job=%s token=%q reservation=%s remaining=%d retryOutbox=%d; want TERMINATED/PENDING/no-token/SETTLED/0/1",
				runStatus, jobStatus, jobToken, reservationStatus, remaining, retryOutbox)
		}
		f.clock.Advance(2 * time.Millisecond)
		retry, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: f.taskID, JobID: f.jobID, TTL: time.Hour,
			AgentType: "A", PromptHash: supervisorPromptHash, SupervisorIdentity: "supervisor-test-host", CredentialID: "opaque-supervisor-test"})
		if err != nil {
			t.Fatalf("claim due retry after trusted never-started disposition: %v", err)
		}
		if retry.RunID == f.lease.RunID || retry.RunAttempt != f.lease.RunAttempt+1 {
			t.Fatalf("retry lease=(run %s attempt %d), want a distinct run after %d", retry.RunID, retry.RunAttempt, f.lease.RunAttempt)
		}
	})

	t.Run("activation_rechecks_emergency_mode_and_unknown_reservation", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			deny   error
			mutate func(*supervisorFixture) error
		}{
			{
				name: "emergency_mode",
				deny: budget.ErrEmergency,
				mutate: func(f *supervisorFixture) error {
					return storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
						_, _, err := repos.LockOrgBudgetAndTask(ctx, f.orgID, f.taskID)
						if err != nil {
							return err
						}
						_, err = repos.Queries().Exec(ctx, `UPDATE org_budgets SET emergency_mode=true WHERE org_id=$1`, f.orgID)
						return err
					})
				},
			},
			{
				name: "reservation_marked_unknown",
				deny: budget.ErrReservationConflict,
				mutate: func(f *supervisorFixture) error {
					var reservationID string
					if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text FROM budget_reservations WHERE task_id=$1::uuid AND run_id=$2::uuid`,
						f.taskID, f.lease.RunID).Scan(&reservationID); err != nil {
						return err
					}
					return budget.MarkUnknown(f.ctx, f.db.Pool, f.clock, reservationID)
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := supervisorRequireFixture(t)
				runner := supervisorNewProcessRunner(t, nil, f.evidence)
				identityGate := make(chan struct{})
				runner.identityGate = identityGate
				sink := supervisorNewHTTPSink(t, f)
				sup, err := supervisor.New(f.db.Pool, f.clock, runner, sink, f.holds, supervisorTestConfig(f.workRoot))
				if err != nil {
					t.Fatal(err)
				}
				runResult := make(chan error, 1)
				go func() { runResult <- sup.Run(f.ctx, f.lease, f.envelope) }()
				select {
				case <-runner.started:
				case <-time.After(5 * time.Second):
					t.Fatal("runner did not reach registered-start boundary")
				}
				if err := tc.mutate(f); err != nil {
					close(identityGate)
					t.Fatal(err)
				}
				close(identityGate)
				select {
				case err := <-runResult:
					if !errors.Is(err, tc.deny) {
						t.Fatalf("activation denial = %v, want %v", err, tc.deny)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("supervisor did not finish after activation was denied")
				}
				select {
				case <-runner.activated:
					t.Fatal("worker was activated despite changed host authority")
				default:
				}
				hold, err := f.holds.Get(f.ctx, f.lease.RunID)
				if err != nil || hold.State != processholds.StateReaped || hold.ReapEvidence == nil {
					t.Fatalf("activation denial process hold=(%+v,%v), want trusted REAPED", hold, err)
				}
			})
		}
	})

	t.Run("leader_exit_with_unverifiable_descendant_retains_hold_and_workspace", func(t *testing.T) {
		f := supervisorRequireFixture(t)
		runner := supervisorNewProcessRunner(t, []string{"--spawn-descendant"}, f.evidence)
		sink := supervisorNewHTTPSink(t, f)
		sup, err := supervisor.New(f.db.Pool, f.clock, runner, sink, f.holds, supervisorTestConfig(f.workRoot))
		if err != nil {
			t.Fatal(err)
		}
		if err := sup.Run(f.ctx, f.lease, f.envelope); !errors.Is(err, supervisor.ErrWorkspaceOwnership) {
			t.Fatalf("supervisor run with an unverifiable live descendant = %v, want ownership error", err)
		}
		pgid := runner.processGroupID()
		if pgid <= 0 {
			t.Fatal("fixture process group ID was not recorded")
		}
		hold, err := f.holds.Get(f.ctx, f.lease.RunID)
		if err != nil || hold.State != processholds.StateUnknown || hold.ReapEvidence != nil {
			t.Fatalf("live descendant hold=(%+v,%v), want unresolved UNKNOWN", hold, err)
		}
		if _, err := os.Stat(hold.Workspace); err != nil {
			t.Fatalf("unresolved hold workspace was not retained: %v", err)
		}
		supervisorEventually(t, func() bool {
			err := syscall.Kill(-pgid, 0)
			return errors.Is(err, syscall.ESRCH)
		})
		// The trusted test verifier can now release the hold using positive host
		// evidence that the exact process group disappeared on its own.
		if _, err := f.holds.Reaped(f.ctx, f.lease.RunID); err != nil {
			t.Fatalf("reap hold after independently observed group exit: %v", err)
		}
	})

	t.Run("success_registers_safe_process_heartbeats_and_cleans_only_owned_workspace", func(t *testing.T) {
		f := supervisorRequireFixture(t)
		neighbor := filepath.Join(f.workRoot, "neighbor-owned-by-test")
		if err := os.Mkdir(neighbor, 0o700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("APRL_SUPERVISOR_TOKEN", "must-not-reach-child")
		runner := supervisorNewProcessRunner(t, []string{"--hold"}, f.evidence)
		sink := supervisorNewHTTPSink(t, f)
		config := supervisorTestConfig(f.workRoot)
		config.HeartbeatInterval = 20 * time.Millisecond
		sup, err := supervisor.New(f.db.Pool, f.clock, runner, sink, f.holds, config)
		if err != nil {
			t.Fatal(err)
		}
		runDone := make(chan error, 1)
		go func() { runDone <- sup.Run(f.ctx, f.lease, f.envelope) }()
		select {
		case <-runner.activated:
		case err := <-runDone:
			t.Fatalf("worker exited before activation: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("worker did not reach its activated state")
		}
		f.clock.Advance(time.Second)
		supervisorEventually(t, func() bool {
			var changed bool
			err := f.db.Pool.QueryRow(f.ctx, `SELECT process_id>0 AND process_group_id=process_id AND workspace_id IS NOT NULL
				AND process_started_at IS NOT NULL AND last_heartbeat_at>process_started_at AND execution_deadline_at>process_started_at
				FROM agent_runs WHERE id=$1::uuid`, f.lease.RunID).Scan(&changed)
			return err == nil && changed
		})
		if err := runner.release(); err != nil {
			t.Fatalf("release held fixture worker: %v", err)
		}
		select {
		case err := <-runDone:
			if err != nil {
				t.Fatalf("supervisor run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("worker result was not durably accepted")
		}
		var jobStatus, runStatus string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT j.status,r.execution_status FROM jobs j JOIN agent_runs r ON r.job_id=j.id WHERE j.id=$1::uuid`, f.jobID).Scan(&jobStatus, &runStatus); err != nil {
			t.Fatal(err)
		}
		if jobStatus != "COMPLETED" || runStatus != "SUCCESS" {
			t.Fatalf("durable disposition=(%s,%s), want (COMPLETED,SUCCESS)", jobStatus, runStatus)
		}
		if _, err := os.Stat(neighbor); err != nil {
			t.Fatalf("neighbor workspace was removed: %v", err)
		}
		entries, err := os.ReadDir(f.workRoot)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].Name() != filepath.Base(neighbor) {
			t.Fatalf("owned workspace cleanup left entries: %+v", entries)
		}
		if len(runner.environment) == 0 || strings.Contains(strings.Join(runner.environment, "\n"), "TOKEN") || strings.Contains(strings.Join(runner.environment, "\n"), "SECRET") {
			t.Fatalf("child environment was not an explicit safe allowlist: %v", runner.environment)
		}
	})

	t.Run("pause_fences_and_reaps_term_ignoring_process_group", func(t *testing.T) {
		f := supervisorRequireFixture(t)
		runner := supervisorNewProcessRunner(t, []string{"--ignore-term"}, f.evidence)
		sink := supervisorNewHTTPSink(t, f)
		config := supervisorTestConfig(f.workRoot)
		config.HeartbeatInterval = 10 * time.Millisecond
		sup, err := supervisor.New(f.db.Pool, f.clock, runner, sink, f.holds, config)
		if err != nil {
			t.Fatal(err)
		}
		runDone := make(chan error, 1)
		go func() { runDone <- sup.Run(f.ctx, f.lease, f.envelope) }()
		select {
		case <-runner.activated:
		case err := <-runDone:
			t.Fatalf("worker exited before activation: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("worker did not activate")
		}
		if err := supervisorPause(f); err != nil {
			t.Fatal(err)
		}
		if sup.CancelRun(f.taskID, "stale-prior-run") {
			t.Fatal("stale run cancellation targeted the active replacement")
		}
		if !sup.CancelRun(f.taskID, f.lease.RunID) {
			t.Fatal("CancelRun did not find the live owned process")
		}
		select {
		case err := <-runDone:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, leases.ErrStale) {
				t.Fatalf("paused run error=%v, want cancellation or stale lease", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("pause did not terminate and reap the worker process group")
		}
		select {
		case <-runner.waited:
		default:
			t.Fatal("supervisor returned before process Wait/reap completed")
		}
		var runStatus, budgetStatus string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT r.execution_status,b.status FROM agent_runs r JOIN budget_reservations b ON b.run_id=r.id WHERE r.id=$1::uuid`, f.lease.RunID).Scan(&runStatus, &budgetStatus); err != nil {
			t.Fatal(err)
		}
		if runStatus != "TERMINATED" || budgetStatus != "UNKNOWN" {
			t.Fatalf("pause disposition=(%s,%s), want (TERMINATED,UNKNOWN)", runStatus, budgetStatus)
		}
	})

	t.Run("timeout_kills_and_stale_result_is_denied_over_http", func(t *testing.T) {
		f := supervisorRequireFixture(t)
		runner := supervisorNewProcessRunner(t, []string{"--ignore-term"}, f.evidence)
		sink := supervisorNewHTTPSink(t, f)
		config := supervisorTestConfig(f.workRoot)
		// Process spawn, PG registration, and fixture readiness have their own
		// bounded startup window; keep the actual execution deadline separate.
		config.MaxExecution = 5 * time.Second
		config.StartTimeout = 3 * time.Second
		config.TermGrace = 30 * time.Millisecond
		config.KillWait = time.Second
		sup, err := supervisor.New(f.db.Pool, f.clock, runner, sink, f.holds, config)
		if err != nil {
			t.Fatal(err)
		}
		errCh := make(chan error, 1)
		go func() { errCh <- sup.Run(f.ctx, f.lease, f.envelope) }()
		select {
		case <-runner.activated:
		case err := <-errCh:
			t.Fatalf("worker exited before activation: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("worker did not activate")
		}
		select {
		case err := <-errCh:
			if !errors.Is(err, supervisor.ErrTimeout) {
				t.Fatalf("timeout result=%v, want ErrTimeout", err)
			}
			if errors.Is(err, supervisor.ErrKillWaitExceeded) {
				t.Fatalf("deadline exceeded reap wait: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("deadline did not kill and reap the worker")
		}
		select {
		case <-runner.waited:
		default:
			t.Fatal("timeout returned before process Wait/reap completed")
		}
		status, response := supervisorSubmitSeed(f, sink)
		if status != http.StatusConflict || response.Error != "stale_result" {
			t.Fatalf("stale IPC result=(%d,%+v), want 409 stale_result", status, response)
		}
	})
}

func supervisorRequireFixture(t *testing.T) *supervisorFixture {
	t.Helper()
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatal(err)
	}
	var marker [8]byte
	if _, err := rand.Read(marker[:]); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%x", marker[:])
	orgID := "supervisor-" + suffix
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, orgID); err != nil {
		t.Fatal(err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 1, 19, 0, 0, 0, time.UTC))
	evidence := newSupervisorProcessEvidence(manual.Now)
	holds, err := processholds.NewStore(db.Pool, manual, processholds.Config{ResourceScope: "supervisor-test-resource",
		MaxActive: 10000, VerifierTimeout: 2 * time.Second}, evidence)
	if err != nil {
		t.Fatal(err)
	}
	repo := "owner/supervisor-" + suffix
	var taskID, jobID string
	if err := db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version)
		VALUES($1,$2,$3,'supervisor-test','AUTHORING','supervisor-v1') RETURNING id::text`, orgID, repo, "issue:"+suffix).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	envelope := contracts.BudgetEnvelope{MaxCostMicroUSD: 1000, MaxInputTokens: 1000, MaxOutputTokens: 100, MaxCalls: 2, PricingVersion: "supervisor-test-v1"}
	idSuffix := suffix[:12]
	jobID = "c1000000-0000-4000-8000-" + idSuffix
	opID := "c2000000-0000-4000-8000-" + idSuffix
	corrID := "c3000000-0000-4000-8000-" + idSuffix
	job := contracts.Job{Version: contracts.VersionV1, TaskID: taskID, JobID: jobID, Generation: 0, Snapshot: contracts.Snapshot{}, Attempt: 1,
		OperationID: opID, CorrelationID: corrID, Operation: "author", Envelope: envelope}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO jobs(id,task_id,logical_key,operation_type,generation,payload)
		VALUES($1::uuid,$2::uuid,$3,'author',0,$4::jsonb)`, jobID, taskID, "supervisor-job:"+suffix, payload); err != nil {
		t.Fatal(err)
	}
	lease, err := leases.Claim(ctx, db.Pool, manual, leases.ClaimRequest{TaskID: taskID, JobID: jobID, TTL: time.Hour,
		AgentType: "A", PromptHash: supervisorPromptHash, SupervisorIdentity: "supervisor-test-host", CredentialID: "opaque-supervisor-test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budget.Reserve(ctx, db.Pool, manual, lease, envelope); err != nil {
		t.Fatal(err)
	}
	workRoot := t.TempDir()
	evidence.noteReserved(lease.RunID)
	return &supervisorFixture{db: db, ctx: ctx, clock: manual, orgID: orgID, repo: repo, taskID: taskID, jobID: jobID,
		lease: lease, envelope: envelope, job: job, workRoot: workRoot, holds: holds, evidence: evidence}
}

func supervisorTestConfig(root string) supervisor.Config {
	return supervisor.Config{SupervisorIdentity: "supervisor-test-host", CredentialID: "opaque-supervisor-test", WorkspaceRoot: root,
		LeaseTTL: time.Hour, HeartbeatInterval: 50 * time.Millisecond, MaxExecution: 10 * time.Second,
		StartTimeout: time.Second, TermGrace: 40 * time.Millisecond, KillWait: time.Second}
}

type supervisorProcessRunner struct {
	path         string
	args         []string
	started      chan struct{}
	activated    chan struct{}
	waited       chan struct{}
	identityGate <-chan struct{}
	mu           sync.Mutex
	process      *supervisorOSProcess
	environment  []string
	evidence     *supervisorProcessEvidence
}

type supervisorStartErrorRunner struct{ evidence *supervisorProcessEvidence }

func (r supervisorStartErrorRunner) Start(_ context.Context, spec supervisor.ProcessSpec) (supervisor.Process, error) {
	r.evidence.noteStartAttempt(spec.RunID)
	return nil, errors.New("runner failed after ambiguous native start")
}

func supervisorNewProcessRunner(t *testing.T, args []string, evidence *supervisorProcessEvidence) *supervisorProcessRunner {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate integration source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
	path := filepath.Join(t.TempDir(), "fakeagent")
	command := exec.Command("go", "build", "-o", path, "./tests/testutil/fakeagent")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build fixture fakeagent: %v\n%s", err, output)
	}
	return &supervisorProcessRunner{path: path, args: args, started: make(chan struct{}), activated: make(chan struct{}), waited: make(chan struct{}), evidence: evidence}
}

func (r *supervisorProcessRunner) Start(ctx context.Context, spec supervisor.ProcessSpec) (supervisor.Process, error) {
	r.evidence.noteStartAttempt(spec.RunID)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	args := append([]string(nil), r.args...)
	cmd := exec.Command(r.path, args...)
	cmd.Dir = spec.WorkspaceDir
	cmd.Env = sortedEnvironment(spec.Environment)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		killErr := cmd.Process.Kill()
		waitErr := cmd.Wait()
		return nil, errors.Join(fmt.Errorf("inspect fixture process group: %w", err), killErr, waitErr)
	}
	startIdentity, startIdentityErr := supervisorNativeProcessStartIdentity(cmd.Process.Pid)
	process := &supervisorOSProcess{cmd: cmd, stdin: stdin, stdout: &stdout, pgid: pgid, startIdentity: startIdentity,
		waited: r.waited, activated: r.activated, ready: make(chan struct{}),
		identityGate: r.identityGate,
		ignoreTerm:   containsSupervisorArg(args, "--ignore-term"), stderrDone: make(chan struct{})}
	go process.captureStderr(stderrPipe)
	r.mu.Lock()
	r.process = process
	r.environment = append([]string(nil), cmd.Env...)
	r.mu.Unlock()
	if startIdentityErr == nil {
		r.evidence.noteProcess(spec.RunID, processholds.ProcessIdentity{PID: int64(process.PID()), PGID: int64(process.ProcessGroupID()), StartIdentity: startIdentity})
	}
	close(r.started)
	return process, startIdentityErr
}

type supervisorOSProcess struct {
	cmd           *exec.Cmd
	stdin         io.WriteCloser
	stdout        *bytes.Buffer
	pgid          int
	startIdentity string
	identityGate  <-chan struct{}
	waited        chan struct{}
	activated     chan struct{}
	ready         chan struct{}
	ignoreTerm    bool
	stderrDone    chan struct{}
	stderrMu      sync.Mutex
	stderrText    strings.Builder
	waitOnce      sync.Once
	writeMu       sync.Mutex
}

func (p *supervisorOSProcess) PID() int { return p.cmd.Process.Pid }

func (p *supervisorOSProcess) ProcessGroupID() int { return p.pgid }

func (p *supervisorOSProcess) StartIdentity() string {
	if p.identityGate != nil {
		<-p.identityGate
	}
	return p.startIdentity
}

func (p *supervisorOSProcess) Activate(ctx context.Context, result contracts.Result) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.writeMu.Lock()
	err := json.NewEncoder(p.stdin).Encode(result)
	p.writeMu.Unlock()
	if err == nil && p.ignoreTerm {
		select {
		case <-p.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err == nil {
		close(p.activated)
	}
	return err
}

func (p *supervisorOSProcess) release() error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return json.NewEncoder(p.stdin).Encode(struct {
		Release bool `json:"release"`
	}{Release: true})
}

func (r *supervisorProcessRunner) release() error {
	r.mu.Lock()
	process := r.process
	r.mu.Unlock()
	if process == nil {
		return errors.New("fixture process has not started")
	}
	return process.release()
}

func (r *supervisorProcessRunner) processGroupID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.process == nil {
		return 0
	}
	return r.process.ProcessGroupID()
}

func (p *supervisorOSProcess) Wait() (contracts.Result, error) {
	defer p.waitOnce.Do(func() { close(p.waited) })
	err := p.cmd.Wait()
	<-p.stderrDone
	closeErr := p.stdin.Close()
	if err == nil && closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
		err = fmt.Errorf("close fakeagent input: %w", closeErr)
	}
	if err != nil {
		p.stderrMu.Lock()
		stderr := p.stderrText.String()
		p.stderrMu.Unlock()
		return contracts.Result{}, fmt.Errorf("fakeagent process: %w: %s", err, strings.TrimSpace(stderr))
	}
	var result contracts.Result
	if err := json.Unmarshal(p.stdout.Bytes(), &result); err != nil {
		return contracts.Result{}, fmt.Errorf("decode fakeagent result: %w: %s", err, p.stdout.String())
	}
	return result, nil
}

func (p *supervisorOSProcess) captureStderr(reader io.ReadCloser) {
	defer close(p.stderrDone)
	defer func() {
		if err := reader.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			p.stderrMu.Lock()
			p.stderrText.WriteString("close fixture stderr: " + err.Error() + "\n")
			p.stderrMu.Unlock()
		}
	}()
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "__APRL_FAKEAGENT_READY__" {
			select {
			case <-p.ready:
			default:
				close(p.ready)
			}
		}
		p.stderrMu.Lock()
		p.stderrText.WriteString(line)
		p.stderrText.WriteByte('\n')
		p.stderrMu.Unlock()
	}
}

func containsSupervisorArg(arguments []string, target string) bool {
	for _, argument := range arguments {
		if argument == target {
			return true
		}
	}
	return false
}

func (p *supervisorOSProcess) SignalGroup(sig syscall.Signal) error {
	err := syscall.Kill(-p.ProcessGroupID(), sig)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

func supervisorNewHTTPSink(t *testing.T, f *supervisorFixture) *supervisorHTTPSink {
	t.Helper()
	service, err := results.New(f.db.Pool, f.clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	principal := results.Principal{TaskID: f.taskID, RunID: f.lease.RunID, Identity: "supervisor-test-host", CredentialID: "opaque-supervisor-test"}
	handler, err := httpapi.NewResultsHandler(service, supervisorAuthenticator{principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	return &supervisorHTTPSink{handler: handler, principal: principal}
}

type supervisorAuthenticator struct{ principal results.Principal }

func (a supervisorAuthenticator) AuthenticateSupervisor(_ context.Context, request *http.Request) (results.Principal, error) {
	if request.Header.Get("X-Supervisor-Test") != "valid" {
		return results.Principal{}, errors.New("missing test supervisor credential")
	}
	return a.principal, nil
}

type supervisorHTTPSink struct {
	handler   http.Handler
	principal results.Principal
}

type supervisorHTTPResponse struct {
	Accepted    bool   `json:"accepted"`
	OperationID string `json:"operation_id"`
	Error       string `json:"error"`
}

func (s *supervisorHTTPSink) Submit(ctx context.Context, _ results.Principal, result contracts.Result) (results.Accepted, error) {
	body, err := json.Marshal(result)
	if err != nil {
		return results.Accepted{}, err
	}
	request := httptest.NewRequest(http.MethodPost, "/internal/results", bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("X-Supervisor-Test", "valid")
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	var parsed supervisorHTTPResponse
	if err := json.NewDecoder(response.Body).Decode(&parsed); err != nil {
		return results.Accepted{}, err
	}
	if response.Code != http.StatusOK || !parsed.Accepted {
		return results.Accepted{}, fmt.Errorf("result endpoint status %d: %s", response.Code, parsed.Error)
	}
	return results.Accepted{OperationID: parsed.OperationID}, nil
}

func supervisorSubmitSeed(f *supervisorFixture, sink *supervisorHTTPSink) (int, supervisorHTTPResponse) {
	result := contracts.Result{Version: contracts.VersionV1, TaskID: f.taskID, JobID: f.jobID, RunID: f.lease.RunID,
		Generation: f.lease.Generation, LeaseToken: f.lease.Token, Snapshot: f.lease.Snapshot, Attempt: f.lease.Attempt,
		OperationID: f.job.OperationID, CorrelationID: f.job.CorrelationID, Status: "succeeded", Summary: "late result"}
	body, _ := json.Marshal(result)
	request := httptest.NewRequest(http.MethodPost, "/internal/results", bytes.NewReader(body))
	request.Header.Set("X-Supervisor-Test", "valid")
	response := httptest.NewRecorder()
	sink.handler.ServeHTTP(response, request)
	var parsed supervisorHTTPResponse
	if err := json.NewDecoder(response.Body).Decode(&parsed); err != nil {
		parsed.Error = err.Error()
	}
	return response.Code, parsed
}

func supervisorPause(f *supervisorFixture) error {
	return storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		_, locked, err := repos.LockOrgBudgetAndTask(ctx, f.orgID, f.taskID)
		if err != nil {
			return err
		}
		_, err = control.PauseLocked(ctx, repos, locked, f.clock, control.PauseRequest{Authority: control.Authority{
			ActorID: "supervisor-test-human", Repository: f.repo, Permission: control.PermissionMaintain,
			VerifiedAt: f.clock.Now()}, Reason: "supervisor integration test"})
		return err
	})
}

func supervisorEventually(t *testing.T, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("condition did not become true before deadline")
		}
	}
}

func sortedEnvironment(environment map[string]string) []string {
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, key+"="+environment[key])
	}
	return values
}

var _ supervisor.Runner = (*supervisorProcessRunner)(nil)
var _ supervisor.Process = (*supervisorOSProcess)(nil)
var _ supervisor.ResultSubmitter = (*supervisorHTTPSink)(nil)
