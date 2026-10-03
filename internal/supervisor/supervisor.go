// Package supervisor owns the lifetime of one already-admitted worker process.
// It does not claim jobs, create budgets, or grant repository credentials.
package supervisor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrInvalid reports invalid supervisor configuration or process metadata.
	ErrInvalid = errors.New("invalid supervisor request")
	// ErrTimeout reports an execution that exceeded its configured deadline.
	ErrTimeout = errors.New("worker execution deadline exceeded")
	// ErrWorkspaceOwnership reports a workspace that cannot be proven to belong to this run.
	ErrWorkspaceOwnership = errors.New("worker workspace ownership could not be verified")
	// ErrKillWaitExceeded reports that SIGKILL did not produce exit within the configured wait interval.
	ErrKillWaitExceeded = errors.New("worker process exceeded post-SIGKILL wait interval")
	// ErrReservationMissing reports an inference run without durable coverage.
	ErrReservationMissing = errors.New("inference run has no durable RESERVED budget reservation")
)

// Runner starts one process in a blocked state. It must not let the child perform
// work until Process.Activate succeeds after durable process registration.
type Runner interface {
	// Start returns an owned process blocked until Activate is called.
	Start(context.Context, ProcessSpec) (Process, error)
}

// Process is the live handle returned by the injected process or OCI adapter.
// The handle, rather than a persisted PID, is the authority to signal a group.
type Process interface {
	// PID returns the process ID of the owned process-group leader.
	PID() int
	// ProcessGroupID returns the process group the supervisor may signal.
	ProcessGroupID() int
	// StartIdentity is a trusted host/runtime start witness, not a PID-derived guess.
	StartIdentity() string
	// Activate releases the blocked worker with its lease-bound result fields.
	Activate(context.Context, contracts.Result) error
	// Wait reaps the process and returns its result envelope.
	Wait() (contracts.Result, error)
	// SignalGroup sends a signal only to this process's owned group. It must
	// reject stale/unverified group identities and never derive authority from
	// a persisted PID/PGID tuple.
	SignalGroup(syscall.Signal) error
}

// ProcessSpec contains only the task-scoped data needed to start an agent.
// Environment is an explicit safe allowlist and must not be merged with os.Environ.
type ProcessSpec struct {
	TaskID       string            // TaskID binds the process to its durable task.
	JobID        string            // JobID identifies the immutable queued work.
	RunID        string            // RunID identifies the claimed execution attempt.
	WorkspaceID  string            // WorkspaceID is the owned workspace marker identity.
	WorkspaceDir string            // WorkspaceDir is the isolated execution directory.
	Operation    string            // Operation is the trusted job operation type.
	Generation   int64             // Generation is the task fence captured by the lease.
	Deadline     time.Time         // Deadline is the immutable execution deadline.
	Environment  map[string]string // Environment is a safe explicit child environment allowlist.
}

// ResultSubmitter sends a child result through the authenticated host result boundary.
type ResultSubmitter interface {
	// Submit sends the result through the authenticated host result boundary.
	Submit(context.Context, results.Principal, contracts.Result) (results.Accepted, error)
}

// Config bounds process startup, execution, heartbeat, and termination.
type Config struct {
	SupervisorIdentity string        // SupervisorIdentity must match the durable run admission.
	CredentialID       string        // CredentialID is an opaque host credential reference.
	WorkspaceRoot      string        // WorkspaceRoot contains only marker-owned ephemeral workspaces.
	LeaseTTL           time.Duration // LeaseTTL bounds each renewed durable job lease.
	HeartbeatInterval  time.Duration // HeartbeatInterval must be shorter than LeaseTTL.
	MaxExecution       time.Duration // MaxExecution is the immutable process deadline from launch preparation.
	StartTimeout       time.Duration // StartTimeout bounds local runner startup while authority is locked.
	TermGrace          time.Duration // TermGrace is the bounded SIGTERM grace before SIGKILL.
	KillWait           time.Duration // KillWait is the interval before reasserting SIGKILL while reaping.
}

// Supervisor coordinates durable run ownership with a live injected process handle.
type Supervisor struct {
	pool    *pgxpool.Pool
	clock   clock.Clock
	runner  Runner
	results ResultSubmitter
	holds   *processholds.Store
	config  Config
	root    string
	mu      sync.Mutex
	active  map[string]activeExecution
}

type activeExecution struct {
	runID  string
	cancel context.CancelFunc
}

type workspaceMarker struct {
	TaskID      string `json:"task_id"`
	RunID       string `json:"run_id"`
	WorkspaceID string `json:"workspace_id"`
}

type runAdmission struct {
	job          contracts.Job
	identity     string
	credentialID string
}

type waitResult struct {
	result  contracts.Result
	err     error
	endedAt time.Time
}

func startWaiter(proc Process) chan waitResult {
	waitCh := make(chan waitResult, 1)
	go func() {
		result, err := proc.Wait()
		waitCh <- waitResult{result: result, err: err, endedAt: time.Now()}
	}()
	return waitCh
}

// New validates required host adapters and process bounds. A nil runner never
// selects a fixture or fabricated production implementation.
func New(pool *pgxpool.Pool, c clock.Clock, runner Runner, submitter ResultSubmitter, holds *processholds.Store, config Config) (*Supervisor, error) {
	if isNilDependency(pool) || isNilDependency(c) || isNilDependency(runner) || isNilDependency(submitter) || isNilDependency(holds) ||
		strings.TrimSpace(config.SupervisorIdentity) == "" || strings.TrimSpace(config.CredentialID) == "" ||
		strings.TrimSpace(config.WorkspaceRoot) == "" || config.LeaseTTL <= 0 || config.HeartbeatInterval <= 0 ||
		config.HeartbeatInterval >= config.LeaseTTL || config.MaxExecution <= 0 || config.StartTimeout <= 0 ||
		config.StartTimeout >= config.MaxExecution || config.TermGrace <= 0 || config.KillWait <= 0 {
		return nil, fmt.Errorf("construct supervisor: %w", ErrInvalid)
	}
	root, err := filepath.Abs(config.WorkspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve supervisor workspace root: %w", err)
	}
	return &Supervisor{pool: pool, clock: c, runner: runner, results: submitter, holds: holds, config: config, root: root,
		active: make(map[string]activeExecution)}, nil
}

func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Run starts, supervises, and reaps the process for one current durable lease.
// Inference work additionally requires a matching RESERVED typed budget envelope.
func (s *Supervisor) Run(ctx context.Context, lease leases.Lease, envelope contracts.BudgetEnvelope) (retErr error) {
	if s == nil || ctx == nil || !validLease(lease) {
		return fmt.Errorf("run supervised worker: %w", ErrInvalid)
	}
	runCtx, cancel := context.WithCancel(ctx)
	if err := s.registerActive(lease.TaskID, lease.RunID, cancel); err != nil {
		cancel()
		return err
	}
	defer func() {
		cancel()
		s.removeActive(lease.TaskID, lease.RunID)
	}()

	workspaceID, workspaceDir, workspaceInfo, err := s.createWorkspace(lease)
	if err != nil {
		return err
	}
	keepWorkspace := false
	holdReserved, holdResolved := false, false
	defer func() {
		if holdReserved && !holdResolved {
			keepWorkspace = true
		}
		if !keepWorkspace {
			retErr = errors.Join(retErr, s.removeWorkspace(lease, workspaceID, workspaceDir, workspaceInfo))
		}
	}()
	if _, err := s.holds.Reserve(runCtx, lease, workspaceDir, s.config.SupervisorIdentity); err != nil {
		// Reserve may have committed even if its acknowledgement was lost. Keep
		// the workspace whenever a matching unresolved durable hold exists; only
		// a confirmed absence (or an unrelated/reaped hold) permits cleanup.
		lookupCtx, lookupCancel := context.WithTimeout(context.Background(), s.config.StartTimeout)
		reserved, lookupErr := s.holds.Get(lookupCtx, lease.RunID)
		lookupCancel()
		if lookupErr == nil && reserved.Workspace == workspaceDir && reserved.State != processholds.StateReaped {
			holdReserved = true
			keepWorkspace = true
		} else if lookupErr != nil && !errors.Is(lookupErr, processholds.ErrNotFound) {
			keepWorkspace = true
		}
		return fmt.Errorf("reserve host process capacity before runner start: %w", err)
	}
	holdReserved = true
	if _, err := s.holds.BeginStart(runCtx, lease); err != nil {
		return fmt.Errorf("persist one-shot process launch intent: %w", err)
	}

	preparedAt := s.clock.Now().UTC()
	wallStartedAt := time.Now()
	deadline := preparedAt.Add(s.config.MaxExecution)
	var proc Process
	var admission runAdmission
	startAttempted := false
	var runnerStartErr error
	startCtx, startCancel := context.WithTimeout(runCtx, s.config.StartTimeout)
	err = storage.WithUnitOfWork(startCtx, s.pool, s.clock, func(txctx context.Context, repos *storage.Repositories) error {
		admitted, err := s.validateAdmissionLocked(txctx, repos, lease, envelope, true)
		if err != nil {
			return err
		}
		admission = admitted
		spec := processSpec(lease, admitted.job, workspaceID, workspaceDir, deadline)
		startAttempted = true
		proc, runnerStartErr = s.runner.Start(txctx, spec)
		if runnerStartErr != nil && !validProcess(proc) {
			return fmt.Errorf("start bounded worker process: %w", runnerStartErr)
		}
		if !validProcess(proc) {
			return fmt.Errorf("runner returned invalid owned process group: %w", ErrInvalid)
		}
		startedAt := s.clock.Now().UTC()
		tag, err := repos.Queries().Exec(txctx, `UPDATE agent_runs SET process_id=$2,process_group_id=$3,workspace_id=$4::uuid,
			process_started_at=$5,last_heartbeat_at=$5,execution_deadline_at=$6
			WHERE id=$1::uuid AND task_id=$7::uuid AND job_id=$8::uuid AND lease_token=$9::uuid
			AND generation=$10 AND execution_status='RUNNING' AND process_id IS NULL`,
			lease.RunID, int64(proc.PID()), int64(proc.ProcessGroupID()), workspaceID, startedAt, deadline,
			lease.TaskID, lease.JobID, lease.Token, lease.Generation)
		if err != nil {
			return fmt.Errorf("register worker process tuple: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return leases.ErrStale
		}
		return nil
	})
	startCancel()
	if err == nil && runnerStartErr != nil {
		err = fmt.Errorf("start bounded worker process: %w", runnerStartErr)
	}
	var waitCh chan waitResult
	if validProcess(proc) {
		waitCh = startWaiter(proc)
	}
	if err != nil {
		if validProcess(proc) {
			holdStartErr := s.recordProcessStarted(lease.RunID, proc)
			if holdStartErr != nil {
				holdStartErr = errors.Join(holdStartErr, s.markProcessHoldUnknown(lease.RunID, processholds.ReasonStartAmbiguous))
			}
			if killErr := s.stopAndReap(proc, waitCh); killErr != nil {
				keepWorkspace = true
				fenceErr := s.fenceUnknown(lease, "TERMINATED")
				return errors.Join(err, holdStartErr, s.markProcessHoldUnknown(lease.RunID, processholds.ReasonSupervisorLost), killErr, fenceErr)
			}
			reapErr := s.reapProcessHold(lease.RunID)
			holdResolved = reapErr == nil
			fenceErr := s.fenceUnknown(lease, "TERMINATED")
			return errors.Join(err, holdStartErr, reapErr, fenceErr)
		} else if !isNilDependency(proc) {
			keepWorkspace = true
			fenceErr := s.fenceUnknown(lease, "TERMINATED")
			holdErr := s.markProcessHoldUnknown(lease.RunID, processholds.ReasonStartAmbiguous)
			return errors.Join(err, ErrInvalid, holdErr, fenceErr)
		}
		if startAttempted {
			holdErr := s.markProcessHoldUnknown(lease.RunID, processholds.ReasonStartAmbiguous)
			return errors.Join(fmt.Errorf("admit and register worker process: %w", err), holdErr, s.fenceUnknown(lease, "TERMINATED"))
		}
		if err := s.confirmNoPersistedStartEvidence(lease); err != nil {
			keepWorkspace = true
			holdErr := s.markProcessHoldUnknown(lease.RunID, processholds.ReasonRecovery)
			return errors.Join(fmt.Errorf("admit and register worker process: %w", err), holdErr,
				s.fenceUnknown(lease, "TERMINATED"))
		}
		hold, reapErr := s.reapNeverStartedProcessHold(lease.RunID)
		if reapErr != nil {
			keepWorkspace = true
			holdErr := s.markProcessHoldUnknown(lease.RunID, processholds.ReasonSupervisorLost)
			return errors.Join(fmt.Errorf("admit and register worker process: %w", err), reapErr, holdErr,
				s.fenceUnknown(lease, "TERMINATED"))
		}
		holdResolved = true
		disposeErr := s.disposeNeverStarted(lease, hold)
		return errors.Join(fmt.Errorf("admit and register worker process: %w", err), disposeErr)
	}
	if err := s.recordProcessStarted(lease.RunID, proc); err != nil {
		holdErr := errors.Join(err, s.markProcessHoldUnknown(lease.RunID, processholds.ReasonStartAmbiguous))
		stopErr := s.stopAndReap(proc, waitCh)
		if stopErr == nil {
			reapErr := s.reapProcessHold(lease.RunID)
			holdResolved = reapErr == nil
			holdErr = errors.Join(holdErr, reapErr)
		} else {
			keepWorkspace = true
			holdErr = errors.Join(holdErr, s.markProcessHoldUnknown(lease.RunID, processholds.ReasonSupervisorLost))
		}
		return errors.Join(fmt.Errorf("persist trusted process start identity: %w", err), holdErr, stopErr, s.fenceUnknown(lease, "TERMINATED"))
	}

	seed := resultSeed(lease, admission.job)
	activateCtx, activateCancel := context.WithTimeout(runCtx, s.config.StartTimeout)
	err = s.activateCurrent(activateCtx, lease, proc, workspaceID, admission.job, envelope, seed)
	activateCancel()
	if err != nil {
		holdErr := s.markProcessHoldUnknown(lease.RunID, processholds.ReasonStartAmbiguous)
		stopErr := s.stopAndReap(proc, waitCh)
		if stopErr != nil {
			keepWorkspace = true
			holdErr = errors.Join(holdErr, s.markProcessHoldUnknown(lease.RunID, processholds.ReasonSupervisorLost))
		} else {
			reapErr := s.reapProcessHold(lease.RunID)
			holdResolved = reapErr == nil
			holdErr = errors.Join(holdErr, reapErr)
		}
		finalErr := s.fenceUnknown(lease, "TERMINATED")
		return errors.Join(fmt.Errorf("activate registered worker: %w", err), holdErr, stopErr, finalErr)
	}

	ticker := time.NewTicker(s.config.HeartbeatInterval)
	defer ticker.Stop()
	remaining := s.config.MaxExecution - time.Since(wallStartedAt)
	if remaining <= 0 {
		remaining = time.Nanosecond
	}
	timeout := time.NewTimer(remaining)
	defer timeout.Stop()
	activeLease := lease
	wallDeadline := wallStartedAt.Add(s.config.MaxExecution)
	for {
		select {
		case waited := <-waitCh:
			if drainErr := s.drainGroup(proc); drainErr != nil {
				holdErr := errors.Join(s.markProcessHoldUnknown(lease.RunID, processholds.ReasonSupervisorLost), drainErr)
				unknownErr := s.fenceUnknown(activeLease, "TERMINATED")
				keepWorkspace = true
				return errors.Join(fmt.Errorf("drain owned worker process group: %w", drainErr), holdErr, unknownErr)
			}
			if err := s.reapProcessHold(lease.RunID); err != nil {
				keepWorkspace = true
				holdErr := errors.Join(err, s.markProcessHoldUnknown(lease.RunID, processholds.ReasonSupervisorLost))
				unknownErr := s.fenceUnknown(activeLease, "TERMINATED")
				return errors.Join(fmt.Errorf("verify durable process group reaping: %w", err), holdErr, unknownErr)
			}
			holdResolved = true
			if waited.endedAt.After(wallDeadline) {
				fenceErr := s.fenceUnknown(activeLease, "TERMINATED")
				cleanupErr := s.removeWorkspace(lease, workspaceID, workspaceDir, workspaceInfo)
				if cleanupErr == nil {
					keepWorkspace = true
				}
				return errors.Join(ErrTimeout, fenceErr, cleanupErr)
			}
			if waited.err != nil {
				unknownErr := s.fenceUnknown(activeLease, "FAILED")
				return errors.Join(fmt.Errorf("worker process exited without a valid result: %w", waited.err), unknownErr)
			}
			if err := waited.result.Validate(); err != nil {
				unknownErr := s.fenceUnknown(activeLease, "FAILED")
				return errors.Join(fmt.Errorf("validate worker result: %w", err), unknownErr)
			}
			if !matchesResultSeed(waited.result, seed) {
				unknownErr := s.fenceUnknown(activeLease, "FAILED")
				return errors.Join(fmt.Errorf("worker result changed its admitted identity: %w", ErrInvalid), unknownErr)
			}
			if runCtx.Err() != nil {
				unknownErr := s.fenceUnknown(activeLease, "TERMINATED")
				return errors.Join(runCtx.Err(), unknownErr)
			}
			principal := results.Principal{TaskID: lease.TaskID, RunID: lease.RunID,
				Identity: admission.identity, CredentialID: admission.credentialID}
			if _, err := s.results.Submit(runCtx, principal, waited.result); err != nil {
				unknownErr := s.fenceUnknown(activeLease, "FAILED")
				return errors.Join(fmt.Errorf("submit worker result through host boundary: %w", err), unknownErr)
			}
			if err := s.removeWorkspace(lease, workspaceID, workspaceDir, workspaceInfo); err != nil {
				keepWorkspace = true
				return fmt.Errorf("remove completed worker workspace: %w", err)
			}
			keepWorkspace = true // already removed after marker verification
			return nil
		case <-runCtx.Done():
			resolved, stopErr, holdErr := s.stopAndResolveHold(lease.RunID, proc, waitCh)
			if resolved {
				holdResolved = true
			}
			fenceErr := s.fenceUnknown(activeLease, "TERMINATED")
			if stopErr == nil {
				if err := s.removeWorkspace(lease, workspaceID, workspaceDir, workspaceInfo); err != nil {
					stopErr = err
				} else {
					keepWorkspace = true
				}
			} else {
				keepWorkspace = true
			}
			return errors.Join(runCtx.Err(), holdErr, fenceErr, stopErr)
		case <-timeout.C:
			resolved, stopErr, holdErr := s.stopAndResolveHold(lease.RunID, proc, waitCh)
			if resolved {
				holdResolved = true
			}
			fenceErr := s.fenceUnknown(activeLease, "TERMINATED")
			if stopErr == nil {
				if err := s.removeWorkspace(lease, workspaceID, workspaceDir, workspaceInfo); err != nil {
					stopErr = err
				} else {
					keepWorkspace = true
				}
			} else {
				keepWorkspace = true
			}
			return errors.Join(ErrTimeout, holdErr, fenceErr, stopErr)
		case <-ticker.C:
			nextLease, err := leases.Heartbeat(runCtx, s.pool, s.clock, activeLease, s.config.LeaseTTL)
			if err == nil {
				err = s.recordHeartbeat(runCtx, nextLease, proc)
			}
			if err != nil {
				resolved, stopErr, holdErr := s.stopAndResolveHold(lease.RunID, proc, waitCh)
				if resolved {
					holdResolved = true
				}
				fenceErr := s.fenceUnknown(activeLease, "TERMINATED")
				if stopErr == nil {
					if cleanupErr := s.removeWorkspace(lease, workspaceID, workspaceDir, workspaceInfo); cleanupErr != nil {
						stopErr = cleanupErr
					} else {
						keepWorkspace = true
					}
				} else {
					keepWorkspace = true
				}
				return errors.Join(fmt.Errorf("renew worker ownership heartbeat: %w", err), holdErr, fenceErr, stopErr)
			}
			activeLease = nextLease
		}
	}
}

// CancelRun fences IPC for one immutable run and asks its live owner to stop.
// It never looks up or signals a PID loaded from Postgres.
func (s *Supervisor) CancelRun(taskID, runID string) bool {
	if s == nil || strings.TrimSpace(taskID) == "" || strings.TrimSpace(runID) == "" {
		return false
	}
	s.mu.Lock()
	active := s.active[taskID]
	s.mu.Unlock()
	if active.cancel == nil || active.runID != runID {
		return false
	}
	active.cancel()
	return true
}

func (s *Supervisor) registerActive(taskID, runID string, cancel context.CancelFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.active[taskID]; exists {
		return leases.ErrBusy
	}
	s.active[taskID] = activeExecution{runID: runID, cancel: cancel}
	return nil
}

func (s *Supervisor) removeActive(taskID, runID string) {
	s.mu.Lock()
	if active := s.active[taskID]; active.runID == runID {
		delete(s.active, taskID)
	}
	s.mu.Unlock()
}

func (s *Supervisor) validateAdmissionLocked(ctx context.Context, repos *storage.Repositories, lease leases.Lease, envelope contracts.BudgetEnvelope, requireUnstarted bool) (runAdmission, error) {
	var orgID string
	if err := repos.Queries().QueryRow(ctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, lease.TaskID).Scan(&orgID); err != nil {
		return runAdmission{}, err
	}
	orgBudget, locked, err := repos.LockOrgBudgetAndTask(ctx, orgID, lease.TaskID)
	if err != nil {
		return runAdmission{}, err
	}
	if orgBudget.Record().EmergencyMode {
		return runAdmission{}, budget.ErrEmergency
	}
	if err := leases.ValidateLocked(ctx, repos, lease, s.clock); err != nil {
		return runAdmission{}, err
	}
	var payload []byte
	var runStatus, identity, credentialID string
	var processID, processGroupID *int64
	var persistedWorkspace *string
	err = repos.Queries().QueryRow(ctx, `SELECT j.payload,r.execution_status,r.supervisor_identity,r.supervisor_credential_id,
		r.process_id,r.process_group_id,r.workspace_id::text
		FROM jobs j JOIN agent_runs r ON r.job_id=j.id AND r.task_id=j.task_id
		WHERE j.id=$1::uuid AND j.task_id=$2::uuid AND r.id=$3::uuid AND r.lease_token=$4::uuid
		AND r.generation=$5 AND r.execution_status='RUNNING' FOR UPDATE OF j,r`,
		lease.JobID, lease.TaskID, lease.RunID, lease.Token, lease.Generation).Scan(&payload, &runStatus, &identity, &credentialID,
		&processID, &processGroupID, &persistedWorkspace)
	if errors.Is(err, pgx.ErrNoRows) {
		return runAdmission{}, leases.ErrStale
	}
	if err != nil {
		return runAdmission{}, fmt.Errorf("load durable process admission: %w", err)
	}
	if runStatus != "RUNNING" || identity != s.config.SupervisorIdentity || credentialID != s.config.CredentialID {
		return runAdmission{}, leases.ErrStale
	}
	if requireUnstarted && (processID != nil || processGroupID != nil || persistedWorkspace != nil) {
		return runAdmission{}, fmt.Errorf("durable run already has a process tuple: %w", ErrWorkspaceOwnership)
	}
	job, err := contracts.DecodeJob(payload)
	if err != nil {
		return runAdmission{}, fmt.Errorf("decode process job: %w", err)
	}
	if job.TaskID != lease.TaskID || job.JobID != lease.JobID || job.Generation != lease.Generation ||
		job.Attempt != lease.Attempt || job.Snapshot != lease.Snapshot || job.LeaseToken != "" || job.RunID != "" ||
		lease.InferenceRequired != leases.RequiresInference(job.Operation) {
		return runAdmission{}, leases.ErrStale
	}
	if leases.RequiresInference(job.Operation) {
		if err := envelope.Validate(); err != nil {
			return runAdmission{}, fmt.Errorf("validate supervisor budget envelope: %w", err)
		}
		if job.Envelope != (contracts.BudgetEnvelope{}) && job.Envelope != envelope {
			return runAdmission{}, budget.ErrReservationConflict
		}
		var raw []byte
		var reservationStatus string
		err := repos.Queries().QueryRow(ctx, `SELECT admission_envelope,status FROM budget_reservations WHERE task_id=$1::uuid AND run_id=$2::uuid`,
			lease.TaskID, lease.RunID).Scan(&raw, &reservationStatus)
		if errors.Is(err, pgx.ErrNoRows) {
			return runAdmission{}, ErrReservationMissing
		}
		if err != nil {
			return runAdmission{}, err
		}
		var reserved contracts.BudgetEnvelope
		if err := json.Unmarshal(raw, &reserved); err != nil || reserved != envelope || reservationStatus != "RESERVED" {
			return runAdmission{}, budget.ErrReservationConflict
		}
	} else if envelope != (contracts.BudgetEnvelope{}) || job.Envelope != (contracts.BudgetEnvelope{}) {
		return runAdmission{}, fmt.Errorf("non-inference process received a budget envelope: %w", ErrInvalid)
	}
	if err := repos.RequireTaskGeneration(ctx, locked, lease.Generation); err != nil {
		return runAdmission{}, err
	}
	return runAdmission{job: job, identity: identity, credentialID: credentialID}, nil
}

func (s *Supervisor) activateCurrent(ctx context.Context, lease leases.Lease, proc Process, expectedWorkspaceID string,
	expectedJob contracts.Job, envelope contracts.BudgetEnvelope, seed contracts.Result) error {
	return storage.WithUnitOfWork(ctx, s.pool, s.clock, func(txctx context.Context, repos *storage.Repositories) error {
		var orgID string
		if err := repos.Queries().QueryRow(txctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, lease.TaskID).Scan(&orgID); err != nil {
			return err
		}
		orgBudget, locked, err := repos.LockOrgBudgetAndTask(txctx, orgID, lease.TaskID)
		if err != nil {
			return err
		}
		if orgBudget.Record().EmergencyMode {
			return budget.ErrEmergency
		}
		if err := leases.ValidateLocked(txctx, repos, lease, s.clock); err != nil {
			return err
		}
		var rawJob []byte
		if err := repos.Queries().QueryRow(txctx, `SELECT payload FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`,
			lease.JobID, lease.TaskID).Scan(&rawJob); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return leases.ErrStale
			}
			return err
		}
		currentJob, err := contracts.DecodeJob(rawJob)
		if err != nil || currentJob != expectedJob || currentJob.TaskID != lease.TaskID || currentJob.JobID != lease.JobID ||
			currentJob.Generation != lease.Generation || currentJob.Attempt != lease.Attempt || currentJob.Snapshot != lease.Snapshot {
			return leases.ErrStale
		}
		if lease.InferenceRequired {
			if err := envelope.Validate(); err != nil ||
				(expectedJob.Envelope != (contracts.BudgetEnvelope{}) && expectedJob.Envelope != envelope) ||
				!leases.RequiresInference(expectedJob.Operation) {
				return budget.ErrReservationConflict
			}
			var rawEnvelope []byte
			var reservationStatus string
			err := repos.Queries().QueryRow(txctx, `SELECT admission_envelope,status FROM budget_reservations
				WHERE task_id=$1::uuid AND run_id=$2::uuid FOR UPDATE`, lease.TaskID, lease.RunID).
				Scan(&rawEnvelope, &reservationStatus)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrReservationMissing
			}
			if err != nil {
				return err
			}
			var reserved contracts.BudgetEnvelope
			if json.Unmarshal(rawEnvelope, &reserved) != nil || reserved != envelope || reservationStatus != "RESERVED" {
				return budget.ErrReservationConflict
			}
		} else if envelope != (contracts.BudgetEnvelope{}) || expectedJob.Envelope != (contracts.BudgetEnvelope{}) ||
			leases.RequiresInference(expectedJob.Operation) {
			return budget.ErrReservationConflict
		}
		var processID, groupID, generation int64
		var workspaceID string
		var identity, credentialID string
		if err := repos.Queries().QueryRow(txctx, `SELECT process_id,process_group_id,workspace_id::text,generation,
			supervisor_identity,supervisor_credential_id FROM agent_runs
			WHERE id=$1::uuid AND task_id=$2::uuid AND lease_token=$3::uuid AND generation=$4
			AND execution_status='RUNNING' FOR UPDATE`,
			lease.RunID, lease.TaskID, lease.Token, lease.Generation).Scan(&processID, &groupID, &workspaceID, &generation, &identity, &credentialID); err != nil {
			return err
		}
		if processID != int64(proc.PID()) || groupID != int64(proc.ProcessGroupID()) ||
			workspaceID != expectedWorkspaceID || generation != lease.Generation ||
			identity != s.config.SupervisorIdentity || credentialID != s.config.CredentialID {
			return ErrWorkspaceOwnership
		}
		if err := repos.RequireTaskGeneration(txctx, locked, lease.Generation); err != nil {
			return err
		}
		if err := proc.Activate(txctx, seed); err != nil {
			return fmt.Errorf("release registered worker process: %w", err)
		}
		return nil
	})
}

func (s *Supervisor) recordHeartbeat(ctx context.Context, lease leases.Lease, proc Process) error {
	return storage.WithUnitOfWork(ctx, s.pool, s.clock, func(txctx context.Context, repos *storage.Repositories) error {
		var orgID string
		if err := repos.Queries().QueryRow(txctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, lease.TaskID).Scan(&orgID); err != nil {
			return err
		}
		_, _, err := repos.LockOrgBudgetAndTask(txctx, orgID, lease.TaskID)
		if err != nil {
			return err
		}
		if err := leases.ValidateLocked(txctx, repos, lease, s.clock); err != nil {
			return err
		}
		now := s.clock.Now().UTC()
		tag, err := repos.Queries().Exec(txctx, `UPDATE agent_runs SET last_heartbeat_at=GREATEST(last_heartbeat_at,$2)
			WHERE id=$1::uuid AND task_id=$3::uuid AND lease_token=$4::uuid AND execution_status='RUNNING'
			AND process_id=$5 AND process_group_id=$6`, lease.RunID, now, lease.TaskID, lease.Token, int64(proc.PID()), int64(proc.ProcessGroupID()))
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return leases.ErrStale
		}
		return nil
	})
}

func (s *Supervisor) fenceUnknown(lease leases.Lease, completion string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completeErr := leases.Complete(ctx, s.pool, s.clock, lease, completion)
	if errors.Is(completeErr, leases.ErrStale) {
		completeErr = nil
	}
	unknownErr := s.markUnknown(ctx, lease)
	return errors.Join(completeErr, unknownErr)
}

func (s *Supervisor) recordProcessStarted(runID string, proc Process) error {
	if !validProcess(proc) || strings.TrimSpace(proc.StartIdentity()) == "" {
		return fmt.Errorf("trusted process start identity missing: %w", ErrInvalid)
	}
	nativeStart, err := processStartIdentity(proc.PID())
	if err != nil || nativeStart != proc.StartIdentity() {
		return errors.Join(fmt.Errorf("runner process start identity differs from host evidence: %w", ErrInvalid), err)
	}
	pgid, err := syscall.Getpgid(proc.PID())
	if err != nil || pgid != proc.ProcessGroupID() {
		return errors.Join(fmt.Errorf("runner process group differs from host evidence: %w", ErrInvalid), err)
	}
	identity := processholds.ProcessIdentity{PID: int64(proc.PID()), PGID: int64(proc.ProcessGroupID()), StartIdentity: proc.StartIdentity()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = s.holds.Started(ctx, runID, identity)
	return err
}

func (s *Supervisor) markProcessHoldUnknown(runID, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hold, err := s.holds.Get(ctx, runID)
	if err != nil {
		return err
	}
	if hold.State == processholds.StateReaped {
		return nil
	}
	_, err = s.holds.Unknown(ctx, runID, reason)
	return err
}

func (s *Supervisor) reapProcessHold(runID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.holds.Reaped(ctx, runID)
	return err
}

func (s *Supervisor) reapNeverStartedProcessHold(runID string) (processholds.Hold, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hold, err := s.holds.Reaped(ctx, runID)
	if err != nil {
		return processholds.Hold{}, err
	}
	if hold.ReapEvidence == nil || hold.ReapEvidence.Kind != processholds.ProofNeverStarted || hold.Process != nil {
		return processholds.Hold{}, fmt.Errorf("process hold lacks trusted never-started proof: %w", processholds.ErrInvalid)
	}
	return hold, nil
}

// confirmNoPersistedStartEvidence serializes the last durable pre-start check
// with control and result writes. A process tuple, usage charge, result receipt,
// or mutation intent prevents releasing capacity as never-started.
func (s *Supervisor) confirmNoPersistedStartEvidence(lease leases.Lease) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return storage.WithUnitOfWork(ctx, s.pool, s.clock, func(txctx context.Context, repos *storage.Repositories) error {
		var orgID string
		if err := repos.Queries().QueryRow(txctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, lease.TaskID).Scan(&orgID); err != nil {
			return err
		}
		if _, _, err := repos.LockOrgBudgetAndTask(txctx, orgID, lease.TaskID); err != nil {
			return err
		}
		var generation, attempt int64
		var identity, credentialID string
		var processID, processGroupID *int64
		var workspaceID *string
		var evidence bool
		err := repos.Queries().QueryRow(txctx, `SELECT r.generation,r.attempt_number,r.supervisor_identity,r.supervisor_credential_id,
			r.process_id,r.process_group_id,r.workspace_id::text,
			(r.process_id IS NOT NULL OR r.process_group_id IS NOT NULL OR r.workspace_id IS NOT NULL
			 OR EXISTS(SELECT 1 FROM cost_entries ce JOIN budget_reservations b ON b.id=ce.reservation_id WHERE b.run_id=r.id)
			 OR EXISTS(SELECT 1 FROM worker_result_receipts w WHERE w.run_id=r.id)
			 OR EXISTS(SELECT 1 FROM github_operations op WHERE op.request->>'run_id'=r.id::text))
			FROM agent_runs r WHERE r.id=$1::uuid AND r.task_id=$2::uuid AND r.job_id=$3::uuid
			AND r.lease_token=$4::uuid FOR UPDATE`, lease.RunID, lease.TaskID, lease.JobID, lease.Token).
			Scan(&generation, &attempt, &identity, &credentialID, &processID, &processGroupID, &workspaceID, &evidence)
		if errors.Is(err, pgx.ErrNoRows) {
			return leases.ErrStale
		}
		if err != nil {
			return err
		}
		if generation != lease.Generation || attempt != int64(lease.RunAttempt) || identity != s.config.SupervisorIdentity ||
			credentialID != s.config.CredentialID {
			return leases.ErrStale
		}
		if evidence || processID != nil || processGroupID != nil || workspaceID != nil {
			return ErrWorkspaceOwnership
		}
		return nil
	})
}

// disposeNeverStarted fences the exact run after ProofNeverStarted, releases
// only genuinely unused budget, and republishes eligible work using the same
// one-millisecond due-time policy as queue-side unstarted-admission cleanup.
func (s *Supervisor) disposeNeverStarted(lease leases.Lease, proof processholds.Hold) error {
	if proof.RunID != lease.RunID || proof.TaskID != lease.TaskID || proof.JobID != lease.JobID ||
		proof.Generation != lease.Generation || proof.ReapEvidence == nil || proof.ReapEvidence.Kind != processholds.ProofNeverStarted ||
		proof.Process != nil {
		return fmt.Errorf("never-started hold proof does not match the execution lease: %w", processholds.ErrInvalid)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return storage.WithUnitOfWork(ctx, s.pool, s.clock, func(txctx context.Context, repos *storage.Repositories) error {
		var orgID string
		if err := repos.Queries().QueryRow(txctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, lease.TaskID).Scan(&orgID); err != nil {
			return err
		}
		_, locked, err := repos.LockOrgBudgetAndTask(txctx, orgID, lease.TaskID)
		if err != nil {
			return err
		}
		var jobStatus, jobToken string
		if err := repos.Queries().QueryRow(txctx, `SELECT status,COALESCE(lease_token::text,'') FROM jobs
			WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`, lease.JobID, lease.TaskID).Scan(&jobStatus, &jobToken); err != nil {
			return err
		}
		var runStatus string
		var generation, attempt int64
		var identity, credentialID string
		var processID, processGroupID *int64
		var workspaceID *string
		var evidence bool
		err = repos.Queries().QueryRow(txctx, `SELECT r.execution_status,r.generation,r.attempt_number,r.supervisor_identity,r.supervisor_credential_id,
			r.process_id,r.process_group_id,r.workspace_id::text,
			(r.process_id IS NOT NULL OR r.process_group_id IS NOT NULL OR r.workspace_id IS NOT NULL
			 OR EXISTS(SELECT 1 FROM cost_entries ce JOIN budget_reservations b ON b.id=ce.reservation_id WHERE b.run_id=r.id)
			 OR EXISTS(SELECT 1 FROM worker_result_receipts w WHERE w.run_id=r.id)
			 OR EXISTS(SELECT 1 FROM github_operations op WHERE op.request->>'run_id'=r.id::text))
			FROM agent_runs r WHERE r.id=$1::uuid AND r.task_id=$2::uuid AND r.job_id=$3::uuid
			AND r.lease_token=$4::uuid FOR UPDATE OF r`, lease.RunID, lease.TaskID, lease.JobID, lease.Token).
			Scan(&runStatus, &generation, &attempt, &identity, &credentialID, &processID, &processGroupID, &workspaceID, &evidence)
		if errors.Is(err, pgx.ErrNoRows) {
			return leases.ErrStale
		}
		if err != nil {
			return err
		}
		if generation != lease.Generation || attempt != int64(lease.RunAttempt) || identity != s.config.SupervisorIdentity ||
			credentialID != s.config.CredentialID {
			return leases.ErrStale
		}
		if evidence || processID != nil || processGroupID != nil || workspaceID != nil {
			return fmt.Errorf("durable execution evidence conflicts with never-started proof: %w", ErrWorkspaceOwnership)
		}
		now := s.clock.Now().UTC()
		canRetry := runStatus == "RUNNING" && jobStatus == "LEASED" && jobToken == lease.Token &&
			locked.Record().Generation == lease.Generation && locked.Record().Snapshot == lease.Snapshot &&
			!deniedTaskState(locked.Record().State)
		if canRetry {
			// This is a proven pre-start denial, so no work or provider usage
			// occurred. Keep the logical job retryable using queue's normal
			// deferred-dispatch path, then settle its unused reservation.
			err := leases.RetryLocked(txctx, repos, lease, s.clock, now.Add(time.Millisecond), "TERMINATED")
			if err == nil {
				return settleUnstartedReservation(txctx, repos, s, lease)
			}
			if !errors.Is(err, leases.ErrStale) {
				return err
			}
		}
		if runStatus == "RUNNING" {
			tag, err := repos.Queries().Exec(txctx, `UPDATE agent_runs SET execution_status='TERMINATED',finished_at=$2
				WHERE id=$1::uuid AND task_id=$3::uuid AND job_id=$4::uuid AND lease_token=$5::uuid
				AND generation=$6 AND attempt_number=$7 AND execution_status='RUNNING'
				AND process_id IS NULL AND process_group_id IS NULL AND workspace_id IS NULL`,
				lease.RunID, now, lease.TaskID, lease.JobID, lease.Token, lease.Generation, lease.RunAttempt)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return leases.ErrStale
			}
		}
		if jobStatus == "LEASED" && jobToken == lease.Token {
			if _, err := repos.Queries().Exec(txctx, `UPDATE jobs SET status='CANCELLED',lease_token=NULL,lease_expires_at=NULL
				WHERE id=$1::uuid AND task_id=$2::uuid AND status='LEASED' AND lease_token=$3::uuid`, lease.JobID, lease.TaskID, lease.Token); err != nil {
				return err
			}
		}
		return settleUnstartedReservation(txctx, repos, s, lease)
	})
}

func settleUnstartedReservation(ctx context.Context, repos *storage.Repositories, s *Supervisor, lease leases.Lease) error {
	var reservationID string
	err := repos.Queries().QueryRow(ctx, `SELECT id::text FROM budget_reservations WHERE task_id=$1::uuid AND run_id=$2::uuid FOR UPDATE`,
		lease.TaskID, lease.RunID).Scan(&reservationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return budget.SettleReservationLocked(ctx, repos, s.clock,
		budget.SettlementRequest{ReservationID: reservationID, RunID: lease.RunID, FinalUsageKnown: true})
}

func deniedTaskState(state string) bool {
	return state == "PAUSED" || state == "ESCALATED" || state == "MERGED" || state == "CLOSED"
}

func (s *Supervisor) stopAndResolveHold(runID string, proc Process, waitCh <-chan waitResult) (resolved bool, stopErr, holdErr error) {
	holdErr = s.markProcessHoldUnknown(runID, processholds.ReasonSupervisorLost)
	stopErr = s.stopAndReap(proc, waitCh)
	if stopErr != nil {
		return false, stopErr, errors.Join(holdErr, s.markProcessHoldUnknown(runID, processholds.ReasonSupervisorLost))
	}
	reapErr := s.reapProcessHold(runID)
	return reapErr == nil, nil, errors.Join(holdErr, reapErr)
}

func (s *Supervisor) markUnknown(ctx context.Context, lease leases.Lease) error {
	var reservationID string
	err := s.pool.QueryRow(ctx, `SELECT id::text FROM budget_reservations WHERE task_id=$1::uuid AND run_id=$2::uuid`, lease.TaskID, lease.RunID).Scan(&reservationID)
	if errors.Is(err, pgx.ErrNoRows) {
		if lease.InferenceRequired {
			return ErrReservationMissing
		}
		return nil
	}
	if err != nil {
		return err
	}
	return budget.MarkUnknown(ctx, s.pool, s.clock, reservationID)
}

func (s *Supervisor) stopAndReap(proc Process, waitCh <-chan waitResult) error {
	if proc == nil {
		return nil
	}
	if !validProcess(proc) {
		return ErrInvalid
	}
	if waitCh == nil {
		waitResultCh := make(chan waitResult, 1)
		waitCh = waitResultCh
		go func() {
			result, err := proc.Wait()
			waitResultCh <- waitResult{result: result, err: err, endedAt: time.Now()}
		}()
	}
	termErr := signalOwnedGroup(proc, syscall.SIGTERM)
	grace := time.NewTimer(s.config.TermGrace)
	defer grace.Stop()
	select {
	case <-waitCh:
		return errors.Join(ignoreProcessDone(termErr), s.drainGroup(proc))
	case <-grace.C:
	}
	killErr := signalOwnedGroup(proc, syscall.SIGKILL)
	killWait := time.NewTimer(s.config.KillWait)
	defer killWait.Stop()
	select {
	case <-waitCh:
		return errors.Join(ignoreProcessDone(termErr), ignoreProcessDone(killErr), s.drainGroup(proc))
	case <-killWait.C:
		reassertErr := signalOwnedGroup(proc, syscall.SIGKILL)
		return errors.Join(ignoreProcessDone(termErr), ignoreProcessDone(killErr), ignoreProcessDone(reassertErr), ErrKillWaitExceeded)
	}
}

func (s *Supervisor) drainGroup(proc Process) error {
	termErr := ignoreProcessDone(signalOwnedGroup(proc, syscall.SIGTERM))
	if err := s.waitGroupGone(proc, s.config.TermGrace); err == nil {
		return termErr
	}
	killErr := ignoreProcessDone(signalOwnedGroup(proc, syscall.SIGKILL))
	if err := s.waitGroupGone(proc, s.config.KillWait); err != nil {
		return errors.Join(termErr, killErr, ErrKillWaitExceeded)
	}
	return errors.Join(termErr, killErr)
}

func (s *Supervisor) waitGroupGone(proc Process, limit time.Duration) error {
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := signalOwnedGroup(proc, 0); errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return nil
		} else if err != nil {
			return err
		}
		select {
		case <-deadline.C:
			return ErrKillWaitExceeded
		case <-ticker.C:
		}
	}
}

// signalOwnedGroup revalidates the native leader identity immediately before
// every signal. If the leader has exited while its group remains, the numeric
// PGID alone cannot prove ownership, so the supervisor fails closed and keeps
// the durable hold for host recovery. There is an unavoidable small race
// between this check and kill(2) on systems without an atomic process-group
// handle; runners must apply their own host identity checks in SignalGroup.
func signalOwnedGroup(proc Process, sig syscall.Signal) error {
	if !validProcess(proc) || strings.TrimSpace(proc.StartIdentity()) == "" {
		return ErrInvalid
	}
	identity, err := processStartIdentity(proc.PID())
	if err != nil {
		groupErr := proc.SignalGroup(0)
		if errors.Is(groupErr, syscall.ESRCH) || errors.Is(groupErr, os.ErrProcessDone) {
			return os.ErrProcessDone
		}
		return errors.Join(ErrWorkspaceOwnership, err, groupErr)
	}
	if identity != proc.StartIdentity() {
		return ErrWorkspaceOwnership
	}
	pgid, err := syscall.Getpgid(proc.PID())
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	if pgid != proc.ProcessGroupID() {
		return ErrWorkspaceOwnership
	}
	return proc.SignalGroup(sig)
}

func validProcess(proc Process) bool {
	return !isNilDependency(proc) && proc.PID() > 0 && proc.ProcessGroupID() > 0 && proc.PID() == proc.ProcessGroupID()
}

func ignoreProcessDone(err error) error {
	if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func (s *Supervisor) createWorkspace(lease leases.Lease) (string, string, os.FileInfo, error) {
	if err := os.MkdirAll(s.root, 0o700); err != nil {
		return "", "", nil, fmt.Errorf("create workspace root: %w", err)
	}
	root, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		return "", "", nil, fmt.Errorf("resolve workspace root: %w", err)
	}
	dir, err := os.MkdirTemp(root, "aprl-run-")
	if err != nil {
		return "", "", nil, fmt.Errorf("create owned run workspace: %w", err)
	}
	removeCreated := func(cause error) error {
		return errors.Join(cause, os.RemoveAll(dir))
	}
	id, err := randomUUID()
	if err != nil {
		return "", "", nil, removeCreated(err)
	}
	marker := workspaceMarker{TaskID: lease.TaskID, RunID: lease.RunID, WorkspaceID: id}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return "", "", nil, removeCreated(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, ".aprl-owned-workspace"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", "", nil, removeCreated(fmt.Errorf("create workspace ownership marker: %w", err))
	}
	_, writeErr := file.Write(encoded)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return "", "", nil, removeCreated(fmt.Errorf("write workspace ownership marker: %w", err))
	}
	info, err := os.Stat(dir)
	if err != nil {
		return "", "", nil, removeCreated(fmt.Errorf("inspect owned worker workspace: %w", err))
	}
	return id, dir, info, nil
}

func (s *Supervisor) removeWorkspace(lease leases.Lease, workspaceID, dir string, owner os.FileInfo) error {
	if workspaceID == "" || dir == "" || owner == nil {
		return ErrWorkspaceOwnership
	}
	root, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		return fmt.Errorf("resolve workspace cleanup root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return fmt.Errorf("resolve workspace for cleanup: %w", err)
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return ErrWorkspaceOwnership
	}
	dirInfo, err := os.Lstat(dir)
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(owner, dirInfo) {
		return ErrWorkspaceOwnership
	}
	info, err := os.Lstat(filepath.Join(resolved, ".aprl-owned-workspace"))
	if err != nil || !info.Mode().IsRegular() {
		return ErrWorkspaceOwnership
	}
	markerFile, err := os.Open(filepath.Join(resolved, ".aprl-owned-workspace"))
	if err != nil {
		return ErrWorkspaceOwnership
	}
	encoded, readErr := io.ReadAll(io.LimitReader(markerFile, 4097))
	closeErr := markerFile.Close()
	if readErr != nil || closeErr != nil || len(encoded) > 4096 {
		return ErrWorkspaceOwnership
	}
	var marker workspaceMarker
	if json.Unmarshal(encoded, &marker) != nil || marker.TaskID != lease.TaskID || marker.RunID != lease.RunID || marker.WorkspaceID != workspaceID {
		return ErrWorkspaceOwnership
	}
	if err := os.RemoveAll(resolved); err != nil {
		return fmt.Errorf("remove owned worker workspace: %w", err)
	}
	return nil
}

func processSpec(lease leases.Lease, job contracts.Job, workspaceID, workspaceDir string, deadline time.Time) ProcessSpec {
	return ProcessSpec{TaskID: lease.TaskID, JobID: lease.JobID, RunID: lease.RunID, WorkspaceID: workspaceID,
		WorkspaceDir: workspaceDir, Operation: job.Operation, Generation: lease.Generation, Deadline: deadline,
		Environment: map[string]string{"APRL_TASK_ID": lease.TaskID, "APRL_JOB_ID": lease.JobID,
			"APRL_RUN_ID": lease.RunID, "APRL_WORKSPACE_ID": workspaceID, "APRL_WORKSPACE": workspaceDir,
			"APRL_OPERATION": job.Operation}}
}

func resultSeed(lease leases.Lease, job contracts.Job) contracts.Result {
	return contracts.Result{Version: contracts.VersionV1, TaskID: lease.TaskID, JobID: lease.JobID, RunID: lease.RunID,
		Generation: lease.Generation, LeaseToken: lease.Token, Snapshot: lease.Snapshot, Attempt: lease.Attempt,
		OperationID: job.OperationID, CorrelationID: job.CorrelationID}
}

func matchesResultSeed(result, seed contracts.Result) bool {
	return result.Version == seed.Version && result.TaskID == seed.TaskID && result.JobID == seed.JobID && result.RunID == seed.RunID &&
		result.Generation == seed.Generation && result.LeaseToken == seed.LeaseToken && result.Snapshot == seed.Snapshot &&
		result.Attempt == seed.Attempt && result.OperationID == seed.OperationID && result.CorrelationID == seed.CorrelationID
}

func validLease(lease leases.Lease) bool {
	return lease.TaskID != "" && lease.JobID != "" && lease.RunID != "" && lease.Token != "" && lease.Generation >= 0 &&
		lease.Attempt > 0 && lease.RunAttempt > 0 && !lease.ExpiresAt.IsZero()
}

func randomUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate workspace identity: %w", err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}
