// Package processholds durably accounts for supervised process capacity. A hold
// is not permission to execute; it is a fail-closed resource reservation tied to
// one immutable task, job, run, lease nonce, workspace, and supervisor.
package processholds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// StateReserved means capacity is durably held before process start.
	StateReserved = "RESERVED"
	// StateStarted records the host-owned process-group identity.
	StateStarted = "STARTED"
	// StateUnknown means execution may exist and capacity remains charged.
	StateUnknown = "UNKNOWN"
	// StateReaped means trusted host evidence released the resource accounting.
	StateReaped = "REAPED"

	// ProofNeverStarted is trusted evidence that process launch was not attempted.
	ProofNeverStarted = "NEVER_STARTED"
	// ProofGroupDrained is trusted evidence that the exact owned group has exited.
	ProofGroupDrained = "GROUP_DRAINED"

	// ReasonStartAmbiguous records uncertain Runner.Start completion.
	ReasonStartAmbiguous = "START_AMBIGUOUS"
	// ReasonSupervisorLost records loss of the trusted supervisor observation.
	ReasonSupervisorLost = "SUPERVISOR_LOST"
	// ReasonRecovery records a hold requiring host recovery reconciliation.
	ReasonRecovery = "RECOVERY_REQUIRED"

	maxEvidenceBytes = 4096
)

var (
	// ErrInvalid indicates malformed process hold input or evidence.
	ErrInvalid = errors.New("invalid process hold")
	// ErrConflict indicates a durable process hold identity or transition conflict.
	ErrConflict = errors.New("process hold conflict")
	// ErrCapacity indicates the trusted resource scope has no available capacity.
	ErrCapacity = errors.New("process capacity exhausted")
	// ErrNotFound indicates the requested process hold does not exist.
	ErrNotFound = errors.New("process hold not found")
	// ErrUnresolved indicates the task already has an unreaped process hold.
	ErrUnresolved = errors.New("task has unresolved process hold")
)

// Config is trusted host policy. Scope, capacity and a bounded verification
// deadline are mandatory; there is no resource-scope or capacity default.
type Config struct {
	ResourceScope   string
	MaxActive       int
	VerifierTimeout time.Duration
}

// Validate enforces explicit trusted scope and finite resource limits.
func (c Config) Validate() error {
	if !safeText(c.ResourceScope, 128) || c.MaxActive <= 0 || c.VerifierTimeout <= 0 || c.VerifierTimeout > 30*time.Second {
		return fmt.Errorf("resource scope, positive capacity, and verifier timeout <=30s required: %w", ErrInvalid)
	}
	return nil
}

// ProcessIdentity is the full host-owned process-group identity. PID or PGID
// alone is never sufficient because operating systems can reuse process IDs.
type ProcessIdentity struct {
	PID           int64  `json:"pid"`
	PGID          int64  `json:"pgid"`
	StartIdentity string `json:"start_identity"`
}

func (p ProcessIdentity) validate() error {
	if p.PID <= 0 || p.PGID <= 0 || !safeText(p.StartIdentity, 256) {
		return fmt.Errorf("complete positive process identity required: %w", ErrInvalid)
	}
	return nil
}

// Hold is durable resource accounting, not an execution capability.
type Hold struct {
	RunID         string           `json:"run_id"`
	TaskID        string           `json:"task_id"`
	JobID         string           `json:"job_id"`
	Generation    int64            `json:"generation"`
	ResourceScope string           `json:"resource_scope"`
	LeaseTokenSHA string           `json:"lease_token_sha256"`
	Workspace     string           `json:"workspace"`
	SupervisorID  string           `json:"supervisor_id"`
	State         string           `json:"state"`
	Process       *ProcessIdentity `json:"process,omitempty"`
	UnknownReason string           `json:"unknown_reason,omitempty"`
	ReapEvidence  *ReapEvidence    `json:"reap_evidence,omitempty"`
	Revision      int64            `json:"revision"`
	CreatedAt     time.Time        `json:"created_at"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

// ReapEvidence stores only bounded typed identity facts, with no free-form
// diagnostic field in which credentials or worker content could be persisted.
type ReapEvidence struct {
	Kind          string           `json:"kind"`
	RunID         string           `json:"run_id"`
	TaskID        string           `json:"task_id"`
	JobID         string           `json:"job_id"`
	Generation    int64            `json:"generation"`
	ResourceScope string           `json:"resource_scope"`
	LeaseTokenSHA string           `json:"lease_token_sha256"`
	Workspace     string           `json:"workspace"`
	SupervisorID  string           `json:"supervisor_id"`
	Process       *ProcessIdentity `json:"process,omitempty"`
	VerifiedAt    time.Time        `json:"verified_at"`
	VerifierID    string           `json:"verifier_id"`
}

// ProcessEvidenceVerifier supplies trusted host facts. It must inspect durable
// supervisor ownership/recovery state; worker JSON or PID existence alone is
// insufficient. The store bounds verifier calls with Config.VerifierTimeout.
type ProcessEvidenceVerifier interface {
	VerifyNeverStarted(context.Context, Hold) (ReapEvidence, error)
	VerifyGroupDrained(context.Context, Hold) (ReapEvidence, error)
}

// Store persists holds and requires trusted host evidence to release them.
type Store struct {
	pool     *pgxpool.Pool
	clock    clock.Clock
	config   Config
	verifier ProcessEvidenceVerifier
}

// NewStore creates a fail-closed process hold store with explicit host policy.
func NewStore(pool *pgxpool.Pool, clk clock.Clock, config Config, verifier ProcessEvidenceVerifier) (*Store, error) {
	if pool == nil || isNil(clk) || isNil(verifier) {
		return nil, fmt.Errorf("PostgreSQL, clock, and trusted process evidence verifier required: %w", ErrInvalid)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Store{pool: pool, clock: clk, config: config, verifier: verifier}, nil
}

// Reserve persists a hold before Runner.Start. It verifies the live durable
// lease under the task lock, then serializes per-scope capacity before insert.
// The lease nonce is stored only as a SHA-256 digest.
func (s *Store) Reserve(ctx context.Context, lease leases.Lease, workspace, supervisorID string) (Hold, error) {
	if !s.valid() || ctx == nil || !validUUID(lease.TaskID) || !validUUID(lease.JobID) || !validUUID(lease.RunID) || !validUUID(lease.Token) ||
		lease.Generation < 0 || !safeText(workspace, 1024) || !safeText(supervisorID, 256) {
		return Hold{}, ErrInvalid
	}
	var result Hold
	err := storage.WithUnitOfWork(ctx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		if _, err := repos.LockTask(ctx, lease.TaskID); err != nil {
			return err
		}
		if err := lockScope(ctx, repos, s.config.ResourceScope); err != nil {
			return err
		}
		if err := leases.ValidateLocked(ctx, repos, lease, s.clock); err != nil {
			return fmt.Errorf("validate durable lease before process reservation: %w", err)
		}
		var persistedSupervisor string
		var generation int64
		if err := repos.Queries().QueryRow(ctx, `SELECT supervisor_identity,generation FROM agent_runs
			WHERE id=$1::uuid AND task_id=$2::uuid AND job_id=$3::uuid`, lease.RunID, lease.TaskID, lease.JobID).
			Scan(&persistedSupervisor, &generation); err != nil {
			return fmt.Errorf("read process run ownership: %w", err)
		}
		if persistedSupervisor != supervisorID || generation != lease.Generation {
			return fmt.Errorf("process reservation identity differs from durable run: %w", ErrConflict)
		}
		var existing Hold
		err := scanHold(repos.Queries().QueryRow(ctx, holdSelect+` WHERE run_id=$1::uuid FOR UPDATE`, lease.RunID), &existing)
		if err == nil {
			candidate := reservation(lease, s.config.ResourceScope, workspace, supervisorID, existing.CreatedAt)
			if !sameReservation(existing, candidate) || existing.State != StateReserved {
				return fmt.Errorf("run already has a non-reservable process hold: %w", ErrConflict)
			}
			result = existing
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var active int
		if err := repos.Queries().QueryRow(ctx, `SELECT count(*) FROM process_holds WHERE resource_scope=$1 AND state <> 'REAPED'`, s.config.ResourceScope).Scan(&active); err != nil {
			return fmt.Errorf("count unresolved resource holds: %w", err)
		}
		if active >= s.config.MaxActive {
			return ErrCapacity
		}
		var durableLeaseExpiry time.Time
		if err := repos.Queries().QueryRow(ctx, `SELECT lease_expires_at FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid`, lease.JobID, lease.TaskID).Scan(&durableLeaseExpiry); err != nil {
			return fmt.Errorf("recheck durable lease expiry before process reservation: %w", err)
		}
		now := s.clock.Now().UTC()
		if !durableLeaseExpiry.After(now) {
			return leases.ErrStale
		}
		result = reservation(lease, s.config.ResourceScope, workspace, supervisorID, now)
		_, err = repos.Queries().Exec(ctx, `INSERT INTO process_holds
			(run_id,task_id,job_id,generation,resource_scope,lease_token_sha256,workspace,supervisor_identity,state,revision,created_at,updated_at)
			VALUES($1::uuid,$2::uuid,$3::uuid,$4,$5,$6,$7,$8,'RESERVED',1,$9,$9)`, result.RunID, result.TaskID, result.JobID,
			result.Generation, result.ResourceScope, result.LeaseTokenSHA, result.Workspace, result.SupervisorID, now)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.ConstraintName == "process_holds_one_unresolved_task" {
				return fmt.Errorf("task already owns an unresolved process hold: %w", ErrUnresolved)
			}
			return fmt.Errorf("persist process hold before runner start: %w", err)
		}
		return nil
	})
	return result, err
}

// Started records host Runner.Start identity. A late identity can fill an
// UNKNOWN hold but never changes that hold back to STARTED.
func (s *Store) Started(ctx context.Context, runID string, process ProcessIdentity) (Hold, error) {
	if err := process.validate(); err != nil {
		return Hold{}, err
	}
	return s.mutate(ctx, runID, func(h Hold) (Hold, bool, error) {
		switch h.State {
		case StateStarted:
			if !sameProcess(h.Process, &process) {
				return Hold{}, false, ErrConflict
			}
			return h, false, nil
		case StateUnknown:
			if h.Process != nil && !sameProcess(h.Process, &process) {
				return Hold{}, false, ErrConflict
			}
			h.Process = copyProcess(&process)
		default:
			return Hold{}, false, ErrConflict
		}
		h.Revision++
		h.UpdatedAt = s.clock.Now().UTC()
		return h, true, nil
	})
}

// BeginStart durably consumes the sole pre-start reservation before the host
// invokes Runner.Start. Only the caller that commits RESERVED→UNKNOWN receives
// permission to attempt launch; a retry after an ambiguous response conflicts.
// UNKNOWN remains charged and never becomes STARTED, even when Started later
// records the process identity.
func (s *Store) BeginStart(ctx context.Context, lease leases.Lease) (Hold, error) {
	if !validUUID(lease.TaskID) || !validUUID(lease.JobID) || !validUUID(lease.RunID) || !validUUID(lease.Token) {
		return Hold{}, ErrInvalid
	}
	return s.mutateWithLease(ctx, lease, func(h Hold) (Hold, bool, error) {
		if !sameLease(h, lease) || h.State != StateReserved {
			return Hold{}, false, ErrConflict
		}
		h.State, h.UnknownReason = StateUnknown, ReasonStartAmbiguous
		h.Revision++
		h.UpdatedAt = s.clock.Now().UTC()
		return h, true, nil
	})
}

// Unknown records an ambiguous start or lost supervisor observation. It never
// authorizes Runner.Start and never releases capacity, including after
// cancellation, expiry, or restart.
func (s *Store) Unknown(ctx context.Context, runID, reason string) (Hold, error) {
	if !validReason(reason) {
		return Hold{}, ErrInvalid
	}
	return s.mutate(ctx, runID, func(h Hold) (Hold, bool, error) {
		switch h.State {
		case StateReserved, StateStarted:
			h.State, h.UnknownReason = StateUnknown, reason
		case StateUnknown:
			if h.UnknownReason == reason {
				return h, false, nil
			}
			// A new trusted supervisor observation can refine why execution
			// remains uncertain. It changes only the audit reason; UNKNOWN
			// continues to consume capacity and never authorizes execution.
			h.UnknownReason = reason
		default:
			return Hold{}, false, ErrConflict
		}
		h.Revision++
		h.UpdatedAt = s.clock.Now().UTC()
		return h, true, nil
	})
}

// Reaped releases accounting only after trusted host verification of either
// never-started execution or the exact owned process group being drained.
func (s *Store) Reaped(ctx context.Context, runID string) (Hold, error) {
	return s.mutate(ctx, runID, func(h Hold) (Hold, bool, error) {
		if h.State == StateReaped {
			return h, false, nil
		}
		verifyCtx, cancel := context.WithTimeout(ctx, s.config.VerifierTimeout)
		defer cancel()
		var proof ReapEvidence
		var err error
		if h.Process == nil {
			proof, err = s.verifier.VerifyNeverStarted(verifyCtx, h)
		} else {
			proof, err = s.verifier.VerifyGroupDrained(verifyCtx, h)
		}
		if err != nil {
			return Hold{}, false, fmt.Errorf("verify process hold release: %w", err)
		}
		if err := verifyCtx.Err(); err != nil {
			return Hold{}, false, fmt.Errorf("process hold verifier exceeded its deadline: %w", err)
		}
		now := s.clock.Now().UTC()
		if err := validateEvidence(h, proof, now); err != nil {
			return Hold{}, false, err
		}
		h.State, h.ReapEvidence, h.UnknownReason = StateReaped, copyEvidence(&proof), ""
		h.Revision++
		h.UpdatedAt = now
		return h, true, nil
	})
}

// Get returns one durable hold by its stable RunID/hold ID.
func (s *Store) Get(ctx context.Context, runID string) (Hold, error) {
	if !s.valid() || ctx == nil || !validUUID(runID) {
		return Hold{}, ErrInvalid
	}
	var h Hold
	err := scanHold(s.pool.QueryRow(ctx, holdSelect+` WHERE run_id=$1::uuid`, runID), &h)
	return h, err
}

// ListUnresolved includes every hold still consuming capacity, regardless of
// mutable job status, lease expiry, task pause/cancellation, or process restart.
func (s *Store) ListUnresolved(ctx context.Context) ([]Hold, error) {
	if !s.valid() || ctx == nil {
		return nil, ErrInvalid
	}
	rows, err := s.pool.Query(ctx, holdSelect+` WHERE resource_scope=$1 AND state <> 'REAPED' ORDER BY created_at,run_id`, s.config.ResourceScope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var holds []Hold
	for rows.Next() {
		var h Hold
		if err := scanHold(rows, &h); err != nil {
			return nil, err
		}
		holds = append(holds, h)
	}
	return holds, rows.Err()
}

// HasUnresolvedTask is suitable for status displays. Lease admission must use
// HasUnresolvedTaskLocked under the task lock to prevent a race with Reserve.
func (s *Store) HasUnresolvedTask(ctx context.Context, taskID string) (bool, error) {
	if !s.valid() || ctx == nil || !validUUID(taskID) {
		return false, ErrInvalid
	}
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM process_holds WHERE task_id=$1::uuid AND state <> 'REAPED')`, taskID).Scan(&exists)
	return exists, err
}

// HasUnresolvedTaskLocked checks task fencing inside the caller transaction
// after LockTask. The caller remains responsible for commit/rollback.
func (s *Store) HasUnresolvedTaskLocked(ctx context.Context, repos *storage.Repositories, taskID string) (bool, error) {
	if !s.valid() || ctx == nil || repos == nil || !validUUID(taskID) {
		return false, ErrInvalid
	}
	var exists bool
	err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM process_holds WHERE task_id=$1::uuid AND state <> 'REAPED')`, taskID).Scan(&exists)
	return exists, err
}

func (s *Store) mutate(ctx context.Context, runID string, transition func(Hold) (Hold, bool, error)) (Hold, error) {
	return s.mutateInternal(ctx, runID, nil, transition)
}

func (s *Store) mutateWithLease(ctx context.Context, lease leases.Lease, transition func(Hold) (Hold, bool, error)) (Hold, error) {
	return s.mutateInternal(ctx, lease.RunID, &lease, transition)
}

func (s *Store) mutateInternal(ctx context.Context, runID string, lease *leases.Lease, transition func(Hold) (Hold, bool, error)) (Hold, error) {
	if !s.valid() || ctx == nil || !validUUID(runID) || transition == nil {
		return Hold{}, ErrInvalid
	}
	var seed Hold
	if err := scanHold(s.pool.QueryRow(ctx, holdSelect+` WHERE run_id=$1::uuid`, runID), &seed); err != nil {
		return Hold{}, err
	}
	var result Hold
	err := storage.WithUnitOfWork(ctx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		if _, err := repos.LockTask(ctx, seed.TaskID); err != nil {
			return err
		}
		if err := lockScope(ctx, repos, seed.ResourceScope); err != nil {
			return err
		}
		if lease != nil {
			if err := leases.ValidateLocked(ctx, repos, *lease, s.clock); err != nil {
				return fmt.Errorf("validate live lease before process launch intent: %w", err)
			}
		}
		var current Hold
		if err := scanHold(repos.Queries().QueryRow(ctx, holdSelect+` WHERE run_id=$1::uuid FOR UPDATE`, runID), &current); err != nil {
			return err
		}
		if current.TaskID != seed.TaskID || current.ResourceScope != seed.ResourceScope || current.RunID != runID || current.ResourceScope != s.config.ResourceScope {
			return ErrConflict
		}
		updated, changed, err := transition(current)
		if err != nil {
			return err
		}
		if !changed {
			result = current
			return nil
		}
		if updated.Revision != current.Revision+1 || updated.RunID != current.RunID || updated.TaskID != current.TaskID || updated.JobID != current.JobID ||
			updated.Generation != current.Generation || updated.ResourceScope != current.ResourceScope || updated.LeaseTokenSHA != current.LeaseTokenSHA ||
			updated.Workspace != current.Workspace || updated.SupervisorID != current.SupervisorID || !updated.CreatedAt.Equal(current.CreatedAt) {
			return ErrConflict
		}
		if err := validateHold(updated); err != nil {
			return err
		}
		var pid, pgid, startIdentity any
		if updated.Process != nil {
			pid, pgid, startIdentity = updated.Process.PID, updated.Process.PGID, updated.Process.StartIdentity
		}
		var evidence any
		if updated.ReapEvidence != nil {
			encoded, err := json.Marshal(updated.ReapEvidence)
			if err != nil || len(encoded) > maxEvidenceBytes {
				return ErrInvalid
			}
			evidence = string(encoded)
		}
		tag, err := repos.Queries().Exec(ctx, `UPDATE process_holds SET state=$2,process_id=$3,process_group_id=$4,process_start_identity=$5,
			unknown_reason=$6,reap_evidence=$7::jsonb,revision=$8,updated_at=$9 WHERE run_id=$1::uuid AND revision=$10`,
			updated.RunID, updated.State, pid, pgid, startIdentity, nullableText(updated.UnknownReason), evidence, updated.Revision, updated.UpdatedAt, current.Revision)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrConflict
		}
		result = updated
		return nil
	})
	return result, err
}

const holdSelect = `SELECT run_id::text,task_id::text,job_id::text,generation,resource_scope,lease_token_sha256,workspace,
	supervisor_identity,state,process_id,process_group_id,process_start_identity,unknown_reason,reap_evidence,revision,created_at,updated_at FROM process_holds`

type rowScanner interface{ Scan(...any) error }

func scanHold(row rowScanner, h *Hold) error {
	var pid, pgid *int64
	var startID, reason *string
	var raw []byte
	err := row.Scan(&h.RunID, &h.TaskID, &h.JobID, &h.Generation, &h.ResourceScope, &h.LeaseTokenSHA, &h.Workspace,
		&h.SupervisorID, &h.State, &pid, &pgid, &startID, &reason, &raw, &h.Revision, &h.CreatedAt, &h.UpdatedAt)
	if err != nil {
		return err
	}
	if pid != nil || pgid != nil || startID != nil {
		if pid == nil || pgid == nil || startID == nil {
			return ErrConflict
		}
		h.Process = &ProcessIdentity{PID: *pid, PGID: *pgid, StartIdentity: *startID}
	}
	if reason != nil {
		h.UnknownReason = *reason
	}
	if len(raw) > 0 {
		var proof ReapEvidence
		if err := json.Unmarshal(raw, &proof); err != nil {
			return ErrConflict
		}
		h.ReapEvidence = &proof
	}
	if err := validateHold(*h); err != nil {
		return fmt.Errorf("invalid persisted process hold: %w", err)
	}
	return nil
}

func lockScope(ctx context.Context, repos *storage.Repositories, scope string) error {
	_, err := repos.Queries().Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, scope)
	return err
}
func reservation(lease leases.Lease, scope, workspace, supervisorID string, now time.Time) Hold {
	sum := sha256.Sum256([]byte(lease.Token))
	return Hold{RunID: lease.RunID, TaskID: lease.TaskID, JobID: lease.JobID, Generation: lease.Generation, ResourceScope: scope,
		LeaseTokenSHA: hex.EncodeToString(sum[:]), Workspace: workspace, SupervisorID: supervisorID, State: StateReserved, Revision: 1,
		CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
}
func sameReservation(a, b Hold) bool {
	return a.RunID == b.RunID && a.TaskID == b.TaskID && a.JobID == b.JobID && a.Generation == b.Generation && a.ResourceScope == b.ResourceScope &&
		a.LeaseTokenSHA == b.LeaseTokenSHA && a.Workspace == b.Workspace && a.SupervisorID == b.SupervisorID
}
func sameLease(h Hold, lease leases.Lease) bool {
	sum := sha256.Sum256([]byte(lease.Token))
	return h.RunID == lease.RunID && h.TaskID == lease.TaskID && h.JobID == lease.JobID && h.Generation == lease.Generation &&
		h.LeaseTokenSHA == hex.EncodeToString(sum[:])
}
func validateEvidence(h Hold, e ReapEvidence, now time.Time) error {
	if e.RunID != h.RunID || e.TaskID != h.TaskID || e.JobID != h.JobID || e.Generation != h.Generation || e.ResourceScope != h.ResourceScope ||
		e.LeaseTokenSHA != h.LeaseTokenSHA || e.Workspace != h.Workspace || e.SupervisorID != h.SupervisorID || !safeText(e.VerifierID, 128) ||
		e.VerifiedAt.IsZero() || e.VerifiedAt.Before(h.UpdatedAt) || e.VerifiedAt.After(now) {
		return fmt.Errorf("host reaping evidence does not bind hold: %w", ErrConflict)
	}
	switch e.Kind {
	case ProofNeverStarted:
		if h.Process != nil || e.Process != nil {
			return fmt.Errorf("never-started proof cannot carry process identity: %w", ErrConflict)
		}
	case ProofGroupDrained:
		if h.Process == nil || e.Process == nil || !sameProcess(h.Process, e.Process) || e.Process.validate() != nil {
			return fmt.Errorf("drain proof does not bind full process identity: %w", ErrConflict)
		}
	default:
		return ErrInvalid
	}
	encoded, err := json.Marshal(e)
	if err != nil || len(encoded) > maxEvidenceBytes {
		return fmt.Errorf("host evidence exceeds bounded schema: %w", ErrInvalid)
	}
	return nil
}
func validateHold(h Hold) error {
	if !validUUID(h.RunID) || !validUUID(h.TaskID) || !validUUID(h.JobID) || h.Generation < 0 || !safeText(h.ResourceScope, 128) || len(h.LeaseTokenSHA) != 64 || !safeHex(h.LeaseTokenSHA) ||
		!safeText(h.Workspace, 1024) || !safeText(h.SupervisorID, 256) || h.Revision <= 0 || h.CreatedAt.IsZero() || h.UpdatedAt.IsZero() || h.UpdatedAt.Before(h.CreatedAt) {
		return ErrInvalid
	}
	switch h.State {
	case StateReserved:
		if h.Process != nil || h.UnknownReason != "" || h.ReapEvidence != nil {
			return ErrInvalid
		}
	case StateStarted:
		if h.Process == nil || h.Process.validate() != nil || h.UnknownReason != "" || h.ReapEvidence != nil {
			return ErrInvalid
		}
	case StateUnknown:
		if (h.Process != nil && h.Process.validate() != nil) || !validReason(h.UnknownReason) || h.ReapEvidence != nil {
			return ErrInvalid
		}
	case StateReaped:
		if h.ReapEvidence == nil || h.UnknownReason != "" {
			return ErrInvalid
		}
		if err := validateEvidence(h, *h.ReapEvidence, h.UpdatedAt); err != nil {
			return err
		}
		if h.ReapEvidence.Kind == ProofGroupDrained && (h.Process == nil || !sameProcess(h.Process, h.ReapEvidence.Process)) {
			return ErrInvalid
		}
		if h.ReapEvidence.Kind == ProofNeverStarted && h.Process != nil {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func safeText(s string, maxBytes int) bool {
	if s == "" || len(s) > maxBytes || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func safeHex(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
func validUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	for i, r := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return s != "00000000-0000-0000-0000-000000000000"
}
func validReason(s string) bool {
	return s == ReasonStartAmbiguous || s == ReasonSupervisorLost || s == ReasonRecovery
}
func copyProcess(p *ProcessIdentity) *ProcessIdentity {
	if p == nil {
		return nil
	}
	clone := *p
	return &clone
}
func sameProcess(a, b *ProcessIdentity) bool { return a != nil && b != nil && *a == *b }
func copyEvidence(e *ReapEvidence) *ReapEvidence {
	if e == nil {
		return nil
	}
	clone := *e
	clone.Process = copyProcess(e.Process)
	return &clone
}
func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}
func (s *Store) valid() bool {
	return s != nil && s.pool != nil && !isNil(s.clock) && !isNil(s.verifier) && s.config.Validate() == nil
}
