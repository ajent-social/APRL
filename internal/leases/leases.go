// Package leases implements the Postgres execution ownership fence. Redis
// delivery is deliberately absent: only durable task/job/run state grants work.
package leases

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrInvalid reports malformed lease input or an unsupported job operation.
	ErrInvalid = errors.New("invalid lease request")
	// ErrBusy reports that another job currently owns an unexpired task lease.
	ErrBusy = errors.New("task already has an active execution lease")
	// ErrNotDue reports durable work whose most recent retry is scheduled later.
	ErrNotDue = errors.New("execution retry is not due")
	// ErrStale reports a lease that no longer matches current durable authority.
	ErrStale = errors.New("execution lease is stale")
)

// ClaimRequest supplies the immutable, auditable identity of the execution
// being admitted. CredentialID is an opaque supervisor credential reference,
// never the credential secret itself.
type ClaimRequest struct {
	TaskID             string
	JobID              string
	TTL                time.Duration
	AgentType          string
	PromptHash         string
	SupervisorIdentity string
	CredentialID       string
}

// Lease is a durable admission tuple. Attempt is the logical job's attempt;
// RunAttempt is the per-job execution sequence stored in agent_runs.
type Lease struct {
	TaskID            string
	JobID             string
	RunID             string
	Token             string
	Generation        int64
	Snapshot          contracts.Snapshot
	Attempt           int32
	RunAttempt        int32
	ExpiresAt         time.Time
	InferenceRequired bool
}

// Claim creates a fresh unguessable lease and immutable agent_runs admission
// record atomically. The caller must complete budget admission before starting
// any process; this method itself performs no inference or remote operation.
func Claim(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, req ClaimRequest) (Lease, error) {
	if ctx == nil || pool == nil || c == nil || req.TTL <= 0 || !validID(req.TaskID) || !validID(req.JobID) ||
		!validAgentType(req.AgentType) || !validHash(req.PromptHash) ||
		strings.TrimSpace(req.SupervisorIdentity) == "" || strings.TrimSpace(req.CredentialID) == "" {
		return Lease{}, fmt.Errorf("claim lease: %w", ErrInvalid)
	}
	var admitted Lease
	err := storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		lockedTask, err := repos.LockTask(ctx, req.TaskID)
		if err != nil {
			return err
		}
		now := c.Now().UTC()
		task := lockedTask.Record()
		if deniedTaskState(task.State) {
			return fmt.Errorf("claim lease for task in %s: %w", task.State, ErrStale)
		}
		if err := fenceExpired(ctx, repos, req.TaskID, now); err != nil {
			return err
		}
		var active bool
		if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM jobs WHERE task_id=$1::uuid AND status='LEASED' AND lease_expires_at > $2
		)`, req.TaskID, now).Scan(&active); err != nil {
			return fmt.Errorf("check active task lease: %w", err)
		}
		if active {
			return ErrBusy
		}

		var jobGeneration int64
		var status, operationType string
		var payload []byte
		var headSHA, baseSHA *string
		err = repos.Queries().QueryRow(ctx, `SELECT generation, status, operation_type, payload, expected_head_sha, expected_base_sha
			FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`, req.JobID, req.TaskID).
			Scan(&jobGeneration, &status, &operationType, &payload, &headSHA, &baseSHA)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("claim job %s: %w", req.JobID, storage.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("lock claim job: %w", err)
		}
		if status != "PENDING" {
			return fmt.Errorf("job status %s: %w", status, ErrStale)
		}
		var delayed bool
		if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM outbox o WHERE o.job_id=$1::uuid AND o.kind='DISPATCH'
			AND o.next_attempt_at > $2 AND o.payload->>'retry_from_run_id'=(
				SELECT id::text FROM agent_runs WHERE job_id=$1::uuid ORDER BY attempt_number DESC LIMIT 1
			)
		)`, req.JobID, now).Scan(&delayed); err != nil {
			return fmt.Errorf("check durable retry deadline: %w", err)
		}
		if delayed {
			return ErrNotDue
		}
		job, err := contracts.DecodeJob(payload)
		if err != nil {
			return fmt.Errorf("decode durable job: %w", err)
		}
		mappedType, supported := AgentTypeForOperation(operationType)
		if job.LeaseToken != "" || job.RunID != "" || operationType != job.Operation || !supported || req.AgentType != mappedType {
			return fmt.Errorf("queued job operation or requested agent role is inconsistent: %w", ErrInvalid)
		}
		if job.TaskID != req.TaskID || job.JobID != req.JobID || job.Generation != jobGeneration ||
			job.Generation != task.Generation || !sameSnapshot(job.Snapshot, task.Snapshot) ||
			!sameOptional(headSHA, task.Snapshot.HeadSHA) || !sameOptional(baseSHA, task.Snapshot.BaseSHA) {
			return fmt.Errorf("job generation or snapshot differs from current task: %w", ErrStale)
		}
		if job.Attempt <= 0 {
			return fmt.Errorf("job attempt is invalid: %w", ErrInvalid)
		}
		if deniedTaskState(task.State) {
			return fmt.Errorf("task state %s: %w", task.State, ErrStale)
		}
		now = c.Now().UTC()
		if err := fenceExpired(ctx, repos, req.TaskID, now); err != nil {
			return err
		}

		var token, runID string
		if err := repos.Queries().QueryRow(ctx, `SELECT gen_random_uuid()::text, gen_random_uuid()::text`).Scan(&token, &runID); err != nil {
			return fmt.Errorf("generate lease identity: %w", err)
		}
		var runAttempt int32
		if err := repos.Queries().QueryRow(ctx, `SELECT COALESCE(MAX(attempt_number),0)+1 FROM agent_runs WHERE job_id=$1::uuid`, req.JobID).Scan(&runAttempt); err != nil {
			return fmt.Errorf("allocate run attempt: %w", err)
		}
		expires := now.Add(req.TTL)
		if _, err := repos.Queries().Exec(ctx, `INSERT INTO agent_runs
			(id,task_id,job_id,attempt_number,agent_type,codex_prompt_hash,generation,lease_token,
			expected_head_sha,expected_base_sha,expected_integration_sha,supervisor_identity,
			supervisor_credential_id,execution_status,started_at)
			VALUES($1::uuid,$2::uuid,$3::uuid,$4,$5,$6,$7,$8::uuid,$9,$10,$11,$12,$13,'RUNNING',$14)`,
			runID, req.TaskID, req.JobID, runAttempt, req.AgentType, req.PromptHash, task.Generation,
			token, nullable(task.Snapshot.HeadSHA), nullable(task.Snapshot.BaseSHA), nullable(task.Snapshot.IntegrationSHA),
			req.SupervisorIdentity, req.CredentialID, now); err != nil {
			return fmt.Errorf("record lease admission: %w", err)
		}
		if _, err := repos.Queries().Exec(ctx, `UPDATE jobs SET status='LEASED',lease_token=$2::uuid,lease_expires_at=$3
			WHERE id=$1::uuid`, req.JobID, token, expires); err != nil {
			return fmt.Errorf("persist job lease: %w", err)
		}
		admitted = Lease{TaskID: req.TaskID, JobID: req.JobID, RunID: runID, Token: token,
			Generation: task.Generation, Snapshot: task.Snapshot, Attempt: job.Attempt, RunAttempt: runAttempt, ExpiresAt: expires,
			InferenceRequired: RequiresInference(operationType)}
		return nil
	})
	if err != nil {
		return Lease{}, err
	}
	return admitted, nil
}

// Heartbeat extends an unexpired lease using its current durable fence.
func Heartbeat(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, lease Lease, ttl time.Duration) (Lease, error) {
	if ctx == nil || pool == nil || c == nil || ttl <= 0 {
		return Lease{}, fmt.Errorf("heartbeat lease: %w", ErrInvalid)
	}
	var updated Lease
	err := storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		now, err := validateLocked(ctx, repos, lease, c)
		if err != nil {
			return err
		}
		expires := now.Add(ttl)
		tag, err := repos.Queries().Exec(ctx, `UPDATE jobs SET lease_expires_at=$4
			WHERE id=$1::uuid AND task_id=$2::uuid AND lease_token=$3::uuid AND status='LEASED'`, lease.JobID, lease.TaskID, lease.Token, expires)
		if err != nil {
			return fmt.Errorf("renew execution lease: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return ErrStale
		}
		updated = lease
		updated.ExpiresAt = expires
		return nil
	})
	if err != nil {
		return Lease{}, err
	}
	return updated, nil
}

// Validate checks a lease in a short transaction. Mutating callers should use
// ValidateLocked in the same transaction as their admission or durable write.
func Validate(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, lease Lease) error {
	if ctx == nil || pool == nil || c == nil {
		return fmt.Errorf("validate lease: %w", ErrInvalid)
	}
	return storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		return ValidateLocked(ctx, repos, lease, c)
	})
}

// ValidateLocked verifies the task, job and run fence inside the caller's UOW.
// It obtains locks in the global task/PR then job order.
func ValidateLocked(ctx context.Context, repos *storage.Repositories, lease Lease, c clock.Clock) error {
	if repos == nil || !validID(lease.TaskID) || !validID(lease.JobID) || !validID(lease.RunID) || !validID(lease.Token) {
		return fmt.Errorf("validate lease: %w", ErrInvalid)
	}
	if c == nil {
		return fmt.Errorf("validate lease clock: %w", ErrInvalid)
	}
	_, err := validateLocked(ctx, repos, lease, c)
	return err
}

func validateLocked(ctx context.Context, repos *storage.Repositories, lease Lease, c clock.Clock) (time.Time, error) {
	if repos == nil || c == nil || !validID(lease.TaskID) || !validID(lease.JobID) || !validID(lease.RunID) || !validID(lease.Token) {
		return time.Time{}, fmt.Errorf("validate lease: %w", ErrInvalid)
	}
	locked, err := repos.LockTask(ctx, lease.TaskID)
	if err != nil {
		return time.Time{}, err
	}
	task := locked.Record()
	if deniedTaskState(task.State) || task.Generation != lease.Generation || !sameSnapshot(task.Snapshot, lease.Snapshot) {
		return time.Time{}, fmt.Errorf("task state, generation, or snapshot changed: %w", ErrStale)
	}
	var jobStatus, jobToken, operationType string
	var jobGeneration int64
	var expires time.Time
	var head, base *string
	var payload []byte
	err = repos.Queries().QueryRow(ctx, `SELECT status,COALESCE(lease_token::text,''),generation,COALESCE(lease_expires_at,'epoch'::timestamptz),
		operation_type,expected_head_sha,expected_base_sha,payload FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`,
		lease.JobID, lease.TaskID).Scan(&jobStatus, &jobToken, &jobGeneration, &expires, &operationType, &head, &base, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrStale
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("lock durable lease job: %w", err)
	}
	var runStatus, runToken, agentType string
	var runGeneration int64
	var runAttempt int32
	err = repos.Queries().QueryRow(ctx, `SELECT execution_status,lease_token::text,generation,attempt_number,agent_type FROM agent_runs
		WHERE id=$1::uuid AND task_id=$2::uuid AND job_id=$3::uuid FOR UPDATE`, lease.RunID, lease.TaskID, lease.JobID).
		Scan(&runStatus, &runToken, &runGeneration, &runAttempt, &agentType)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrStale
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("lock durable lease run: %w", err)
	}
	// Capture time only after task, PR, job, and run rows are locked, so waits
	// cannot make an expired lease appear live.
	now := c.Now().UTC()
	job, err := contracts.DecodeJob(payload)
	if err != nil {
		return time.Time{}, fmt.Errorf("decode durable lease job: %w", err)
	}
	mappedType, supported := AgentTypeForOperation(operationType)
	if operationType != job.Operation || !supported || mappedType != agentType || !validAgentType(agentType) || job.LeaseToken != "" || job.RunID != "" {
		return time.Time{}, ErrStale
	}
	if jobStatus != "LEASED" || jobToken != lease.Token || jobGeneration != lease.Generation || !expires.After(now) ||
		runStatus != "RUNNING" || runToken != lease.Token || runGeneration != lease.Generation || runAttempt != lease.RunAttempt ||
		job.TaskID != lease.TaskID || job.JobID != lease.JobID || job.Generation != lease.Generation || job.Attempt != lease.Attempt ||
		!sameSnapshot(job.Snapshot, lease.Snapshot) || !sameOptional(head, lease.Snapshot.HeadSHA) || !sameOptional(base, lease.Snapshot.BaseSHA) {
		return time.Time{}, ErrStale
	}
	return now, nil
}

// CompleteLocked records execution disposition in the caller's transaction.
// It does not perform a lifecycle transition or accept a worker result.
func CompleteLocked(ctx context.Context, repos *storage.Repositories, lease Lease, status string, c clock.Clock) error {
	if status != "SUCCESS" && status != "FAILED" && status != "TERMINATED" {
		return fmt.Errorf("complete lease status: %w", ErrInvalid)
	}
	now, err := validateLocked(ctx, repos, lease, c)
	if err != nil {
		return err
	}
	now = now.UTC()
	tag, err := repos.Queries().Exec(ctx, `UPDATE agent_runs SET execution_status=$2,finished_at=$3 WHERE id=$1::uuid AND execution_status='RUNNING'`, lease.RunID, status, now)
	if err != nil {
		return fmt.Errorf("complete agent run: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStale
	}
	jobStatus := "FAILED"
	if status == "SUCCESS" {
		jobStatus = "COMPLETED"
	}
	if status == "TERMINATED" {
		jobStatus = "CANCELLED"
	}
	tag, err = repos.Queries().Exec(ctx, `UPDATE jobs SET status=$2 WHERE id=$1::uuid AND status='LEASED' AND lease_token=$3::uuid`, lease.JobID, jobStatus, lease.Token)
	if err != nil {
		return fmt.Errorf("complete leased job: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStale
	}
	return nil
}

// Complete is a convenience wrapper for callers without a larger transaction.
func Complete(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, lease Lease, status string) error {
	if ctx == nil || pool == nil || c == nil {
		return fmt.Errorf("complete lease: %w", ErrInvalid)
	}
	return storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		return CompleteLocked(ctx, repos, lease, status, c)
	})
}

func fenceExpired(ctx context.Context, repos *storage.Repositories, taskID string, now time.Time) error {
	rows, err := repos.Queries().Query(ctx, `SELECT id::text,lease_token::text FROM jobs WHERE task_id=$1::uuid AND status='LEASED' AND lease_expires_at <= $2 FOR UPDATE`, taskID, now)
	if err != nil {
		return fmt.Errorf("find expired task leases: %w", err)
	}
	type expired struct{ id, token string }
	var found []expired
	for rows.Next() {
		var item expired
		if err := rows.Scan(&item.id, &item.token); err != nil {
			rows.Close()
			return fmt.Errorf("read expired task leases: %w", err)
		}
		found = append(found, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read expired task leases: %w", err)
	}
	rows.Close()
	for _, item := range found {
		if _, err := repos.Queries().Exec(ctx, `UPDATE agent_runs SET execution_status='TERMINATED',finished_at=$3 WHERE job_id=$1::uuid AND lease_token=$2::uuid AND execution_status='RUNNING'`, item.id, item.token, now); err != nil {
			return fmt.Errorf("fence expired run: %w", err)
		}
		if _, err := repos.Queries().Exec(ctx, `UPDATE jobs SET status='PENDING',lease_token=NULL,lease_expires_at=NULL WHERE id=$1::uuid AND lease_token=$2::uuid AND status='LEASED'`, item.id, item.token); err != nil {
			return fmt.Errorf("release expired job lease: %w", err)
		}
	}
	return nil
}

func deniedTaskState(state string) bool {
	return state == "PAUSED" || state == "ESCALATED" || state == "MERGED" || state == "CLOSED"
}
func sameSnapshot(a, b contracts.Snapshot) bool {
	return a.HeadSHA == b.HeadSHA && a.BaseSHA == b.BaseSHA && a.IntegrationSHA == b.IntegrationSHA
}
func sameOptional(value *string, expected string) bool {
	if expected == "" {
		return value == nil
	}
	return value != nil && *value == expected
}
func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func validHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func validAgentType(value string) bool { return value == "A" || value == "B" || value == "C" }

// AgentTypeForOperation maps the durable job operation to its trusted host
// identity. Unknown operations fail closed. Control-only operations deliberately
// reuse an identity code but have RequiresInference=false.
func AgentTypeForOperation(operation string) (string, bool) {
	switch operation {
	case "author":
		return "A", true
	case "review":
		return "B", true
	case "fix":
		return "C", true
	case "ci_reconcile":
		return "A", true
	case "reply":
		return "C", true
	default:
		return "", false
	}
}

// RequiresInference marks only author/review/fix work for budget admission.
// A lease never proves that a remote operation was executed.
func RequiresInference(operation string) bool {
	return operation == "author" || operation == "review" || operation == "fix"
}
func validID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// RetryLocked finishes this execution and schedules the same logical job for a
// later attempt. The finished run and due outbox row are durable evidence for
// acknowledging the old delivery. Billing reconciliation remains the caller's
// responsibility; retry never releases unknown reservation coverage.
func RetryLocked(ctx context.Context, repos *storage.Repositories, lease Lease, c clock.Clock, nextAttemptAt time.Time, executionStatus string) error {
	if executionStatus != "SUCCESS" && executionStatus != "FAILED" && executionStatus != "TERMINATED" {
		return fmt.Errorf("retry execution status: %w", ErrInvalid)
	}
	now, err := validateLocked(ctx, repos, lease, c)
	if err != nil {
		return err
	}
	if !nextAttemptAt.After(now) {
		return fmt.Errorf("retry deadline must be in the future: %w", ErrInvalid)
	}
	if _, err := repos.Queries().Exec(ctx, `UPDATE agent_runs SET execution_status=$2,finished_at=$3 WHERE id=$1::uuid`, lease.RunID, executionStatus, now); err != nil {
		return fmt.Errorf("finish deferred run: %w", err)
	}
	if _, err := repos.Queries().Exec(ctx, `UPDATE jobs SET status='PENDING',lease_token=NULL,lease_expires_at=NULL WHERE id=$1::uuid`, lease.JobID); err != nil {
		return fmt.Errorf("release deferred job: %w", err)
	}
	if _, err := repos.Queries().Exec(ctx, `INSERT INTO outbox(task_id,job_id,kind,payload,created_at,next_attempt_at)
		VALUES($1::uuid,$2::uuid,'DISPATCH',jsonb_build_object('job_id',$2::text,'retry_from_run_id',$3::text),$4,$5)`, lease.TaskID, lease.JobID, lease.RunID, now, nextAttemptAt.UTC()); err != nil {
		return fmt.Errorf("schedule deferred logical job: %w", err)
	}
	return nil
}

// Retry applies RetryLocked in its own task-locked transaction.
func Retry(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, lease Lease, nextAttemptAt time.Time, executionStatus string) error {
	return storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		if _, err := repos.LockTask(ctx, lease.TaskID); err != nil {
			return err
		}
		return RetryLocked(ctx, repos, lease, c, nextAttemptAt, executionStatus)
	})
}
