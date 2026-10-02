// Package control applies authorized task controls and shared remediation limits.
package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/lifecycle"
	"github.com/ajent-social/APRL/internal/storage"
)

const (
	// PauseLabel is the durable lifecycle label whose removal never resumes work.
	PauseLabel = "bots:paused"
	// OperationAuthor identifies initial author execution.
	OperationAuthor = "author"
	// OperationCIReconcile identifies CI reconciliation work.
	OperationCIReconcile = "ci_reconcile"
	// OperationFix identifies bounded C remediation work.
	OperationFix   = "fix"
	maxEvidenceAge = 5 * time.Minute
)

var (
	// ErrUnauthorized reports missing or insufficient verified repository authority.
	ErrUnauthorized = errors.New("repository control is unauthorized")
	// ErrInvalid reports malformed control input or an invalid lifecycle request.
	ErrInvalid = errors.New("invalid control request")
	// ErrTerminal reports an attempt to control a terminal task.
	ErrTerminal = errors.New("terminal task cannot be controlled")
	// ErrKillSwitch reports that organization emergency mode blocks resume.
	ErrKillSwitch = errors.New("organization emergency mode blocks resume")
	// ErrAttemptLimit reports a consumed remediation limit that requires operator review.
	ErrAttemptLimit = errors.New("remediation attempt limit is exhausted")
	// ErrRemoteObservation reports a missing, stale, or mismatched remote observation.
	ErrRemoteObservation = errors.New("remote host observation is missing or stale")
)

// Permission is the repository permission verified by the trusted host adapter.
type Permission string

const (
	// PermissionWrite authorizes repository write operations.
	PermissionWrite Permission = "write"
	// PermissionMaintain authorizes repository maintenance operations.
	PermissionMaintain Permission = "maintain"
	// PermissionAdmin authorizes repository administration operations.
	PermissionAdmin Permission = "admin"
)

// Authority is verified actor and repository permission evidence from the host adapter.
type Authority struct {
	ActorID    string     // ActorID is the authenticated GitHub account ID.
	Repository string     // Repository is the full repository whose permission was checked.
	Permission Permission // Permission is the current host-verified access level.
	VerifiedAt time.Time  // VerifiedAt is when the host adapter verified this permission.
}

// PauseRequest describes an explicit, authorized stop command.
type PauseRequest struct {
	Authority Authority // Authority is current repository write/maintain/admin evidence.
	Reason    string    // Reason is the audit reason for the pause.
}

// RemotePRState is the latest remotely observed pull-request state.
type RemotePRState string

const (
	// RemotePRAbsent means the task has no attached pull request.
	RemotePRAbsent RemotePRState = "absent"
	// RemotePROpen means the current pull request remains open.
	RemotePROpen RemotePRState = "open"
	// RemotePRClosed means GitHub reports the pull request closed without merge.
	RemotePRClosed RemotePRState = "closed"
	// RemotePRMerged means GitHub reports the pull request merged.
	RemotePRMerged RemotePRState = "merged"
)

// RemoteObservation is a fresh read-only host observation used to resume safely.
type RemoteObservation struct {
	Repository string             // Repository is the repository returned by the host API.
	State      RemotePRState      // State is the observed pull-request state.
	PRNumber   int32              // PRNumber binds an attached observation to the existing PR.
	Snapshot   contracts.Snapshot // Snapshot is the current remote head/base pair.
	BaseRef    string             // BaseRef is the observed target branch for the attached PR.
	IsDraft    bool               // IsDraft is the current draft status returned by the host API.
	IssueState string             // IssueState is open/closed for an unattached issue-backed task.
	ObservedAt time.Time          // ObservedAt is when the host read completed.
}

// ResumePolicy is the refreshed, operator-capped task policy applied on resume.
type ResumePolicy struct {
	Version                   string   // Version identifies the refreshed policy revision.
	TaskBudgetLimitMicroUSD   int64    // TaskBudgetLimitMicroUSD is the task ceiling in integer micro-USD.
	GlobalBudgetLimitMicroUSD int64    // GlobalBudgetLimitMicroUSD is the operator ceiling in integer micro-USD.
	MaxReviewCycles           int32    // MaxReviewCycles is the refreshed shared CI/review remediation limit.
	GlobalMaxReviewCycles     int32    // GlobalMaxReviewCycles is the operator ceiling for remediation attempts.
	AllowedTargetBranches     []string // AllowedTargetBranches is the refreshed target branch allowlist.
}

// ReplyIntentMaterializer creates result-owned successor replies inside the resume transaction.
// It must use the current locked generation and only materialize a confirmed matching head.
type ReplyIntentMaterializer func(context.Context, *storage.Repositories, storage.LockedTask) error

// ResumeRequest supplies current authority, remote state, and refreshed policy.
type ResumeRequest struct {
	Authority          Authority               // Authority is current repository write/maintain/admin evidence.
	Remote             RemoteObservation       // Remote is the fresh PR/task state read from the host.
	Policy             ResumePolicy            // Policy contains the refreshed policy and global budget cap.
	Reason             string                  // Reason is the audit reason for resume.
	MaterializeReplies ReplyIntentMaterializer // MaterializeReplies is the results-owned pending push-reply callback.
}

// PauseLocked fences active ownership and records cancellation intents transactionally.
// The caller must hold organization-budget then task locks; it performs no Redis operation.
func PauseLocked(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, c clock.Clock, request PauseRequest) (storage.LockedTask, error) {
	if ctx == nil || repos == nil || c == nil || strings.TrimSpace(request.Reason) == "" {
		return storage.LockedTask{}, fmt.Errorf("pause task: %w", ErrInvalid)
	}
	task := locked.Record()
	if err := validateAuthority(request.Authority, task.RepoFullName, c.Now()); err != nil {
		return storage.LockedTask{}, err
	}
	if task.State == string(lifecycle.Paused) {
		if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
			return storage.LockedTask{}, err
		}
		return locked, nil
	}
	return FenceLocked(ctx, repos, locked, c, lifecycle.Paused, request.Authority.ActorID, "pause", request.Reason)
}

// ApplyPauseLabelLocked pauses on addition; removal never resumes and reasserts the durable label while paused.
func ApplyPauseLabelLocked(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, c clock.Clock, label string, added bool, authority Authority, reason string) (storage.LockedTask, bool, error) {
	if ctx == nil || repos == nil || c == nil || label != PauseLabel {
		return storage.LockedTask{}, false, fmt.Errorf("apply control label: %w", ErrInvalid)
	}
	if !added {
		task := locked.Record()
		if task.State == string(lifecycle.Paused) || task.State == string(lifecycle.Escalated) {
			if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
				return storage.LockedTask{}, false, err
			}
			if err := enqueueLifecycleLabel(ctx, repos, task.ID, task.PRID, task.Generation, task.State, c.Now().UTC()); err != nil {
				return storage.LockedTask{}, false, err
			}
		}
		return locked, false, nil
	}
	updated, err := PauseLocked(ctx, repos, locked, c, PauseRequest{Authority: authority, Reason: reason})
	return updated, err == nil && updated.Record().State != locked.Record().State, err
}

// ResumeLocked refreshes policy and remote snapshot, preserves counters, and queues current-generation work.
// The caller must hold organization-budget then task locks.
func ResumeLocked(ctx context.Context, repos *storage.Repositories, budget storage.LockedOrgBudget, locked storage.LockedTask, c clock.Clock, request ResumeRequest) (storage.LockedTask, error) {
	if ctx == nil || repos == nil || c == nil || strings.TrimSpace(request.Reason) == "" || strings.TrimSpace(request.Policy.Version) == "" || request.Policy.GlobalBudgetLimitMicroUSD <= 0 || request.Policy.TaskBudgetLimitMicroUSD <= 0 || request.Policy.TaskBudgetLimitMicroUSD > request.Policy.GlobalBudgetLimitMicroUSD || request.Policy.MaxReviewCycles <= 0 || request.Policy.GlobalMaxReviewCycles <= 0 || request.Policy.MaxReviewCycles > request.Policy.GlobalMaxReviewCycles {
		return storage.LockedTask{}, fmt.Errorf("resume task: %w", ErrInvalid)
	}
	task := locked.Record()
	if task.State != string(lifecycle.Paused) && task.State != string(lifecycle.Escalated) {
		return storage.LockedTask{}, fmt.Errorf("resume task from %s: %w", task.State, ErrInvalid)
	}
	now := c.Now().UTC()
	if err := validateAuthority(request.Authority, task.RepoFullName, now); err != nil {
		return storage.LockedTask{}, err
	}
	if err := validateRemoteObservation(ctx, repos, request.Remote, task, request.Policy.AllowedTargetBranches, now); err != nil {
		return storage.LockedTask{}, err
	}
	if budget.Record().OrgID != task.OrgID {
		return storage.LockedTask{}, fmt.Errorf("resume task with unrelated organization lock: %w", ErrInvalid)
	}
	var emergencyMode bool
	if err := repos.Queries().QueryRow(ctx, `SELECT emergency_mode FROM org_budgets WHERE org_id=$1`, task.OrgID).Scan(&emergencyMode); err != nil {
		return storage.LockedTask{}, fmt.Errorf("refresh organization emergency mode on resume: %w", err)
	}
	if emergencyMode {
		return storage.LockedTask{}, ErrKillSwitch
	}
	if task.CycleCount >= task.MaxReviewCycles || task.CycleCount >= request.Policy.MaxReviewCycles {
		return storage.LockedTask{}, ErrAttemptLimit
	}
	if task.Generation == math.MaxInt64 {
		return storage.LockedTask{}, lifecycle.ErrGenerationExhausted
	}
	if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
		return storage.LockedTask{}, err
	}

	nextState := lifecycle.Authoring
	operation := OperationAuthor
	var snapshot contracts.Snapshot
	if task.PRID != nil {
		nextState = lifecycle.WaitingCI
		operation = OperationCIReconcile
		snapshot = contracts.Snapshot{HeadSHA: request.Remote.Snapshot.HeadSHA, BaseSHA: request.Remote.Snapshot.BaseSHA}
	}
	now = c.Now().UTC()
	if err := cancelJobsLocked(ctx, repos, task.ID, now); err != nil {
		return storage.LockedTask{}, err
	}
	if _, err := repos.UpdateTaskState(ctx, locked, task.Generation, task.Generation+1, string(nextState)); err != nil {
		return storage.LockedTask{}, err
	}
	command, err := repos.Queries().Exec(ctx, `UPDATE tasks SET budget_limit_micro_usd=$2,max_review_cycles=$3,policy_version=$4,updated_at=$5 WHERE id=$1::uuid AND generation=$6`, task.ID, request.Policy.TaskBudgetLimitMicroUSD, request.Policy.MaxReviewCycles, request.Policy.Version, now, task.Generation+1)
	if err != nil {
		return storage.LockedTask{}, fmt.Errorf("refresh task policy on resume: %w", err)
	}
	if command.RowsAffected() != 1 {
		return storage.LockedTask{}, storage.ErrStaleGeneration
	}
	if task.PRID != nil {
		command, err = repos.Queries().Exec(ctx, `UPDATE prs SET head_sha=$2,base_sha=$3,base_ref=$4,integration_sha=NULL,approved_head_sha=NULL,approved_base_sha=NULL,human_approval_id=NULL,ci_status='PENDING',ci_deadline_at=NULL,is_draft=$5 WHERE task_id=$1::uuid`, task.ID, snapshot.HeadSHA, snapshot.BaseSHA, request.Remote.BaseRef, request.Remote.IsDraft)
		if err != nil {
			return storage.LockedTask{}, fmt.Errorf("refresh remote pull-request snapshot on resume: %w", err)
		}
		if command.RowsAffected() != 1 {
			return storage.LockedTask{}, storage.ErrNotFound
		}
	}
	updated, err := repos.LockTask(ctx, task.ID)
	if err != nil {
		return storage.LockedTask{}, err
	}
	if updated.Record().Generation != task.Generation+1 || !sameSnapshot(updated.Record().Snapshot, snapshot) {
		return storage.LockedTask{}, storage.ErrStaleGeneration
	}
	if task.PRID != nil && request.MaterializeReplies != nil {
		if err := ensurePendingReplies(ctx, repos, updated, request.Remote, request.MaterializeReplies); err != nil {
			return storage.LockedTask{}, err
		}
	}
	if _, err := insertControlAction(ctx, repos, task, request.Authority.ActorID, "resume", request.Reason, task.Generation+1, now); err != nil {
		return storage.LockedTask{}, err
	}
	if err := enqueueLifecycleLabel(ctx, repos, task.ID, updated.Record().PRID, updated.Record().Generation, string(nextState), now); err != nil {
		return storage.LockedTask{}, err
	}
	if _, _, err := enqueueControlJob(ctx, repos, updated, operation, nil); err != nil {
		return storage.LockedTask{}, err
	}
	return updated, nil
}

// FenceLocked advances generation, invalidates approval, revokes queued leases, and persists cancellation outbox rows.
// The caller must hold organization-budget then task locks.
func FenceLocked(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, c clock.Clock, next lifecycle.State, actorID, action, reason string) (storage.LockedTask, error) {
	if ctx == nil || repos == nil || c == nil || (next != lifecycle.Paused && next != lifecycle.Escalated) || strings.TrimSpace(actorID) == "" || strings.TrimSpace(action) == "" || strings.TrimSpace(reason) == "" {
		return storage.LockedTask{}, fmt.Errorf("fence task: %w", ErrInvalid)
	}
	task := locked.Record()
	if task.State == string(lifecycle.Merged) || task.State == string(lifecycle.Closed) {
		return storage.LockedTask{}, ErrTerminal
	}
	if task.State == string(next) {
		if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
			return storage.LockedTask{}, err
		}
		return locked, nil
	}
	if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
		return storage.LockedTask{}, err
	}
	if task.Generation == math.MaxInt64 {
		return storage.LockedTask{}, lifecycle.ErrGenerationExhausted
	}
	now := c.Now().UTC()
	updated, err := repos.UpdateTaskState(ctx, locked, task.Generation, task.Generation+1, string(next))
	if err != nil {
		return storage.LockedTask{}, err
	}
	if task.PRID != nil {
		command, updateErr := repos.Queries().Exec(ctx, `UPDATE prs SET approved_head_sha=NULL,approved_base_sha=NULL,human_approval_id=NULL,ci_status=CASE WHEN $2='PAUSED' THEN 'CANCELLED' ELSE ci_status END WHERE task_id=$1::uuid`, task.ID, string(next))
		if updateErr != nil {
			return storage.LockedTask{}, fmt.Errorf("invalidate pull-request approval while fencing: %w", updateErr)
		}
		if command.RowsAffected() != 1 {
			return storage.LockedTask{}, storage.ErrNotFound
		}
	}
	if err := cancelJobsLocked(ctx, repos, task.ID, now); err != nil {
		return storage.LockedTask{}, err
	}
	if _, err := insertControlAction(ctx, repos, task, actorID, action, reason, task.Generation+1, now); err != nil {
		return storage.LockedTask{}, err
	}
	if err := enqueueLifecycleLabel(ctx, repos, task.ID, task.PRID, task.Generation+1, string(next), now); err != nil {
		return storage.LockedTask{}, err
	}
	return updated, nil
}

// EscalateLocked durably escalates and fences a task, preserving execution and billing history.
// The caller must hold organization-budget then task locks.
func EscalateLocked(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, c clock.Clock, reason, details string) (storage.LockedTask, error) {
	if ctx == nil || repos == nil || c == nil || strings.TrimSpace(reason) == "" {
		return storage.LockedTask{}, fmt.Errorf("escalate task: %w", ErrInvalid)
	}
	task := locked.Record()
	if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
		return storage.LockedTask{}, err
	}
	if task.State == string(lifecycle.Escalated) {
		var exists bool
		if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM escalations WHERE task_id=$1::uuid AND reason=$2 AND resolved_at IS NULL)`, task.ID, reason).Scan(&exists); err != nil {
			return storage.LockedTask{}, err
		}
		if exists {
			return locked, nil
		}
	}
	updated, err := FenceLocked(ctx, repos, locked, c, lifecycle.Escalated, "system", "escalate", reason)
	if err != nil {
		return storage.LockedTask{}, err
	}
	if _, err := repos.Queries().Exec(ctx, `INSERT INTO escalations(task_id,reason,details,created_at) VALUES($1::uuid,$2,$3,$4)`, task.ID, reason, details, c.Now().UTC()); err != nil {
		return storage.LockedTask{}, fmt.Errorf("record task escalation: %w", err)
	}
	return updated, nil
}

// AdmitFixLocked consumes one task-wide C attempt or escalates at the CI/review shared limit.
// The caller queues the admitted attempt with QueueFixLocked in the same transaction.
func AdmitFixLocked(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, c clock.Clock) (storage.LockedTask, int32, bool, error) {
	if ctx == nil || repos == nil || c == nil {
		return storage.LockedTask{}, 0, false, fmt.Errorf("admit fix: %w", ErrInvalid)
	}
	task := locked.Record()
	if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
		return storage.LockedTask{}, 0, false, err
	}
	if task.State == string(lifecycle.Paused) || task.State == string(lifecycle.Escalated) || task.State == string(lifecycle.Merged) || task.State == string(lifecycle.Closed) {
		return storage.LockedTask{}, 0, false, fmt.Errorf("admit fix in %s: %w", task.State, ErrInvalid)
	}
	if task.CycleCount >= task.MaxReviewCycles {
		updated, err := EscalateLocked(ctx, repos, locked, c, "REMEDIATION_LIMIT", fmt.Sprintf("cycle_count=%d max_review_cycles=%d", task.CycleCount, task.MaxReviewCycles))
		return updated, task.CycleCount, false, err
	}
	if task.CycleCount == math.MaxInt32 {
		return storage.LockedTask{}, task.CycleCount, false, ErrAttemptLimit
	}
	if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
		return storage.LockedTask{}, 0, false, err
	}
	next := task.CycleCount + 1
	command, err := repos.Queries().Exec(ctx, `UPDATE tasks SET cycle_count=$2,updated_at=$3 WHERE id=$1::uuid AND generation=$4 AND cycle_count=$5 AND cycle_count < max_review_cycles`, task.ID, next, c.Now().UTC(), task.Generation, task.CycleCount)
	if err != nil {
		return storage.LockedTask{}, 0, false, fmt.Errorf("increment remediation cycle: %w", err)
	}
	if command.RowsAffected() != 1 {
		return storage.LockedTask{}, 0, false, ErrAttemptLimit
	}
	updated, err := repos.LockTask(ctx, task.ID)
	if err != nil {
		return storage.LockedTask{}, 0, false, err
	}
	if updated.Record().CycleCount != next {
		return storage.LockedTask{}, 0, false, storage.ErrStaleGeneration
	}
	return updated, next, true, nil
}

// QueueFixLocked inserts the stable logical job for a previously admitted remediation attempt.
func QueueFixLocked(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, attempt int32) (storage.JobRecord, bool, error) {
	if ctx == nil || repos == nil || attempt <= 0 {
		return storage.JobRecord{}, false, fmt.Errorf("queue fix job: %w", ErrInvalid)
	}
	task := locked.Record()
	if task.CycleCount != attempt || repos.RequireTaskGeneration(ctx, locked, task.Generation) != nil {
		return storage.JobRecord{}, false, storage.ErrStaleGeneration
	}
	return enqueueControlJob(ctx, repos, locked, OperationFix, &attempt)
}

func validateAuthority(authority Authority, repository string, now time.Time) error {
	if strings.TrimSpace(authority.ActorID) == "" || authority.Repository != repository || authority.VerifiedAt.IsZero() || authority.VerifiedAt.After(now) || now.Sub(authority.VerifiedAt) > maxEvidenceAge {
		return ErrUnauthorized
	}
	switch authority.Permission {
	case PermissionWrite, PermissionMaintain, PermissionAdmin:
		return nil
	default:
		return ErrUnauthorized
	}
}

func validateRemoteObservation(ctx context.Context, repos *storage.Repositories, remote RemoteObservation, task storage.Task, allowedBranches []string, now time.Time) error {
	if remote.Repository != task.RepoFullName || remote.ObservedAt.IsZero() || remote.ObservedAt.After(now) || now.Sub(remote.ObservedAt) > maxEvidenceAge {
		return ErrRemoteObservation
	}
	if task.PRID == nil {
		if !strings.HasPrefix(task.SourceKey, "issue:") || remote.State != RemotePRAbsent || remote.IssueState != "open" || remote.PRNumber != 0 || remote.Snapshot != (contracts.Snapshot{}) || strings.TrimSpace(remote.BaseRef) != "" {
			return ErrRemoteObservation
		}
		return nil
	}
	if remote.State != RemotePROpen || remote.PRNumber <= 0 || strings.TrimSpace(remote.BaseRef) == "" || remote.Snapshot.IntegrationSHA != "" || remote.Snapshot.Validate(true) != nil || !allowedBranch(remote.BaseRef, allowedBranches) {
		return ErrRemoteObservation
	}
	var prNumber int32
	var repo string
	if err := repos.Queries().QueryRow(ctx, `SELECT pr_number,repo_full_name FROM prs WHERE id=$1`, *task.PRID).Scan(&prNumber, &repo); err != nil {
		return fmt.Errorf("verify attached PR identity on resume: %w", err)
	}
	if prNumber != remote.PRNumber || repo != remote.Repository {
		return ErrRemoteObservation
	}
	return nil
}

func allowedBranch(branch string, patterns []string) bool {
	for _, pattern := range patterns {
		if strings.HasSuffix(pattern, "*") && strings.HasPrefix(branch, strings.TrimSuffix(pattern, "*")) {
			return true
		}
		if branch == pattern {
			return true
		}
	}
	return false
}

func ensurePendingReplies(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, remote RemoteObservation, materialize ReplyIntentMaterializer) error {
	if locked.Record().PRID == nil || materialize == nil {
		return nil
	}
	var pending, matching int
	if err := repos.Queries().QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE op.result->>'head_sha'=$2 AND COALESCE((op.result->>'handoff_generation')::bigint,0)<$3)
		FROM github_operations op WHERE op.task_id=$1::uuid AND op.operation_type='push' AND op.status='CONFIRMED'
		AND CASE WHEN jsonb_typeof(op.request->'reply_intents')='array' THEN jsonb_array_length(op.request->'reply_intents') ELSE 0 END>0
		AND COALESCE((op.result->>'reply_jobs_created')::boolean,FALSE)=FALSE`, locked.Record().ID, remote.Snapshot.HeadSHA, locked.Record().Generation).Scan(&pending, &matching); err != nil {
		return fmt.Errorf("inspect pending confirmed-push replies: %w", err)
	}
	if pending == 0 || pending != matching {
		return nil
	}
	if err := materialize(ctx, repos, locked); err != nil {
		return fmt.Errorf("materialize pending push replies on resume: %w", err)
	}
	return nil
}

func cancelJobsLocked(ctx context.Context, repos *storage.Repositories, taskID string, now time.Time) error {
	// RUNNING means the host admitted the run; this durable revoke is not proof
	// that the supervisor process has been physically reaped.
	if _, err := repos.Queries().Exec(ctx, `WITH revoked AS (
		UPDATE agent_runs ar SET execution_status='TERMINATED',finished_at=$2
		WHERE ar.task_id=$1::uuid AND ar.execution_status='RUNNING'
		AND EXISTS (SELECT 1 FROM jobs j WHERE j.id=ar.job_id AND j.task_id=ar.task_id AND j.status IN ('PENDING','LEASED'))
		RETURNING ar.id
	)
	UPDATE budget_reservations br SET status='UNKNOWN'
	FROM revoked r WHERE br.run_id=r.id AND br.status IN ('RESERVED','SETTLING')`, taskID, now); err != nil {
		return fmt.Errorf("revoke active runs and retain unknown budget coverage: %w", err)
	}
	rows, err := repos.Queries().Query(ctx, `UPDATE jobs SET status='CANCELLED',lease_token=NULL,lease_expires_at=NULL WHERE task_id=$1::uuid AND status IN ('PENDING','LEASED') RETURNING id::text`, taskID)
	if err != nil {
		return fmt.Errorf("cancel task jobs: %w", err)
	}
	var jobs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("read cancelled task jobs: %w", err)
		}
		jobs = append(jobs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read cancelled task jobs: %w", err)
	}
	rows.Close()
	for _, jobID := range jobs {
		if _, err := repos.Queries().Exec(ctx, `INSERT INTO outbox(task_id,job_id,kind,payload,created_at,next_attempt_at) VALUES($1::uuid,$2::uuid,'CANCEL',jsonb_build_object('job_id',$2::uuid),$3,$3)`, taskID, jobID, now); err != nil {
			return fmt.Errorf("persist cancellation outbox for job %s: %w", jobID, err)
		}
	}
	return nil
}

func insertControlAction(ctx context.Context, repos *storage.Repositories, task storage.Task, actorID, action, reason string, generation int64, now time.Time) (string, error) {
	var actionID string
	err := repos.Queries().QueryRow(ctx, `INSERT INTO control_actions(task_id,actor_id,action,reason,previous_generation,new_generation,created_at) VALUES($1::uuid,$2,$3,$4,$5,$6,$7) RETURNING id::text`, task.ID, actorID, action, reason, task.Generation, generation, now).Scan(&actionID)
	if err != nil {
		return "", fmt.Errorf("record task control action: %w", err)
	}
	return actionID, nil
}

func enqueueLifecycleLabel(ctx context.Context, repos *storage.Repositories, taskID string, prID *int64, generation int64, state string, now time.Time) error {
	if prID == nil {
		return nil
	}
	payload, err := json.Marshal(struct {
		TaskID     string `json:"task_id"`
		PRID       int64  `json:"pr_id"`
		Generation int64  `json:"generation"`
		State      string `json:"state"`
		Label      string `json:"label"`
	}{TaskID: taskID, PRID: *prID, Generation: generation, State: state, Label: lifecycleLabel(state)})
	if err != nil {
		return err
	}
	if _, err := repos.Queries().Exec(ctx, `INSERT INTO outbox(task_id,kind,payload,created_at,next_attempt_at) VALUES($1::uuid,'LABEL_SYNC',$2::jsonb,$3,$3)`, taskID, string(payload), now); err != nil {
		return fmt.Errorf("persist lifecycle label outbox: %w", err)
	}
	return nil
}

func lifecycleLabel(state string) string {
	switch state {
	case string(lifecycle.Paused):
		return PauseLabel
	case string(lifecycle.Escalated):
		return "escalate:human"
	case string(lifecycle.Authoring):
		return "agent:authoring"
	case string(lifecycle.WaitingCI), string(lifecycle.InReview):
		return "agent:reviewing"
	case string(lifecycle.ChangesRequested):
		return "changes-requested"
	case string(lifecycle.Fixing):
		return "agent:fixing"
	case string(lifecycle.ReadyToMerge):
		return "ready-to-merge"
	default:
		return ""
	}
}

func enqueueControlJob(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, operation string, remediationAttempt *int32) (storage.JobRecord, bool, error) {
	task := locked.Record()
	logicalKey := fmt.Sprintf("task:%s:generation:%d:%s", task.ID, task.Generation, operation)
	if remediationAttempt != nil {
		logicalKey = fmt.Sprintf("task:%s:generation:%d:cycle:%d:%s", task.ID, task.Generation, *remediationAttempt, operation)
	}
	jobID := stableUUID("job:" + logicalKey)
	job := contracts.Job{Version: contracts.VersionV1, TaskID: task.ID, JobID: jobID, Generation: task.Generation, Snapshot: task.Snapshot, Attempt: 1, OperationID: stableUUID("operation:" + logicalKey), CorrelationID: stableUUID("correlation:" + logicalKey), Operation: operation}
	payload, err := json.Marshal(job)
	if err != nil {
		return storage.JobRecord{}, false, err
	}
	return repos.InsertJobAndOutbox(ctx, locked, storage.JobInput{ID: jobID, LogicalKey: logicalKey, OperationType: operation, PRID: task.PRID, RemediationAttempt: remediationAttempt, Payload: payload})
}

func stableUUID(value string) string {
	digest := sha256.Sum256([]byte("aprl.router.v1:" + value))
	id := digest[:16]
	id[6] = id[6]&0x0f | 0x50
	id[8] = id[8]&0x3f | 0x80
	encoded := hex.EncodeToString(id)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func sameSnapshot(left, right contracts.Snapshot) bool {
	return left.HeadSHA == right.HeadSHA && left.BaseSHA == right.BaseSHA && left.IntegrationSHA == right.IntegrationSHA
}
