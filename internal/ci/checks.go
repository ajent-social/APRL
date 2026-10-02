// Package ci aggregates trusted GitHub checks against one pinned PR snapshot.
package ci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/control"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/lifecycle"
	"github.com/ajent-social/APRL/internal/policy"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
)

// Outcome is the fail-closed aggregate state of the configured required checks.
type Outcome string

const (
	// OperationCIReconcile identifies durable CI polling jobs.
	OperationCIReconcile = "ci_reconcile"
	// OperationReview identifies reviewer B jobs.
	OperationReview = "review"
	// OperationFix identifies fixer C jobs.
	OperationFix = "fix"
)

const (
	// OutcomePassed means every required check passed on the current pinned snapshot.
	OutcomePassed Outcome = "passed"
	// OutcomeFailed means at least one required check reported an explicit failure.
	OutcomeFailed Outcome = "failed"
	// OutcomePending means one or more checks are missing or still running.
	OutcomePending Outcome = "pending"
	// OutcomeInconclusive means a required check is cancelled, neutral, skipped, or unknown.
	OutcomeInconclusive Outcome = "inconclusive"
	// OutcomeTimedOut means the persisted CI deadline passed before admission.
	OutcomeTimedOut Outcome = "timed_out"
)

// Observation is an actual producer's check/status observation and tested snapshot.
type Observation struct {
	Name           string          // Name is the exact GitHub check name.
	Producer       policy.Producer // Producer is the authenticated producer identity.
	HeadSHA        string          // HeadSHA is the tested pull-request commit.
	BaseSHA        string          // BaseSHA is the tested target commit.
	IntegrationSHA string          // IntegrationSHA is the tested merge commit.
	Status         string          // Status is the producer's lifecycle state.
	Conclusion     string          // Conclusion is the producer's terminal result, when present.
	Attempt        int             // Attempt orders reruns for one named producer check.
	UpdatedAt      time.Time       // UpdatedAt orders otherwise equal attempts.
}

// CheckResult records the aggregate state of one required check.
type CheckResult struct {
	Name       string          // Name is the configured check name.
	Producer   policy.Producer // Producer is the configured trusted producer.
	Outcome    Outcome         // Outcome is this check's fail-closed state.
	Status     string          // Status is the latest matching observation status.
	Conclusion string          // Conclusion is the latest matching observation result.
	Stale      bool            // Stale reports older snapshot observations for this check.
}

// Aggregate is the result for one complete pinned integration snapshot.
type Aggregate struct {
	Snapshot          contracts.Snapshot // Snapshot is the complete tested tuple.
	Outcome           Outcome            // Outcome is the aggregate admission result.
	Required          int                // Required is the policy's required check count.
	Passed            int                // Passed is the count of successful required checks.
	Failed            int                // Failed is the count of explicitly failed checks.
	Pending           int                // Pending is the count of missing or running checks.
	Inconclusive      int                // Inconclusive is the count of non-pass terminal/unknown checks.
	StaleObservations int                // StaleObservations counts same-check evidence for older tuples.
	Checks            []CheckResult      // Checks contains one result per required check.
}

// Evaluate accepts only exact required name/producer/snapshot matches.
// Unknown and non-success conclusions never satisfy a required check.
func Evaluate(snapshot contracts.Snapshot, config policy.CIConfig, observations []Observation) (Aggregate, error) {
	if err := snapshot.Validate(true); err != nil {
		return Aggregate{}, fmt.Errorf("validate CI snapshot: %w", err)
	}
	if snapshot.IntegrationSHA == "" {
		return Aggregate{}, fmt.Errorf("CI snapshot must include valid head/base/integration SHAs")
	}
	if err := config.ValidateTiming(); err != nil {
		return Aggregate{}, err
	}
	if err := config.Validate(); err != nil && err != policy.ErrEmptyRequiredChecks {
		return Aggregate{}, err
	}
	if len(config.RequiredChecks) == 0 {
		return Aggregate{Snapshot: snapshot, Outcome: OutcomeInconclusive}, nil
	}
	result := Aggregate{Snapshot: snapshot, Outcome: OutcomePassed, Required: len(config.RequiredChecks), Checks: make([]CheckResult, 0, len(config.RequiredChecks))}
	for _, required := range config.RequiredChecks {
		var candidates []Observation
		stale := false
		for _, observed := range observations {
			if observed.Name != required.Name || observed.Producer != required.Producer {
				continue
			}
			if observed.HeadSHA != snapshot.HeadSHA || observed.BaseSHA != snapshot.BaseSHA || observed.IntegrationSHA != snapshot.IntegrationSHA {
				stale = true
				result.StaleObservations++
				continue
			}
			candidates = append(candidates, observed)
		}
		check := CheckResult{Name: required.Name, Producer: required.Producer, Outcome: OutcomePending, Stale: stale}
		if len(candidates) > 0 {
			latest, ambiguous := latestObservation(candidates)
			check.Status, check.Conclusion = strings.ToLower(strings.TrimSpace(latest.Status)), strings.ToLower(strings.TrimSpace(latest.Conclusion))
			check.Outcome = observationOutcome(check.Status, check.Conclusion)
			if ambiguous {
				check.Outcome = OutcomeInconclusive
			}
		}
		result.Checks = append(result.Checks, check)
		switch check.Outcome {
		case OutcomePassed:
			result.Passed++
		case OutcomeFailed:
			result.Failed++
		case OutcomePending:
			result.Pending++
		case OutcomeInconclusive:
			result.Inconclusive++
		}
	}
	switch {
	case result.Failed > 0:
		result.Outcome = OutcomeFailed
	case result.Pending > 0:
		result.Outcome = OutcomePending
	case result.Inconclusive > 0:
		result.Outcome = OutcomeInconclusive
	}
	return result, nil
}

func latestObservation(observations []Observation) (Observation, bool) {
	latest := observations[0]
	ambiguous := false
	for _, candidate := range observations[1:] {
		if candidate.Attempt > latest.Attempt || (candidate.Attempt == latest.Attempt && candidate.UpdatedAt.After(latest.UpdatedAt)) {
			latest = candidate
			ambiguous = false
			continue
		}
		if candidate.Attempt == latest.Attempt && candidate.UpdatedAt.Equal(latest.UpdatedAt) && (candidate.Status != latest.Status || candidate.Conclusion != latest.Conclusion) {
			ambiguous = true
		}
	}
	return latest, ambiguous
}

func observationOutcome(status, conclusion string) Outcome {
	switch status {
	case "completed":
		switch conclusion {
		case "success":
			return OutcomePassed
		case "failure":
			return OutcomeFailed
		default:
			return OutcomeInconclusive
		}
	case "success":
		if conclusion == "" {
			return OutcomePassed
		}
		return OutcomeInconclusive
	case "failure":
		return OutcomeFailed
	case "queued", "in_progress", "pending", "requested", "waiting":
		return OutcomePending
	}
	switch conclusion {
	case "", "cancelled", "neutral", "skipped", "timed_out", "action_required", "startup_failure", "stale":
		return OutcomeInconclusive
	default:
		return OutcomeInconclusive
	}
}

// PinIntegration completes the old CI run before changing its snapshot, then
// creates one successor CI job bound to the newly pinned integration SHA.
// The caller must hold the organization-budget lock before the task lock.
func PinIntegration(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, lease leases.Lease, c clock.Clock, config policy.CIConfig, integrationSHA string) (storage.LockedTask, bool, error) {
	if ctx == nil || repos == nil || c == nil || config.ValidateTiming() != nil {
		return storage.LockedTask{}, false, fmt.Errorf("pin integration requires valid transaction, clock, and CI timing")
	}
	task := locked.Record()
	if task.State != string(lifecycle.WaitingCI) || task.PRID == nil {
		return storage.LockedTask{}, false, ErrCIState
	}
	if err := validateCILease(ctx, repos, task, lease, c); err != nil {
		return storage.LockedTask{}, false, err
	}
	if integrationSHA == "" {
		return storage.LockedTask{}, false, ErrCIFence
	}
	observed := task.Snapshot
	observed.IntegrationSHA = integrationSHA
	if err := observed.Validate(true); err != nil {
		return storage.LockedTask{}, false, ErrCIFence
	}
	if observed.IntegrationSHA == task.Snapshot.IntegrationSHA {
		return locked, false, nil
	}
	transition, err := lifecycle.ObserveSnapshot(lifecycle.WaitingCI, task.Generation, task.Snapshot, observed, true)
	if err != nil {
		return storage.LockedTask{}, false, err
	}
	if transition.Generation <= task.Generation {
		return storage.LockedTask{}, false, ErrCIFence
	}
	if err := leases.CompleteLocked(ctx, repos, lease, "SUCCESS", c); err != nil {
		return storage.LockedTask{}, false, err
	}
	_, err = repos.UpdateTaskState(ctx, locked, task.Generation, transition.Generation, string(lifecycle.WaitingCI))
	if err != nil {
		return storage.LockedTask{}, false, err
	}
	deadline := c.Now().UTC().Add(config.WaitTimeout)
	command, err := repos.Queries().Exec(ctx, `UPDATE prs SET integration_sha=$2,approved_head_sha=NULL,approved_base_sha=NULL,human_approval_id=NULL,ci_status='PENDING',ci_deadline_at=$3 WHERE task_id=$1::uuid`, task.ID, integrationSHA, deadline)
	if err != nil {
		return storage.LockedTask{}, false, err
	}
	if command.RowsAffected() != 1 {
		return storage.LockedTask{}, false, storage.ErrNotFound
	}
	updated, err := repos.LockTask(ctx, task.ID)
	if err != nil {
		return storage.LockedTask{}, false, err
	}
	if !sameSnapshot(updated.Record().Snapshot, observed) {
		return storage.LockedTask{}, false, ErrCIFence
	}
	if _, _, err := enqueueLogicalJob(ctx, repos, updated, OperationCIReconcile, nil); err != nil {
		return storage.LockedTask{}, false, err
	}
	return updated, true, nil
}

// Reconcile evaluates all required checks under the current snapshot and
// durably retries, escalates, queues C, or queues B in the same transaction.
// The caller must hold the organization-budget lock before the task lock.
func Reconcile(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, lease leases.Lease, c clock.Clock, config policy.CIConfig, observations []Observation) (Aggregate, error) {
	if ctx == nil || repos == nil || c == nil || config.ValidateTiming() != nil {
		return Aggregate{}, fmt.Errorf("reconcile CI requires valid transaction, clock, and CI timing")
	}
	task := locked.Record()
	if task.PRID == nil || task.Snapshot.IntegrationSHA == "" {
		return Aggregate{}, ErrCIFence
	}
	if task.State != string(lifecycle.WaitingCI) {
		if task.State == string(lifecycle.InReview) {
			if task.Generation != lease.Generation || task.ID != lease.TaskID || !sameSnapshot(task.Snapshot, lease.Snapshot) {
				return Aggregate{}, ErrCIFence
			}
			completed, err := ciCompletionRecorded(ctx, repos, task, lease)
			if err != nil {
				return Aggregate{}, err
			}
			if completed {
				return Aggregate{Snapshot: task.Snapshot, Outcome: OutcomePassed, Required: len(config.RequiredChecks), Passed: len(config.RequiredChecks)}, nil
			}
		}
		return Aggregate{}, ErrCIState
	}
	if err := validateCILease(ctx, repos, task, lease, c); err != nil {
		return Aggregate{}, err
	}
	aggregate, err := Evaluate(task.Snapshot, config, observations)
	if err != nil {
		return Aggregate{}, err
	}
	var deadline *time.Time
	if err := repos.Queries().QueryRow(ctx, `SELECT ci_deadline_at FROM prs WHERE id=$1 FOR UPDATE`, *task.PRID).Scan(&deadline); err != nil {
		return Aggregate{}, err
	}
	now := c.Now().UTC()
	if deadline == nil {
		newDeadline := now.Add(config.WaitTimeout)
		if _, err := repos.Queries().Exec(ctx, `UPDATE prs SET ci_deadline_at=$2,ci_status='PENDING' WHERE id=$1`, *task.PRID, newDeadline); err != nil {
			return Aggregate{}, err
		}
		deadline = &newDeadline
	}
	if !now.Before(*deadline) {
		if err := leases.CompleteLocked(ctx, repos, lease, "SUCCESS", c); err != nil {
			return Aggregate{}, err
		}
		if err := setPRCIStatus(ctx, repos, *task.PRID, "ERROR"); err != nil {
			return Aggregate{}, err
		}
		if _, err := control.EscalateLocked(ctx, repos, locked, c, "CI_TIMEOUT", aggregateSummary(aggregate)); err != nil {
			return Aggregate{}, err
		}
		aggregate.Outcome = OutcomeTimedOut
		return aggregate, nil
	}
	switch aggregate.Outcome {
	case OutcomePassed:
		if !policy.CanAdmitReview(policy.ReviewAdmission{TaskState: task.State, CurrentSnapshot: task.Snapshot, TestedSnapshot: aggregate.Snapshot, RequiredChecks: aggregate.Required, ChecksPassed: true}) {
			return Aggregate{}, ErrCIFence
		}
		if err := leases.CompleteLocked(ctx, repos, lease, "SUCCESS", c); err != nil {
			return Aggregate{}, err
		}
		if err := setPRCIStatus(ctx, repos, *task.PRID, "SUCCESS"); err != nil {
			return Aggregate{}, err
		}
		if _, err := repos.UpdateTaskState(ctx, locked, task.Generation, task.Generation, string(lifecycle.InReview)); err != nil {
			return Aggregate{}, err
		}
		refreshed, err := repos.LockTask(ctx, task.ID)
		if err != nil {
			return Aggregate{}, err
		}
		if _, _, err := enqueueLogicalJob(ctx, repos, refreshed, OperationReview, nil); err != nil {
			return Aggregate{}, err
		}
	case OutcomeFailed:
		if err := leases.CompleteLocked(ctx, repos, lease, "SUCCESS", c); err != nil {
			return Aggregate{}, err
		}
		if err := setPRCIStatus(ctx, repos, *task.PRID, "FAILURE"); err != nil {
			return Aggregate{}, err
		}
		admittedTask, nextCycle, admitted, err := control.AdmitFixLocked(ctx, repos, locked, c)
		if err != nil {
			return Aggregate{}, err
		}
		if !admitted {
			break
		}
		updated, err := repos.UpdateTaskState(ctx, admittedTask, task.Generation, task.Generation, string(lifecycle.ChangesRequested))
		if err != nil {
			return Aggregate{}, err
		}
		if _, _, err := control.QueueFixLocked(ctx, repos, updated, nextCycle); err != nil {
			return Aggregate{}, err
		}
	case OutcomePending, OutcomeInconclusive:
		next := now.Add(config.PollInterval)
		if next.After(*deadline) {
			next = *deadline
		}
		if err := leases.RetryLocked(ctx, repos, lease, c, next, "SUCCESS"); err != nil {
			return Aggregate{}, err
		}
	}
	return aggregate, nil
}

var (
	// ErrCIFence indicates the task, lease, or tested snapshot is no longer current.
	ErrCIFence = errors.New("CI task, lease, or snapshot fence mismatch")
	// ErrCIState indicates CI work was requested outside WAITING_CI.
	ErrCIState = errors.New("task is not waiting for CI")
)

func validateCILease(ctx context.Context, repos *storage.Repositories, task storage.Task, lease leases.Lease, c clock.Clock) error {
	if lease.TaskID != task.ID || lease.Generation != task.Generation || lease.JobID == "" || lease.InferenceRequired || !sameSnapshot(lease.Snapshot, task.Snapshot) {
		return ErrCIFence
	}
	if err := leases.ValidateLocked(ctx, repos, lease, c); err != nil {
		return err
	}
	var operation string
	if err := repos.Queries().QueryRow(ctx, `SELECT operation_type FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid`, lease.JobID, task.ID).Scan(&operation); err != nil {
		return err
	}
	if operation != OperationCIReconcile {
		return ErrCIFence
	}
	return nil
}

func sameSnapshot(left, right contracts.Snapshot) bool {
	return left.HeadSHA == right.HeadSHA && left.BaseSHA == right.BaseSHA && left.IntegrationSHA == right.IntegrationSHA
}

func enqueueLogicalJob(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, operation string, remediationAttempt *int32) (storage.JobRecord, bool, error) {
	task := locked.Record()
	logicalKey := fmt.Sprintf("task:%s:generation:%d:%s", task.ID, task.Generation, operation)
	if remediationAttempt != nil {
		logicalKey = fmt.Sprintf("task:%s:generation:%d:cycle:%d:%s", task.ID, task.Generation, *remediationAttempt, operation)
	}
	jobID := stableUUID("job:" + logicalKey)
	job := contracts.Job{Version: 1, TaskID: task.ID, JobID: jobID, Generation: task.Generation, Snapshot: task.Snapshot, Attempt: 1, OperationID: stableUUID("operation:" + logicalKey), CorrelationID: stableUUID("correlation:" + logicalKey), Operation: operation}
	payload, err := json.Marshal(job)
	if err != nil {
		return storage.JobRecord{}, false, err
	}
	return repos.InsertJobAndOutbox(ctx, locked, storage.JobInput{ID: jobID, LogicalKey: logicalKey, OperationType: operation, PRID: task.PRID, Payload: payload, RemediationAttempt: remediationAttempt})
}

func stableUUID(value string) string {
	digest := sha256.Sum256([]byte("aprl.router.v1:" + value))
	id := digest[:16]
	id[6] = id[6]&0x0f | 0x50
	id[8] = id[8]&0x3f | 0x80
	encoded := hex.EncodeToString(id)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func setPRCIStatus(ctx context.Context, repos *storage.Repositories, prID int64, status string) error {
	command, err := repos.Queries().Exec(ctx, `UPDATE prs SET ci_status=$2 WHERE id=$1`, prID, status)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return storage.ErrNotFound
	}
	return nil
}

func aggregateSummary(aggregate Aggregate) string {
	parts := make([]string, 0, len(aggregate.Checks))
	for _, check := range aggregate.Checks {
		parts = append(parts, check.Name+":"+string(check.Outcome))
	}
	return strings.Join(parts, ",")
}

func ciCompletionRecorded(ctx context.Context, repos *storage.Repositories, task storage.Task, lease leases.Lease) (bool, error) {
	var jobStatus, operationType, runStatus, runTaskID, runJobID, runToken, agentType string
	var jobGeneration, runGeneration int64
	var runAttempt int32
	var head, base, integration *string
	err := repos.Queries().QueryRow(ctx, `SELECT j.status,j.generation,j.operation_type,ar.execution_status,ar.task_id::text,ar.job_id::text,ar.generation,ar.attempt_number,ar.lease_token::text,ar.agent_type,ar.expected_head_sha,ar.expected_base_sha,ar.expected_integration_sha FROM jobs j JOIN agent_runs ar ON ar.job_id=j.id WHERE j.id=$1::uuid AND ar.id=$2::uuid`, lease.JobID, lease.RunID).Scan(&jobStatus, &jobGeneration, &operationType, &runStatus, &runTaskID, &runJobID, &runGeneration, &runAttempt, &runToken, &agentType, &head, &base, &integration)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if jobStatus != "COMPLETED" || operationType != OperationCIReconcile || runStatus != "SUCCESS" || jobGeneration != task.Generation || runGeneration != task.Generation || runTaskID != task.ID || runJobID != lease.JobID || runAttempt != lease.RunAttempt || runToken != lease.Token || agentType != "A" || !equalNullableSHA(head, task.Snapshot.HeadSHA) || !equalNullableSHA(base, task.Snapshot.BaseSHA) || !equalNullableSHA(integration, task.Snapshot.IntegrationSHA) {
		return false, nil
	}
	var reviewExists bool
	if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE task_id=$1::uuid AND generation=$2 AND operation_type=$3)`, task.ID, task.Generation, OperationReview).Scan(&reviewExists); err != nil {
		return false, err
	}
	return reviewExists, nil
}

func equalNullableSHA(value *string, expected string) bool {
	if expected == "" {
		return value == nil
	}
	return value != nil && *value == expected
}
