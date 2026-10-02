package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	// ErrNotFound indicates that an addressed storage row does not exist.
	ErrNotFound = errors.New("storage row not found")
	// ErrLockOrder indicates the caller attempted to acquire locks out of order.
	ErrLockOrder = errors.New("storage lock order violation")
	// ErrStaleGeneration indicates the task generation fence no longer matches.
	ErrStaleGeneration = errors.New("stale task generation")
	// ErrDeliveryConflict indicates a delivery identifier was reused with new content.
	ErrDeliveryConflict = errors.New("webhook delivery ID reused with different content")
	// ErrDeliveryDisposition indicates a processed delivery has another disposition.
	ErrDeliveryDisposition = errors.New("webhook delivery already has a conflicting disposition")
	// ErrJobConflict indicates a logical key was reused with different job content.
	ErrJobConflict = errors.New("logical job key reused with different content")
	// ErrInvalidJob indicates a queued job failed storage or contract validation.
	ErrInvalidJob = errors.New("invalid storage job")
	// ErrInvalidDelivery indicates a webhook receipt is malformed.
	ErrInvalidDelivery = errors.New("invalid webhook delivery")
	// ErrLockHandle indicates a row lock handle is invalid for this unit of work.
	ErrLockHandle = errors.New("row lock handle does not belong to this unit of work")
)

// DBTX is transaction-bound SQL access. It deliberately omits transaction
// control so repositories cannot commit or roll back their caller's unit.
type DBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Repositories bind domain persistence to a single UnitOfWork transaction.
// Use Queries only for subsystem rows that are protected by a task lock; never
// make a related pool-level write outside the current unit of work.
type Repositories struct {
	tx               pgx.Tx
	queries          DBTX
	clock            clock.Clock
	lockedTasks      map[string]int64
	lockedOrgBudgets map[string]struct{}
}

// Queries returns SQL operations bound to the current unit of work. It has no
// Begin/Commit/Rollback methods. Call LockTask or LockOrgBudgetAndTask first
// before mutating rows related to a task. If direct queries change a locked
// task's generation or its PR snapshot, call LockTask again before using a
// task handle so its cached fence matches the transaction's current row.
func (r *Repositories) Queries() DBTX {
	return r.queries
}

// Task is a snapshot of the authoritative task row held under a row lock.
type Task struct {
	ID                  string
	OrgID               string
	RepoFullName        string
	SourceKey           string
	OwnerID             string
	State               string
	Generation          int64
	CycleCount          int32
	MaxReviewCycles     int32
	BudgetLimitMicroUSD int64
	SpentMicroUSD       int64
	ReservedMicroUSD    int64
	PolicyVersion       string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	PRID                *int64
	Snapshot            contracts.Snapshot
}

// LockedTask proves the task row was locked in the transaction that owns it.
// Its record is copied out so callers cannot mutate the tracked lock snapshot.
type LockedTask struct {
	record       Task
	repositories *Repositories
}

// Record returns a copy of the locked task snapshot.
func (l LockedTask) Record() Task {
	return l.record
}

// LockedOrgBudget proves the organization budget row was locked in this unit.
type LockedOrgBudget struct {
	record       OrgBudget
	repositories *Repositories
}

// Record returns a copy of the locked organization budget snapshot.
func (l LockedOrgBudget) Record() OrgBudget {
	return l.record
}

// OrgBudget is a snapshot of the organization-wide rolling budget row.
type OrgBudget struct {
	OrgID                string
	RollingLimitMicroUSD int64
	EmergencyMode        bool
}

// LockedSnapshot returns the snapshot values committed to an execution.
type LockedSnapshot struct {
	HeadSHA        string
	BaseSHA        string
	IntegrationSHA string
}

// DeliveryReceipt contains raw GitHub inbox identity and JSON payload. The
// normalized task-correlated event is created later by the router.
type DeliveryReceipt struct {
	DeliveryID string
	EventType  string
	Payload    json.RawMessage
}

// JobInput is a queued logical job and its transactional outbox dispatch.
// ID may be supplied by the caller; when empty, the validated contracts.Job
// payload's job_id is used. SourceDeliveryID is nil for timer-created work.
type JobInput struct {
	ID                 string
	LogicalKey         string
	OperationType      string
	PRID               *int64
	SourceDeliveryID   *string
	RemediationAttempt *int32
	Payload            json.RawMessage
}

// JobRecord returns the stable logical job identity after insertion/replay.
type JobRecord struct {
	ID            string
	TaskID        string
	LogicalKey    string
	OperationType string
	Generation    int64
	Snapshot      contracts.Snapshot
}

// LockTask obtains the task row lock. A task-only lock is suitable for state
// routing and may not be followed by an organization budget lock in this UOW.
func (r *Repositories) LockTask(ctx context.Context, taskID string) (LockedTask, error) {
	var task Task
	err := r.tx.QueryRow(ctx, `
		SELECT t.id::text, t.org_id, t.repo_full_name, t.source_key, t.owner_id, t.state,
		       t.generation, t.cycle_count, t.max_review_cycles, t.budget_limit_micro_usd,
		       t.spent_micro_usd, t.reserved_micro_usd, t.policy_version, t.created_at, t.updated_at
		FROM tasks t WHERE t.id = $1::uuid FOR UPDATE`, taskID).Scan(
		&task.ID, &task.OrgID, &task.RepoFullName, &task.SourceKey, &task.OwnerID, &task.State,
		&task.Generation, &task.CycleCount, &task.MaxReviewCycles, &task.BudgetLimitMicroUSD,
		&task.SpentMicroUSD, &task.ReservedMicroUSD, &task.PolicyVersion, &task.CreatedAt, &task.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return LockedTask{}, fmt.Errorf("lock task %q: %w", taskID, ErrNotFound)
	}
	if err != nil {
		return LockedTask{}, fmt.Errorf("lock task %q: %w", taskID, err)
	}
	var headSHA, baseSHA, integrationSHA *string
	err = r.tx.QueryRow(ctx, `SELECT id, head_sha, base_sha, integration_sha FROM prs WHERE task_id = $1::uuid FOR UPDATE`, taskID).Scan(&task.PRID, &headSHA, &baseSHA, &integrationSHA)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return LockedTask{}, fmt.Errorf("lock task %q attached PR snapshot: %w", taskID, err)
	}
	if err == nil {
		if headSHA != nil {
			task.Snapshot.HeadSHA = *headSHA
		}
		if baseSHA != nil {
			task.Snapshot.BaseSHA = *baseSHA
		}
		if integrationSHA != nil {
			task.Snapshot.IntegrationSHA = *integrationSHA
		}
	}
	r.lockedTasks[task.ID] = task.Generation
	return LockedTask{record: task, repositories: r}, nil
}

// LockOrgBudgetAndTask locks the organization row first, then the task row, in
// the global admission order required by RFC-0001 and ADR 003.
func (r *Repositories) LockOrgBudgetAndTask(ctx context.Context, orgID, taskID string) (LockedOrgBudget, LockedTask, error) {
	if len(r.lockedTasks) != 0 {
		if _, alreadyLocked := r.lockedOrgBudgets[orgID]; !alreadyLocked {
			return LockedOrgBudget{}, LockedTask{}, ErrLockOrder
		}
	}
	var budget OrgBudget
	err := r.tx.QueryRow(ctx, `
		SELECT org_id, rolling_limit_micro_usd, emergency_mode
		FROM org_budgets WHERE org_id = $1 FOR UPDATE`, orgID).Scan(
		&budget.OrgID, &budget.RollingLimitMicroUSD, &budget.EmergencyMode)
	if errors.Is(err, pgx.ErrNoRows) {
		return LockedOrgBudget{}, LockedTask{}, fmt.Errorf("lock organization budget %q: %w", orgID, ErrNotFound)
	}
	if err != nil {
		return LockedOrgBudget{}, LockedTask{}, fmt.Errorf("lock organization budget %q: %w", orgID, err)
	}
	r.lockedOrgBudgets[budget.OrgID] = struct{}{}
	task, err := r.LockTask(ctx, taskID)
	if err != nil {
		return LockedOrgBudget{}, LockedTask{}, err
	}
	if task.record.OrgID != budget.OrgID {
		return LockedOrgBudget{}, LockedTask{}, fmt.Errorf("task %q is not owned by organization %q: %w", taskID, orgID, ErrNotFound)
	}
	return LockedOrgBudget{record: budget, repositories: r}, task, nil
}

// RequireTaskGeneration is a post-lock fence for generation-sensitive work.
// It must be used before task-related writes and never trusts a Redis message.
func (r *Repositories) RequireTaskGeneration(ctx context.Context, locked LockedTask, expected int64) error {
	if err := r.validateLockedTask(locked); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if expected < 0 || r.lockedTasks[locked.record.ID] != expected {
		return ErrStaleGeneration
	}
	return nil
}

// UpdateTaskState changes a locked task's lifecycle and generation. Generation
// may stay fixed or advance exactly once; stale or detached handles reject.
func (r *Repositories) UpdateTaskState(ctx context.Context, locked LockedTask, expectedGeneration, nextGeneration int64, nextState string) (LockedTask, error) {
	if err := r.RequireTaskGeneration(ctx, locked, expectedGeneration); err != nil {
		return LockedTask{}, err
	}
	if nextGeneration < expectedGeneration || nextGeneration-expectedGeneration > 1 || strings.TrimSpace(nextState) == "" {
		return LockedTask{}, fmt.Errorf("update task state: invalid next generation or state")
	}
	now := r.clock.Now().UTC()
	updated, err := scanTask(r.tx.QueryRow(ctx, `
		UPDATE tasks SET state = $3, generation = $4, updated_at = $5
		WHERE id = $1::uuid AND generation = $2
		RETURNING id::text, org_id, repo_full_name, source_key, owner_id, state,
		          generation, cycle_count, max_review_cycles, budget_limit_micro_usd,
		          spent_micro_usd, reserved_micro_usd, policy_version, created_at, updated_at`,
		locked.record.ID, expectedGeneration, nextState, nextGeneration, now))
	if errors.Is(err, pgx.ErrNoRows) {
		return LockedTask{}, ErrStaleGeneration
	}
	if err != nil {
		return LockedTask{}, fmt.Errorf("update task %q state: %w", locked.record.ID, err)
	}
	updated.PRID = locked.record.PRID
	updated.Snapshot = locked.record.Snapshot
	r.lockedTasks[updated.ID] = updated.Generation
	return LockedTask{record: updated, repositories: r}, nil
}

// StoreWebhookDelivery inserts an inbox record once. It returns false for a
// semantically identical JSONB replay and ErrDeliveryConflict when the same
// GitHub delivery ID is reused with a different event or payload.
func (r *Repositories) StoreWebhookDelivery(ctx context.Context, receipt DeliveryReceipt) (bool, error) {
	if strings.TrimSpace(receipt.DeliveryID) == "" || len(receipt.DeliveryID) > 100 || strings.TrimSpace(receipt.EventType) == "" || !jsonObject(receipt.Payload) {
		return false, ErrInvalidDelivery
	}
	now := r.clock.Now().UTC()
	var inserted string
	err := r.tx.QueryRow(ctx, `
		INSERT INTO webhook_deliveries (delivery_id, event_type, payload, received_at, disposition)
		VALUES ($1, $2, $3::jsonb, $4, 'INBOX')
		ON CONFLICT (delivery_id) DO NOTHING
		RETURNING delivery_id`, receipt.DeliveryID, receipt.EventType, string(receipt.Payload), now).Scan(&inserted)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("store webhook delivery %q: %w", receipt.DeliveryID, err)
	}
	var identical bool
	err = r.tx.QueryRow(ctx, `
		SELECT event_type = $2 AND payload = $3::jsonb
		FROM webhook_deliveries WHERE delivery_id = $1 FOR UPDATE`,
		receipt.DeliveryID, receipt.EventType, string(receipt.Payload)).Scan(&identical)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("read webhook delivery replay %q: %w", receipt.DeliveryID, ErrNotFound)
	}
	if err != nil {
		return false, fmt.Errorf("compare webhook delivery replay %q: %w", receipt.DeliveryID, err)
	}
	if !identical {
		return false, ErrDeliveryConflict
	}
	return false, nil
}

// SetDeliveryDisposition marks the inbox row terminal inside the same UOW as
// task, job and outbox changes. INBOX remains retryable until that commit.
// Repeating the same final disposition is idempotent; changing it is rejected.
func (r *Repositories) SetDeliveryDisposition(ctx context.Context, deliveryID, disposition string) error {
	disposition = strings.TrimSpace(disposition)
	if deliveryID == "" || disposition == "" || disposition == "INBOX" {
		return fmt.Errorf("set webhook disposition: delivery ID and final disposition are required")
	}
	command, err := r.tx.Exec(ctx, `
		UPDATE webhook_deliveries
		SET disposition = $2, processed_at = $3, routing_attempts = routing_attempts + 1
		WHERE delivery_id = $1 AND disposition = 'INBOX' AND processed_at IS NULL`,
		deliveryID, disposition, r.clock.Now().UTC())
	if err != nil {
		return fmt.Errorf("set webhook delivery %q disposition: %w", deliveryID, err)
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	var existingDisposition string
	var processedAt *time.Time
	err = r.tx.QueryRow(ctx, `SELECT disposition, processed_at FROM webhook_deliveries WHERE delivery_id = $1 FOR UPDATE`, deliveryID).Scan(&existingDisposition, &processedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("set webhook delivery %q disposition: %w", deliveryID, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("read webhook delivery %q disposition: %w", deliveryID, err)
	}
	if processedAt != nil && existingDisposition == disposition {
		return nil
	}
	return fmt.Errorf("delivery %q has disposition %q (processed=%t): %w", deliveryID, existingDisposition, processedAt != nil, ErrDeliveryDisposition)
}

// InsertJobAndOutbox stores a strict queued contracts.Job and a dispatch hint
// atomically. The task handle proves the lock and current generation fence.
func (r *Repositories) InsertJobAndOutbox(ctx context.Context, locked LockedTask, input JobInput) (JobRecord, bool, error) {
	if err := r.validateLockedTask(locked); err != nil {
		return JobRecord{}, false, err
	}
	if err := r.RequireTaskGeneration(ctx, locked, locked.record.Generation); err != nil {
		return JobRecord{}, false, err
	}
	if strings.TrimSpace(input.LogicalKey) == "" || strings.TrimSpace(input.OperationType) == "" {
		return JobRecord{}, false, ErrInvalidJob
	}
	contractJob, err := contracts.DecodeJob(input.Payload)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("decode job contract: %w", err)
	}
	if contractJob.LeaseToken != "" || contractJob.RunID != "" || contractJob.TaskID != locked.record.ID || contractJob.Generation != locked.record.Generation || contractJob.Operation != input.OperationType {
		return JobRecord{}, false, ErrInvalidJob
	}
	if input.ID != "" && input.ID != contractJob.JobID {
		return JobRecord{}, false, fmt.Errorf("requested job ID does not match payload job ID: %w", ErrInvalidJob)
	}
	if input.RemediationAttempt != nil && *input.RemediationAttempt < 1 {
		return JobRecord{}, false, ErrInvalidJob
	}
	if input.PRID != nil && *input.PRID <= 0 {
		return JobRecord{}, false, ErrInvalidJob
	}
	if input.PRID != nil && (locked.record.PRID == nil || *input.PRID != *locked.record.PRID) {
		return JobRecord{}, false, ErrInvalidJob
	}
	if input.PRID == nil && locked.record.PRID != nil {
		prID := *locked.record.PRID
		input.PRID = &prID
	}
	if input.SourceDeliveryID != nil && strings.TrimSpace(*input.SourceDeliveryID) == "" {
		return JobRecord{}, false, ErrInvalidJob
	}
	if !sameSnapshot(contractJob.Snapshot, locked.recordSnapshot()) {
		return JobRecord{}, false, ErrInvalidJob
	}
	input.ID = contractJob.JobID
	var prID, sourceDeliveryID, remediationAttempt any
	if input.PRID != nil {
		prID = *input.PRID
	}
	if input.SourceDeliveryID != nil {
		sourceDeliveryID = *input.SourceDeliveryID
	}
	if input.RemediationAttempt != nil {
		remediationAttempt = *input.RemediationAttempt
	}
	var insertedID string
	err = r.tx.QueryRow(ctx, `
		INSERT INTO jobs (id, task_id, pr_id, source_delivery_id, logical_key, operation_type,
		                  generation, expected_head_sha, expected_base_sha, remediation_attempt, payload)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
		ON CONFLICT (logical_key) DO NOTHING
		RETURNING id::text`, input.ID, locked.record.ID, prID, sourceDeliveryID, input.LogicalKey,
		input.OperationType, contractJob.Generation, nullableSHA(contractJob.Snapshot.HeadSHA),
		nullableSHA(contractJob.Snapshot.BaseSHA), remediationAttempt, string(input.Payload)).Scan(&insertedID)
	if err == nil {
		if err := r.insertDispatchOutbox(ctx, locked.record.ID, insertedID); err != nil {
			return JobRecord{}, false, err
		}
		return JobRecord{ID: insertedID, TaskID: locked.record.ID, LogicalKey: input.LogicalKey, OperationType: input.OperationType, Generation: contractJob.Generation, Snapshot: contractJob.Snapshot}, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return JobRecord{}, false, fmt.Errorf("insert logical job %q: %w", input.LogicalKey, err)
	}
	var existing JobRecord
	var existingPRID *int64
	var existingDeliveryID *string
	var existingAttempt *int32
	var existingHeadSHA, existingBaseSHA *string
	var samePayload bool
	err = r.tx.QueryRow(ctx, `
		SELECT id::text, task_id::text, logical_key, operation_type, generation,
		       pr_id, source_delivery_id, remediation_attempt, expected_head_sha, expected_base_sha,
		       payload = $2::jsonb
		FROM jobs WHERE logical_key = $1 FOR UPDATE`, input.LogicalKey, string(input.Payload)).Scan(
		&existing.ID, &existing.TaskID, &existing.LogicalKey, &existing.OperationType,
		&existing.Generation, &existingPRID, &existingDeliveryID, &existingAttempt,
		&existingHeadSHA, &existingBaseSHA, &samePayload)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("read existing logical job %q: %w", input.LogicalKey, err)
	}
	if existing.ID != input.ID || existing.TaskID != locked.record.ID || existing.OperationType != input.OperationType || existing.Generation != contractJob.Generation || !equalNullable(existingPRID, input.PRID) || !equalNullable(existingDeliveryID, input.SourceDeliveryID) || !equalNullable(existingAttempt, input.RemediationAttempt) || !equalNullable(existingHeadSHA, nullableString(contractJob.Snapshot.HeadSHA)) || !equalNullable(existingBaseSHA, nullableString(contractJob.Snapshot.BaseSHA)) || !samePayload {
		return JobRecord{}, false, ErrJobConflict
	}
	var outboxExists bool
	if err := r.tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM outbox WHERE job_id = $1::uuid AND kind = 'DISPATCH')`, existing.ID).Scan(&outboxExists); err != nil {
		return JobRecord{}, false, fmt.Errorf("check dispatch outbox for job %q: %w", existing.ID, err)
	}
	if !outboxExists {
		if err := r.insertDispatchOutbox(ctx, locked.record.ID, existing.ID); err != nil {
			return JobRecord{}, false, err
		}
	}
	existing.Snapshot = contractJob.Snapshot
	return existing, false, nil
}

func (r *Repositories) insertDispatchOutbox(ctx context.Context, taskID, jobID string) error {
	_, err := r.tx.Exec(ctx, `
		INSERT INTO outbox (task_id, job_id, kind, payload, created_at)
		VALUES ($1::uuid, $2::uuid, 'DISPATCH', jsonb_build_object('job_id', $2::uuid), $3)`,
		taskID, jobID, r.clock.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert dispatch outbox for job %q: %w", jobID, err)
	}
	return nil
}

func (r *Repositories) validateLockedTask(locked LockedTask) error {
	if locked.repositories != r {
		return ErrLockHandle
	}
	if generation, exists := r.lockedTasks[locked.record.ID]; !exists || generation != locked.record.Generation {
		return ErrLockHandle
	}
	return nil
}

func scanTask(row pgx.Row) (Task, error) {
	var task Task
	err := row.Scan(&task.ID, &task.OrgID, &task.RepoFullName, &task.SourceKey, &task.OwnerID,
		&task.State, &task.Generation, &task.CycleCount, &task.MaxReviewCycles,
		&task.BudgetLimitMicroUSD, &task.SpentMicroUSD, &task.ReservedMicroUSD,
		&task.PolicyVersion, &task.CreatedAt, &task.UpdatedAt)
	return task, err
}

func (l LockedTask) recordSnapshot() contracts.Snapshot {
	return l.record.Snapshot
}

func sameSnapshot(left, right contracts.Snapshot) bool {
	return left.HeadSHA == right.HeadSHA && left.BaseSHA == right.BaseSHA && left.IntegrationSHA == right.IntegrationSHA
}

func nullableSHA(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func equalNullable[T comparable](left, right *T) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func jsonObject(payload []byte) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(payload, &object) == nil && object != nil
}
