package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrMalformedDelivery reports a stream entry with invalid required fields.
	ErrMalformedDelivery = errors.New("malformed queue delivery")
	// ErrUnsupportedKind leaves unsupported side-effect hints pending.
	ErrUnsupportedKind = errors.New("unsupported queue delivery kind")
	// ErrLeaseActive means a valid live PostgreSQL owner prevented admission.
	ErrLeaseActive = errors.New("logical job still has an active PostgreSQL lease")
	// ErrNotDisposed means no durable disposition permits transport acknowledgement.
	ErrNotDisposed = errors.New("logical job has no durable terminal disposition")
	// ErrInferenceAdmissionRequired keeps inference jobs pending without a budget adapter.
	ErrInferenceAdmissionRequired = errors.New("paid inference is fail-closed pending budget admission")
	// ErrReservationMissing reports an admission adapter that did not persist coverage.
	ErrReservationMissing = errors.New("inference run has no durable RESERVED budget reservation")
)

// Executor hands a newly claimed lease to the next host-owned execution layer.
// Returning nil is insufficient for XACK; the durable job must be terminal.
type Executor func(context.Context, leases.Lease) error

// BudgetAdmission atomically reserves coverage for a newly created run.
// It must not start a process or make provider requests; only Executor starts work.
// Errors never authorize execution; the host reconciles any persisted reservation
// before allowing a durable retry, including an ambiguous commit response.
type BudgetAdmission func(context.Context, leases.Lease, contracts.Job) error

// ConsumerConfig carries host identity and bounded lease settings.
type ConsumerConfig struct {
	LeaseTTL           time.Duration
	ReclaimIdle        time.Duration
	PromptHash         string
	SupervisorIdentity string
	CredentialID       string
	BatchSize          int64
	Block              time.Duration
	OperationTimeout   time.Duration
	BudgetAdmission    BudgetAdmission
}

// Consumer binds Redis transport to durable Postgres job authority.
type Consumer struct {
	pool     *pgxpool.Pool
	clock    clock.Clock
	streams  *Streams
	config   ConsumerConfig
	executor Executor
	budget   BudgetAdmission
}

// NewConsumer creates a consumer that never trusts Redis fields as job authority.
func NewConsumer(pool *pgxpool.Pool, c clock.Clock, streams *Streams, config ConsumerConfig, executor Executor) (*Consumer, error) {
	if pool == nil || c == nil || streams == nil || executor == nil || config.LeaseTTL <= 0 || config.ReclaimIdle < 0 ||
		config.BatchSize < 1 || config.BatchSize > maxReadCount || config.Block < 0 || config.Block > maxReadBlock || config.OperationTimeout <= 0 ||
		!validHash(config.PromptHash) || strings.TrimSpace(config.SupervisorIdentity) == "" || strings.TrimSpace(config.CredentialID) == "" {
		return nil, ErrInvalid
	}
	return &Consumer{pool: pool, clock: c, streams: streams, config: config, executor: executor, budget: config.BudgetAdmission}, nil
}

// Handle checks durable state, claims the Postgres lease, and acknowledges only
// after a terminal durable job disposition is visible.
func (c *Consumer) Handle(ctx context.Context, consumer string, delivery Delivery) error {
	if c == nil || ctx == nil || ctx.Err() != nil || strings.TrimSpace(consumer) == "" {
		return ErrInvalid
	}
	if delivery.EntryID == "" || !validUUID(delivery.JobID) {
		return fmt.Errorf("entry %q: %w", delivery.EntryID, ErrMalformedDelivery)
	}
	if delivery.Kind != DispatchKind {
		return fmt.Errorf("entry %q kind %q: %w", delivery.EntryID, delivery.Kind, ErrUnsupportedKind)
	}
	workCtx, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
	defer cancel()
	state, err := c.readJob(workCtx, delivery.JobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("job %s is not durably known: %w", delivery.JobID, ErrNotDisposed)
		}
		return err
	}
	if terminal(state.Status) {
		return c.streams.Ack(workCtx, delivery.EntryID)
	}
	if state.Status == "LEASED" && state.LeaseExpires.After(c.clock.Now().UTC()) {
		return ErrLeaseActive
	}
	job, decodeErr := contracts.DecodeJob(state.Payload)
	if decodeErr != nil {
		if err := c.reject(workCtx, delivery.JobID, state.TaskID, "REJECTED", false); err != nil {
			return fmt.Errorf("durably reject malformed job: %w", err)
		}
		return c.streams.Ack(workCtx, delivery.EntryID)
	}
	if job.TaskID != state.TaskID || job.JobID != delivery.JobID || job.Operation != state.OperationType {
		if err := c.reject(workCtx, delivery.JobID, state.TaskID, "REJECTED", false); err != nil {
			return fmt.Errorf("durably reject mismatched job: %w", err)
		}
		return c.streams.Ack(workCtx, delivery.EntryID)
	}
	if state.Generation != job.Generation || state.TaskGeneration != job.Generation || state.Snapshot != job.Snapshot || deniedState(state.TaskState) {
		if err := c.reject(workCtx, delivery.JobID, state.TaskID, "CANCELLED", true); err != nil {
			return fmt.Errorf("durably reject stale job: %w", err)
		}
		return c.streams.Ack(workCtx, delivery.EntryID)
	}
	agentType, supported := leases.AgentTypeForOperation(state.OperationType)
	if !supported {
		if err := c.reject(workCtx, delivery.JobID, state.TaskID, "REJECTED", false); err != nil {
			return fmt.Errorf("durably reject unsupported job: %w", err)
		}
		return c.streams.Ack(workCtx, delivery.EntryID)
	}
	requiresInference := leases.RequiresInference(job.Operation)
	if requiresInference && c.budget == nil {
		return ErrInferenceAdmissionRequired
	}
	lease, err := leases.Claim(workCtx, c.pool, c.clock, leases.ClaimRequest{TaskID: state.TaskID, JobID: delivery.JobID,
		TTL: c.config.LeaseTTL, AgentType: agentType, PromptHash: c.config.PromptHash,
		SupervisorIdentity: c.config.SupervisorIdentity, CredentialID: c.config.CredentialID})
	if err != nil {
		if errors.Is(err, leases.ErrBusy) {
			return ErrLeaseActive
		}
		if errors.Is(err, leases.ErrStale) {
			if err := c.reject(workCtx, delivery.JobID, state.TaskID, "CANCELLED", true); err != nil {
				return fmt.Errorf("durably reject stale claim: %w", err)
			}
			return c.streams.Ack(workCtx, delivery.EntryID)
		}
		return fmt.Errorf("claim Postgres execution lease: %w", err)
	}
	if requiresInference {
		if err := c.budget(workCtx, lease, job); err != nil {
			if cleanupErr := c.disposeUnstarted(workCtx, lease); cleanupErr != nil {
				return fmt.Errorf("budget admission denied (%v); dispose unstarted admission: %w", err, cleanupErr)
			}
			return fmt.Errorf("budget admission denied before execution: %w", err)
		}
		reserved, err := c.hasReservation(workCtx, lease, job)
		if err != nil || !reserved {
			if cleanupErr := c.disposeUnstarted(workCtx, lease); cleanupErr != nil {
				return fmt.Errorf("reservation verification failed (%v); dispose unstarted admission: %w", err, cleanupErr)
			}
			if err != nil {
				return err
			}
			return ErrReservationMissing
		}
	}
	if err := leases.Validate(workCtx, c.pool, c.clock, lease); err != nil {
		if cleanupErr := c.disposeUnstarted(workCtx, lease); cleanupErr != nil {
			return fmt.Errorf("execution handoff denied (%v); dispose unstarted admission: %w", err, cleanupErr)
		}
		return fmt.Errorf("validate execution handoff after admission: %w", err)
	}
	callbackErr := c.executor(workCtx, lease)
	finalState, readErr := c.readJob(workCtx, delivery.JobID)
	if readErr != nil {
		return fmt.Errorf("read job disposition after execution handoff: %w", readErr)
	}
	if !terminal(finalState.Status) {
		if finalState.Status == "PENDING" {
			valid, err := c.hasRetryDisposition(workCtx, delivery.JobID, lease.RunID)
			if err != nil {
				return err
			}
			if valid {
				return c.streams.Ack(workCtx, delivery.EntryID)
			}
		}
		if callbackErr != nil {
			return fmt.Errorf("execution handoff failed: %w", callbackErr)
		}
		return ErrNotDisposed
	}
	return c.streams.Ack(workCtx, delivery.EntryID)
}

// disposeUnstarted is host evidence: Handle has not invoked the executor.
// Recorded launch, billing, result or mutation evidence always retains coverage.
func (c *Consumer) disposeUnstarted(ctx context.Context, lease leases.Lease) error {
	// A cancelled admission context must not prevent bounded host cleanup.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.OperationTimeout)
	defer cancel()
	return storage.WithUnitOfWork(cleanupCtx, c.pool, c.clock, func(ctx context.Context, repos *storage.Repositories) error {
		var orgID string
		if err := repos.Queries().QueryRow(ctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, lease.TaskID).Scan(&orgID); err != nil {
			return err
		}
		_, task, err := repos.LockOrgBudgetAndTask(ctx, orgID, lease.TaskID)
		if err != nil {
			return err
		}
		var jobStatus, jobToken string
		if err := repos.Queries().QueryRow(ctx, `SELECT status,COALESCE(lease_token::text,'') FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`, lease.JobID, lease.TaskID).Scan(&jobStatus, &jobToken); err != nil {
			return err
		}
		var runStatus string
		var evidence bool
		if err := repos.Queries().QueryRow(ctx, `SELECT r.execution_status,
			r.process_id IS NOT NULL OR EXISTS(SELECT 1 FROM cost_entries ce JOIN budget_reservations b ON b.id=ce.reservation_id WHERE b.run_id=r.id)
			OR EXISTS(SELECT 1 FROM worker_result_receipts w WHERE w.run_id=r.id)
			OR EXISTS(SELECT 1 FROM github_operations op WHERE op.request->>'run_id'=r.id::text)
			FROM agent_runs r WHERE r.id=$1::uuid AND r.task_id=$2::uuid AND r.job_id=$3::uuid
			AND r.lease_token=$4::uuid AND r.generation=$5 AND r.attempt_number=$6
			AND r.supervisor_identity=$7 AND r.supervisor_credential_id=$8 FOR UPDATE OF r`,
			lease.RunID, lease.TaskID, lease.JobID, lease.Token, lease.Generation, lease.RunAttempt,
			c.config.SupervisorIdentity, c.config.CredentialID).Scan(&runStatus, &evidence); err != nil {
			return err
		}
		now := c.clock.Now().UTC()
		if runStatus == "RUNNING" {
			if _, err := repos.Queries().Exec(ctx, `UPDATE agent_runs SET execution_status='TERMINATED',finished_at=$2 WHERE id=$1::uuid`, lease.RunID, now); err != nil {
				return err
			}
		}
		var reservationID string
		err = repos.Queries().QueryRow(ctx, `SELECT id::text FROM budget_reservations WHERE run_id=$1::uuid`, lease.RunID).Scan(&reservationID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil {
			if evidence || (runStatus != "RUNNING" && runStatus != "TERMINATED") {
				if err := budget.MarkUnknownLocked(ctx, repos, c.clock, reservationID); err != nil {
					return err
				}
			} else if err := budget.SettleReservationLocked(ctx, repos, c.clock, budget.SettlementRequest{ReservationID: reservationID, RunID: lease.RunID, FinalUsageKnown: true}); err != nil {
				return err
			}
		}
		// Never overwrite a replacement owner or resurrect revoked work.
		if jobStatus == "LEASED" && jobToken == lease.Token {
			status := "CANCELLED"
			if !deniedState(task.Record().State) && task.Record().Generation == lease.Generation && task.Record().Snapshot == lease.Snapshot {
				status = "PENDING"
			}
			if _, err := repos.Queries().Exec(ctx, `UPDATE jobs SET status=$2,lease_token=NULL,lease_expires_at=NULL WHERE id=$1::uuid AND lease_token=$3::uuid`, lease.JobID, status, lease.Token); err != nil {
				return err
			}
			if status == "PENDING" {
				if _, err := repos.Queries().Exec(ctx, `INSERT INTO outbox(task_id,job_id,kind,payload,created_at,next_attempt_at) VALUES($1::uuid,$2::uuid,'DISPATCH',jsonb_build_object('job_id',$2::text,'retry_from_run_id',$3::text),$4,$5)`, lease.TaskID, lease.JobID, lease.RunID, now, now.Add(time.Millisecond)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (c *Consumer) hasReservation(ctx context.Context, lease leases.Lease, job contracts.Job) (bool, error) {
	var payload json.RawMessage
	var cost int64
	var pricingVersion string
	err := c.pool.QueryRow(ctx, `SELECT admission_envelope,envelope_micro_usd,pricing_version
		FROM budget_reservations WHERE task_id=$1::uuid AND run_id=$2::uuid AND status='RESERVED'`, lease.TaskID, lease.RunID).
		Scan(&payload, &cost, &pricingVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("verify inference reservation: %w", err)
	}
	var envelope contracts.BudgetEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return false, fmt.Errorf("decode inference reservation envelope: %w", err)
	}
	if envelope.Validate() != nil || envelope.MaxCostMicroUSD != cost || envelope.PricingVersion != pricingVersion {
		return false, nil
	}
	if job.Envelope != (contracts.BudgetEnvelope{}) && job.Envelope != envelope {
		return false, nil
	}
	return true, nil
}

// Run creates the group if needed, reclaims idle PEL entries, then reads new
// entries. Expected per-entry admission/disposition errors remain pending
// without stopping unrelated work. Infrastructure and unexpected executor
// errors stop the loop so the host can reconcile or restart it.
func (c *Consumer) Run(ctx context.Context, consumer string) error {
	if c == nil || ctx == nil || strings.TrimSpace(consumer) == "" {
		return ErrInvalid
	}
	if err := c.streams.EnsureGroup(ctx); err != nil {
		return err
	}
	cursor := "0-0"
	for ctx.Err() == nil {
		deliveries, next, err := c.streams.Reclaim(ctx, consumer, c.config.ReclaimIdle, cursor, c.config.BatchSize)
		if err != nil && ctx.Err() == nil {
			return err
		}
		if next != "" {
			cursor = next
		}
		for _, delivery := range deliveries {
			if err := c.Handle(ctx, consumer, delivery); err != nil && !pendingDeliveryError(err) && ctx.Err() == nil {
				return err
			}
		}
		fresh, err := c.streams.Read(ctx, consumer, c.config.Block, c.config.BatchSize)
		if err != nil && ctx.Err() == nil {
			return err
		}
		for _, delivery := range fresh {
			if err := c.Handle(ctx, consumer, delivery); err != nil && !pendingDeliveryError(err) && ctx.Err() == nil {
				return err
			}
		}
	}
	return ctx.Err()
}

func pendingDeliveryError(err error) bool {
	return errors.Is(err, ErrLeaseActive) || errors.Is(err, ErrMalformedDelivery) ||
		errors.Is(err, ErrUnsupportedKind) || errors.Is(err, ErrNotDisposed) ||
		errors.Is(err, ErrInferenceAdmissionRequired) || errors.Is(err, ErrReservationMissing) ||
		errors.Is(err, leases.ErrNotDue) || errors.Is(err, leases.ErrStale) ||
		errors.Is(err, budget.ErrEmergency) || errors.Is(err, budget.ErrTaskBudget) ||
		errors.Is(err, budget.ErrOrganizationBudget) || errors.Is(err, budget.ErrInvalidEnvelope) ||
		errors.Is(err, budget.ErrReservationConflict) || errors.Is(err, budget.ErrAdmission)
}

type jobState struct {
	TaskID         string
	Status         string
	OperationType  string
	Generation     int64
	TaskGeneration int64
	TaskState      string
	Snapshot       contracts.Snapshot
	LeaseExpires   time.Time
	Payload        []byte
}

func (c *Consumer) readJob(ctx context.Context, jobID string) (jobState, error) {
	var state jobState
	var head, base, integration *string
	err := c.pool.QueryRow(ctx, `SELECT j.task_id::text,j.status,j.operation_type,j.generation,t.generation,t.state,
		COALESCE(j.lease_expires_at,'epoch'::timestamptz),j.payload,p.head_sha,p.base_sha,p.integration_sha
		FROM jobs j JOIN tasks t ON t.id=j.task_id LEFT JOIN prs p ON p.task_id=t.id WHERE j.id=$1::uuid`, jobID).
		Scan(&state.TaskID, &state.Status, &state.OperationType, &state.Generation, &state.TaskGeneration, &state.TaskState, &state.LeaseExpires, &state.Payload, &head, &base, &integration)
	if err != nil {
		return jobState{}, err
	}
	if head != nil {
		state.Snapshot.HeadSHA = *head
	}
	if base != nil {
		state.Snapshot.BaseSHA = *base
	}
	if integration != nil {
		state.Snapshot.IntegrationSHA = *integration
	}
	return state, nil
}

func (c *Consumer) reject(ctx context.Context, jobID, taskID, status string, onlyStale bool) error {
	return storage.WithUnitOfWork(ctx, c.pool, c.clock, func(ctx context.Context, repos *storage.Repositories) error {
		task, err := repos.LockTask(ctx, taskID)
		if err != nil {
			return err
		}
		row := repos.Queries()
		var jobStatus string
		var generation int64
		if err := row.QueryRow(ctx, `SELECT status,generation FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`, jobID, taskID).Scan(&jobStatus, &generation); err != nil {
			return err
		}
		if terminal(jobStatus) {
			return nil
		}
		if jobStatus == "LEASED" {
			var expires time.Time
			var token string
			if err := row.QueryRow(ctx, `SELECT lease_expires_at,lease_token::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&expires, &token); err != nil {
				return err
			}
			if expires.After(c.clock.Now().UTC()) {
				return ErrLeaseActive
			}
			now := c.clock.Now().UTC()
			if _, err := row.Exec(ctx, `UPDATE agent_runs SET execution_status='TERMINATED',finished_at=$3
				WHERE job_id=$1::uuid AND lease_token=$2::uuid AND execution_status='RUNNING'`, jobID, token, now); err != nil {
				return err
			}
			tag, err := row.Exec(ctx, `UPDATE jobs SET status=$2,lease_token=NULL,lease_expires_at=NULL
				WHERE id=$1::uuid AND status='LEASED' AND lease_token=$3::uuid AND lease_expires_at <= $4`, jobID, status, token, now)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return ErrNotDisposed
			}
			return nil
		}
		if onlyStale && generation == task.Record().Generation && !deniedState(task.Record().State) {
			var head, base, integration *string
			if err := row.QueryRow(ctx, `SELECT head_sha,base_sha,integration_sha FROM prs WHERE task_id=$1::uuid`, taskID).Scan(&head, &base, &integration); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			var snapshot contracts.Snapshot
			if head != nil {
				snapshot.HeadSHA = *head
			}
			if base != nil {
				snapshot.BaseSHA = *base
			}
			if integration != nil {
				snapshot.IntegrationSHA = *integration
			}
			if snapshot == task.Record().Snapshot {
				return ErrNotDisposed
			}
		}
		_, err = row.Exec(ctx, `UPDATE jobs SET status=$2,lease_token=NULL,lease_expires_at=NULL WHERE id=$1::uuid AND status='PENDING'`, jobID, status)
		return err
	})
}

func terminal(status string) bool {
	return status == "COMPLETED" || status == "FAILED" || status == "CANCELLED" || status == "REJECTED"
}

func (c *Consumer) hasRetryDisposition(ctx context.Context, jobID, runID string) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM agent_runs r JOIN outbox o ON o.task_id=r.task_id AND o.job_id=r.job_id
		WHERE r.id=$2::uuid AND r.job_id=$1::uuid AND r.execution_status <> 'RUNNING'
		AND o.kind='DISPATCH' AND o.payload->>'retry_from_run_id'=r.id::text
	)`, jobID, runID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check durable retry disposition: %w", err)
	}
	return exists, nil
}

func deniedState(state string) bool {
	return state == "PAUSED" || state == "ESCALATED" || state == "MERGED" || state == "CLOSED"
}

func validHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
