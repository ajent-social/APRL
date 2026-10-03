// Package reconcile repairs durable projections lost across service and queue
// restarts. It treats PostgreSQL rows as authority and external messages as
// repeatable hints.
package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ajent-social/APRL/internal/broker"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/control"
	"github.com/ajent-social/APRL/internal/lifecycle"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrInvalid rejects invalid reconciliation dependencies or configuration.
	ErrInvalid = errors.New("invalid reconciliation configuration")
	// ErrNotReady reports unqualified startup process inventory.
	ErrNotReady = errors.New("process-hold inventory is not ready")
)

const maxBatchSize = 100

// Publisher republishes a stable durable job identity as a queue hint.
type Publisher interface {
	Publish(context.Context, string, string) (string, error)
}

// OperationReconciler only looks up an already admitted remote operation. The
// broker implementation never reissues the mutation from Reconcile.
type OperationReconciler interface {
	Reconcile(context.Context, string) (broker.Operation, error)
}

// HoldInventoryReady must fail until startup has inventoried every configured
// process-hold scope. It is checked before lease fencing, remote lookups, label
// output, or queue publication.
type HoldInventoryReady func(context.Context) error

// FenceExpiredTask applies the task-locked lease expiry transition. It must not
// release or delete process holds.
type FenceExpiredTask func(context.Context, *pgxpool.Pool, clock.Clock, string) error

// LabelIntent is a state-free request to make the host-derived lifecycle label
// output match the current durable task projection.
type LabelIntent struct {
	OutboxID   string
	TaskID     string
	Repository string
	PRID       int64
	PRNumber   int64
	Generation int64
	State      string
	Snapshot   contracts.Snapshot
	Label      string
}

// LabelOutputter may inspect and repair external labels, but must never update
// task state from an observed label. It is called without database locks.
type LabelOutputter interface {
	ApplyLifecycleLabel(context.Context, LabelIntent) error
}

// Config bounds each reconciliation pass and every external call.
type Config struct {
	BatchSize     int
	RPCTimeout    time.Duration
	MaxRetryDelay time.Duration
}

// Validate enforces finite batch, timeout and retry bounds.
func (c Config) Validate() error {
	if c.BatchSize < 1 || c.BatchSize > maxBatchSize || c.RPCTimeout <= 0 || c.RPCTimeout > 30*time.Second ||
		c.MaxRetryDelay <= 0 || c.MaxRetryDelay > time.Hour {
		return ErrInvalid
	}
	return nil
}

// Service owns only bounded reconciliation reads/projections. Its trusted
// callbacks and adapters are mandatory; there is no fake or live-provider
// default.
type Service struct {
	pool       *pgxpool.Pool
	clock      clock.Clock
	publisher  Publisher
	operations OperationReconciler
	holdReady  HoldInventoryReady
	fence      FenceExpiredTask
	labels     LabelOutputter
	config     Config

	cursorMu sync.Mutex
	cursors  map[string]string
}

// New requires explicit trusted repair dependencies and finite configuration.
func New(pool *pgxpool.Pool, clk clock.Clock, publisher Publisher, operations OperationReconciler,
	holdReady HoldInventoryReady, fence FenceExpiredTask, labels LabelOutputter, config Config) (*Service, error) {
	if pool == nil || isNil(clk) || isNil(publisher) || isNil(operations) || holdReady == nil || fence == nil || isNil(labels) {
		return nil, fmt.Errorf("database, clock, queue, broker, hold inventory, lease fence, and label outputter are required: %w", ErrInvalid)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Service{pool: pool, clock: clk, publisher: publisher, operations: operations, holdReady: holdReady,
		fence: fence, labels: labels, config: config, cursors: make(map[string]string)}, nil
}

// Report records bounded work attempted in one pass.
type Report struct {
	ExpiredTasks      int
	RemoteLookups     int
	MergedTasks       int
	LabelIntents      int
	PublishedJobHints int
}

// RunOnce checks startup hold inventory first, fences expired authority,
// reconciles only remote observations, repairs labels from durable state, and
// republishes due PENDING job IDs. The bounded keyset cursors prevent a
// persistent UNKNOWN operation or queue backlog from starving later rows.
func (s *Service) RunOnce(ctx context.Context) (Report, error) {
	var report Report
	if s == nil || ctx == nil || ctx.Err() != nil || s.pool == nil || s.clock == nil {
		return report, ErrInvalid
	}
	gateCtx, gateCancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	gateErr := s.holdReady(gateCtx)
	gateCtxErr := gateCtx.Err()
	gateCancel()
	if gateErr != nil || gateCtxErr != nil {
		cause := gateErr
		if cause == nil {
			cause = gateCtxErr
		}
		return report, fmt.Errorf("startup process-hold inventory: %w: %v", ErrNotReady, cause)
	}

	expired, err := s.expiredTaskIDs(ctx)
	if err != nil {
		return report, err
	}
	for _, taskID := range expired {
		callCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
		err := s.fence(callCtx, s.pool, s.clock, taskID)
		cancel()
		if err != nil {
			return report, fmt.Errorf("fence expired leases for task %s: %w", taskID, err)
		}
		report.ExpiredTasks++
	}

	operations, err := s.uncertainOperationIDs(ctx)
	if err != nil {
		return report, err
	}
	var firstErr error
	for _, operationID := range operations {
		callCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
		operation, reconcileErr := s.operations.Reconcile(callCtx, operationID)
		cancel()
		report.RemoteLookups++
		if reconcileErr != nil {
			if !errors.Is(reconcileErr, broker.ErrUnknown) && !errors.Is(reconcileErr, broker.ErrInFlight) && firstErr == nil {
				firstErr = fmt.Errorf("reconcile remote operation %s: %w", operationID, reconcileErr)
			}
			continue
		}
		if operation.Status == "CONFIRMED" && operation.Type == broker.ActionMerge {
			projected, projectErr := s.projectConfirmedMerge(ctx, operationID)
			if projectErr != nil && firstErr == nil {
				firstErr = fmt.Errorf("project confirmed merge %s: %w", operationID, projectErr)
			}
			if projected {
				report.MergedTasks++
			}
		}
	}

	// Retry a previously confirmed merge projection if an earlier DB attempt
	// failed after the remote receipt became durable.
	confirmed, err := s.confirmedMergeIDs(ctx)
	if err != nil {
		return report, err
	}
	for _, operationID := range confirmed {
		projected, projectErr := s.projectConfirmedMerge(ctx, operationID)
		if projectErr != nil && firstErr == nil {
			firstErr = fmt.Errorf("project confirmed merge %s: %w", operationID, projectErr)
		}
		if projected {
			report.MergedTasks++
		}
	}

	labelIDs, err := s.dueLabelIntentIDs(ctx)
	if err != nil {
		return report, err
	}
	for _, outboxID := range labelIDs {
		if err := s.applyLabelIntent(ctx, outboxID); err != nil && firstErr == nil {
			firstErr = err
		} else if err == nil {
			report.LabelIntents++
		}
	}

	jobIDs, err := s.duePendingJobIDs(ctx)
	if err != nil {
		return report, err
	}
	for _, jobID := range jobIDs {
		callCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
		_, publishErr := s.publisher.Publish(callCtx, queue.DispatchKind, jobID)
		cancel()
		if publishErr != nil {
			if retryErr := s.scheduleDispatchRepair(ctx, jobID); retryErr != nil {
				failure := fmt.Errorf("republish durable pending job %s failed: %w", jobID, errors.Join(publishErr, retryErr))
				if firstErr == nil {
					firstErr = failure
				} else {
					firstErr = errors.Join(firstErr, failure)
				}
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("republish durable pending job %s: %w", jobID, publishErr)
			}
			continue
		}
		if err := s.finishDispatchRepair(ctx, jobID); err != nil {
			ackErr := fmt.Errorf("acknowledge durable pending job repair %s: %w", jobID, err)
			if firstErr == nil {
				firstErr = ackErr
			} else {
				firstErr = errors.Join(firstErr, ackErr)
			}
			continue
		}
		report.PublishedJobHints++
	}
	return report, firstErr
}

func (s *Service) expiredTaskIDs(ctx context.Context) ([]string, error) {
	now := s.clock.Now().UTC()
	return s.pageIDs(ctx, "expired", func(cursor string) ([]string, error) {
		query := `SELECT DISTINCT task_id::text FROM jobs WHERE status='LEASED' AND lease_expires_at <= $1 AND task_id > $2::uuid ORDER BY task_id LIMIT $3`
		ids, err := s.queryIDs(ctx, query, now, nullableUUIDCursor(cursor), s.config.BatchSize)
		if err != nil || len(ids) != 0 || cursor == "" {
			return ids, err
		}
		return s.queryIDs(ctx, query, now, nullableUUIDCursor(""), s.config.BatchSize)
	})
}

func (s *Service) uncertainOperationIDs(ctx context.Context) ([]string, error) {
	return s.pageIDs(ctx, "operation", func(cursor string) ([]string, error) {
		query := `SELECT id::text FROM github_operations WHERE status IN ('UNKNOWN','IN_FLIGHT') AND id > $1::uuid ORDER BY id LIMIT $2`
		ids, err := s.queryIDs(ctx, query, nullableUUIDCursor(cursor), s.config.BatchSize)
		if err != nil || len(ids) != 0 || cursor == "" {
			return ids, err
		}
		return s.queryIDs(ctx, query, nullableUUIDCursor(""), s.config.BatchSize)
	})
}

func (s *Service) confirmedMergeIDs(ctx context.Context) ([]string, error) {
	return s.pageIDs(ctx, "merge", func(cursor string) ([]string, error) {
		query := `SELECT op.id::text FROM github_operations op JOIN tasks t ON t.id=op.task_id
			WHERE op.status='CONFIRMED' AND op.operation_type='merge' AND op.job_id IS NULL AND t.state='READY_TO_MERGE' AND op.id > $1::uuid
			ORDER BY op.id LIMIT $2`
		ids, err := s.queryIDs(ctx, query, nullableUUIDCursor(cursor), s.config.BatchSize)
		if err != nil || len(ids) != 0 || cursor == "" {
			return ids, err
		}
		return s.queryIDs(ctx, query, nullableUUIDCursor(""), s.config.BatchSize)
	})
}

func (s *Service) dueLabelIntentIDs(ctx context.Context) ([]string, error) {
	return s.pageIDs(ctx, "label", func(cursor string) ([]string, error) {
		query := `SELECT id::text FROM outbox WHERE kind='LABEL_SYNC' AND published_at IS NULL AND next_attempt_at <= $1 AND id > $2::uuid ORDER BY id LIMIT $3`
		ids, err := s.queryIDs(ctx, query, s.clock.Now().UTC(), nullableUUIDCursor(cursor), s.config.BatchSize)
		if err != nil || len(ids) != 0 || cursor == "" {
			return ids, err
		}
		return s.queryIDs(ctx, query, s.clock.Now().UTC(), nullableUUIDCursor(""), s.config.BatchSize)
	})
}

func (s *Service) duePendingJobIDs(ctx context.Context) ([]string, error) {
	return s.pageIDs(ctx, "job", func(cursor string) ([]string, error) {
		query := `SELECT j.id::text FROM jobs j JOIN tasks t ON t.id=j.task_id
		WHERE j.status='PENDING' AND j.generation=t.generation AND t.state NOT IN ('PAUSED','ESCALATED','MERGED','CLOSED')
			AND j.id > $2::uuid AND NOT EXISTS (
				SELECT 1 FROM outbox o WHERE o.task_id=j.task_id AND o.job_id=j.id AND o.kind='DISPATCH' AND o.published_at IS NULL AND o.next_attempt_at > $1
					AND COALESCE(o.payload->>'repair_generation',j.generation::text)=j.generation::text
				AND (o.payload->>'retry_from_run_id' IS NULL OR o.payload->>'retry_from_run_id'=(
					SELECT r.id::text FROM agent_runs r WHERE r.job_id=j.id ORDER BY r.attempt_number DESC LIMIT 1)))
			ORDER BY j.id LIMIT $3`
		ids, err := s.queryIDs(ctx, query, s.clock.Now().UTC(), nullableUUIDCursor(cursor), s.config.BatchSize)
		if err != nil || len(ids) != 0 || cursor == "" {
			return ids, err
		}
		return s.queryIDs(ctx, query, s.clock.Now().UTC(), nullableUUIDCursor(""), s.config.BatchSize)
	})
}

// scheduleDispatchRepair records bounded durable backoff after the direct
// Redis repair path fails. It performs only PostgreSQL work and never holds a
// transaction open across Publisher.Publish.
func (s *Service) scheduleDispatchRepair(ctx context.Context, jobID string) error {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.config.RPCTimeout)
	defer cancel()
	return storage.WithUnitOfWork(callCtx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		var taskID string
		if err := repos.Queries().QueryRow(ctx, `SELECT task_id::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&taskID); err != nil {
			return err
		}
		locked, err := repos.LockTask(ctx, taskID)
		if err != nil {
			return err
		}
		task := locked.Record()
		var jobGeneration int64
		var jobStatus string
		if err := repos.Queries().QueryRow(ctx, `SELECT generation,status FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`, jobID, taskID).
			Scan(&jobGeneration, &jobStatus); err != nil {
			return err
		}
		if jobStatus != "PENDING" || jobGeneration != task.Generation || !dispatchRepairTaskEligible(task.State) {
			return nil
		}
		now := s.clock.Now().UTC()
		var latestRunID *string
		if err := repos.Queries().QueryRow(ctx, `SELECT id::text FROM agent_runs WHERE job_id=$1::uuid ORDER BY attempt_number DESC LIMIT 1`, jobID).
			Scan(&latestRunID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var pendingRepairID string
		err = repos.Queries().QueryRow(ctx, `SELECT id::text FROM outbox
			WHERE task_id=$1::uuid AND job_id=$2::uuid AND kind='DISPATCH' AND published_at IS NULL AND next_attempt_at>$3
			AND COALESCE(payload->>'repair_generation',$4::bigint::text)=$4::bigint::text
			AND (payload->>'retry_from_run_id' IS NULL OR payload->>'retry_from_run_id'=$5)
			ORDER BY next_attempt_at DESC,created_at DESC,id DESC LIMIT 1 FOR UPDATE`, taskID, jobID, now, jobGeneration, latestRunID).
			Scan(&pendingRepairID)
		if err == nil {
			// Keep the existing later deadline: duplicate reconciliation workers
			// must not defeat a persisted backoff by repeatedly moving it.
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var repairID string
		var attempts int32
		err = repos.Queries().QueryRow(ctx, `SELECT id::text,attempt_count FROM outbox
			WHERE task_id=$1::uuid AND job_id=$2::uuid AND kind='DISPATCH' AND published_at IS NULL AND next_attempt_at<=$3
			AND COALESCE(payload->>'repair_generation',$4::bigint::text)=$4::bigint::text
			AND (payload->>'retry_from_run_id' IS NULL OR payload->>'retry_from_run_id'=$5)
			ORDER BY next_attempt_at,created_at DESC,id DESC LIMIT 1 FOR UPDATE`, taskID, jobID, now, jobGeneration, latestRunID).
			Scan(&repairID, &attempts)
		if errors.Is(err, pgx.ErrNoRows) {
			err = repos.Queries().QueryRow(ctx, `SELECT id::text,attempt_count FROM outbox
				WHERE task_id=$1::uuid AND job_id=$2::uuid AND kind='DISPATCH' AND published_at IS NOT NULL
				AND COALESCE(payload->>'repair_generation',$3::bigint::text)=$3::bigint::text
				ORDER BY published_at DESC,created_at DESC,id DESC LIMIT 1 FOR UPDATE`, taskID, jobID, jobGeneration).
				Scan(&repairID, &attempts)
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		nextAttemptAt := now.Add(retryDelay(attempts, s.config.MaxRetryDelay))
		if repairID == "" {
			_, err = repos.Queries().Exec(ctx, `INSERT INTO outbox(task_id,job_id,kind,payload,created_at,next_attempt_at,attempt_count)
				VALUES($1::uuid,$2::uuid,'DISPATCH',jsonb_build_object('job_id',$2::text,'repair_generation',$3::bigint,
				'retry_from_run_id',$4::text),$5,$6,1)`, taskID, jobID, jobGeneration, latestRunID, now, nextAttemptAt)
			return err
		}
		_, err = repos.Queries().Exec(ctx, `UPDATE outbox SET payload=payload||jsonb_build_object('job_id',$2::text,'repair_generation',$3::bigint,'retry_from_run_id',$6::text),
			published_at=NULL,attempt_count=CASE WHEN attempt_count<2147483647 THEN attempt_count+1 ELSE attempt_count END,next_attempt_at=$4
			WHERE id=$1::uuid AND task_id=$5::uuid AND job_id=$2::uuid AND kind='DISPATCH'`, repairID, jobID, jobGeneration, nextAttemptAt, taskID, latestRunID)
		return err
	})
}

// finishDispatchRepair acknowledges a repair intent only after Redis confirms
// the stable job hint. A pause racing the publish cannot revive the durable job:
// the consumer still consults the cancelled/stale PostgreSQL job row.
func (s *Service) finishDispatchRepair(ctx context.Context, jobID string) error {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.config.RPCTimeout)
	defer cancel()
	return storage.WithUnitOfWork(callCtx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		var taskID string
		var generation int64
		if err := repos.Queries().QueryRow(ctx, `SELECT task_id::text,generation FROM jobs WHERE id=$1::uuid`, jobID).Scan(&taskID, &generation); err != nil {
			return err
		}
		if _, err := repos.LockTask(ctx, taskID); err != nil {
			return err
		}
		var latestRunID *string
		if err := repos.Queries().QueryRow(ctx, `SELECT id::text FROM agent_runs WHERE job_id=$1::uuid ORDER BY attempt_number DESC LIMIT 1`, jobID).
			Scan(&latestRunID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		_, err := repos.Queries().Exec(ctx, `UPDATE outbox SET published_at=$4,attempt_count=CASE WHEN attempt_count<2147483647 THEN attempt_count+1 ELSE attempt_count END
			WHERE task_id=$1::uuid AND job_id=$2::uuid AND kind='DISPATCH' AND published_at IS NULL
			AND COALESCE(payload->>'repair_generation',$3::bigint::text)=$3::bigint::text
			AND (payload->>'retry_from_run_id' IS NULL OR payload->>'retry_from_run_id'=$5)`, taskID, jobID, generation, s.clock.Now().UTC(), latestRunID)
		return err
	})
}

func dispatchRepairTaskEligible(state string) bool {
	return state != "PAUSED" && state != "ESCALATED" && state != "MERGED" && state != "CLOSED"
}

func (s *Service) queryIDs(ctx context.Context, query string, args ...any) ([]string, error) {
	callCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	defer cancel()
	rows, err := s.pool.Query(callCtx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Service) pageIDs(_ context.Context, key string, fetch func(string) ([]string, error)) ([]string, error) {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	cursor := s.cursors[key]
	ids, err := fetch(cursor)
	if err != nil {
		return nil, fmt.Errorf("read %s reconciliation page: %w", key, err)
	}
	if len(ids) != 0 {
		s.cursors[key] = ids[len(ids)-1]
	} else {
		s.cursors[key] = ""
	}
	return ids, nil
}

func (s *Service) applyLabelIntent(ctx context.Context, outboxID string) error {
	var taskID string
	var raw []byte
	var attempts int32
	callCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	err := s.pool.QueryRow(callCtx, `SELECT task_id::text,payload,attempt_count FROM outbox WHERE id=$1::uuid AND kind='LABEL_SYNC' AND published_at IS NULL`, outboxID).
		Scan(&taskID, &raw, &attempts)
	cancel()
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read label intent %s: %w", outboxID, err)
	}
	var stored struct {
		TaskID     string `json:"task_id"`
		PRID       int64  `json:"pr_id"`
		Generation int64  `json:"generation"`
		State      string `json:"state"`
		Label      string `json:"label"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil || stored.TaskID != taskID || stored.PRID <= 0 || stored.Generation < 0 || !validLifecycleState(stored.State) {
		return s.retryLabel(ctx, outboxID, attempts, fmt.Errorf("malformed label intent"))
	}
	var intent LabelIntent
	callCtx, cancel = context.WithTimeout(ctx, s.config.RPCTimeout)
	err = s.pool.QueryRow(callCtx, `SELECT t.id::text,t.repo_full_name,t.state,t.generation,p.id,p.pr_number,
		p.head_sha,p.base_sha,COALESCE(p.integration_sha,'')
		FROM tasks t JOIN prs p ON p.task_id=t.id WHERE t.id=$1::uuid`, taskID).
		Scan(&intent.TaskID, &intent.Repository, &intent.State, &intent.Generation, &intent.PRID, &intent.PRNumber,
			&intent.Snapshot.HeadSHA, &intent.Snapshot.BaseSHA, &intent.Snapshot.IntegrationSHA)
	cancel()
	if errors.Is(err, pgx.ErrNoRows) {
		return s.markLabelDone(ctx, outboxID)
	}
	if err != nil {
		return fmt.Errorf("read current label target %s: %w", taskID, err)
	}
	intent.OutboxID, intent.Label = outboxID, lifecycleLabel(intent.State)
	callCtx, cancel = context.WithTimeout(ctx, s.config.RPCTimeout)
	err = s.labels.ApplyLifecycleLabel(callCtx, intent)
	cancel()
	if err != nil {
		return s.retryLabel(ctx, outboxID, attempts, fmt.Errorf("apply current lifecycle label for %s: %w", taskID, err))
	}
	return s.finishLabelIntent(ctx, outboxID, intent)
}

// finishLabelIntent reacquires task and PR locks only after external I/O. If
// the durable projection changed while that I/O was in flight, this row stays
// due so it can repair the newest projection even when a newer intent was
// already acknowledged by another reconciler.
func (s *Service) finishLabelIntent(ctx context.Context, outboxID string, applied LabelIntent) error {
	unitCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	defer cancel()
	return storage.WithUnitOfWork(unitCtx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		locked, err := repos.LockTask(ctx, applied.TaskID)
		if err != nil {
			return err
		}
		task := locked.Record()
		if task.PRID == nil {
			return markLabelDoneLocked(ctx, repos, outboxID, s.clock.Now().UTC())
		}
		var prNumber int64
		if err := repos.Queries().QueryRow(ctx, `SELECT pr_number FROM prs WHERE id=$1 FOR UPDATE`, *task.PRID).Scan(&prNumber); err != nil {
			return err
		}
		current := LabelIntent{TaskID: task.ID, Repository: task.RepoFullName, PRID: *task.PRID, PRNumber: prNumber,
			Generation: task.Generation, State: task.State, Snapshot: task.Snapshot}
		if !sameLabelProjection(applied, current) {
			now := s.clock.Now().UTC()
			tag, err := repos.Queries().Exec(ctx, `UPDATE outbox SET published_at=NULL,attempt_count=attempt_count+1,next_attempt_at=$2
				WHERE id=$1::uuid AND kind='LABEL_SYNC'`, outboxID, now)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 0 {
				return nil
			}
			payload, err := json.Marshal(struct {
				TaskID     string `json:"task_id"`
				PRID       int64  `json:"pr_id"`
				Generation int64  `json:"generation"`
				State      string `json:"state"`
				Label      string `json:"label"`
			}{TaskID: current.TaskID, PRID: current.PRID, Generation: current.Generation,
				State: current.State, Label: lifecycleLabel(current.State)})
			if err != nil {
				return err
			}
			_, err = repos.Queries().Exec(ctx, `INSERT INTO outbox(task_id,kind,payload,created_at,next_attempt_at)
				VALUES($1::uuid,'LABEL_SYNC',$2::jsonb,$3,$3)`, current.TaskID, string(payload), now)
			return err
		}
		return markLabelDoneLocked(ctx, repos, outboxID, s.clock.Now().UTC())
	})
}

func markLabelDoneLocked(ctx context.Context, repos *storage.Repositories, outboxID string, now time.Time) error {
	_, err := repos.Queries().Exec(ctx, `UPDATE outbox SET published_at=$2,attempt_count=attempt_count+1
		WHERE id=$1::uuid AND kind='LABEL_SYNC' AND published_at IS NULL`, outboxID, now)
	return err
}

func (s *Service) markLabelDone(ctx context.Context, outboxID string) error {
	callCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	defer cancel()
	_, err := s.pool.Exec(callCtx, `UPDATE outbox SET published_at=$2,attempt_count=attempt_count+1
		WHERE id=$1::uuid AND kind='LABEL_SYNC' AND published_at IS NULL`, outboxID, s.clock.Now().UTC())
	return err
}

func sameLabelProjection(left, right LabelIntent) bool {
	return left.TaskID == right.TaskID && left.Repository == right.Repository && left.PRID == right.PRID &&
		left.PRNumber == right.PRNumber && left.Generation == right.Generation && left.State == right.State &&
		left.Snapshot == right.Snapshot
}

func (s *Service) retryLabel(ctx context.Context, outboxID string, attempts int32, cause error) error {
	delay := retryDelay(attempts, s.config.MaxRetryDelay)
	callCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	defer cancel()
	_, err := s.pool.Exec(callCtx, `UPDATE outbox SET attempt_count=attempt_count+1,next_attempt_at=$2 WHERE id=$1::uuid AND kind='LABEL_SYNC' AND published_at IS NULL`, outboxID, s.clock.Now().UTC().Add(delay))
	if err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (s *Service) projectConfirmedMerge(ctx context.Context, operationID string) (bool, error) {
	projected := false
	unitCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	defer cancel()
	err := storage.WithUnitOfWork(unitCtx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		var ownerTask string
		if err := repos.Queries().QueryRow(ctx, `SELECT task_id::text FROM github_operations WHERE id=$1::uuid`, operationID).Scan(&ownerTask); err != nil {
			return err
		}
		locked, err := repos.LockTask(ctx, ownerTask)
		if err != nil {
			return err
		}
		task := locked.Record()
		var jobID, operationType, identity, status string
		var generation int64
		var requestJSON, resultJSON []byte
		err = repos.Queries().QueryRow(ctx, `SELECT COALESCE(job_id::text,''),generation,operation_type,identity,status,request,result
			FROM github_operations WHERE id=$1::uuid FOR UPDATE`, operationID).
			Scan(&jobID, &generation, &operationType, &identity, &status, &requestJSON, &resultJSON)
		if err != nil {
			return err
		}
		if task.State != string(lifecycle.ReadyToMerge) || task.PRID == nil || task.Generation != generation || jobID != "" ||
			operationType != string(broker.ActionMerge) || identity != "A" || status != "CONFIRMED" {
			return nil
		}
		var request struct {
			ExpectedHeadSHA string `json:"expected_head_sha"`
			BaseSHA         string `json:"base_sha"`
			IntegrationSHA  string `json:"integration_sha"`
			TargetBranch    string `json:"target_branch"`
		}
		var receipt broker.RemoteReceipt
		if json.Unmarshal(requestJSON, &request) != nil || json.Unmarshal(resultJSON, &receipt) != nil || !receipt.Merged ||
			request.ExpectedHeadSHA == "" || request.ExpectedHeadSHA != task.Snapshot.HeadSHA || request.BaseSHA != task.Snapshot.BaseSHA ||
			request.IntegrationSHA != task.Snapshot.IntegrationSHA || request.TargetBranch == "" || receipt.HeadSHA != request.ExpectedHeadSHA {
			return nil
		}
		var prRepo, prHead, prBase, prIntegration, baseRef string
		if err := repos.Queries().QueryRow(ctx, `SELECT repo_full_name,head_sha,base_sha,COALESCE(integration_sha,''),base_ref FROM prs WHERE id=$1 FOR UPDATE`, *task.PRID).
			Scan(&prRepo, &prHead, &prBase, &prIntegration, &baseRef); err != nil {
			return err
		}
		if prRepo != task.RepoFullName || prHead != request.ExpectedHeadSHA || prBase != request.BaseSHA ||
			prIntegration != request.IntegrationSHA || baseRef != request.TargetBranch {
			return nil
		}
		now := s.clock.Now().UTC()
		tag, err := repos.Queries().Exec(ctx, `UPDATE tasks SET state=$2,updated_at=$3 WHERE id=$1::uuid AND generation=$4 AND state=$5`,
			ownerTask, lifecycle.Merged, now, generation, lifecycle.ReadyToMerge)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return nil
		}
		payload, err := json.Marshal(struct {
			TaskID     string `json:"task_id"`
			PRID       int64  `json:"pr_id"`
			Generation int64  `json:"generation"`
			State      string `json:"state"`
			Label      string `json:"label"`
		}{TaskID: task.ID, PRID: *task.PRID, Generation: generation, State: string(lifecycle.Merged), Label: ""})
		if err != nil {
			return err
		}
		if _, err := repos.Queries().Exec(ctx, `INSERT INTO outbox(task_id,kind,payload,created_at,next_attempt_at)
			VALUES($1::uuid,'LABEL_SYNC',$2::jsonb,$3,$3)`, task.ID, string(payload), now); err != nil {
			return err
		}
		projected = true
		return nil
	})
	return projected, err
}

func validLifecycleState(state string) bool {
	switch lifecycle.State(state) {
	case lifecycle.Authoring, lifecycle.WaitingCI, lifecycle.InReview, lifecycle.ChangesRequested, lifecycle.Fixing,
		lifecycle.ReadyToMerge, lifecycle.Paused, lifecycle.Escalated, lifecycle.Merged, lifecycle.Closed:
		return true
	default:
		return false
	}
}

func lifecycleLabel(state string) string {
	switch lifecycle.State(state) {
	case lifecycle.Paused:
		return control.PauseLabel
	case lifecycle.Escalated:
		return "escalate:human"
	case lifecycle.Authoring:
		return "agent:authoring"
	case lifecycle.WaitingCI, lifecycle.InReview:
		return "agent:reviewing"
	case lifecycle.ChangesRequested:
		return "changes-requested"
	case lifecycle.Fixing:
		return "agent:fixing"
	case lifecycle.ReadyToMerge:
		return "ready-to-merge"
	default:
		return ""
	}
}

func retryDelay(attempts int32, maximum time.Duration) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	delay := time.Second
	for i := int32(0); i < attempts && delay < maximum; i++ {
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func nullableUUIDCursor(value string) any {
	if value == "" {
		return "00000000-0000-0000-0000-000000000000"
	}
	return value
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}
