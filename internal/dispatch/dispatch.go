// Package dispatch publishes durable Postgres outbox work to its owned transport.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultBatchLimit = 32
	defaultTimeout    = 3 * time.Second
	maxRetryDelay     = 5 * time.Minute
)

var (
	// ErrInvalid reports invalid dispatcher configuration or durable outbox input.
	ErrInvalid = errors.New("invalid outbox dispatch")
	// ErrNoHandler reports fail-closed handling of a non-DISPATCH outbox kind.
	ErrNoHandler = errors.New("no handler for outbox kind")
)

// OutboxItem is one durable outbox row. Payload is copied before it is exposed.
type OutboxItem struct {
	ID        string
	TaskID    string
	JobID     string
	Kind      string
	Payload   json.RawMessage
	Attempts  int32
	CreatedAt time.Time
}

// Handler processes non-DISPATCH outbox work idempotently. It is invoked while
// only that outbox row is locked and must honor the supplied bounded context.
type Handler func(context.Context, OutboxItem) error

// Dispatcher publishes due outbox rows. Redis remains a hint; Postgres owns due time.
type Dispatcher struct {
	pool          *pgxpool.Pool
	clock         clock.Clock
	streams       *queue.Streams
	handlers      map[string]Handler
	batchLimit    int
	operationTime time.Duration
}

// NewDispatcher constructs a bounded dispatcher. Optional handlers are keyed
// by CANCEL, LABEL_SYNC, or NOTIFY; missing handlers fail closed and retry.
func NewDispatcher(pool *pgxpool.Pool, c clock.Clock, streams *queue.Streams, handlers map[string]Handler) (*Dispatcher, error) {
	if pool == nil || c == nil || streams == nil {
		return nil, ErrInvalid
	}
	copyHandlers := make(map[string]Handler, len(handlers))
	for kind, handler := range handlers {
		if kind == queue.DispatchKind || (kind != "CANCEL" && kind != "LABEL_SYNC" && kind != "NOTIFY") || handler == nil {
			return nil, ErrInvalid
		}
		copyHandlers[kind] = handler
	}
	return &Dispatcher{pool: pool, clock: c, streams: streams, handlers: copyHandlers,
		batchLimit: defaultBatchLimit, operationTime: defaultTimeout}, nil
}

// DispatchDue publishes at most limit due rows. XADD and published_at update
// are bounded in one transaction holding only the outbox row lock; a commit
// failure may duplicate the same stable job UUID, which consumers deduplicate.
func (d *Dispatcher) DispatchDue(ctx context.Context, limit int) (int, error) {
	if d == nil || ctx == nil || ctx.Err() != nil || limit < 1 || limit > d.batchLimit {
		return 0, ErrInvalid
	}
	processed := 0
	var firstErr error
	for processed < limit {
		rowCtx, cancel := context.WithTimeout(ctx, d.operationTime)
		tx, err := d.pool.Begin(rowCtx)
		if err != nil {
			cancel()
			return processed, fmt.Errorf("begin outbox dispatch: %w", err)
		}
		var item OutboxItem
		err = tx.QueryRow(rowCtx, `SELECT id::text,task_id::text,COALESCE(job_id::text,''),kind,payload,attempt_count,created_at
			FROM outbox WHERE published_at IS NULL AND next_attempt_at <= $1
			ORDER BY next_attempt_at,created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`, d.clock.Now().UTC()).
			Scan(&item.ID, &item.TaskID, &item.JobID, &item.Kind, &item.Payload, &item.Attempts, &item.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			dispatchRollback(tx)
			cancel()
			return processed, firstErr
		}
		if err != nil {
			dispatchRollback(tx)
			cancel()
			return processed, fmt.Errorf("read due outbox row: %w", err)
		}
		operationErr := d.process(rowCtx, item)
		if operationErr != nil {
			if firstErr == nil {
				firstErr = operationErr
			}
			delay := retryDelay(item.Attempts)
			if _, err := tx.Exec(rowCtx, `UPDATE outbox SET attempt_count=attempt_count+1,next_attempt_at=$2 WHERE id=$1::uuid`, item.ID, d.clock.Now().UTC().Add(delay)); err != nil {
				dispatchRollback(tx)
				cancel()
				return processed, fmt.Errorf("reschedule failed outbox row: %w", err)
			}
		} else {
			if _, err := tx.Exec(rowCtx, `UPDATE outbox SET published_at=$2,attempt_count=attempt_count+1 WHERE id=$1::uuid AND published_at IS NULL`, item.ID, d.clock.Now().UTC()); err != nil {
				dispatchRollback(tx)
				cancel()
				return processed, fmt.Errorf("mark outbox row published: %w", err)
			}
		}
		if err := tx.Commit(rowCtx); err != nil {
			dispatchRollback(tx)
			cancel()
			return processed, fmt.Errorf("commit outbox dispatch: %w", err)
		}
		cancel()
		processed++
	}
	return processed, firstErr
}

func dispatchRollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (d *Dispatcher) process(ctx context.Context, item OutboxItem) error {
	if item.Kind == queue.DispatchKind {
		if !validUUID(item.JobID) {
			return fmt.Errorf("outbox %s has invalid dispatch job identity: %w", item.ID, ErrInvalid)
		}
		_, err := d.streams.Publish(ctx, item.Kind, item.JobID)
		return err
	}
	handler := d.handlers[item.Kind]
	if handler == nil {
		return fmt.Errorf("outbox %s kind %q: %w", item.ID, item.Kind, ErrNoHandler)
	}
	return handler(ctx, item)
}

func retryDelay(attempts int32) time.Duration {
	if attempts >= 16 {
		return maxRetryDelay
	}
	multiplier := math.Pow(2, float64(attempts))
	delay := time.Second * time.Duration(multiplier)
	if delay > maxRetryDelay {
		return maxRetryDelay
	}
	return delay
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || strings.TrimSpace(value) != value {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return false
	}
	return value != "00000000-0000-0000-0000-000000000000"
}
