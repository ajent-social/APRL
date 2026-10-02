package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WithUnitOfWork executes all state, disposition, job, outbox and caller SQL
// changes in one PostgreSQL transaction. Caller queries must use Repositories.Queries.
func WithUnitOfWork(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, fn func(context.Context, *Repositories) error) error {
	if ctx == nil || pool == nil || c == nil || fn == nil {
		return fmt.Errorf("unit of work requires context, pool, clock, and callback")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin unit of work: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	repos := &Repositories{tx: tx, queries: tx, clock: c, lockedTasks: make(map[string]int64), lockedOrgBudgets: make(map[string]struct{})}
	if err := fn(ctx, repos); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit unit of work: %w", err)
	}
	return nil
}
