// Package storage provides APRL's durable PostgreSQL schema operations.
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ajent-social/APRL/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationAdvisoryLock int64 = 4_120_260_104

// Migrate applies the embedded core migration atomically and safely under
// concurrent service startup. The caller controls the connection search_path.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("migrate APRL schema: nil PostgreSQL pool")
	}

	sql, err := migrations.ReadCore()
	if err != nil {
		return fmt.Errorf("read core migration: %w", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin schema migration transaction: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationAdvisoryLock); err != nil {
		return fmt.Errorf("lock schema migration: %w", err)
	}
	var ledgerExists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&ledgerExists); err != nil {
		return fmt.Errorf("inspect schema migration ledger: %w", err)
	}
	if ledgerExists {
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, migrations.CoreVersion).Scan(&applied); err != nil {
			return fmt.Errorf("read schema migration ledger: %w", err)
		}
		if applied {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit schema migration check: %w", err)
			}
			return nil
		}
	}

	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("apply core schema migration %s: %w", migrations.CoreVersion, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit core schema migration %s: %w", migrations.CoreVersion, err)
	}
	return nil
}
