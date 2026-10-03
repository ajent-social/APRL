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
		return fmt.Errorf("inspect migration ledger: %w", err)
	}
	planSQL, err := migrations.ReadPlanTasks()
	if err != nil {
		return fmt.Errorf("read plan task migration: %w", err)
	}
	holdSQL, err := migrations.ReadProcessHolds()
	if err != nil {
		return fmt.Errorf("read process hold migration: %w", err)
	}
	for _, migration := range []struct {
		version string
		sql     []byte
	}{{migrations.CoreVersion, sql}, {"002_plan_tasks", planSQL}, {"003_process_holds", holdSQL}} {
		applied := false
		if ledgerExists {
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, migration.version).Scan(&applied); err != nil {
				return fmt.Errorf("read migration ledger: %w", err)
			}
		}
		if applied {
			continue
		}
		if _, err := tx.Exec(ctx, string(migration.sql)); err != nil {
			return fmt.Errorf("apply migration %s: %w", migration.version, err)
		}
		ledgerExists = true
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1) ON CONFLICT DO NOTHING`, migration.version); err != nil {
			return fmt.Errorf("record migration %s: %w", migration.version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}
