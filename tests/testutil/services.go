// Package testutil provides fail-closed setup for tests that require APRL services.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// DatabaseFixture owns a dedicated per-test schema within the configured APRL test database.
type DatabaseFixture struct {
	Pool   *pgxpool.Pool
	Schema string
}

// RedisFixture owns the keys with its generated prefix in the configured APRL test Redis.
type RedisFixture struct {
	Client    *redis.Client
	KeyPrefix string
}

// RequireDatabase validates explicit ownership, connects to PostgreSQL, and creates a unique schema.
// Cleanup drops only that generated schema and closes this test's pool.
func RequireDatabase(t testing.TB) DatabaseFixture {
	t.Helper()
	requireOwner(t)
	databaseURL, err := config.DatabaseURLFromEnv(os.Getenv)
	if err != nil {
		t.Fatalf("PostgreSQL fixture setup failed: %v", err)
	}

	schema := "aprl_test_" + uniqueToken(t)
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("PostgreSQL fixture setup failed: cannot parse test database connection: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatalf("PostgreSQL fixture setup failed: cannot configure test database connection: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("PostgreSQL fixture setup failed: cannot connect to %s: %v", config.TestDatabaseURLEnv, err)
	}

	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		pool.Close()
		t.Fatalf("PostgreSQL fixture setup failed: cannot create owned schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("PostgreSQL fixture cleanup failed for owned schema %s: %v", schema, err)
		}
		pool.Close()
	})
	return DatabaseFixture{Pool: pool, Schema: schema}
}

// RequireRedis validates explicit ownership and connectivity, and returns a unique key prefix.
func RequireRedis(t testing.TB) RedisFixture {
	t.Helper()
	requireOwner(t)
	redisURL, err := config.RedisURLFromEnv(os.Getenv)
	if err != nil {
		t.Fatalf("Redis fixture setup failed: %v", err)
	}

	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("Redis fixture setup failed: invalid %s: %v", config.TestRedisURLEnv, err)
	}
	client := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("Redis fixture setup failed: cannot connect to %s: %v", config.TestRedisURLEnv, err)
	}
	prefix := "aprl_test:" + uniqueToken(t) + ":"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		var cursor uint64
		for {
			keys, next, err := client.Scan(cleanupCtx, cursor, prefix+"*", 100).Result()
			if err != nil {
				t.Errorf("Redis fixture cleanup failed for owned key prefix %s: %v", prefix, err)
				break
			}
			if len(keys) != 0 {
				if err := client.Del(cleanupCtx, keys...).Err(); err != nil {
					t.Errorf("Redis fixture cleanup failed for owned key prefix %s: %v", prefix, err)
					break
				}
			}
			if next == 0 {
				break
			}
			cursor = next
		}
		if err := client.Close(); err != nil {
			t.Errorf("Redis fixture client close failed: %v", err)
		}
	})
	return RedisFixture{Client: client, KeyPrefix: prefix}
}

// RequireServices is a convenience for tests requiring both backing services.
func RequireServices(t testing.TB) (DatabaseFixture, RedisFixture) {
	t.Helper()
	database := RequireDatabase(t)
	redisFixture := RequireRedis(t)
	return database, redisFixture
}

func requireOwner(t testing.TB) {
	t.Helper()
	if owner := os.Getenv("TEST_RESOURCE_OWNER"); owner == "" {
		t.Fatal("integration test setup failed: TEST_RESOURCE_OWNER must be nonempty before setting up APRL integration test resources")
	}
}

func uniqueToken(t testing.TB) string {
	t.Helper()
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("cannot allocate unique test resource name: %v", err)
	}
	return hex.EncodeToString(raw[:])
}
