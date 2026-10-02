package unit

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/ajent-social/APRL/internal/config"
	"github.com/ajent-social/APRL/tests/testutil"
)

func TestBootstrap(t *testing.T) {
	if os.Getenv("APRL_TEST_HELPER_CHILD") == "1" {
		testutil.RequireDatabase(t)
		return
	}

	t.Run("validates isolated service URLs without connecting", func(t *testing.T) {
		databaseURL := "postgres://aprl:unused@127.0.0.1:5432/aprl_test"
		redisURL := "redis://127.0.0.1:6379/0"
		services, err := config.ServiceURLsFromEnv(func(name string) string {
			switch name {
			case config.TestDatabaseURLEnv:
				return databaseURL
			case config.TestRedisURLEnv:
				return redisURL
			default:
				return ""
			}
		})
		if err != nil {
			t.Fatalf("ServiceURLsFromEnv() error = %v", err)
		}
		if services.Database != databaseURL || services.Redis != redisURL {
			t.Fatalf("ServiceURLsFromEnv() = %+v, want supplied isolated URLs", services)
		}
	})

	t.Run("rejects missing or malformed URLs", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			databaseURL string
			redisURL    string
			wantError   string
		}{
			{name: "missing database", redisURL: "redis://localhost/0", wantError: config.TestDatabaseURLEnv},
			{name: "missing redis", databaseURL: "postgres://localhost/aprl", wantError: config.TestRedisURLEnv},
			{name: "malformed database", databaseURL: "http://localhost/db", redisURL: "redis://localhost/0", wantError: "must be an absolute URL"},
			{name: "malformed redis", databaseURL: "postgres://localhost/aprl", redisURL: "http://localhost/0", wantError: "must be an absolute URL"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := config.ServiceURLsFromEnv(func(name string) string {
					switch name {
					case config.TestDatabaseURLEnv:
						return tc.databaseURL
					case config.TestRedisURLEnv:
						return tc.redisURL
					default:
						return ""
					}
				})
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("ServiceURLsFromEnv() = (%+v, %v), want error containing %q", got, err, tc.wantError)
				}
			})
		}
	})

	t.Run("fixture setup fails instead of skipping when service URLs are absent", func(t *testing.T) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestBootstrap$")
		cmd.Env = append(filteredEnv(os.Environ(), "TEST_DATABASE_URL", "TEST_REDIS_URL"),
			"APRL_TEST_HELPER_CHILD=1", "TEST_RESOURCE_OWNER=aprl_unit_test")
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatal("child test succeeded without required service URLs")
		}
		want := "PostgreSQL fixture setup failed: required integration service URL " + config.TestDatabaseURLEnv + " is unset"
		if !strings.Contains(string(output), want) {
			t.Fatalf("child failure did not explain the missing service URL; wanted %q:\n%s", want, output)
		}
	})
}

func filteredEnv(env []string, keys ...string) []string {
	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		remove := false
		for _, key := range keys {
			if strings.HasPrefix(entry, key+"=") {
				remove = true
				break
			}
		}
		if !remove {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}
