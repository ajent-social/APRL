// Package config validates configuration shared by APRL processes and tests.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

const (
	// TestDatabaseURLEnv names the required PostgreSQL integration test URL.
	TestDatabaseURLEnv = "TEST_DATABASE_URL"
	// TestRedisURLEnv names the required Redis integration test URL.
	TestRedisURLEnv = "TEST_REDIS_URL"
)

// ServiceURLs contains the URLs for the isolated services used by integration tests.
type ServiceURLs struct {
	Database string
	Redis    string
}

// ServiceURLsFromEnv requires both test service URLs and validates their schemes and hosts.
func ServiceURLsFromEnv(getenv func(string) string) (ServiceURLs, error) {
	databaseURL, err := DatabaseURLFromEnv(getenv)
	if err != nil {
		return ServiceURLs{}, err
	}
	redisURL, err := RedisURLFromEnv(getenv)
	if err != nil {
		return ServiceURLs{}, err
	}
	return ServiceURLs{Database: databaseURL, Redis: redisURL}, nil
}

// DatabaseURLFromEnv independently requires and validates the PostgreSQL test URL.
func DatabaseURLFromEnv(getenv func(string) string) (string, error) {
	return requiredURL(getenv, TestDatabaseURLEnv, "postgres", "postgresql")
}

// RedisURLFromEnv independently requires and validates the Redis test URL.
func RedisURLFromEnv(getenv func(string) string) (string, error) {
	return requiredURL(getenv, TestRedisURLEnv, "redis", "rediss")
}

// TestServiceURLsFromEnv validates the required integration service URLs in the process environment.
func TestServiceURLsFromEnv() (ServiceURLs, error) {
	return ServiceURLsFromEnv(os.Getenv)
}

func requiredURL(getenv func(string) string, name string, schemes ...string) (string, error) {
	if getenv == nil {
		return "", fmt.Errorf("cannot read required integration service URL %s: environment reader is nil", name)
	}
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return "", fmt.Errorf("required integration service URL %s is unset", name)
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || !contains(schemes, parsed.Scheme) {
		return "", fmt.Errorf("%s must be an absolute URL using %s", name, strings.Join(schemes, " or "))
	}
	return value, nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
