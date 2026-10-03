package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/app"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	healthWebhookSecret = "api-health-test-webhook-secret"
	healthAuthToken     = "api-health-test-supervisor-token"
)

type healthStartupRecovery struct {
	mu         sync.Mutex
	recoverErr error
	readyErr   error
	recovered  bool
}

func (r *healthStartupRecovery) Recover(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recoverErr != nil {
		return r.recoverErr
	}
	r.recovered = true
	return nil
}

func (r *healthStartupRecovery) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.recovered {
		return errors.New("recovery has not completed")
	}
	return r.readyErr
}

type healthSupervisorAuthenticator struct{}

func (healthSupervisorAuthenticator) AuthenticateSupervisor(context.Context, *http.Request) (results.Principal, error) {
	return results.Principal{TaskID: "health-test-task", RunID: "health-test-run", Identity: "health-test-host", CredentialID: "health-test-credential"}, nil
}

type healthResponse struct {
	Status       string   `json:"status"`
	Dependencies []string `json:"dependencies"`
}

func TestHealth(t *testing.T) {
	database := testutil.RequireDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := storage.Migrate(ctx, database.Pool); err != nil {
		t.Fatalf("migrate API health fixture: %v", err)
	}

	t.Run("health_routes_report_liveness_and_ready_state", func(t *testing.T) {
		recovery := &healthStartupRecovery{}
		service := healthNewAPI(t, database.Pool, recovery)
		server, stop := healthStartAPI(t, service, recovery)
		defer stop()

		healthRequire(t, server.Client(), server.URL+"/healthz", http.StatusOK, "alive", nil)
		healthRequire(t, server.Client(), server.URL+"/readyz", http.StatusOK, "ready", nil)
		resultResponse, err := server.Client().Get(server.URL + "/internal/results")
		if err != nil {
			t.Fatalf("request API results route: %v", err)
		}
		if err := resultResponse.Body.Close(); err != nil {
			t.Errorf("close API results response: %v", err)
		}
		if resultResponse.StatusCode == http.StatusNotFound {
			t.Fatal("API role does not expose authenticated results route")
		}

		recovery.setReadyError(errors.New("password=not-for-health-response"))
		response := healthRequest(t, server.Client(), server.URL+"/readyz")
		if response.StatusCode != http.StatusServiceUnavailable || response.Body.Status != "not_ready" {
			t.Fatalf("unavailable recovery readiness = (%d,%+v), want 503 not_ready", response.StatusCode, response.Body)
		}
		if strings.Join(response.Body.Dependencies, ",") != "startup_recovery" {
			t.Fatalf("not-ready dependencies = %v, want [startup_recovery]", response.Body.Dependencies)
		}
		if !healthSorted(response.Body.Dependencies) {
			t.Fatalf("dependency names are not sorted: %v", response.Body.Dependencies)
		}
		for _, private := range []string{"password=not-for-health-response", healthWebhookSecret, healthAuthToken, "postgres://"} {
			if strings.Contains(string(response.Raw), private) {
				t.Fatalf("readiness response disclosed %q: %s", private, response.Raw)
			}
		}
		healthRequire(t, server.Client(), server.URL+"/healthz", http.StatusOK, "alive", nil)
	})

	t.Run("postgres_unavailable_keeps_liveness_alive", func(t *testing.T) {
		unavailableConfig, err := pgxpool.ParseConfig("postgres://aprl-health-test@127.0.0.1:1/aprl_health_test?connect_timeout=1&sslmode=disable")
		if err != nil {
			t.Fatalf("configure isolated unavailable PostgreSQL client: %v", err)
		}
		unavailablePool, err := pgxpool.NewWithConfig(ctx, unavailableConfig)
		if err != nil {
			t.Fatalf("create isolated unavailable PostgreSQL client: %v", err)
		}
		t.Cleanup(unavailablePool.Close)
		recovery := &healthStartupRecovery{recovered: true}
		service := healthNewAPI(t, unavailablePool, recovery)
		server := httptest.NewServer(service.app.Handler())
		t.Cleanup(server.Close)

		healthRequire(t, server.Client(), server.URL+"/healthz", http.StatusOK, "alive", nil)
		healthRequire(t, server.Client(), server.URL+"/readyz", http.StatusServiceUnavailable, "not_ready", []string{"postgres", "startup_recovery"})
	})

	t.Run("fresh_recovery_readiness_blocks_webhook_mutation", func(t *testing.T) {
		recovery := &healthStartupRecovery{}
		service := healthNewAPI(t, database.Pool, recovery)
		server, stop := healthStartAPI(t, service, recovery)
		defer stop()
		recovery.setReadyError(errors.New("inventory evidence expired"))

		payload := []byte(`{"action":"opened","repository":{"full_name":"owner/repo"}}`)
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/webhooks/github", bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("construct webhook request: %v", err)
		}
		request.Header.Set("X-Hub-Signature-256", healthSign(payload))
		request.Header.Set("X-GitHub-Delivery", "health-recovery-unavailable")
		request.Header.Set("X-GitHub-Event", "issues")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("send webhook while recovery readiness is unavailable: %v", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read/close webhook response: read=%v close=%v", readErr, closeErr)
		}
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("webhook during unavailable recovery returned %d body %s, want 503", response.StatusCode, body)
		}
		var count int
		if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM webhook_deliveries WHERE delivery_id=$1`, "health-recovery-unavailable").Scan(&count); err != nil {
			t.Fatalf("count webhook delivery after readiness failure: %v", err)
		}
		if count != 0 {
			t.Fatalf("unready API persisted %d webhook deliveries, want none", count)
		}
	})

	t.Run("constructors_reject_missing_or_typed_nil_dependencies", func(t *testing.T) {
		valid := healthAPIDependencies(database.Pool, &healthStartupRecovery{})
		if _, err := app.NewAPI(healthAppConfig(), app.APIDependencies{Pool: database.Pool, Clock: valid.Clock, RouterConfig: valid.RouterConfig}); err == nil {
			t.Fatal("NewAPI accepted missing webhook secret, supervisor authenticator, and startup recovery")
		}
		var typedNilRecovery *healthStartupRecovery
		missingRecovery := healthAPIDependencies(database.Pool, typedNilRecovery)
		if _, err := app.NewAPI(healthAppConfig(), missingRecovery); err == nil {
			t.Fatal("NewAPI accepted typed-nil startup recovery")
		}
		var typedNilAuthenticator *healthSupervisorAuthenticator
		typedNilAuth := healthAPIDependencies(database.Pool, &healthStartupRecovery{})
		typedNilAuth.SupervisorAuth = typedNilAuthenticator
		if _, err := app.NewAPI(healthAppConfig(), typedNilAuth); err == nil {
			t.Fatal("NewAPI accepted typed-nil supervisor authenticator")
		}
	})
}

type healthService struct {
	app     *app.App
	address string
}

func healthNewAPI(t *testing.T, pool *pgxpool.Pool, recovery app.StartupRecovery) healthService {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve API listener address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved API listener: %v", err)
	}
	config := healthAppConfig()
	config.ListenAddr = address
	service, err := app.NewAPI(config, healthAPIDependencies(pool, recovery))
	if err != nil {
		t.Fatalf("construct API application: %v", err)
	}
	return healthService{app: service, address: address}
}

func healthAppConfig() app.Config {
	return app.Config{
		Role: app.RoleAPI, ListenAddr: "127.0.0.1:0",
		StartupTimeout: 2 * time.Second, RecoveryTimeout: time.Second, RPCDeadline: 250 * time.Millisecond,
		PollInterval: 50 * time.Millisecond, ShutdownTimeout: time.Second, ReadinessTimeout: 250 * time.Millisecond,
		ReadinessPollInterval: 10 * time.Millisecond, MaxRequestBodyBytes: 1 << 20, DispatchBatch: 1,
		ConsumerName: "api-health-test",
	}
}

func healthAPIDependencies(pool *pgxpool.Pool, recovery app.StartupRecovery) app.APIDependencies {
	return app.APIDependencies{
		Pool: pool, Clock: clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)),
		RouterConfig: router.Config{Repositories: map[string]router.RepositoryPolicy{"owner/repo": {OrgID: "health-test-org", PolicyVersion: "v1", TaskBudgetLimitMicroUSD: 1000}}, MaxTaskBudgetMicroUSD: 1000}, WebhookSecret: []byte(healthWebhookSecret),
		SupervisorAuth: healthSupervisorAuthenticator{}, StartupRecovery: recovery,
	}
}

type healthHTTPServer struct {
	URL    string
	client *http.Client
}

func (s *healthHTTPServer) Client() *http.Client { return s.client }

func healthStartAPI(t *testing.T, service healthService, recovery *healthStartupRecovery) (*healthHTTPServer, func()) {
	t.Helper()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.app.Run(runCtx) }()
	baseURL := "http://" + service.address
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if recovery.isRecovered() {
			response, err := client.Get(baseURL + "/healthz")
			if err == nil {
				if _, copyErr := io.Copy(io.Discard, response.Body); copyErr != nil {
					cancel()
					t.Fatalf("drain liveness response: %v", copyErr)
				}
				if closeErr := response.Body.Close(); closeErr != nil {
					cancel()
					t.Fatalf("close liveness response: %v", closeErr)
				}
				if response.StatusCode == http.StatusOK {
					break
				}
			}
		}
		select {
		case err := <-done:
			cancel()
			t.Fatalf("API Run returned before health listener became ready: %v", err)
		case <-time.After(10 * time.Millisecond):
			if time.Now().After(deadline) {
				cancel()
				t.Fatal("API health listener did not become ready before deadline")
			}
		}
	}
	server := &healthHTTPServer{URL: baseURL, client: client}
	stop := func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		if err := service.app.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shut down API: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("API Run returned on shutdown: %v", err)
			}
		case <-shutdownCtx.Done():
			t.Errorf("API Run did not join before shutdown deadline")
		}
	}
	return server, stop
}

type healthHTTPResult struct {
	StatusCode int
	Body       healthResponse
	Raw        []byte
}

func healthRequest(t *testing.T, client *http.Client, endpoint string) healthHTTPResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("construct %s request: %v", endpoint, err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("request %s: %v", endpoint, err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close %s response body: %v", endpoint, err)
		}
	}()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		t.Fatalf("read %s response: %v", endpoint, err)
	}
	var body healthResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %s response: %v", endpoint, err)
	}
	return healthHTTPResult{StatusCode: response.StatusCode, Body: body, Raw: raw}
}

func healthRequire(t *testing.T, client *http.Client, endpoint string, wantCode int, wantStatus string, wantDependencies []string) {
	t.Helper()
	response := healthRequest(t, client, endpoint)
	if response.StatusCode != wantCode || response.Body.Status != wantStatus {
		t.Fatalf("%s response=(%d,%+v), want (%d,%s)", endpoint, response.StatusCode, response.Body, wantCode, wantStatus)
	}
	if wantDependencies != nil && strings.Join(response.Body.Dependencies, ",") != strings.Join(wantDependencies, ",") {
		t.Fatalf("%s dependencies=%v, want %v", endpoint, response.Body.Dependencies, wantDependencies)
	}
}

func (r *healthStartupRecovery) setReadyError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readyErr = err
}

func (r *healthStartupRecovery) isRecovered() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recovered
}

func healthSorted(values []string) bool {
	for i := 1; i < len(values); i++ {
		if values[i-1] > values[i] {
			return false
		}
	}
	return true
}

func healthSign(payload []byte) string {
	mac := hmac.New(sha256.New, []byte(healthWebhookSecret))
	_, _ = mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
