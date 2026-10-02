package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/httpapi"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

const webhookTestSecret = "test-only-github-hmac-secret"

type webhookResponse struct {
	DeliveryID string `json:"delivery_id"`
	Accepted   bool   `json:"accepted"`
	Duplicate  bool   `json:"duplicate,omitempty"`
	Error      string `json:"error,omitempty"`
}

func TestWebhooks(t *testing.T) {
	database := testutil.RequireDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := storage.Migrate(ctx, database.Pool); err != nil {
		t.Fatalf("migrate owned test schema: %v", err)
	}
	serviceClock := clock.NewManual(time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC))
	secret := []byte(webhookTestSecret)
	handler, err := httpapi.NewWebhookHandler(secret, database.Pool, serviceClock)
	if err != nil {
		t.Fatalf("construct webhook handler: %v", err)
	}
	for index := range secret {
		secret[index] = 0
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := server.Client()

	t.Run("constructor_fails_closed", func(t *testing.T) {
		if _, err := httpapi.NewWebhookHandler(nil, database.Pool, serviceClock); err == nil {
			t.Fatal("handler accepted an empty webhook secret")
		}
		if _, err := httpapi.NewWebhookHandler([]byte(webhookTestSecret), nil, serviceClock); err == nil {
			t.Fatal("handler accepted a nil PostgreSQL pool")
		}
		if _, err := httpapi.NewWebhookHandler([]byte(webhookTestSecret), database.Pool, nil); err == nil {
			t.Fatal("handler accepted a nil clock")
		}
	})

	t.Run("commits_exact_signed_bytes_before_acknowledging", func(t *testing.T) {
		deliveryID := "webhook-new-exact-bytes"
		eventType := "issues"
		payload := []byte(" \n{\n  \"action\" : \"opened\", \"repository\" : {\"full_name\":\"owner/repo\"}\n}\t")
		response := webhooksPost(t, client, server.URL, deliveryID, eventType, payload, webhooksSign(payload))
		webhooksRequireStatus(t, response, http.StatusAccepted)
		if response.body.DeliveryID != deliveryID || !response.body.Accepted || response.body.Duplicate {
			t.Fatalf("new delivery response = %+v", response.body)
		}
		webhooksRequireFields(t, response, "delivery_id", "accepted")
		var persisted json.RawMessage
		var event string
		var disposition string
		if err := database.Pool.QueryRow(ctx, `SELECT payload, event_type, disposition FROM webhook_deliveries WHERE delivery_id = $1`, deliveryID).Scan(&persisted, &event, &disposition); err != nil {
			t.Fatalf("read committed inbox delivery: %v", err)
		}
		var gotPayload map[string]any
		if err := json.Unmarshal(persisted, &gotPayload); err != nil {
			t.Fatalf("decode committed payload: %v", err)
		}
		if gotPayload["action"] != "opened" || event != eventType || disposition != "INBOX" {
			t.Fatalf("persisted inbox record = payload %v, event %q, disposition %q", gotPayload, event, disposition)
		}
	})

	t.Run("identical_replay_returns_duplicate_success", func(t *testing.T) {
		deliveryID := "webhook-identical-replay"
		payload := []byte(`{"action":"opened"}`)
		first := webhooksPost(t, client, server.URL, deliveryID, "pull_request", payload, webhooksSign(payload))
		webhooksRequireStatus(t, first, http.StatusAccepted)
		replay := webhooksPost(t, client, server.URL, deliveryID, "pull_request", payload, webhooksSign(payload))
		webhooksRequireStatus(t, replay, http.StatusOK)
		if replay.body.DeliveryID != deliveryID || !replay.body.Accepted || !replay.body.Duplicate {
			t.Fatalf("duplicate response = %+v", replay.body)
		}
		webhooksRequireFields(t, replay, "delivery_id", "accepted", "duplicate")
		var count int
		if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM webhook_deliveries WHERE delivery_id = $1`, deliveryID).Scan(&count); err != nil {
			t.Fatalf("count durable delivery rows: %v", err)
		}
		if count != 1 {
			t.Fatalf("identical replay stored %d rows, want 1", count)
		}
	})

	t.Run("invalid_signature_precedes_json_parsing", func(t *testing.T) {
		payload := []byte(`{malformed`)
		response := webhooksPost(t, client, server.URL, "webhook-invalid-signature", "issues", payload, "sha256=00")
		webhooksRequireError(t, response, http.StatusUnauthorized, "invalid_signature")
		missingSignature := webhooksPost(t, client, server.URL, "webhook-missing-signature", "issues", payload, "")
		webhooksRequireError(t, missingSignature, http.StatusUnauthorized, "invalid_signature")
	})

	t.Run("valid_signature_with_invalid_request_returns_400", func(t *testing.T) {
		payload := []byte(`{malformed`)
		response := webhooksPost(t, client, server.URL, "webhook-invalid-payload", "issues", payload, webhooksSign(payload))
		webhooksRequireError(t, response, http.StatusBadRequest, "invalid_request")

		missingHeader := webhooksPost(t, client, server.URL, "", "issues", []byte(`{"action":"opened"}`), webhooksSign([]byte(`{"action":"opened"}`)))
		webhooksRequireError(t, missingHeader, http.StatusBadRequest, "invalid_request")
	})

	t.Run("conflicting_delivery_id_reuse_is_rejected", func(t *testing.T) {
		deliveryID := "webhook-conflicting-replay"
		firstPayload := []byte(`{"action":"opened"}`)
		first := webhooksPost(t, client, server.URL, deliveryID, "issues", firstPayload, webhooksSign(firstPayload))
		webhooksRequireStatus(t, first, http.StatusAccepted)
		conflictingPayload := []byte(`{"action":"closed"}`)
		conflict := webhooksPost(t, client, server.URL, deliveryID, "issues", conflictingPayload, webhooksSign(conflictingPayload))
		webhooksRequireError(t, conflict, http.StatusBadRequest, "invalid_request")
		var persisted json.RawMessage
		if err := database.Pool.QueryRow(ctx, `SELECT payload FROM webhook_deliveries WHERE delivery_id = $1`, deliveryID).Scan(&persisted); err != nil {
			t.Fatalf("read original delivery after conflicting replay: %v", err)
		}
		var original map[string]any
		if err := json.Unmarshal(persisted, &original); err != nil {
			t.Fatalf("decode original delivery after conflict: %v", err)
		}
		if original["action"] != "opened" {
			t.Fatalf("conflicting replay changed inbox payload: %v", original)
		}
	})

	t.Run("oversized_body_returns_invalid_request", func(t *testing.T) {
		payload := []byte(strings.Repeat("x", (10<<20)+1))
		response := webhooksPost(t, client, server.URL, "webhook-oversized", "issues", payload, webhooksSign(payload))
		webhooksRequireError(t, response, http.StatusBadRequest, "invalid_request")
	})

	t.Run("deferred_commit_failure_returns_503_without_persisting", func(t *testing.T) {
		ddl := `
CREATE FUNCTION aprl_fail_webhook_commit() RETURNS trigger
LANGUAGE plpgsql AS $trigger$
BEGIN
    RAISE EXCEPTION 'injected deferred webhook commit failure';
END;
$trigger$;
CREATE CONSTRAINT TRIGGER aprl_fail_webhook_commit
AFTER INSERT ON webhook_deliveries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION aprl_fail_webhook_commit();`
		if _, err := database.Pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("install owned deferred-constraint commit fault: %v", err)
		}
		deliveryID := "webhook-commit-failure"
		payload := []byte(`{"action":"opened"}`)
		response := webhooksPost(t, client, server.URL, deliveryID, "issues", payload, webhooksSign(payload))
		webhooksRequireError(t, response, http.StatusServiceUnavailable, "storage_unavailable")
		var count int
		if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM webhook_deliveries WHERE delivery_id = $1`, deliveryID).Scan(&count); err != nil {
			t.Fatalf("count delivery after failed commit: %v", err)
		}
		if count != 0 {
			t.Fatalf("failed commit persisted %d deliveries, want none", count)
		}
	})
}

type webhooksHTTPResponse struct {
	status int
	body   webhookResponse
	fields map[string]json.RawMessage
}

func webhooksPost(t *testing.T, client *http.Client, endpoint, deliveryID, eventType string, payload []byte, signature string) webhooksHTTPResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/webhooks/github", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("construct webhook request: %v", err)
	}
	request.Header.Set("X-Hub-Signature-256", signature)
	if deliveryID != "" {
		request.Header.Set("X-GitHub-Delivery", deliveryID)
	}
	if eventType != "" {
		request.Header.Set("X-GitHub-Event", eventType)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("send webhook request: %v", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close webhook response body: %v", err)
		}
	}()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		t.Fatalf("read webhook response: %v", err)
	}
	if response.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("webhook response Content-Type = %q, want application/json", response.Header.Get("Content-Type"))
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("decode webhook response fields: %v", err)
	}
	var decoded webhookResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode webhook response JSON: %v", err)
	}
	return webhooksHTTPResponse{status: response.StatusCode, body: decoded, fields: fields}
}

func webhooksRequireStatus(t *testing.T, response webhooksHTTPResponse, want int) {
	t.Helper()
	if response.status != want {
		t.Fatalf("webhook status = %d with response %+v, want %d", response.status, response.body, want)
	}
}

func webhooksRequireError(t *testing.T, response webhooksHTTPResponse, status int, code string) {
	t.Helper()
	webhooksRequireStatus(t, response, status)
	if response.body.Error != code || response.body.DeliveryID != "" || response.body.Accepted || response.body.Duplicate {
		t.Fatalf("webhook error response = %+v, want only error %q", response.body, code)
	}
	webhooksRequireFields(t, response, "error")
}

func webhooksRequireFields(t *testing.T, response webhooksHTTPResponse, want ...string) {
	t.Helper()
	if len(response.fields) != len(want) {
		t.Fatalf("webhook response fields = %v, want exactly %v", response.fields, want)
	}
	for _, key := range want {
		if _, ok := response.fields[key]; !ok {
			t.Fatalf("webhook response fields = %v, missing %q", response.fields, key)
		}
	}
}

func webhooksSign(payload []byte) string {
	mac := hmac.New(sha256.New, []byte(webhookTestSecret))
	_, _ = mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
