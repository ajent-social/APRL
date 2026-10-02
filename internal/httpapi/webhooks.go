// Package httpapi contains APRL's HTTP boundary adapters.
package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	webhookPath           = "/webhooks/github"
	maxWebhookBodyBytes   = 10 << 20
	maxWebhookRequestTime = 5 * time.Second
	maxWebhookStorageTime = 3 * time.Second
)

// WebhookHandler accepts signed GitHub deliveries and persists them in the durable inbox.
type WebhookHandler struct {
	secret []byte
	pool   *pgxpool.Pool
	clock  clock.Clock
}

type webhookResponse struct {
	DeliveryID string `json:"delivery_id,omitempty"`
	Accepted   bool   `json:"accepted,omitempty"`
	Duplicate  bool   `json:"duplicate,omitempty"`
	Error      string `json:"error,omitempty"`
}

// NewWebhookHandler constructs the GitHub webhook endpoint. Secret, pool, and clock are required.
func NewWebhookHandler(secret []byte, pool *pgxpool.Pool, c clock.Clock) (*WebhookHandler, error) {
	if len(secret) == 0 {
		return nil, errors.New("webhook HMAC secret is required")
	}
	if pool == nil {
		return nil, errors.New("webhook PostgreSQL pool is required")
	}
	if c == nil {
		return nil, errors.New("webhook clock is required")
	}
	return &WebhookHandler{secret: append([]byte(nil), secret...), pool: pool, clock: c}, nil
}

// ServeHTTP handles POST /webhooks/github.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != webhookPath {
		writeWebhookJSON(w, http.StatusNotFound, webhookResponse{Error: "invalid_request"})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeWebhookJSON(w, http.StatusMethodNotAllowed, webhookResponse{Error: "invalid_request"})
		return
	}
	if r.Body == nil {
		writeWebhookJSON(w, http.StatusBadRequest, webhookResponse{Error: "invalid_request"})
		return
	}

	deadline := time.Now().Add(maxWebhookRequestTime)
	if err := http.NewResponseController(w).SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		writeWebhookJSON(w, http.StatusBadRequest, webhookResponse{Error: "invalid_request"})
		return
	}
	requestCtx, cancel := context.WithTimeout(r.Context(), maxWebhookRequestTime)
	defer cancel()
	r = r.WithContext(requestCtx)

	body, err := readWebhookBody(w, r.Body)
	if err != nil {
		writeWebhookJSON(w, http.StatusBadRequest, webhookResponse{Error: "invalid_request"})
		return
	}
	if !validGitHubSignature(h.secret, body, r.Header.Values("X-Hub-Signature-256")) {
		writeWebhookJSON(w, http.StatusUnauthorized, webhookResponse{Error: "invalid_signature"})
		return
	}

	deliveryID, ok := singleHeader(r.Header.Values("X-GitHub-Delivery"))
	if !ok || strings.TrimSpace(deliveryID) == "" || deliveryID != strings.TrimSpace(deliveryID) || len(deliveryID) > 100 {
		writeWebhookJSON(w, http.StatusBadRequest, webhookResponse{Error: "invalid_request"})
		return
	}
	eventType, ok := singleHeader(r.Header.Values("X-GitHub-Event"))
	if !ok || strings.TrimSpace(eventType) == "" || eventType != strings.TrimSpace(eventType) {
		writeWebhookJSON(w, http.StatusBadRequest, webhookResponse{Error: "invalid_request"})
		return
	}
	if !isJSONObject(body) {
		writeWebhookJSON(w, http.StatusBadRequest, webhookResponse{Error: "invalid_request"})
		return
	}

	storageCtx, storageCancel := context.WithTimeout(requestCtx, maxWebhookStorageTime)
	defer storageCancel()
	inserted := false
	err = storage.WithUnitOfWork(storageCtx, h.pool, h.clock, func(ctx context.Context, repos *storage.Repositories) error {
		var storeErr error
		inserted, storeErr = repos.StoreWebhookDelivery(ctx, storage.DeliveryReceipt{
			DeliveryID: deliveryID,
			EventType:  eventType,
			Payload:    json.RawMessage(body),
		})
		return storeErr
	})
	if errors.Is(err, storage.ErrDeliveryConflict) || errors.Is(err, storage.ErrInvalidDelivery) {
		writeWebhookJSON(w, http.StatusBadRequest, webhookResponse{Error: "invalid_request"})
		return
	}
	if err != nil {
		writeWebhookJSON(w, http.StatusServiceUnavailable, webhookResponse{Error: "storage_unavailable"})
		return
	}
	status := http.StatusAccepted
	response := webhookResponse{DeliveryID: deliveryID, Accepted: true}
	if !inserted {
		status = http.StatusOK
		response.Duplicate = true
	}
	writeWebhookJSON(w, status, response)
}

func readWebhookBody(w http.ResponseWriter, body io.ReadCloser) (payload []byte, err error) {
	defer func() {
		if closeErr := body.Close(); closeErr != nil {
			payload = nil
			err = errors.Join(err, errors.New("close webhook request body"))
		}
	}()
	limited := http.MaxBytesReader(w, body, maxWebhookBodyBytes)
	payload, err = io.ReadAll(limited)
	if err != nil {
		return nil, errors.New("read bounded webhook body")
	}
	return payload, nil
}

func validGitHubSignature(secret, body []byte, signatures []string) bool {
	if len(signatures) != 1 || !strings.HasPrefix(signatures[0], "sha256=") {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(signatures[0], "sha256="))
	if err != nil || len(provided) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	if _, err := mac.Write(body); err != nil {
		return false
	}
	return hmac.Equal(provided, mac.Sum(nil))
}

func singleHeader(values []string) (string, bool) {
	if len(values) != 1 {
		return "", false
	}
	return values[0], true
}

func isJSONObject(payload []byte) bool {
	if !json.Valid(payload) {
		return false
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(payload, &value); err != nil {
		return false
	}
	return value != nil
}

func writeWebhookJSON(w http.ResponseWriter, status int, response webhookResponse) {
	body, err := json.Marshal(response)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":"storage_unavailable"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(append(body, '\n')); err != nil {
		return
	}
}

var _ http.Handler = (*WebhookHandler)(nil)
