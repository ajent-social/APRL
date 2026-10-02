package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/results"
)

const (
	resultsPath           = "/internal/results"
	maxResultsBodyBytes   = 1 << 20
	maxResultsRequestTime = 8 * time.Second
)

// SupervisorAuthenticator derives a host supervisor principal from trusted
// request authentication material. Implementations must ignore JSON identity fields.
type SupervisorAuthenticator interface {
	AuthenticateSupervisor(context.Context, *http.Request) (results.Principal, error)
}

// ResultsHandler accepts strict worker result envelopes for one trusted host principal.
type ResultsHandler struct {
	service *results.Service
	auth    SupervisorAuthenticator
}

type resultsResponse struct {
	Accepted    bool   `json:"accepted,omitempty"`
	OperationID string `json:"operation_id,omitempty"`
	Error       string `json:"error,omitempty"`
}

// NewResultsHandler constructs the authenticated result endpoint.
func NewResultsHandler(service *results.Service, auth SupervisorAuthenticator) (*ResultsHandler, error) {
	if service == nil || auth == nil {
		return nil, errors.New("results service and trusted supervisor authenticator are required")
	}
	return &ResultsHandler{service: service, auth: auth}, nil
}

// ServeHTTP accepts POST /internal/results from an authenticated host supervisor.
func (h *ResultsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != resultsPath {
		writeResultsJSON(w, http.StatusNotFound, resultsResponse{Error: "invalid_result"})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeResultsJSON(w, http.StatusMethodNotAllowed, resultsResponse{Error: "invalid_result"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), maxResultsRequestTime)
	defer cancel()
	principal, err := h.auth.AuthenticateSupervisor(ctx, r)
	if err != nil || strings.TrimSpace(principal.TaskID) == "" || strings.TrimSpace(principal.RunID) == "" ||
		strings.TrimSpace(principal.Identity) == "" || strings.TrimSpace(principal.CredentialID) == "" {
		writeResultsJSON(w, http.StatusUnauthorized, resultsResponse{Error: "unauthorized"})
		return
	}
	if r.Body == nil {
		writeResultsJSON(w, http.StatusBadRequest, resultsResponse{Error: "invalid_result"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxResultsBodyBytes))
	closeErr := r.Body.Close()
	if err != nil || closeErr != nil {
		writeResultsJSON(w, http.StatusBadRequest, resultsResponse{Error: "invalid_result"})
		return
	}
	result, err := contracts.DecodeResult(body)
	if err != nil {
		writeResultsJSON(w, http.StatusBadRequest, resultsResponse{Error: "invalid_result"})
		return
	}
	accepted, err := h.service.Submit(ctx, principal, result)
	if err != nil {
		switch {
		case errors.Is(err, results.ErrForbidden):
			writeResultsJSON(w, http.StatusForbidden, resultsResponse{Error: "forbidden"})
		case errors.Is(err, results.ErrStale), errors.Is(err, results.ErrConflict), errors.Is(err, results.ErrInvalid),
			errors.Is(err, contracts.ErrInvalidContract):
			writeResultsJSON(w, http.StatusConflict, resultsResponse{Error: "stale_result"})
		default:
			writeResultsJSON(w, http.StatusServiceUnavailable, resultsResponse{Error: "storage_unavailable"})
		}
		return
	}
	writeResultsJSON(w, http.StatusOK, resultsResponse{Accepted: true, OperationID: accepted.OperationID})
}

func writeResultsJSON(w http.ResponseWriter, status int, value resultsResponse) {
	body, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":"storage_unavailable"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

var _ http.Handler = (*ResultsHandler)(nil)
