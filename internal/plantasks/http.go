package plantasks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const deliveryV1HTTPTimeout = 10 * time.Second

// ErrInvalidDeliveryV1HTTPHandler reports missing or invalid HTTP dependencies.
var ErrInvalidDeliveryV1HTTPHandler = errors.New("invalid code-delivery/v1 HTTP handler")

// DeliveryV1BearerAuthenticator resolves a bearer token to a trusted opaque caller ID.
// Implementations must honor ctx cancellation and deadlines; the handler passes
// a finite context but cannot safely terminate a dependency that ignores it.
type DeliveryV1BearerAuthenticator interface {
	Authenticate(context.Context, string) (string, error)
}

// DeliveryV1LifecycleService is the caller-scoped durable delegation boundary.
// Implementations must honor ctx cancellation and deadlines.
type DeliveryV1LifecycleService interface {
	Submit(context.Context, []byte) (DelegationBinding, error)
	Get(context.Context, string) (DelegationBinding, error)
	Cancel(context.Context, string) (DelegationBinding, error)
}

// DeliveryV1ObservationProjector projects only authoritative lifecycle facts.
// Implementations must honor ctx cancellation and deadlines.
type DeliveryV1ObservationProjector interface {
	Project(context.Context, DelegationBinding) (DeliveryV1Observation, error)
}

// DeliveryV1HTTPCallerAuthenticator reads the caller established by the HTTPS handler.
// Its private context key cannot be populated by request headers or JSON.
type DeliveryV1HTTPCallerAuthenticator struct{}

type deliveryV1HTTPCallerContextKey struct{}

func (DeliveryV1HTTPCallerAuthenticator) AuthenticatedCaller(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", ErrDelegationUnauthenticated
	}
	callerID, ok := ctx.Value(deliveryV1HTTPCallerContextKey{}).(string)
	if !ok || !deliveryV1ValidID(callerID) {
		return "", ErrDelegationUnauthenticated
	}
	return callerID, nil
}

// NewDeliveryV1HTTPHandler constructs the frozen caller-scoped HTTPS API.
func NewDeliveryV1HTTPHandler(auth DeliveryV1BearerAuthenticator, service DeliveryV1LifecycleService, projector DeliveryV1ObservationProjector) (http.Handler, error) {
	if isNilDependency(auth) || isNilDependency(service) || isNilDependency(projector) {
		return nil, ErrInvalidDeliveryV1HTTPHandler
	}
	return &deliveryV1HTTPHandler{auth: auth, service: service, projector: projector}, nil
}

type deliveryV1HTTPHandler struct {
	auth      DeliveryV1BearerAuthenticator
	service   DeliveryV1LifecycleService
	projector DeliveryV1ObservationProjector
}

type deliveryV1HTTPError struct {
	Error deliveryV1HTTPErrorCode `json:"error"`
}

type deliveryV1HTTPErrorCode struct {
	Code string `json:"code"`
}

type deliveryV1HTTPOperation uint8

const (
	deliveryV1HTTPUnknown deliveryV1HTTPOperation = iota
	deliveryV1HTTPResource
	deliveryV1HTTPCancel
)

func (h *deliveryV1HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || r == nil || w == nil {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "service_unavailable")
		return
	}
	operation, delegationID, malformedPath := deliveryV1HTTPRoute(r.URL)
	if operation == deliveryV1HTTPUnknown {
		if malformedPath {
			deliveryV1WriteHTTPError(w, http.StatusBadRequest, "invalid_path")
		} else {
			deliveryV1WriteHTTPError(w, http.StatusNotFound, "not_found")
		}
		return
	}
	if operation == deliveryV1HTTPCancel {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			deliveryV1WriteHTTPError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
	} else if r.Method != http.MethodGet && r.Method != http.MethodPut {
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut)
		deliveryV1WriteHTTPError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if r.TLS == nil {
		deliveryV1WriteHTTPError(w, http.StatusBadRequest, "https_required")
		return
	}
	requestContext, cancel := context.WithTimeout(r.Context(), deliveryV1HTTPTimeout)
	defer cancel()
	deadline, _ := requestContext.Deadline()
	controller := http.NewResponseController(w)
	if err := controller.SetReadDeadline(deadline); err != nil {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "service_unavailable")
		return
	}
	if err := controller.SetWriteDeadline(deadline); err != nil {
		_ = controller.SetReadDeadline(time.Time{})
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "service_unavailable")
		return
	}
	defer func() {
		_ = controller.SetReadDeadline(time.Time{})
		_ = controller.SetWriteDeadline(time.Time{})
	}()
	if operation == deliveryV1HTTPResource && r.Method == http.MethodGet {
		if err := deliveryV1RequireEmptyBody(r.Body); err != nil {
			status, code := deliveryV1HTTPMapError(err)
			deliveryV1WriteHTTPError(w, status, code)
			return
		}
	}
	if r.Method != http.MethodGet {
		if !deliveryV1JSONContentType(r.Header.Get("Content-Type")) {
			deliveryV1WriteHTTPError(w, http.StatusUnsupportedMediaType, "unsupported_media_type")
			return
		}
	}
	callerID, err := deliveryV1Authenticate(requestContext, r.Header.Values("Authorization"), h.auth)
	if err != nil {
		status, code := deliveryV1HTTPMapError(err)
		if status != http.StatusServiceUnavailable {
			w.Header().Set("WWW-Authenticate", "Bearer")
		}
		deliveryV1WriteHTTPError(w, status, code)
		return
	}
	ctx := context.WithValue(requestContext, deliveryV1HTTPCallerContextKey{}, callerID)

	var binding DelegationBinding
	switch {
	case operation == deliveryV1HTTPResource && r.Method == http.MethodPut:
		body, readErr := deliveryV1ReadBody(w, r)
		if readErr != nil {
			status, code := deliveryV1HTTPMapError(readErr)
			deliveryV1WriteHTTPError(w, status, code)
			return
		}
		request, decodeErr := DecodeDeliveryV1Request(body)
		if decodeErr != nil {
			status, code := deliveryV1HTTPMapError(decodeErr)
			deliveryV1WriteHTTPError(w, status, code)
			return
		}
		if request.CallerID != callerID {
			deliveryV1WriteHTTPError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if request.DelegationID != delegationID {
			deliveryV1WriteHTTPError(w, http.StatusBadRequest, "binding_mismatch")
			return
		}
		binding, err = h.service.Submit(ctx, body)
	case operation == deliveryV1HTTPResource:
		binding, err = h.service.Get(ctx, delegationID)
	case operation == deliveryV1HTTPCancel:
		body, readErr := deliveryV1ReadBody(w, r)
		if readErr != nil {
			status, code := deliveryV1HTTPMapError(readErr)
			deliveryV1WriteHTTPError(w, status, code)
			return
		}
		if !deliveryV1EmptyJSONObject(body) {
			deliveryV1WriteHTTPError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		binding, err = h.service.Cancel(ctx, delegationID)
	}
	if err != nil {
		status, code := deliveryV1HTTPMapError(err)
		deliveryV1WriteHTTPError(w, status, code)
		return
	}
	if ctx.Err() != nil {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "service_unavailable")
		return
	}
	if binding.CallerID != callerID || binding.Request.CallerID != callerID || binding.DelegationID != delegationID || binding.Request.DelegationID != delegationID {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "service_unavailable")
		return
	}
	if binding.Status == DelegationDenied {
		deliveryV1WriteHTTPError(w, http.StatusForbidden, "policy_denied")
		return
	}
	if binding.Status == DelegationIntent || binding.Status == DelegationCancelled || binding.Cancelled && binding.LifecycleID == "" {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "observation_unavailable")
		return
	}
	if binding.Status != DelegationAdmitted || binding.LifecycleID == "" || binding.Sequence <= 0 || binding.State == nil {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "service_unavailable")
		return
	}
	requestDigest, digestErr := binding.Request.Digest()
	if digestErr != nil || requestDigest != binding.RequestDigest || binding.State.Lifecycle.ID != binding.LifecycleID || binding.State.Revision != binding.LifecycleRevision || binding.State.Cancelled != binding.Cancelled {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "service_unavailable")
		return
	}
	observation, projectErr := h.projector.Project(ctx, binding)
	if projectErr != nil || ctx.Err() != nil {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "observation_unavailable")
		return
	}
	if observation.DelegationID != binding.DelegationID || observation.RequestDigest != binding.RequestDigest || observation.LifecycleID != binding.LifecycleID || observation.Sequence != uint64(binding.Sequence) {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "observation_unavailable")
		return
	}
	encoded, encodeErr := EncodeDeliveryV1Observation(observation, binding.Request)
	if encodeErr != nil || len(encoded) > DeliveryV1MaxJSONBytes || ctx.Err() != nil {
		deliveryV1WriteHTTPError(w, http.StatusServiceUnavailable, "observation_unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(encoded)
}

func deliveryV1Authenticate(ctx context.Context, headers []string, auth DeliveryV1BearerAuthenticator) (string, error) {
	if len(headers) != 1 {
		return "", ErrDelegationUnauthenticated
	}
	parts := strings.Fields(headers[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", ErrDelegationUnauthenticated
	}
	callerID, err := auth.Authenticate(ctx, parts[1])
	if ctx.Err() != nil {
		return "", ErrDelegationUnavailable
	}
	if errors.Is(err, ErrDelegationUnavailable) {
		return "", ErrDelegationUnavailable
	}
	if err != nil || !deliveryV1ValidID(callerID) {
		return "", ErrDelegationUnauthenticated
	}
	return callerID, nil
}

func deliveryV1HTTPRoute(requestURL *url.URL) (deliveryV1HTTPOperation, string, bool) {
	if requestURL == nil {
		return deliveryV1HTTPUnknown, "", true
	}
	const prefix = "/v1/delegations/"
	escapedPath := requestURL.EscapedPath()
	if !strings.HasPrefix(escapedPath, prefix) {
		return deliveryV1HTTPUnknown, "", false
	}
	remainder := strings.TrimPrefix(escapedPath, prefix)
	parts := strings.Split(remainder, "/")
	operation := deliveryV1HTTPResource
	if len(parts) == 2 && parts[1] == "cancel" {
		operation = deliveryV1HTTPCancel
	} else if len(parts) != 1 {
		return deliveryV1HTTPUnknown, "", false
	}
	delegationID, err := url.PathUnescape(parts[0])
	if err != nil || !deliveryV1ValidID(delegationID) {
		return deliveryV1HTTPUnknown, "", true
	}
	return operation, delegationID, false
}

func deliveryV1JSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "application/json")
}

func deliveryV1ReadBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, ErrDeliveryV1Malformed
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, DeliveryV1MaxJSONBytes+1))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return nil, ErrDeliveryV1Oversize
		}
		return nil, ErrDeliveryV1Malformed
	}
	if len(body) > DeliveryV1MaxJSONBytes {
		return nil, ErrDeliveryV1Oversize
	}
	return body, nil
}

func deliveryV1RequireEmptyBody(body io.ReadCloser) error {
	if body == nil {
		return nil
	}
	content, err := io.ReadAll(io.LimitReader(body, DeliveryV1MaxJSONBytes+1))
	if len(content) > DeliveryV1MaxJSONBytes {
		return ErrDeliveryV1Oversize
	}
	if err != nil || len(content) != 0 {
		return ErrDeliveryV1Malformed
	}
	return nil
}

func deliveryV1EmptyJSONObject(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil || fields == nil {
		return false
	}
	return len(fields) == 0
}

func deliveryV1HTTPMapError(err error) (int, string) {
	switch {
	case errors.Is(err, ErrDeliveryV1Oversize):
		return http.StatusRequestEntityTooLarge, "request_too_large"
	case errors.Is(err, ErrDeliveryV1Malformed), errors.Is(err, ErrDeliveryV1Invalid):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, ErrDelegationUnauthenticated):
		return http.StatusUnauthorized, "unauthorized"
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, ErrDelegationConflict):
		return http.StatusConflict, "conflict"
	case errors.Is(err, ErrDelegationUnavailable):
		return http.StatusServiceUnavailable, "service_unavailable"
	default:
		return http.StatusServiceUnavailable, "service_unavailable"
	}
}

func deliveryV1WriteHTTPError(w http.ResponseWriter, status int, code string) {
	if w == nil {
		return
	}
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(deliveryV1HTTPError{Error: deliveryV1HTTPErrorCode{Code: code}})
}

var _ DeliveryV1CallerAuthenticator = DeliveryV1HTTPCallerAuthenticator{}
