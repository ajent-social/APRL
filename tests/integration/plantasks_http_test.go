package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/plantasks"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

type plantasksHTTPBearerAuth struct {
	mu     sync.Mutex
	tokens map[string]string
	err    error
}

func (a *plantasksHTTPBearerAuth) Authenticate(ctx context.Context, token string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return "", a.err
	}
	caller, ok := a.tokens[token]
	if !ok {
		return "", errors.New("private bearer verifier diagnostic")
	}
	return caller, nil
}

type plantasksHTTPOperationProjector struct {
	mu            sync.Mutex
	mode          string
	err           error
	barrier       <-chan struct{}
	entered       chan struct{}
	ignoreContext bool
}

type plantasksHTTPTestLifecycleService struct{}

func (plantasksHTTPTestLifecycleService) Submit(context.Context, []byte) (plantasks.DelegationBinding, error) {
	return plantasks.DelegationBinding{}, errors.New("test service must not be called")
}

func (plantasksHTTPTestLifecycleService) Get(context.Context, string) (plantasks.DelegationBinding, error) {
	return plantasks.DelegationBinding{}, errors.New("test service must not be called")
}

func (plantasksHTTPTestLifecycleService) Cancel(context.Context, string) (plantasks.DelegationBinding, error) {
	return plantasks.DelegationBinding{}, errors.New("test service must not be called")
}

func (p *plantasksHTTPOperationProjector) Project(ctx context.Context, binding plantasks.DelegationBinding) (plantasks.DeliveryV1Observation, error) {
	if p.barrier != nil {
		if p.entered != nil {
			select {
			case p.entered <- struct{}{}:
			default:
			}
		}
		if p.ignoreContext {
			<-p.barrier
		} else {
			select {
			case <-p.barrier:
			case <-ctx.Done():
				return plantasks.DeliveryV1Observation{}, ctx.Err()
			}
		}
	}
	p.mu.Lock()
	mode, projectErr := p.mode, p.err
	p.mu.Unlock()
	if projectErr != nil {
		return plantasks.DeliveryV1Observation{}, projectErr
	}
	if binding.State == nil || binding.LifecycleID == "" || binding.Status != plantasks.DelegationAdmitted {
		return plantasks.DeliveryV1Observation{}, errors.New("binding has no projectable admitted lifecycle")
	}
	digest, err := binding.Request.Digest()
	if err != nil {
		return plantasks.DeliveryV1Observation{}, err
	}
	observation := plantasks.DeliveryV1Observation{
		Version: plantasks.DeliveryV1ProtocolVersion, DelegationID: binding.DelegationID,
		RequestDigest: digest, LifecycleID: binding.LifecycleID, Sequence: uint64(binding.Sequence),
		State: "admitted", Accounting: plantasks.DeliveryV1Accounting{},
	}
	if binding.Cancelled {
		observation.State = "canceled"
	}
	ids := make([]string, 0, len(binding.State.Tasks))
	for id := range binding.State.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		task := binding.State.Tasks[id]
		kind := string(task.Stage)
		if task.Stage == plantasks.StageRereview {
			kind = "re_review"
		}
		childState := "pending"
		if binding.Cancelled {
			childState = "canceled"
		}
		child := plantasks.DeliveryV1ChildTask{ID: task.ID, Kind: kind, State: childState,
			FindingIDs: append([]string(nil), task.FindingIDs...)}
		for _, dependency := range task.Dependencies {
			child.DependsOn = append(child.DependsOn, dependency.TaskID)
		}
		if task.PR != nil {
			child.PRURL, child.HeadCommit = task.PR.URL, task.PR.HeadSHA
		}
		observation.Children = append(observation.Children, child)
	}
	switch mode {
	case "wrong_delegation":
		observation.DelegationID = "other-delegation"
	case "wrong_digest":
		observation.RequestDigest = strings.Repeat("0", 64)
	case "wrong_lifecycle":
		observation.LifecycleID = "00000000-0000-4000-8000-000000000001"
	case "wrong_sequence":
		observation.Sequence++
	case "invent_landed":
		observation.State = "landed"
	case "oversized":
		findingIDs := make([]string, 256)
		for i := range findingIDs {
			findingIDs[i] = fmt.Sprintf("f%0127x", i)
		}
		for i := 0; i < 40; i++ {
			observation.Children = append(observation.Children, plantasks.DeliveryV1ChildTask{
				ID: fmt.Sprintf("large-child-%03d", i), Kind: "author", State: "pending",
				FindingIDs: append([]string(nil), findingIDs...),
			})
		}
	}
	return observation, nil
}

func (p *plantasksHTTPOperationProjector) setMode(mode string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode = mode
}

type plantasksHTTPFixture struct {
	server    *httptest.Server
	client    *http.Client
	database  testutil.DatabaseFixture
	handler   http.Handler
	bearer    *plantasksHTTPBearerAuth
	service   *plantasks.Delegations
	manual    *clock.Manual
	policy    *plantasksDelegationPolicy
	projector *plantasksHTTPOperationProjector
}

func newPlantasksHTTPFixture(t *testing.T, policy *plantasksDelegationPolicy, projector *plantasksHTTPOperationProjector) plantasksHTTPFixture {
	t.Helper()
	ctx := context.Background()
	database := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, database.Pool); err != nil {
		t.Fatalf("migrate HTTPS delegation test schema: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store := plantasksDelegationStore(t, database.Pool, manual)
	auth := &plantasks.DeliveryV1HTTPCallerAuthenticator{}
	service, err := plantasks.NewDelegations(store, auth, policy, &plantasksDelegationInitialFactory{})
	if err != nil {
		t.Fatalf("construct real PostgreSQL delegation service: %v", err)
	}
	bearer := &plantasksHTTPBearerAuth{tokens: map[string]string{
		"owner-token": "caller-example",
		"other-token": "caller-other",
	}}
	handler, err := plantasks.NewDeliveryV1HTTPHandler(bearer, service, projector)
	if err != nil {
		t.Fatalf("construct HTTPS handler: %v", err)
	}
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return plantasksHTTPFixture{server: server, client: server.Client(), database: database, handler: handler, bearer: bearer,
		service: service, manual: manual, policy: policy, projector: projector}
}

func plantasksHTTPRequest(t *testing.T, client *http.Client, method, endpoint, token, contentType string, body []byte) (int, http.Header, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("construct HTTPS request: %v", err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("send HTTPS request: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		t.Fatalf("read HTTPS response: %v", err)
	}
	return response.StatusCode, response.Header.Clone(), responseBody
}

func plantasksHTTPErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode redacted error response %q: %v", body, err)
	}
	if response.Error.Code == "" {
		t.Fatalf("error response lacks stable code: %q", body)
	}
	return response.Error.Code
}

func TestPlanTaskDeliveryV1HTTPHandlerRejectsTypedNilDependencies(t *testing.T) {
	validAuth := &plantasksHTTPBearerAuth{tokens: map[string]string{"owner-token": "caller-example"}}
	validService := plantasksHTTPTestLifecycleService{}
	validProjector := &plantasksHTTPOperationProjector{}
	var nilAuth *plantasksHTTPBearerAuth
	if _, err := plantasks.NewDeliveryV1HTTPHandler(nilAuth, validService, validProjector); !errors.Is(err, plantasks.ErrInvalidDeliveryV1HTTPHandler) {
		t.Fatalf("typed-nil authenticator error=%v, want invalid-handler sentinel", err)
	}
	var nilService *plantasks.Delegations
	if _, err := plantasks.NewDeliveryV1HTTPHandler(validAuth, nilService, validProjector); !errors.Is(err, plantasks.ErrInvalidDeliveryV1HTTPHandler) {
		t.Fatalf("typed-nil service error=%v, want invalid-handler sentinel", err)
	}
	var nilProjector *plantasksHTTPOperationProjector
	if _, err := plantasks.NewDeliveryV1HTTPHandler(validAuth, validService, nilProjector); !errors.Is(err, plantasks.ErrInvalidDeliveryV1HTTPHandler) {
		t.Fatalf("typed-nil projector error=%v, want invalid-handler sentinel", err)
	}
}

func plantasksHTTPAssertError(t *testing.T, status int, body []byte, wantStatus int, wantCode string, forbidden ...string) {
	t.Helper()
	if status != wantStatus {
		t.Fatalf("HTTP status=%d body=%s, want %d", status, body, wantStatus)
	}
	if code := plantasksHTTPErrorCode(t, body); code != wantCode {
		t.Fatalf("HTTP error code=%q, want %q; body=%s", code, wantCode, body)
	}
	for _, text := range forbidden {
		if strings.Contains(string(body), text) {
			t.Fatalf("HTTP error disclosed forbidden text %q: %s", text, body)
		}
	}
}

func TestPlanTaskDeliveryV1HTTPSPutGetReplayAndCallerScopedCancel(t *testing.T) {
	policy := &plantasksDelegationPolicy{}
	projector := &plantasksHTTPOperationProjector{}
	fixture := newPlantasksHTTPFixture(t, policy, projector)
	requestBody, request := plantasksDelegationRequest(t)
	pathID := url.PathEscape(request.DelegationID)
	endpoint := fixture.server.URL + "/v1/delegations/" + pathID
	status, _, putBody := plantasksHTTPRequest(t, fixture.client, http.MethodPut, endpoint, "owner-token", "application/json", requestBody)
	if status != http.StatusOK {
		t.Fatalf("admitted PUT status=%d body=%s", status, putBody)
	}
	observation, err := plantasks.DecodeDeliveryV1Observation(putBody, request)
	if err != nil || observation.LifecycleID == "" || observation.Sequence == 0 || observation.State != "admitted" {
		t.Fatalf("decode admitted observation=%+v err=%v", observation, err)
	}

	status, _, replayBody := plantasksHTTPRequest(t, fixture.client, http.MethodPut, endpoint, "owner-token", "application/json; charset=utf-8", requestBody)
	if status != http.StatusOK || !bytes.Equal(replayBody, putBody) {
		t.Fatalf("identical PUT replay status=%d body=%s first=%s", status, replayBody, putBody)
	}
	status, _, getBody := plantasksHTTPRequest(t, fixture.client, http.MethodGet, endpoint, "owner-token", "", nil)
	if status != http.StatusOK || !bytes.Equal(getBody, putBody) {
		t.Fatalf("caller-scoped GET status=%d body=%s first=%s", status, getBody, putBody)
	}

	status, _, foreignGetBody := plantasksHTTPRequest(t, fixture.client, http.MethodGet, endpoint, "other-token", "", nil)
	missing := fixture.server.URL + "/v1/delegations/not-present"
	missingStatus, _, missingBody := plantasksHTTPRequest(t, fixture.client, http.MethodGet, missing, "owner-token", "", nil)
	if foreignGetBodyStatus(status, foreignGetBody, missingStatus, missingBody) != nil {
		t.Fatalf("foreign and missing GET responses differ: foreign=%d %s missing=%d %s", status, foreignGetBody, missingStatus, missingBody)
	}
	if code := plantasksHTTPErrorCode(t, foreignGetBody); code != "not_found" {
		t.Fatalf("foreign GET code=%q, want not_found", code)
	}

	cancelEndpoint := endpoint + "/cancel"
	status, _, foreignCancelBody := plantasksHTTPRequest(t, fixture.client, http.MethodPost, cancelEndpoint, "other-token", "application/json", []byte("{\n  }"))
	if status != http.StatusNotFound || !bytes.Equal(foreignCancelBody, foreignGetBody) {
		t.Fatalf("foreign cancel disclosed binding: status=%d body=%s", status, foreignCancelBody)
	}
	status, _, cancelBody := plantasksHTTPRequest(t, fixture.client, http.MethodPost, cancelEndpoint, "owner-token", "application/json", []byte("{\n  }"))
	if status != http.StatusOK {
		t.Fatalf("admitted cancel status=%d body=%s", status, cancelBody)
	}
	canceled, err := plantasks.DecodeDeliveryV1Observation(cancelBody, request)
	if err != nil || canceled.LifecycleID != observation.LifecycleID || canceled.Sequence <= observation.Sequence || canceled.State != "canceled" {
		t.Fatalf("cancellation observation=%+v err=%v", canceled, err)
	}
	status, _, cancelReplay := plantasksHTTPRequest(t, fixture.client, http.MethodPost, cancelEndpoint, "owner-token", "application/json", []byte("{}"))
	if status != http.StatusOK || !bytes.Equal(cancelReplay, cancelBody) {
		t.Fatalf("cancel replay status=%d body=%s first=%s", status, cancelReplay, cancelBody)
	}
}

func foreignGetBodyStatus(foreignStatus int, foreignBody []byte, missingStatus int, missingBody []byte) error {
	if foreignStatus != missingStatus || !bytes.Equal(foreignBody, missingBody) {
		return errors.New("caller-scoped not-found responses differ")
	}
	return nil
}

func TestPlanTaskDeliveryV1HTTPRejectsUntrustedBindingsWithoutPersistence(t *testing.T) {
	fixture := newPlantasksHTTPFixture(t, &plantasksDelegationPolicy{}, &plantasksHTTPOperationProjector{})
	raw, request := plantasksDelegationRequest(t)
	endpoint := fixture.server.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
	wrongCaller := request
	wrongCaller.CallerID = "caller-other"
	wrongCallerBody, err := plantasks.EncodeDeliveryV1Request(wrongCaller)
	if err != nil {
		t.Fatalf("encode mismatched caller request: %v", err)
	}
	wrongPath := fixture.server.URL + "/v1/delegations/different-id"
	unsupportedVersion := bytes.Replace(raw, []byte(`"version":"code-delivery/v1",`), []byte(`"version":"code-delivery/v2",`), 1)
	cancelTrailing := []byte(`{} {}`)
	duplicateKey := bytes.Replace(raw, []byte(`"version":"code-delivery/v1",`), []byte(`"version":"code-delivery/v1","version":"code-delivery/v1",`), 1)
	unknownKey := bytes.Replace(raw, []byte(`"version":"code-delivery/v1",`), []byte(`"version":"code-delivery/v1","untrusted":true,`), 1)
	trailing := append(append([]byte(nil), raw...), []byte(` {}`)...)
	tooLarge := bytes.Repeat([]byte(" "), (1<<20)+1)
	for _, test := range []struct {
		name        string
		method      string
		endpoint    string
		token       string
		contentType string
		body        []byte
		wantStatus  int
		wantCode    string
	}{
		{name: "missing bearer", method: http.MethodPut, endpoint: endpoint, contentType: "application/json", body: raw, wantStatus: http.StatusUnauthorized, wantCode: "unauthorized"},
		{name: "unknown bearer", method: http.MethodPut, endpoint: endpoint, token: "missing-token", contentType: "application/json", body: raw, wantStatus: http.StatusUnauthorized, wantCode: "unauthorized"},
		{name: "body caller differs from trusted principal", method: http.MethodPut, endpoint: endpoint, token: "owner-token", contentType: "application/json", body: wrongCallerBody, wantStatus: http.StatusUnauthorized, wantCode: "unauthorized"},
		{name: "path differs from body delegation", method: http.MethodPut, endpoint: wrongPath, token: "owner-token", contentType: "application/json", body: raw, wantStatus: http.StatusBadRequest, wantCode: "binding_mismatch"},
		{name: "duplicate top-level member", method: http.MethodPut, endpoint: endpoint, token: "owner-token", contentType: "application/json", body: duplicateKey, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "unknown top-level member", method: http.MethodPut, endpoint: endpoint, token: "owner-token", contentType: "application/json", body: unknownKey, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "unsupported protocol version", method: http.MethodPut, endpoint: endpoint, token: "owner-token", contentType: "application/json", body: unsupportedVersion, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "trailing JSON value", method: http.MethodPut, endpoint: endpoint, token: "owner-token", contentType: "application/json", body: trailing, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "unsupported content type", method: http.MethodPut, endpoint: endpoint, token: "owner-token", contentType: "text/plain", body: raw, wantStatus: http.StatusUnsupportedMediaType, wantCode: "unsupported_media_type"},
		{name: "oversized request", method: http.MethodPut, endpoint: endpoint, token: "owner-token", contentType: "application/json", body: tooLarge, wantStatus: http.StatusRequestEntityTooLarge, wantCode: "request_too_large"},
		{name: "GET body is rejected", method: http.MethodGet, endpoint: endpoint, token: "owner-token", body: []byte("{}"), wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "cancel accepts only empty object", method: http.MethodPost, endpoint: endpoint + "/cancel", token: "owner-token", contentType: "application/json", body: []byte(`{"force":true}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "cancel rejects trailing JSON", method: http.MethodPost, endpoint: endpoint + "/cancel", token: "owner-token", contentType: "application/json", body: cancelTrailing, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "unsupported method", method: http.MethodPatch, endpoint: endpoint, token: "owner-token", wantStatus: http.StatusMethodNotAllowed, wantCode: "method_not_allowed"},
		{name: "encoded slash is invalid path", method: http.MethodGet, endpoint: fixture.server.URL + "/v1/delegations/a%2Fb", token: "owner-token", wantStatus: http.StatusBadRequest, wantCode: "invalid_path"},
	} {
		t.Run(test.name, func(t *testing.T) {
			status, headers, body := plantasksHTTPRequest(t, fixture.client, test.method, test.endpoint, test.token, test.contentType, test.body)
			plantasksHTTPAssertError(t, status, body, test.wantStatus, test.wantCode)
			if test.wantStatus == http.StatusUnauthorized && headers.Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("unauthorized response omitted bearer challenge: %#v", headers)
			}
			if test.name == "unsupported method" && !strings.Contains(headers.Get("Allow"), http.MethodPut) {
				t.Fatalf("unsupported method response omitted Allow: %#v", headers)
			}
		})
	}
	duplicateAuthRequest, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("construct duplicate-authorization request: %v", err)
	}
	duplicateAuthRequest.Header.Add("Authorization", "Bearer owner-token")
	duplicateAuthRequest.Header.Add("Authorization", "Bearer other-token")
	duplicateAuthRequest.Header.Set("Content-Type", "application/json")
	duplicateAuthResponse, err := fixture.client.Do(duplicateAuthRequest)
	if err != nil {
		t.Fatalf("send duplicate-authorization request: %v", err)
	}
	duplicateAuthBody, readErr := io.ReadAll(duplicateAuthResponse.Body)
	closeErr := duplicateAuthResponse.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read/close duplicate-authorization response: read=%v close=%v", readErr, closeErr)
	}
	plantasksHTTPAssertError(t, duplicateAuthResponse.StatusCode, duplicateAuthBody, http.StatusUnauthorized, "unauthorized")

	if bindings, lifecycles, receipts := plantasksDelegationCounts(context.Background(), t, fixture.database.Pool, request.CallerID, request.DelegationID); bindings != 0 || lifecycles != 0 || receipts != 0 {
		t.Fatalf("untrusted request cases persisted data: bindings=%d lifecycles=%d receipts=%d", bindings, lifecycles, receipts)
	}

	plain := httptest.NewServer(fixture.handler)
	t.Cleanup(plain.Close)
	plainEndpoint := plain.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
	requestMessage, err := http.NewRequest(http.MethodPut, plainEndpoint, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("construct plaintext request: %v", err)
	}
	requestMessage.Header.Set("Authorization", "Bearer owner-token")
	requestMessage.Header.Set("Content-Type", "application/json")
	requestMessage.Header.Set("X-Forwarded-Proto", "https")
	response, err := http.DefaultClient.Do(requestMessage)
	if err != nil {
		t.Fatalf("send plaintext request: %v", err)
	}
	plainBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read/close plaintext response: read=%v close=%v", readErr, closeErr)
	}
	plantasksHTTPAssertError(t, response.StatusCode, plainBody, http.StatusBadRequest, "https_required")
}

func TestPlanTaskDeliveryV1HTTPRedactsUnavailableAuthenticatorErrors(t *testing.T) {
	fixture := newPlantasksHTTPFixture(t, &plantasksDelegationPolicy{}, &plantasksHTTPOperationProjector{})
	fixture.bearer.err = plantasks.ErrDelegationUnavailable
	raw, request := plantasksDelegationRequest(t)
	endpoint := fixture.server.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
	status, _, body := plantasksHTTPRequest(t, fixture.client, http.MethodPut, endpoint, "owner-token", "application/json", raw)
	plantasksHTTPAssertError(t, status, body, http.StatusServiceUnavailable, "service_unavailable", "delegation policy or storage unavailable")
	if bindings, lifecycles, receipts := plantasksDelegationCounts(context.Background(), t, fixture.database.Pool, request.CallerID, request.DelegationID); bindings != 0 || lifecycles != 0 || receipts != 0 {
		t.Fatalf("unavailable authenticator persisted binding data: bindings=%d lifecycles=%d receipts=%d", bindings, lifecycles, receipts)
	}
}

func TestPlanTaskDeliveryV1HTTPOpaquePathIDIsUnescapedExactlyOnce(t *testing.T) {
	fixture := newPlantasksHTTPFixture(t, &plantasksDelegationPolicy{}, &plantasksHTTPOperationProjector{})
	raw, request := plantasksDelegationRequest(t)
	request.DelegationID = "opaque%2Fid"
	raw, err := plantasks.EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode opaque delegation ID: %v", err)
	}
	endpoint := fixture.server.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
	status, _, body := plantasksHTTPRequest(t, fixture.client, http.MethodPut, endpoint, "owner-token", "application/json", raw)
	if status != http.StatusOK {
		t.Fatalf("encoded opaque ID PUT status=%d body=%s", status, body)
	}
	observation, err := plantasks.DecodeDeliveryV1Observation(body, request)
	if err != nil || observation.DelegationID != request.DelegationID {
		t.Fatalf("encoded opaque ID observation=%+v err=%v", observation, err)
	}
}

func TestPlanTaskDeliveryV1HTTPDenialIntentAndCancellationDoNotInventLifecycle(t *testing.T) {
	t.Run("denial is stable and redacted", func(t *testing.T) {
		policy := &plantasksDelegationPolicy{denyReason: "private policy decision detail"}
		fixture := newPlantasksHTTPFixture(t, policy, &plantasksHTTPOperationProjector{})
		raw, request := plantasksDelegationRequest(t)
		endpoint := fixture.server.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
		status, _, denied := plantasksHTTPRequest(t, fixture.client, http.MethodPut, endpoint, "owner-token", "application/json", raw)
		plantasksHTTPAssertError(t, status, denied, http.StatusForbidden, "policy_denied", "private policy decision detail")
		status, _, replay := plantasksHTTPRequest(t, fixture.client, http.MethodGet, endpoint, "owner-token", "", nil)
		if status != http.StatusForbidden || !bytes.Equal(denied, replay) {
			t.Fatalf("denied GET replay changed status/body: status=%d body=%s denied=%s", status, replay, denied)
		}
		if bindings, lifecycles, receipts := plantasksDelegationCounts(context.Background(), t, fixture.database.Pool, request.CallerID, request.DelegationID); bindings != 1 || lifecycles != 0 || receipts != 0 {
			t.Fatalf("terminal denial created lifecycle data: binding=%d lifecycle=%d receipts=%d", bindings, lifecycles, receipts)
		}
	})

	t.Run("pending cancel remains unavailable without lifecycle", func(t *testing.T) {
		policy := &plantasksDelegationPolicy{temporary: true}
		fixture := newPlantasksHTTPFixture(t, policy, &plantasksHTTPOperationProjector{})
		raw, request := plantasksDelegationRequest(t)
		endpoint := fixture.server.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
		status, _, pending := plantasksHTTPRequest(t, fixture.client, http.MethodPut, endpoint, "owner-token", "application/json", raw)
		plantasksHTTPAssertError(t, status, pending, http.StatusServiceUnavailable, "service_unavailable")
		status, _, canceled := plantasksHTTPRequest(t, fixture.client, http.MethodPost, endpoint+"/cancel", "owner-token", "application/json", []byte("{ }"))
		plantasksHTTPAssertError(t, status, canceled, http.StatusServiceUnavailable, "observation_unavailable")
		if bytes.Contains(canceled, []byte("lifecycle_id")) {
			t.Fatalf("pending cancellation invented an observation: %s", canceled)
		}
		if bindings, lifecycles, receipts := plantasksDelegationCounts(context.Background(), t, fixture.database.Pool, request.CallerID, request.DelegationID); bindings != 1 || lifecycles != 0 || receipts != 0 {
			t.Fatalf("pending cancellation created lifecycle data: binding=%d lifecycle=%d receipts=%d", bindings, lifecycles, receipts)
		}
	})

	t.Run("expiry and mandatory review capacity are denied", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			mutate func(*plantasks.DeliveryV1Request, time.Time)
		}{
			{name: "expired", mutate: func(request *plantasks.DeliveryV1Request, now time.Time) { request.Spec.Envelope.ExpiresAt = now }},
			{name: "one attempt cannot fit independent review", mutate: func(request *plantasks.DeliveryV1Request, _ time.Time) { request.Spec.Envelope.MaxAttempts = 1 }},
		} {
			t.Run(test.name, func(t *testing.T) {
				fixture := newPlantasksHTTPFixture(t, &plantasksDelegationPolicy{}, &plantasksHTTPOperationProjector{})
				raw, request := plantasksDelegationRequest(t)
				test.mutate(&request, fixture.manual.Now())
				raw, err := plantasks.EncodeDeliveryV1Request(request)
				if err != nil {
					t.Fatalf("encode denied request: %v", err)
				}
				endpoint := fixture.server.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
				status, _, body := plantasksHTTPRequest(t, fixture.client, http.MethodPut, endpoint, "owner-token", "application/json", raw)
				plantasksHTTPAssertError(t, status, body, http.StatusForbidden, "policy_denied")
				if bindings, lifecycles, receipts := plantasksDelegationCounts(context.Background(), t, fixture.database.Pool, request.CallerID, request.DelegationID); bindings != 1 || lifecycles != 0 || receipts != 0 {
					t.Fatalf("denial created lifecycle data: binding=%d lifecycle=%d receipts=%d", bindings, lifecycles, receipts)
				}
			})
		}
	})
}

func TestPlanTaskDeliveryV1HTTPProjectorMustMatchDurableBinding(t *testing.T) {
	projector := &plantasksHTTPOperationProjector{mode: "wrong_digest"}
	fixture := newPlantasksHTTPFixture(t, &plantasksDelegationPolicy{}, projector)
	raw, request := plantasksDelegationRequest(t)
	endpoint := fixture.server.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
	status, _, firstError := plantasksHTTPRequest(t, fixture.client, http.MethodPut, endpoint, "owner-token", "application/json", raw)
	plantasksHTTPAssertError(t, status, firstError, http.StatusServiceUnavailable, "observation_unavailable")
	for _, mode := range []string{"wrong_delegation", "wrong_lifecycle", "wrong_sequence", "invent_landed", "oversized"} {
		projector.setMode(mode)
		status, _, body := plantasksHTTPRequest(t, fixture.client, http.MethodGet, endpoint, "owner-token", "", nil)
		plantasksHTTPAssertError(t, status, body, http.StatusServiceUnavailable, "observation_unavailable")
	}
	projector.setMode("")
	status, _, recovered := plantasksHTTPRequest(t, fixture.client, http.MethodGet, endpoint, "owner-token", "", nil)
	if status != http.StatusOK {
		t.Fatalf("valid durable binding failed after projector recovery: status=%d body=%s", status, recovered)
	}
	if bytes.Contains(firstError, []byte("private")) || bytes.Contains(firstError, []byte("digest")) {
		t.Fatalf("projector validation leaked internal details: %s", firstError)
	}
}

func TestPlanTaskDeliveryV1HTTPExpiredRequestContextCannotReturnLateSuccess(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	projector := &plantasksHTTPOperationProjector{barrier: release, entered: entered, ignoreContext: true}
	fixture := newPlantasksHTTPFixture(t, &plantasksDelegationPolicy{}, projector)
	raw, request := plantasksDelegationRequest(t)
	endpoint := fixture.server.URL + "/v1/delegations/" + url.PathEscape(request.DelegationID)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	message, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("construct deadline request: %v", err)
	}
	message.Header.Set("Authorization", "Bearer owner-token")
	message.Header.Set("Content-Type", "application/json")
	type responseResult struct {
		status int
		err    error
	}
	response := make(chan responseResult, 1)
	go func() {
		resp, err := fixture.client.Do(message)
		if err != nil {
			response <- responseResult{err: err}
			return
		}
		_, readErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			err = readErr
		} else if closeErr != nil {
			err = closeErr
		}
		response <- responseResult{status: resp.StatusCode, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("HTTP request did not reach the deliberately blocked projector")
	}
	<-ctx.Done()
	close(release)
	select {
	case result := <-response:
		if result.err == nil && result.status == http.StatusOK {
			t.Fatal("expired HTTP context returned a positive observation")
		}
		if result.err != nil && !errors.Is(result.err, context.DeadlineExceeded) && !strings.Contains(result.err.Error(), "context deadline exceeded") {
			t.Fatalf("expired request result=%v, want context deadline or redacted non-success", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expired request did not finish within bounded wait")
	}
}
