package resultclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/results"
)

const (
	testTaskID      = "11111111-1111-4111-8111-111111111111"
	testJobID       = "22222222-2222-4222-8222-222222222222"
	testRunID       = "33333333-3333-4333-8333-333333333333"
	testOperationID = "44444444-4444-4444-8444-444444444444"
	testCorrelation = "55555555-5555-4555-8555-555555555555"
	testLease       = "66666666-6666-4666-8666-666666666666"
	testToken       = "host-token.secret"
)

type staticTokenSource struct {
	token string
	err   error
	calls atomic.Int32
}

func (s *staticTokenSource) Token(context.Context, results.Principal) (string, error) {
	s.calls.Add(1)
	return s.token, s.err
}

func validTestResult() contracts.Result {
	return contracts.Result{
		Version: contracts.VersionV1, TaskID: testTaskID, JobID: testJobID, RunID: testRunID,
		Generation: 1, LeaseToken: testLease, Attempt: 1, OperationID: testOperationID,
		CorrelationID: testCorrelation, Status: "succeeded", Summary: "finished",
	}
}

func validTestPrincipal() results.Principal {
	return results.Principal{TaskID: testTaskID, RunID: testRunID, Identity: "worker-1", CredentialID: "credential-1"}
}

func trustedTestHTTPClient(t *testing.T, server *httptest.Server) *http.Client {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
}

func TestSubmitPostsSingleAuthenticatedResult(t *testing.T) {
	result := validTestResult()
	principal := validTestPrincipal()
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != resultsPath || r.URL.RawQuery != "" {
			t.Errorf("request target = %s %q?%q", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content type = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		got, err := contracts.DecodeResult(body)
		if err != nil || got != result {
			t.Errorf("decoded result = %#v, err = %v", got, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"accepted":true,"operation_id":%q}`, result.OperationID)
	}))
	t.Cleanup(server.Close)
	tokens := &staticTokenSource{token: testToken}
	client, err := New(server.URL+resultsPath, trustedTestHTTPClient(t, server), tokens)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(client.CloseIdleConnections)
	accepted, err := client.Submit(context.Background(), principal, result)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if accepted.OperationID != result.OperationID || accepted.Duplicate {
		t.Fatalf("accepted = %#v", accepted)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want exactly one", got)
	}
	if got := tokens.calls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want one", got)
	}
}

func TestSubmitMapsHostStatuses(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   error
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, want: ErrUnauthorized},
		{name: "forbidden", status: http.StatusForbidden, want: ErrForbidden},
		{name: "stale", status: http.StatusConflict, want: ErrStale},
		{name: "rate limited", status: http.StatusTooManyRequests, want: ErrUnavailable},
		{name: "unavailable", status: http.StatusServiceUnavailable, want: ErrUnavailable},
		{name: "ambiguous server error", status: http.StatusInternalServerError, want: ErrUnknown},
		{name: "unexpected client status", status: http.StatusBadRequest, want: ErrProtocol},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, `{"error":"sensitive response detail"}`)
			}))
			t.Cleanup(server.Close)
			client, err := New(server.URL+resultsPath, trustedTestHTTPClient(t, server), &staticTokenSource{token: testToken})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(client.CloseIdleConnections)
			_, err = client.Submit(context.Background(), validTestPrincipal(), validTestResult())
			if !errors.Is(err, tc.want) {
				t.Fatalf("Submit error = %v, want errors.Is(_, %v)", err, tc.want)
			}
			if strings.Contains(err.Error(), "sensitive response detail") || strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), server.URL) {
				t.Fatalf("error exposed remote detail: %v", err)
			}
		})
	}
}

func TestSubmitRejectsMalformedAcceptance(t *testing.T) {
	cases := []struct {
		name string
		body func() string
	}{
		{name: "duplicate member", body: func() string { return `{"accepted":true,"accepted":true,"operation_id":"` + testOperationID + `"}` }},
		{name: "extra member", body: func() string { return `{"accepted":true,"operation_id":"` + testOperationID + `","extra":1}` }},
		{name: "trailing JSON", body: func() string { return `{"accepted":true,"operation_id":"` + testOperationID + `"} {}` }},
		{name: "wrong operation", body: func() string { return `{"accepted":true,"operation_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}` }},
		{name: "not accepted", body: func() string { return `{"accepted":false,"operation_id":"` + testOperationID + `"}` }},
		{name: "invalid JSON", body: func() string { return `{` }},
		{name: "oversize", body: func() string {
			return `{"accepted":true,"operation_id":"` + testOperationID + `","padding":"` + strings.Repeat("x", maxMessageBytes) + `"}`
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.body())
			}))
			t.Cleanup(server.Close)
			client, err := New(server.URL+resultsPath, trustedTestHTTPClient(t, server), &staticTokenSource{token: testToken})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(client.CloseIdleConnections)
			_, err = client.Submit(context.Background(), validTestPrincipal(), validTestResult())
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("Submit error = %v, want ErrProtocol", err)
			}
		})
	}
}

func TestSubmitDoesNotFollowRedirect(t *testing.T) {
	var destinationRequests atomic.Int32
	destination := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destinationRequests.Add(1) }))
	t.Cleanup(destination.Close)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", destination.URL+resultsPath)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	client, err := New(server.URL+resultsPath, trustedTestHTTPClient(t, server), &staticTokenSource{token: testToken})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(client.CloseIdleConnections)
	_, err = client.Submit(context.Background(), validTestPrincipal(), validTestResult())
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Submit error = %v, want ErrProtocol", err)
	}
	if got := destinationRequests.Load(); got != 0 {
		t.Fatalf("redirect destination received %d requests", got)
	}
}

func TestSubmitUnknownNetworkOutcomeAndCancellation(t *testing.T) {
	t.Run("closed server", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		client, err := New(server.URL+resultsPath, trustedTestHTTPClient(t, server), &staticTokenSource{token: testToken})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(client.CloseIdleConnections)
		server.Close()
		_, err = client.Submit(context.Background(), validTestPrincipal(), validTestResult())
		if !errors.Is(err, ErrUnknown) {
			t.Fatalf("Submit error = %v, want ErrUnknown", err)
		}
		if strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), testToken) {
			t.Fatalf("error exposed request details: %v", err)
		}
	})

	t.Run("cancel in flight", func(t *testing.T) {
		entered := make(chan struct{})
		server := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			close(entered)
			<-r.Context().Done()
		}))
		t.Cleanup(server.Close)
		client, err := New(server.URL+resultsPath, trustedTestHTTPClient(t, server), &staticTokenSource{token: testToken})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(client.CloseIdleConnections)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, submitErr := client.Submit(ctx, validTestPrincipal(), validTestResult())
			done <- submitErr
		}()
		<-entered
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, ErrUnknown) || !errors.Is(err, context.Canceled) {
				t.Fatalf("Submit error = %v, want ErrUnknown and context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Submit did not return after cancellation")
		}
	})
}

func TestNewRejectsUnpinnedEndpointsAndUntrustedClients(t *testing.T) {
	trusted := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}
	typedNil := (*staticTokenSource)(nil)
	cases := []struct {
		name     string
		endpoint string
		client   *http.Client
		tokens   TokenSource
	}{
		{name: "plain HTTP", endpoint: "http://example.test" + resultsPath, client: trusted, tokens: &staticTokenSource{token: testToken}},
		{name: "userinfo", endpoint: "https://user@example.test" + resultsPath, client: trusted, tokens: &staticTokenSource{token: testToken}},
		{name: "query", endpoint: "https://example.test" + resultsPath + "?x=1", client: trusted, tokens: &staticTokenSource{token: testToken}},
		{name: "fragment", endpoint: "https://example.test" + resultsPath + "#fragment", client: trusted, tokens: &staticTokenSource{token: testToken}},
		{name: "wrong path", endpoint: "https://example.test/other", client: trusted, tokens: &staticTokenSource{token: testToken}},
		{name: "trailing slash", endpoint: "https://example.test" + resultsPath + "/", client: trusted, tokens: &staticTokenSource{token: testToken}},
		{name: "nil client", endpoint: "https://example.test" + resultsPath, client: nil, tokens: &staticTokenSource{token: testToken}},
		{name: "typed nil tokens", endpoint: "https://example.test" + resultsPath, client: trusted, tokens: typedNil},
		{name: "implicit transport", endpoint: "https://example.test" + resultsPath, client: &http.Client{}, tokens: &staticTokenSource{token: testToken}},
		{name: "insecure TLS", endpoint: "https://example.test" + resultsPath, client: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}, tokens: &staticTokenSource{token: testToken}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if client, err := New(tc.endpoint, tc.client, tc.tokens); err == nil {
				client.CloseIdleConnections()
				t.Fatal("New accepted invalid endpoint or client")
			}
		})
	}
}

func TestSubmitRejectsMismatchedPrincipalAndEmptyCredential(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"accepted":true,"operation_id":%q}`, testOperationID)
	}))
	t.Cleanup(server.Close)
	client, err := New(server.URL+resultsPath, trustedTestHTTPClient(t, server), &staticTokenSource{token: ""})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(client.CloseIdleConnections)
	principal := validTestPrincipal()
	principal.RunID = "77777777-7777-4777-8777-777777777777"
	if _, err := client.Submit(context.Background(), principal, validTestResult()); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mismatched principal error = %v, want ErrForbidden", err)
	}
	if _, err := client.Submit(context.Background(), validTestPrincipal(), validTestResult()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("empty token error = %v, want ErrUnauthorized", err)
	}
	client.tokens = &staticTokenSource{token: strings.Repeat("t", maxTokenBytes+1)}
	if _, err := client.Submit(context.Background(), validTestPrincipal(), validTestResult()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("oversize token error = %v, want ErrUnauthorized", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("unauthorized submissions sent %d requests", got)
	}
}

func TestSubmitRejectsOversizeRequestAndCanceledContext(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"accepted":true,"operation_id":%q}`, testOperationID)
	}))
	t.Cleanup(server.Close)
	tokens := &staticTokenSource{token: testToken}
	client, err := New(server.URL+resultsPath, trustedTestHTTPClient(t, server), tokens)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(client.CloseIdleConnections)
	tooLarge := validTestResult()
	tooLarge.Summary = strings.Repeat("x", maxMessageBytes)
	if _, err := client.Submit(context.Background(), validTestPrincipal(), tooLarge); !errors.Is(err, ErrProtocol) {
		t.Fatalf("oversize result error = %v, want ErrProtocol", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Submit(ctx, validTestPrincipal(), validTestResult()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("invalid submissions sent %d requests", got)
	}
	if got := tokens.calls.Load(); got != 0 {
		t.Fatalf("invalid submissions requested tokens %d times", got)
	}
}

func TestDecodeAcceptedRejectsMalformedContentAndTrailingData(t *testing.T) {
	cases := []string{
		`[]`,
		`{"accepted":1,"operation_id":"` + testOperationID + `"}`,
		`{"accepted":true,"operation_id":null}`,
		`{"accepted":true,"operation_id":"` + testOperationID + `"} trailing`,
	}
	for i, input := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			if _, err := decodeAccepted([]byte(input)); !errors.Is(err, ErrProtocol) {
				t.Fatalf("decodeAccepted error = %v, want ErrProtocol", err)
			}
		})
	}
}

func TestNewDoesNotMutateCallerTLSClient(t *testing.T) {
	server, err := url.Parse("https://example.test" + resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
	created, err := New(server.String(), client, &staticTokenSource{token: testToken})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(created.CloseIdleConnections)
	if client.CheckRedirect == nil || transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("New mutated caller-owned HTTP client")
	}
}

var _ TokenSource = (*staticTokenSource)(nil)
var _ interface {
	Submit(context.Context, results.Principal, contracts.Result) (results.Accepted, error)
} = (*Client)(nil)
