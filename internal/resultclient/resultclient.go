// Package resultclient submits worker results through APRL's authenticated
// host boundary. Credentials are obtained from a trusted host source and are
// never included in the result payload or error messages.
package resultclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/results"
)

const (
	resultsPath       = "/internal/results"
	maxMessageBytes   = 1 << 20
	maxTokenBytes     = 4096
	maxRequestTimeout = 8 * time.Second
)

var (
	// ErrUnauthorized means the host did not accept the submitted credential.
	ErrUnauthorized = errors.New("result submission unauthorized")
	// ErrForbidden means the authenticated host principal lacks run authority.
	ErrForbidden = errors.New("result submission forbidden")
	// ErrStale means the result no longer matches the authoritative run fence.
	ErrStale = errors.New("result submission is stale")
	// ErrProtocol means the host response or local transport contract is invalid.
	ErrProtocol = errors.New("result submission protocol violation")
	// ErrUnavailable means the host explicitly rejected the request as unavailable.
	ErrUnavailable = errors.New("result service unavailable")
	// ErrUnknown means the request outcome could not be established. Callers must
	// reconcile the operation with the host before making another submission.
	ErrUnknown = errors.New("result submission outcome unknown")
)

// TokenSource supplies a short-lived host credential for the immutable result
// principal. Implementations must not derive principal fields from the result.
type TokenSource interface {
	Token(context.Context, results.Principal) (string, error)
}

// Client implements supervisor.ResultSubmitter without acquiring authority or
// retrying operations itself.
type Client struct {
	endpoint *url.URL
	http     *http.Client
	tokens   TokenSource
}

// New constructs a client pinned to one HTTPS /internal/results endpoint. The
// supplied transport must use normal certificate verification; redirects are
// always returned to the caller without being followed.
func New(endpoint string, client *http.Client, tokens TokenSource) (*Client, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed == nil || parsed.Scheme != "https" || parsed.Opaque != "" || parsed.Host == "" ||
		parsed.User != nil || parsed.Path != resultsPath || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || strings.TrimSpace(endpoint) != endpoint {
		return nil, fmt.Errorf("construct result client: %w", ErrProtocol)
	}
	if client == nil || isNil(tokens) || client.Timeout < 0 {
		return nil, fmt.Errorf("construct result client: %w", ErrProtocol)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil {
		return nil, fmt.Errorf("construct result client: trusted TLS transport required: %w", ErrProtocol)
	}
	if err := validateTLSConfig(transport); err != nil {
		return nil, fmt.Errorf("construct result client: %w", err)
	}
	trustedTransport := transport.Clone()
	trustedTLSConfig := transport.TLSClientConfig.Clone()
	if trustedTLSConfig.RootCAs != nil {
		trustedTLSConfig.RootCAs = trustedTLSConfig.RootCAs.Clone()
	}
	trustedTransport.TLSClientConfig = trustedTLSConfig
	trustedClient := *client
	trustedClient.Transport = trustedTransport
	trustedClient.Jar = nil
	trustedClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if trustedClient.Timeout == 0 || trustedClient.Timeout > maxRequestTimeout {
		trustedClient.Timeout = maxRequestTimeout
	}
	return &Client{endpoint: parsed, http: &trustedClient, tokens: tokens}, nil
}

func validateTLSConfig(transport *http.Transport) error {
	config := transport.TLSClientConfig
	if config == nil || config.InsecureSkipVerify || transport.DialTLS != nil || transport.DialTLSContext != nil || transport.TLSNextProto != nil {
		return ErrProtocol
	}
	// A nil RootCAs intentionally means the platform trust store. A non-nil pool
	// is cloned by New so callers cannot change trust after construction.
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Submit validates and sends exactly one result. Transport failures are
// ambiguous because the host may have committed before the connection failed.
func (c *Client) Submit(ctx context.Context, principal results.Principal, result contracts.Result) (results.Accepted, error) {
	if c == nil || c.endpoint == nil || c.http == nil || isNil(c.tokens) || ctx == nil {
		return results.Accepted{}, ErrProtocol
	}
	if err := ctx.Err(); err != nil {
		return results.Accepted{}, err
	}
	if !strings.EqualFold(principal.TaskID, result.TaskID) || !strings.EqualFold(principal.RunID, result.RunID) {
		return results.Accepted{}, ErrForbidden
	}
	if err := result.Validate(); err != nil {
		return results.Accepted{}, ErrProtocol
	}

	requestContext, cancel := context.WithTimeout(ctx, maxRequestTimeout)
	defer cancel()
	token, err := c.tokens.Token(requestContext, principal)
	if err != nil {
		if requestContext.Err() != nil {
			return results.Accepted{}, requestContext.Err()
		}
		return results.Accepted{}, ErrUnavailable
	}
	if !validBearerToken(token) {
		return results.Accepted{}, ErrUnauthorized
	}
	body, err := json.Marshal(result)
	if err != nil || len(body) > maxMessageBytes {
		return results.Accepted{}, ErrProtocol
	}

	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, c.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return results.Accepted{}, ErrProtocol
	}
	// POST is not retryable by net/http's transport without an idempotency key;
	// additionally remove GetBody so a transport cannot replay this payload.
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return results.Accepted{}, errors.Join(ErrUnknown, contextError(requestContext.Err()))
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxMessageBytes+1))
	closeErr := response.Body.Close()
	if len(responseBody) > maxMessageBytes && response.StatusCode == http.StatusOK {
		return results.Accepted{}, ErrProtocol
	}
	if readErr != nil || closeErr != nil {
		return results.Accepted{}, errors.Join(ErrUnknown, contextError(requestContext.Err()))
	}

	switch response.StatusCode {
	case http.StatusUnauthorized:
		return results.Accepted{}, ErrUnauthorized
	case http.StatusForbidden:
		return results.Accepted{}, ErrForbidden
	case http.StatusConflict:
		return results.Accepted{}, ErrStale
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return results.Accepted{}, ErrUnavailable
	case http.StatusOK:
	default:
		if response.StatusCode >= 500 {
			return results.Accepted{}, ErrUnknown
		}
		return results.Accepted{}, ErrProtocol
	}
	if !isJSONContentType(response.Header.Get("Content-Type")) {
		return results.Accepted{}, ErrProtocol
	}
	if !utf8.Valid(responseBody) {
		return results.Accepted{}, ErrProtocol
	}
	accepted, err := decodeAccepted(responseBody)
	if err != nil || !accepted.Accepted || accepted.OperationID != result.OperationID {
		return results.Accepted{}, ErrProtocol
	}
	return results.Accepted{OperationID: accepted.OperationID}, nil
}

func contextError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func validBearerToken(token string) bool {
	if token == "" || len(token) > maxTokenBytes {
		return false
	}
	padding := false
	for _, ch := range token {
		if ch == '=' {
			padding = true
			continue
		}
		if padding || !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '.' || ch == '_' || ch == '~' || ch == '+' || ch == '/') {
			return false
		}
	}
	return true
}

func isJSONContentType(raw string) bool {
	mediaType, _, err := mime.ParseMediaType(raw)
	return err == nil && mediaType == "application/json"
}

type acceptedResponse struct {
	Accepted    bool
	OperationID string
}

func decodeAccepted(body []byte) (acceptedResponse, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return acceptedResponse{}, ErrProtocol
	}
	fields := make(map[string]json.RawMessage, 2)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return acceptedResponse{}, ErrProtocol
		}
		key, ok := keyToken.(string)
		if !ok || (key != "accepted" && key != "operation_id") {
			return acceptedResponse{}, ErrProtocol
		}
		if _, exists := fields[key]; exists {
			return acceptedResponse{}, ErrProtocol
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return acceptedResponse{}, ErrProtocol
		}
		fields[key] = value
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || len(fields) != 2 {
		return acceptedResponse{}, ErrProtocol
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return acceptedResponse{}, ErrProtocol
	}
	var accepted bool
	var operationID string
	if json.Unmarshal(fields["accepted"], &accepted) != nil || json.Unmarshal(fields["operation_id"], &operationID) != nil {
		return acceptedResponse{}, ErrProtocol
	}
	return acceptedResponse{Accepted: accepted, OperationID: operationID}, nil
}

// CloseIdleConnections releases keep-alive connections owned by this client.
// Call it when the client is no longer used.
func (c *Client) CloseIdleConnections() {
	if c != nil && c.http != nil {
		c.http.CloseIdleConnections()
	}
}
