package hostauth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/results"
)

const (
	testIssuer = "aprl-test-worker"
	testTaskID = "11111111-1111-4111-8111-111111111111"
	testRunID  = "22222222-2222-4222-8222-222222222222"
)

func TestAuthenticateSupervisorRejectsInvalidCredentialsAndClaims(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	clk := clock.NewManual(now)
	auth := mustAuthenticator(t, clk, defaultConfig())
	principal := validTestPrincipal()
	validToken := mustToken(t, auth, principal)
	key := testKey("active")

	tests := []struct {
		name     string
		token    string
		verifyAt time.Time
	}{
		{name: "tampered signature", token: tamperSignature(validToken), verifyAt: now},
		{name: "wrong signing key", token: signedToken(t, "active", testKey("wrong"), marshalClaims(testClaims(now, principal))), verifyAt: now},
		{name: "expired at exact boundary", token: validToken, verifyAt: now.Add(5 * time.Minute)},
		{name: "future issued by one nanosecond", token: signedToken(t, "active", key, marshalClaims(testClaims(now.Add(time.Nanosecond), principal))), verifyAt: now},
		{name: "lifetime exceeds configured ttl", token: signedToken(t, "active", key, marshalClaims(changeClaims(testClaims(now, principal), func(cl *claims) { cl.ExpiresAt = cl.IssuedAt + int64(6*time.Minute) }))), verifyAt: now},
		{name: "wrong audience", token: signedToken(t, "active", key, marshalClaims(changeClaims(testClaims(now, principal), func(cl *claims) { cl.Audience = "aprl:other" }))), verifyAt: now},
		{name: "wrong issuer", token: signedToken(t, "active", key, marshalClaims(changeClaims(testClaims(now, principal), func(cl *claims) { cl.Issuer = "other-host" }))), verifyAt: now},
		{name: "unknown key id", token: signedToken(t, "unknown", key, marshalClaims(testClaims(now, principal))), verifyAt: now},
		{name: "duplicate claim", token: signedToken(t, "active", key, duplicateVersionClaims(now, principal)), verifyAt: now},
		{name: "unknown claim", token: signedToken(t, "active", key, unknownClaimPayload(now, principal)), verifyAt: now},
		{name: "trailing claim data", token: signedToken(t, "active", key, append(marshalClaims(testClaims(now, principal)), []byte(" {}")...)), verifyAt: now},
		{name: "padded base64", token: validToken + "=", verifyAt: now},
		{name: "uppercase principal UUID", token: signedToken(t, "active", key, marshalClaims(changeClaims(testClaims(now, principal), func(cl *claims) { cl.TaskID = strings.ToUpper(cl.TaskID) }))), verifyAt: now},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk.Set(now)
			if !tt.verifyAt.IsZero() {
				clk.Set(tt.verifyAt)
			}
			req := tlsRequest(tt.token)
			_, err := auth.AuthenticateSupervisor(context.Background(), req)
			if !errors.Is(err, ErrInvalidCredential) {
				t.Fatalf("AuthenticateSupervisor() error = %v, want ErrInvalidCredential", err)
			}
		})
	}
}

func TestAuthenticateSupervisorAcceptsConfiguredKeyRotation(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	clk := clock.NewManual(now)
	oldConfig := defaultConfig()
	oldConfig.ActiveKeyID = "old"
	oldConfig.Keys = map[string][]byte{"old": testKey("old")}
	oldAuth := mustAuthenticator(t, clk, oldConfig)
	token := mustToken(t, oldAuth, validTestPrincipal())

	rotated := defaultConfig()
	rotated.ActiveKeyID = "new"
	rotated.Keys = map[string][]byte{"old": testKey("old"), "new": testKey("new")}
	newAuth := mustAuthenticator(t, clk, rotated)
	got, err := newAuth.AuthenticateSupervisor(context.Background(), tlsRequest(token))
	if err != nil {
		t.Fatalf("AuthenticateSupervisor() error = %v", err)
	}
	if got != validTestPrincipal() {
		t.Fatalf("AuthenticateSupervisor() = %+v, want %+v", got, validTestPrincipal())
	}
}

func TestNewClonesKeysAndRejectsTypedNilClock(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC))
	config := defaultConfig()
	original := append([]byte(nil), config.Keys["active"]...)
	auth := mustAuthenticator(t, clk, config)
	config.Keys["active"][0] ^= 0xff
	config.Keys["added"] = testKey("added")
	token := mustToken(t, auth, validTestPrincipal())
	if _, err := auth.AuthenticateSupervisor(context.Background(), tlsRequest(token)); err != nil {
		t.Fatalf("authenticator did not retain cloned key: %v", err)
	}
	if !hmacEqual(auth.keys["active"], original) {
		t.Fatal("authenticator key changed after caller mutation")
	}

	var nilClock *clock.Manual
	if _, err := New(defaultConfig(), nilClock); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(typed nil clock) error = %v, want ErrInvalidConfig", err)
	}
}

func TestNewValidatesTrustedConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{name: "empty issuer", change: func(c *Config) { c.Issuer = "" }},
		{name: "non-ascii issuer", change: func(c *Config) { c.Issuer = "host-\u00e9" }},
		{name: "wrong audience", change: func(c *Config) { c.Audience = "other" }},
		{name: "missing active key", change: func(c *Config) { c.ActiveKeyID = "missing" }},
		{name: "short key", change: func(c *Config) { c.Keys["active"] = []byte("short") }},
		{name: "unsafe key id", change: func(c *Config) { c.Keys["bad/key"] = testKey("bad"); c.ActiveKeyID = "bad/key" }},
		{name: "framing delimiter in key id", change: func(c *Config) { c.Keys["bad.key"] = testKey("bad-dot"); c.ActiveKeyID = "bad.key" }},
		{name: "ttl too short", change: func(c *Config) { c.TTL = time.Minute - time.Second }},
		{name: "ttl too long", change: func(c *Config) { c.TTL = maxTTL + time.Second }},
		{name: "fractional ttl", change: func(c *Config) { c.TTL = time.Minute + time.Nanosecond }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := defaultConfig()
			tt.change(&config)
			if _, err := New(config, clock.NewManual(time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC))); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestTokenRejectsUnsafePrincipalText(t *testing.T) {
	auth := mustAuthenticator(t, clock.NewManual(time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)), defaultConfig())
	tests := []struct {
		name   string
		change func(*results.Principal)
	}{
		{name: "identity whitespace", change: func(p *results.Principal) { p.Identity = "review worker" }},
		{name: "credential unicode", change: func(p *results.Principal) { p.CredentialID = "cred-\u200b" }},
		{name: "identity non-ascii", change: func(p *results.Principal) { p.Identity = "B\u00e9" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := validTestPrincipal()
			tt.change(&principal)
			if _, err := auth.Token(context.Background(), principal); !errors.Is(err, ErrInvalidPrincipal) {
				t.Fatalf("Token() error = %v, want ErrInvalidPrincipal", err)
			}
		})
	}
}

func TestAuthenticateSupervisorRequiresTLSExactRouteAndOneBearerHeader(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	clk := clock.NewManual(now)
	auth := mustAuthenticator(t, clk, defaultConfig())
	token := mustToken(t, auth, validTestPrincipal())

	tests := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{name: "plain HTTP", mutate: func(r *http.Request) { r.TLS = nil }},
		{name: "wrong method", mutate: func(r *http.Request) { r.Method = http.MethodGet }},
		{name: "wrong path", mutate: func(r *http.Request) { r.URL.Path = "/internal/other" }},
		{name: "escaped path", mutate: func(r *http.Request) { r.URL.RawPath = "/internal/%72esults" }},
		{name: "query", mutate: func(r *http.Request) { r.URL.RawQuery = "x=1" }},
		{name: "wrong scheme", mutate: func(r *http.Request) { r.URL.Scheme = "http" }},
		{name: "duplicate authorization", mutate: func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+token) }},
		{name: "empty bearer", mutate: func(r *http.Request) { r.Header.Set("Authorization", "Bearer ") }},
		{name: "extra bearer text", mutate: func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token+" extra") }},
		{name: "wrong scheme", mutate: func(r *http.Request) { r.Header.Set("Authorization", "Basic "+token) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tlsRequest(token)
			tt.mutate(req)
			_, err := auth.AuthenticateSupervisor(context.Background(), req)
			if err == nil {
				t.Fatal("AuthenticateSupervisor() unexpectedly succeeded")
			}
		})
	}
}

func TestAuthenticateSupervisorUsesSignedPrincipalAndDoesNotReadBody(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC))
	auth := mustAuthenticator(t, clk, defaultConfig())
	principal := validTestPrincipal()
	token := mustToken(t, auth, principal)
	body := &observedBody{Reader: strings.NewReader(`{"task_id":"33333333-3333-4333-8333-333333333333","run_id":"44444444-4444-4444-8444-444444444444","identity":"forged","credential_id":"forged"}`)}
	req := tlsRequest(token)
	req.Body = body
	got, err := auth.AuthenticateSupervisor(context.Background(), req)
	if err != nil {
		t.Fatalf("AuthenticateSupervisor() error = %v", err)
	}
	if got != principal {
		t.Fatalf("AuthenticateSupervisor() = %+v, want signed principal %+v", got, principal)
	}
	if body.read {
		t.Fatal("AuthenticateSupervisor() read caller-controlled request body")
	}

	spoofed := tlsRequest("invalid")
	spoofed.Body = &observedBody{Reader: strings.NewReader(`{"task_id":"` + principal.TaskID + `"}`)}
	if _, err := auth.AuthenticateSupervisor(context.Background(), spoofed); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("body spoof without valid credential error = %v, want ErrInvalidCredential", err)
	}
}

func TestAuthenticateSupervisorAcceptsRealTLSRequest(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC))
	auth := mustAuthenticator(t, clk, defaultConfig())
	token := mustToken(t, auth, validTestPrincipal())
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.AuthenticateSupervisor(r.Context(), r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(principal)
	}))
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/internal/results", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("TLS request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("TLS request status = %d, want 200", response.StatusCode)
	}
}

type observedBody struct {
	io.Reader
	read bool
}

func (b *observedBody) Read(p []byte) (int, error) {
	b.read = true
	return b.Reader.Read(p)
}

func (b *observedBody) Close() error { return nil }

func defaultConfig() Config {
	return Config{Issuer: testIssuer, Audience: Audience, ActiveKeyID: "active", Keys: map[string][]byte{"active": testKey("active")}, TTL: 5 * time.Minute}
}

func validTestPrincipal() results.Principal {
	return results.Principal{TaskID: testTaskID, RunID: testRunID, Identity: "B", CredentialID: "cred-review-1"}
}

func testKey(label string) []byte {
	key := sha256.Sum256([]byte("test-key:" + label))
	return key[:]
}

func testClaims(now time.Time, principal results.Principal) claims {
	issued := now.UnixNano()
	return claims{Version: 1, Issuer: testIssuer, Audience: Audience, TaskID: principal.TaskID, RunID: principal.RunID,
		Identity: principal.Identity, CredentialID: principal.CredentialID, IssuedAt: issued, NotBefore: issued, ExpiresAt: issued + int64(5*time.Minute)}
}

func changeClaims(cl claims, change func(*claims)) claims {
	change(&cl)
	return cl
}

func marshalClaims(cl claims) []byte {
	data, _ := json.Marshal(cl)
	return data
}

func signedToken(t *testing.T, keyID string, key, payload []byte) string {
	t.Helper()
	return tokenVersion + "." + keyID + "." + base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sign(key, keyID, payload))
}

func duplicateVersionClaims(now time.Time, principal results.Principal) []byte {
	cl := testClaims(now, principal)
	canonical := marshalClaims(cl)
	return append([]byte(`{"ver":1,"ver":1,`), canonical[len(`{"ver":1,`):]...)
}

func unknownClaimPayload(now time.Time, principal results.Principal) []byte {
	cl := testClaims(now, principal)
	payload := marshalClaims(cl)
	return append(payload[:len(payload)-1], []byte(`,"extra":"x"}`)...)
}

func tamperSignature(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 4 {
		return token + "x"
	}
	mac, _ := base64.RawURLEncoding.DecodeString(parts[3])
	mac[0] ^= 1
	parts[3] = base64.RawURLEncoding.EncodeToString(mac)
	return strings.Join(parts, ".")
}

func tlsRequest(token string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "https://aprl.test/internal/results", nil)
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

func mustAuthenticator(t *testing.T, clk clock.Clock, config Config) *Authenticator {
	t.Helper()
	auth, err := New(config, clk)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return auth
}

func mustToken(t *testing.T, auth *Authenticator, principal results.Principal) string {
	t.Helper()
	token, err := auth.Token(context.Background(), principal)
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	return token
}

func hmacEqual(a, b []byte) bool {
	return hmac.Equal(a, b)
}
