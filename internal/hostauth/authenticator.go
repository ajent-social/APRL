// Package hostauth issues and verifies short-lived host credentials for worker
// result submission. Credentials are held and used by trusted host processes;
// they are never part of a worker payload or environment.
package hostauth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/httpapi"
	"github.com/ajent-social/APRL/internal/results"
)

const (
	// Audience is the only audience accepted by the worker-result endpoint.
	Audience = "aprl:worker-results:v1"

	tokenVersion      = "v1"
	maxIssuerBytes    = 256
	maxKeyIDBytes     = 64
	maxKeyBytes       = 4096
	maxKeys           = 16
	maxIdentityBytes  = 256
	maxCredentialSize = 256
	maxTokenBytes     = 4096
	minTTL            = time.Minute
	maxTTL            = 15 * time.Minute
)

var (
	// ErrInvalidConfig reports a missing or unsafe trusted host configuration.
	ErrInvalidConfig = errors.New("invalid host authentication configuration")
	// ErrInvalidCredential reports an invalid, expired, or untrusted credential.
	ErrInvalidCredential = errors.New("invalid supervisor credential")
	// ErrInvalidRequest reports a request outside the authenticated transport boundary.
	ErrInvalidRequest = errors.New("invalid supervisor authentication request")
	// ErrInvalidPrincipal reports a principal that cannot be represented safely.
	ErrInvalidPrincipal = errors.New("invalid supervisor principal")
)

// Config contains trusted host policy. Audience must equal Audience; Issuer is
// a stable, nonempty logical identifier selected by the trusted host.
type Config struct {
	Issuer      string            // Issuer is the stable trusted logical host identifier.
	Audience    string            // Audience must equal the fixed worker-results audience.
	ActiveKeyID string            // ActiveKeyID selects the key used for new credentials.
	Keys        map[string][]byte // Keys includes the active key and any retained rotation keys.
	TTL         time.Duration     // TTL is the whole-second credential lifetime, from 1 to 15 minutes.
}

// Authenticator issues credentials with the active key and verifies credentials
// signed by any configured key, allowing bounded key rotation.
type Authenticator struct {
	issuer      string
	audience    string
	activeKeyID string
	keys        map[string][]byte
	ttl         time.Duration
	clock       clock.Clock
}

type claims struct {
	Version      int    `json:"ver"`
	Issuer       string `json:"iss"`
	Audience     string `json:"aud"`
	TaskID       string `json:"task_id"`
	RunID        string `json:"run_id"`
	Identity     string `json:"identity"`
	CredentialID string `json:"credential_id"`
	// iat, nbf and exp use Unix nanoseconds so validity has no whole-second leeway.
	IssuedAt  int64 `json:"iat"`
	NotBefore int64 `json:"nbf"`
	ExpiresAt int64 `json:"exp"`
}

// New validates and clones trusted configuration. It never retains caller-owned
// key slices or maps.
func New(config Config, c clock.Clock) (*Authenticator, error) {
	if nilDependency(c) || !safeText(config.Issuer, maxIssuerBytes) ||
		config.Audience != Audience || !validKeyID(config.ActiveKeyID) ||
		config.TTL < minTTL || config.TTL > maxTTL || config.TTL%time.Second != 0 ||
		len(config.Keys) == 0 || len(config.Keys) > maxKeys {
		return nil, ErrInvalidConfig
	}
	keys := make(map[string][]byte, len(config.Keys))
	for id, key := range config.Keys {
		if !validKeyID(id) || len(key) < sha256.Size || len(key) > maxKeyBytes {
			return nil, ErrInvalidConfig
		}
		keys[id] = append([]byte(nil), key...)
	}
	if _, ok := keys[config.ActiveKeyID]; !ok {
		return nil, ErrInvalidConfig
	}
	return &Authenticator{
		issuer:      config.Issuer,
		audience:    Audience,
		activeKeyID: config.ActiveKeyID,
		keys:        keys,
		ttl:         config.TTL,
		clock:       c,
	}, nil
}

// Token creates a bearer credential for one host-authenticated worker result
// principal. The signed claims are bound to that principal and expire within the
// configured TTL.
func (a *Authenticator) Token(ctx context.Context, principal results.Principal) (string, error) {
	if a == nil || ctx == nil || ctx.Err() != nil {
		return "", ErrInvalidRequest
	}
	if !validPrincipal(principal) {
		return "", ErrInvalidPrincipal
	}
	now := a.clock.Now().UTC()
	if now.IsZero() || now.Unix() <= 0 {
		return "", ErrInvalidConfig
	}
	issued := now.UnixNano()
	if !time.Unix(0, issued).Equal(now) {
		return "", ErrInvalidConfig
	}
	cl := claims{
		Version:      1,
		Issuer:       a.issuer,
		Audience:     a.audience,
		TaskID:       principal.TaskID,
		RunID:        principal.RunID,
		Identity:     principal.Identity,
		CredentialID: principal.CredentialID,
		IssuedAt:     issued,
		NotBefore:    issued,
		ExpiresAt:    issued + int64(a.ttl),
	}
	payload, err := json.Marshal(cl)
	if err != nil {
		return "", ErrInvalidCredential
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	keyID := a.activeKeyID
	mac := sign(a.keys[keyID], keyID, payload)
	token := tokenVersion + "." + keyID + "." + encoded + "." + base64.RawURLEncoding.EncodeToString(mac)
	if len(token) > maxTokenBytes {
		return "", ErrInvalidCredential
	}
	return token, nil
}

// AuthenticateSupervisor implements the authenticated results endpoint's host
// identity boundary. It derives identity only from the bearer credential and
// deliberately does not inspect or consume the request body.
func (a *Authenticator) AuthenticateSupervisor(ctx context.Context, r *http.Request) (results.Principal, error) {
	if a == nil || ctx == nil || ctx.Err() != nil || r == nil || r.URL == nil {
		return results.Principal{}, ErrInvalidRequest
	}
	if r.Method != http.MethodPost || r.TLS == nil ||
		(r.URL.Scheme != "" && r.URL.Scheme != "https") || r.URL.Path != "/internal/results" ||
		r.URL.EscapedPath() != "/internal/results" || r.URL.RawQuery != "" || r.URL.ForceQuery ||
		r.URL.Fragment != "" || r.URL.User != nil {
		return results.Principal{}, ErrInvalidRequest
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || strings.TrimSpace(values[0]) != values[0] {
		return results.Principal{}, ErrInvalidCredential
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	principal, err := a.verify(token)
	if err != nil {
		return results.Principal{}, err
	}
	return principal, nil
}

func (a *Authenticator) verify(token string) (results.Principal, error) {
	if len(token) == 0 || len(token) > maxTokenBytes {
		return results.Principal{}, ErrInvalidCredential
	}
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != tokenVersion || !validKeyID(parts[1]) || parts[2] == "" || parts[3] == "" {
		return results.Principal{}, ErrInvalidCredential
	}
	key, ok := a.keys[parts[1]]
	if !ok {
		return results.Principal{}, ErrInvalidCredential
	}
	payload, err := decodeCanonicalBase64(parts[2])
	if err != nil || len(payload) == 0 || len(payload) > maxTokenBytes {
		return results.Principal{}, ErrInvalidCredential
	}
	providedMAC, err := decodeCanonicalBase64(parts[3])
	if err != nil || len(providedMAC) != sha256.Size || !hmac.Equal(providedMAC, sign(key, parts[1], payload)) {
		return results.Principal{}, ErrInvalidCredential
	}
	cl, err := decodeClaims(payload)
	if err != nil || !validClaims(cl, a.issuer, a.audience, a.ttl, a.clock.Now().UTC()) {
		return results.Principal{}, ErrInvalidCredential
	}
	principal := results.Principal{TaskID: cl.TaskID, RunID: cl.RunID, Identity: cl.Identity, CredentialID: cl.CredentialID}
	if !validPrincipal(principal) {
		return results.Principal{}, ErrInvalidCredential
	}
	return principal, nil
}

func validClaims(cl claims, issuer, audience string, ttl time.Duration, now time.Time) bool {
	if cl.Version != 1 || cl.Issuer != issuer || cl.Audience != audience || now.IsZero() || now.Unix() <= 0 ||
		cl.IssuedAt <= 0 || cl.NotBefore != cl.IssuedAt || cl.ExpiresAt <= cl.NotBefore ||
		cl.ExpiresAt-cl.IssuedAt > int64(ttl) {
		return false
	}
	issuedAt := time.Unix(0, cl.IssuedAt)
	notBefore := time.Unix(0, cl.NotBefore)
	expiresAt := time.Unix(0, cl.ExpiresAt)
	return !now.Before(issuedAt) && !now.Before(notBefore) && now.Before(expiresAt)
}

func decodeClaims(payload []byte) (claims, error) {
	var cl claims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cl); err != nil {
		return claims{}, ErrInvalidCredential
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return claims{}, ErrInvalidCredential
	}
	canonical, err := json.Marshal(cl)
	if err != nil || !bytes.Equal(canonical, payload) {
		return claims{}, ErrInvalidCredential
	}
	return cl, nil
}

func decodeCanonicalBase64(encoded string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return nil, ErrInvalidCredential
	}
	return decoded, nil
}

func sign(key []byte, keyID string, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("aprl:hostauth:" + tokenVersion + ":" + keyID + ":"))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func validPrincipal(principal results.Principal) bool {
	return canonicalUUID(principal.TaskID) && canonicalUUID(principal.RunID) &&
		safeText(principal.Identity, maxIdentityBytes) && safeText(principal.CredentialID, maxCredentialSize)
}

func canonicalUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for i := 0; i < len(value); i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		c := value[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func validKeyID(value string) bool {
	if value == "" || len(value) > maxKeyIDBytes {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func safeText(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes || strings.TrimSpace(value) != value {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func nilDependency(value any) bool {
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

var _ httpapi.SupervisorAuthenticator = (*Authenticator)(nil)
