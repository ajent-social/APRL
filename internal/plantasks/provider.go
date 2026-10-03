package plantasks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

const providerMessageLimit = 1 << 20

// Provider exposes the generic task-provider protocol over a host-selected
// transport. Authentication and policy remain injected into Adapter; repository
// data cannot select credentials, executables, or actor identities.
type Provider struct{ adapter *Adapter }

// NewProvider exposes the buffered protocol over a configured adapter.
func NewProvider(adapter *Adapter) (*Provider, error) {
	if adapter == nil {
		return nil, errors.New("task provider requires admitted adapter")
	}
	return &Provider{adapter: adapter}, nil
}

type providerRequest struct {
	Version          int        `json:"version"`
	Action           string     `json:"action"`
	LifecycleID      string     `json:"lifecycle_id"`
	TaskID           string     `json:"task_id,omitempty"`
	ExpectedRevision *int64     `json:"expected_revision,omitempty"`
	ClaimSHA         string     `json:"claim_sha,omitempty"`
	ExpiresAt        *time.Time `json:"expires_at,omitempty"`
	Receipt          *Receipt   `json:"receipt,omitempty"`
}

// Handle processes one already buffered message. The host transport must apply
// its own I/O deadline before passing a finite memory reader and buffer here.
// Successful replies attest admission or
// recording only; they never attest physical execution or GitHub mutation.
func (p *Provider) Handle(ctx context.Context, input io.Reader, output io.Writer) error {
	if ctx == nil || input == nil || output == nil || p == nil || p.adapter == nil {
		return errors.New("invalid provider transport")
	}
	// Generic streams can block forever despite context cancellation. Fail closed
	// rather than spawning an uninterruptible reader goroutine.
	switch input.(type) {
	case *bytes.Reader, *bytes.Buffer, *strings.Reader:
	default:
		return errors.New("provider requires buffered input")
	}
	if _, ok := output.(*bytes.Buffer); !ok {
		return errors.New("provider requires buffered output")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	data, err := io.ReadAll(io.LimitReader(input, providerMessageLimit+1))
	if err != nil {
		return err
	}
	request := providerRequest{}
	if len(data) > providerMessageLimit || !utf8.Valid(data) || decodeProviderRequest(data, &request) != nil || request.Version != VersionV1 || !validID(request.LifecycleID) {
		return providerFailure(output, "invalid_request")
	}
	switch request.Action {
	case "list":
		if request.TaskID != "" || request.ExpectedRevision != nil || request.ClaimSHA != "" || request.ExpiresAt != nil || request.Receipt != nil {
			return providerFailure(output, "invalid_request")
		}
		snapshot, err := p.adapter.ProviderSnapshot(ctx, request.LifecycleID)
		if err != nil {
			return providerFailure(output, "admission_denied")
		}
		return json.NewEncoder(output).Encode(snapshot)
	case "admit", "result":
		if !validID(request.TaskID) || request.ExpectedRevision == nil || *request.ExpectedRevision < 0 || !shaPattern.MatchString(request.ClaimSHA) {
			return providerFailure(output, "invalid_request")
		}
		if request.Action == "admit" {
			if request.ExpiresAt == nil || request.Receipt != nil {
				return providerFailure(output, "invalid_request")
			}
			err = p.adapter.Admit(ctx, request.LifecycleID, request.TaskID, request.ClaimSHA, *request.ExpectedRevision, *request.ExpiresAt)
		} else {
			if request.Receipt == nil || request.ExpiresAt != nil || request.Receipt.TaskID != request.TaskID || request.Receipt.LifecycleID != request.LifecycleID {
				return providerFailure(output, "invalid_request")
			}
			err = p.adapter.Result(ctx, request.LifecycleID, *request.Receipt, request.ClaimSHA, *request.ExpectedRevision)
		}
		if err != nil {
			return providerFailure(output, "admission_denied")
		}
		return json.NewEncoder(output).Encode(map[string]any{"version": VersionV1, "lifecycle_id": request.LifecycleID, "task_id": request.TaskID, "revision": *request.ExpectedRevision + 1, "admitted": request.Action == "admit", "recorded": request.Action == "result"})
	default:
		return providerFailure(output, "invalid_request")
	}
}

func providerFailure(output io.Writer, code string) error {
	return json.NewEncoder(output).Encode(map[string]any{"version": VersionV1, "error": code})
}

func decodeProviderRequest(data []byte, request *providerRequest) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := uniqueJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(request)
}

// uniqueJSON rejects duplicate members recursively before typed decoding.
func uniqueJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || name != strings.ToLower(name) || seen[name] {
				return errors.New("duplicate or invalid JSON member")
			}
			seen[name] = true
			if err := uniqueJSON(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := uniqueJSON(decoder); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = decoder.Token()
	return err
}
