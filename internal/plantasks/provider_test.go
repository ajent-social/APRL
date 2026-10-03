package plantasks

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestPlanProviderRejectsMalformedMessages(t *testing.T) {
	p := &Provider{adapter: &Adapter{}}
	for _, body := range []string{
		`{"version":1,"version":1,"action":"list","lifecycle_id":"10000000-0000-4000-8000-000000000001"}`,
		`{"version":1,"action":"list","lifecycle_id":"10000000-0000-4000-8000-000000000001","actor":"admin"}`,
		`{"version":1,"action":"result","lifecycle_id":"10000000-0000-4000-8000-000000000001","receipt":{"actor":{"actor_id":"first","actor_id":"second"}}}`,
		`{"version":1,"action":"admit","lifecycle_id":"10000000-0000-4000-8000-000000000001","expected_revision":null}`,
		`{"version":1,"action":"exec","lifecycle_id":"10000000-0000-4000-8000-000000000001"}`,
		`{} {}`,
		`{"Version":1,"action":"list","lifecycle_id":"10000000-0000-4000-8000-000000000001"}`,
		string([]byte{0xff}),
		strings.Repeat(" ", providerMessageLimit+1),
	} {
		var output bytes.Buffer
		if err := p.Handle(context.Background(), strings.NewReader(body), &output); err != nil {
			t.Fatal(err)
		}
		var response map[string]any
		if err := json.Unmarshal(output.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response["error"] != "invalid_request" {
			t.Fatalf("malformed message accepted: %s", output.String())
		}
	}
}

func TestPlanProviderMissingRuntimeFailsClosed(t *testing.T) {
	if _, err := NewProvider(nil); err == nil {
		t.Fatal("nil adapter accepted")
	}
	p := &Provider{adapter: &Adapter{}}
	var output bytes.Buffer
	if err := p.Handle(context.Background(), strings.NewReader(`{"version":1,"action":"list","lifecycle_id":"10000000-0000-4000-8000-000000000001"}`), &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "admission_denied") {
		t.Fatalf("missing runtime appeared available: %s", output.String())
	}
}

func TestPlanProviderRejectsBlockingTransport(t *testing.T) {
	p := &Provider{adapter: &Adapter{}}
	reader, writer := io.Pipe()
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	}()
	var output bytes.Buffer
	if err := p.Handle(context.Background(), reader, &output); err == nil {
		t.Fatal("unbounded input accepted")
	}
	if err := p.Handle(context.Background(), strings.NewReader("{}"), writer); err == nil {
		t.Fatal("unbounded output accepted")
	}
}
