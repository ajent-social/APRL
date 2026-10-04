//go:build aprl_host_task_shim

package system

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/plantasks"
)

const (
	deliveryV1ShimEnv       = "APRL_TEST_GENERIC_TASK_SHIM"
	deliveryV1ClaimEnv      = "APRL_TEST_GENERIC_CLAIM_SCRIPT"
	deliveryV1ShimSHA256    = "7a8958617edc66e45a372e718dfceedff026e824c4946cce59bf7043c805f99b"
	deliveryV1ClaimSHA256   = "1db72f13c5cc071578a63a6c8ed3455d9c5c0aee3c782edc7adf1a43a182c609"
	deliveryV1ProviderLimit = 1 << 20
)

var deliveryV1ClaimSHAPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

type deliveryV1ShimSnapshot struct {
	plantasks.ProviderSnapshot
	DeliveryGateClaimID string           `json:"delivery_gate_claim_id"`
	PoolTasks           []map[string]any `json:"pool_tasks"`
}

type deliveryV1Shim struct {
	fixture         *deliveryV1Fixture
	shimPath        string
	claimPath       string
	registry        string
	remote          string
	checkout        string
	claims          map[string]deliveryV1ShimClaim
	providerCalls   atomic.Int64
	providerErrorMu sync.RWMutex
	providerError   string
}

type deliveryV1ShimClaim struct {
	sha     string
	actorID string
	claimID string
}

func newDeliveryV1Shim(t *testing.T, f *deliveryV1Fixture) *deliveryV1Shim {
	t.Helper()
	if f == nil || f.lifecycleID == "" {
		t.Fatal("submit the v1 fixture before starting the generic task shim")
	}
	shimPath := deliveryV1PinnedHostFile(t, deliveryV1ShimEnv, deliveryV1ShimSHA256)
	claimPath := deliveryV1PinnedHostFile(t, deliveryV1ClaimEnv, deliveryV1ClaimSHA256)
	pythonPath, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required for the selected published-shim test")
	}

	root := t.TempDir()
	remote := filepath.Join(root, "claims.git")
	checkout := filepath.Join(root, "claim-client")
	deliveryV1RunGit(t, root, "init", "--bare", remote)
	if err := os.Mkdir(checkout, 0o700); err != nil {
		t.Fatal("create temporary claim checkout")
	}
	deliveryV1RunGit(t, checkout, "init")
	deliveryV1RunGit(t, checkout, "config", "user.name", "APRL T6.5 fixture")
	deliveryV1RunGit(t, checkout, "config", "user.email", "aprl-t6-5@example.invalid")
	deliveryV1RunGit(t, checkout, "remote", "add", "origin", remote)
	if err := os.WriteFile(filepath.Join(checkout, "fixture.txt"), []byte("temporary claim client\n"), 0o600); err != nil {
		t.Fatal("write temporary claim checkout")
	}
	deliveryV1RunGit(t, checkout, "add", "fixture.txt")
	deliveryV1RunGit(t, checkout, "commit", "-m", "initialize temporary claim client")
	f.claims.setRemote(remote)

	bridgeTokenBytes := make([]byte, 32)
	if _, err := rand.Read(bridgeTokenBytes); err != nil {
		t.Fatal("create temporary provider bridge credential")
	}
	bridgeToken := hex.EncodeToString(bridgeTokenBytes)
	var s *deliveryV1Shim
	bridge := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/provider" || r.TLS == nil ||
			subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+bridgeToken)) != 1 ||
			r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, deliveryV1ProviderLimit+1))
		if err != nil || len(body) == 0 || len(body) > deliveryV1ProviderLimit {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		var request struct {
			LifecycleID string `json:"lifecycle_id"`
		}
		if json.Unmarshal(body, &request) != nil || request.LifecycleID != f.lifecycleID {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		var response bytes.Buffer
		s.providerCalls.Add(1)
		if err := f.provider.Handle(r.Context(), bytes.NewReader(body), &response); err != nil {
			s.setProviderError("provider_handler_error")
			http.Error(w, "provider unavailable", http.StatusServiceUnavailable)
			return
		}
		var providerReply struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(response.Bytes(), &providerReply) == nil {
			s.setProviderError(providerReply.Error)
		} else {
			s.setProviderError("invalid_provider_reply")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(response.Bytes())
	}))
	t.Cleanup(bridge.Close)
	caPath := filepath.Join(root, "provider-bridge-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: bridge.Certificate().Raw}), 0o600); err != nil {
		t.Fatal("write temporary provider bridge trust root")
	}

	// This is only a byte-for-byte loopback transport for the test. Provider
	// selection and argv remain pinned by the temporary host registry.
	const relayScript = `import ssl,sys,urllib.request
try:
    raw=sys.stdin.buffer.read(1048577)
    if not raw or len(raw)>1048576: raise ValueError()
    context=ssl.create_default_context(cafile=sys.argv[2])
    request=urllib.request.Request(sys.argv[1],data=raw,method="POST",headers={"Authorization":"Bearer "+sys.argv[3],"Content-Type":"application/json"})
    with urllib.request.urlopen(request,context=context,timeout=5) as response:
        body=response.read(1048577)
    if len(body)>1048576: raise ValueError()
    sys.stdout.buffer.write(body)
except Exception:
    sys.exit(2)
`
	relayPath := filepath.Join(root, "provider-relay.py")
	if err := os.WriteFile(relayPath, []byte(relayScript), 0o700); err != nil {
		t.Fatal("write temporary provider transport")
	}
	registryPath := filepath.Join(root, "provider-registry.json")
	registry := map[string]any{
		"version": 1,
		"providers": map[string][]string{
			"aprl": {pythonPath, relayPath, bridge.URL + "/provider", caPath, bridgeToken},
		},
	}
	registryBytes, err := json.Marshal(registry)
	if err != nil {
		t.Fatal("encode temporary provider registry")
	}
	if err := os.WriteFile(registryPath, registryBytes, 0o600); err != nil {
		t.Fatal("write temporary provider registry")
	}
	s = &deliveryV1Shim{
		fixture: f, shimPath: shimPath, claimPath: claimPath, registry: registryPath,
		remote: remote, checkout: checkout,
		claims: make(map[string]deliveryV1ShimClaim),
	}
	t.Cleanup(func() {
		for _, claim := range s.claims {
			s.deliveryV1Release(claim.claimID, claim.sha, claim.actorID)
		}
	})
	return s
}

func (s *deliveryV1Shim) ProviderCalls() int64 {
	if s == nil {
		return 0
	}
	return s.providerCalls.Load()
}

func (s *deliveryV1Shim) LastProviderError() string {
	if s == nil {
		return ""
	}
	s.providerErrorMu.RLock()
	defer s.providerErrorMu.RUnlock()
	return s.providerError
}

func (s *deliveryV1Shim) setProviderError(code string) {
	s.providerErrorMu.Lock()
	s.providerError = code
	s.providerErrorMu.Unlock()
}

func deliveryV1PinnedHostFile(t *testing.T, env, wantSHA256 string) string {
	t.Helper()
	path := os.Getenv(env)
	if path == "" || !filepath.IsAbs(path) {
		t.Fatalf("%s must name the pinned trusted host artifact", env)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s trusted host artifact is unavailable", env)
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if actual != wantSHA256 {
		t.Fatalf("%s trusted host artifact checksum mismatch", env)
	}
	return path
}

func (s *deliveryV1Shim) List(t *testing.T) deliveryV1ShimSnapshot {
	t.Helper()
	response, err := s.Call(map[string]any{"action": "list", "lifecycle_id": s.fixture.lifecycleID})
	if err != nil {
		t.Fatalf("published generic task shim list failed: %v", err)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal("encode provider CLI response")
	}
	var snapshot deliveryV1ShimSnapshot
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		t.Fatal("decode provider CLI snapshot")
	}
	if snapshot.LifecycleID != s.fixture.lifecycleID || snapshot.DeliveryGateClaimID == "" || snapshot.PoolTasks == nil {
		t.Fatal("published generic task shim returned an incomplete snapshot")
	}
	return snapshot
}

func (s *deliveryV1Shim) Call(action map[string]any) (map[string]any, error) {
	if s == nil || s.fixture == nil || len(action) == 0 {
		return nil, fmt.Errorf("invalid generic task shim call")
	}
	if _, exists := action["provider"]; exists {
		return nil, fmt.Errorf("provider selection is host-pinned")
	}
	for _, key := range []string{"version", "registry", "argv"} {
		if _, exists := action[key]; exists {
			return nil, fmt.Errorf("provider configuration is host-pinned")
		}
	}
	request := make(map[string]any, len(action)+2)
	request["version"] = 1
	request["provider"] = "aprl"
	for key, value := range action {
		request[key] = value
	}
	input, err := json.Marshal(request)
	if err != nil || len(input) > deliveryV1ProviderLimit {
		return nil, fmt.Errorf("generic task shim request is invalid or oversized")
	}
	ctx, cancel := context.WithTimeout(s.fixture.ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", s.shimPath, "--registry", s.registry, "--provider", "aprl")
	cmd.Dir = s.checkout
	cmd.Stdin = bytes.NewReader(input)
	stdout := &deliveryV1ShimBoundedBuffer{limit: deliveryV1ProviderLimit}
	stderr := &deliveryV1ShimBoundedBuffer{limit: 64 << 10}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = append(os.Environ(), "PYTHONNOUSERSITE=1")
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("published generic task shim rejected provider operation")
	}
	var response map[string]any
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(&response); err != nil || response == nil {
		return nil, fmt.Errorf("published generic task shim returned invalid response")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("published generic task shim returned trailing output")
	}
	return response, nil
}

func (s *deliveryV1Shim) Claim(t *testing.T, taskID, actorID string) string {
	t.Helper()
	claimID, err := plantasks.ClaimTaskID(taskID)
	if err != nil {
		t.Fatal("derive canonical ordinary claim identity")
	}
	ctx, cancel := context.WithTimeout(s.fixture.ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.claimPath, "claim", claimID, "--purpose", "APRL T6.5 composed fixture")
	cmd.Dir = s.checkout
	cmd.Env = append(os.Environ(), "CLAIM_REMOTE="+s.remote, "CLAUDE_CODE_SESSION_ID="+actorID)
	output, err := cmd.Output()
	if err != nil {
		t.Fatal("trusted claim primitive did not produce a claim result")
	}
	fields := strings.Fields(string(output))
	if len(fields) != 3 || fields[0] != "WON:" || fields[1] != claimID || !deliveryV1ClaimSHAPattern.MatchString(fields[2]) {
		t.Fatal("trusted claim primitive did not return literal WON with a commit SHA")
	}
	s.claims[taskID] = deliveryV1ShimClaim{sha: fields[2], actorID: actorID, claimID: claimID}
	return fields[2]
}

func (s *deliveryV1Shim) Release(t *testing.T, taskID, claimSHA string) {
	t.Helper()
	claim, ok := s.claims[taskID]
	if !ok || claim.sha != claimSHA {
		t.Fatal("release requires this fixture's exact WON claim SHA")
	}
	if !s.deliveryV1Release(claim.claimID, claim.sha, claim.actorID) {
		t.Fatal("trusted claim primitive failed compare-and-swap release")
	}
	delete(s.claims, taskID)
}

func (s *deliveryV1Shim) deliveryV1Release(claimID, claimSHA, actorID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.claimPath, "release", claimID, claimSHA)
	cmd.Dir = s.checkout
	cmd.Env = append(os.Environ(), "CLAIM_REMOTE="+s.remote, "CLAUDE_CODE_SESSION_ID="+actorID)
	output, err := cmd.Output()
	return err == nil && strings.TrimSpace(string(output)) == "RELEASED: "+claimID
}

func (s *deliveryV1Shim) Admit(taskID, claimSHA string, expectedRevision int64, expiresAt time.Time) error {
	_, err := s.Call(map[string]any{"action": "admit", "lifecycle_id": s.fixture.lifecycleID, "task_id": taskID,
		"expected_revision": expectedRevision, "claim_sha": claimSHA, "expires_at": expiresAt.UTC().Format(time.RFC3339Nano)})
	return err
}

func (s *deliveryV1Shim) Result(receipt plantasks.Receipt, claimSHA string, expectedRevision int64) error {
	_, err := s.Call(map[string]any{"action": "result", "lifecycle_id": s.fixture.lifecycleID, "task_id": receipt.TaskID,
		"expected_revision": expectedRevision, "claim_sha": claimSHA, "receipt": receipt})
	return err
}

type deliveryV1ShimBoundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *deliveryV1ShimBoundedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, fmt.Errorf("bounded provider output exceeded")
	}
	return b.Buffer.Write(p)
}

func deliveryV1RunGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if err := cmd.Run(); err != nil {
		t.Fatalf("temporary local Git fixture setup failed: %s", args[0])
	}
}

func TestCodeDeliveryV1GenericShimUsesPublishedCLIAndRealClaim(t *testing.T) {
	f := newDeliveryV1Fixture(t, false)
	f.submit(t)
	f.setActor(f.author)
	shim := newDeliveryV1Shim(t, f)
	initial := shim.List(t)
	author := f.task(t, plantasks.StageAuthor)
	if initial.Revision < 1 || initial.DeliveryGateClaimID == "" {
		t.Fatal("generic shim omitted lifecycle revision or stable delivery gate claim")
	}

	claimSHA := shim.Claim(t, author.ID, f.author.ActorID)
	fresh := shim.List(t)
	if fresh.Revision != initial.Revision || !deliveryV1PoolTaskBool(t, fresh.PoolTasks, author.ID, "ready") {
		t.Fatal("fresh provider snapshot did not preserve revision and author readiness after WON")
	}
	if err := shim.Admit(author.ID, claimSHA, fresh.Revision, f.manual.Now().Add(5*time.Minute)); err != nil {
		t.Fatalf("real APRL provider did not admit after literal WON: %v", err)
	}
	pr := f.pr()
	receipt := f.receipt(t, author.ID, plantasks.OutcomeCodingHandoff, &pr, nil)
	if err := shim.Result(receipt, claimSHA, fresh.Revision+1); err != nil {
		t.Fatalf("real APRL provider did not record fenced coding handoff: %v", err)
	}
	shim.Release(t, author.ID, claimSHA)
	completed := shim.List(t)
	if !deliveryV1PoolTaskIsStatus(t, completed.PoolTasks, author.ID, "done") {
		t.Fatal("published generic pool projection did not complete the author row from its receipt")
	}
}

func deliveryV1PoolTaskBool(t *testing.T, tasks []map[string]any, taskID, field string) bool {
	t.Helper()
	for _, task := range tasks {
		if task["task_id"] == taskID {
			value, ok := task[field].(bool)
			return ok && value
		}
	}
	t.Fatalf("generic pool projection lacks task %s", taskID)
	return false
}

func deliveryV1PoolTaskIsStatus(t *testing.T, tasks []map[string]any, taskID, status string) bool {
	t.Helper()
	for _, task := range tasks {
		if task["task_id"] == taskID {
			return task["status"] == status
		}
	}
	t.Fatalf("generic pool projection lacks task %s", taskID)
	return false
}
