//go:build aprl_host_task_shim

package system

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/plantasks"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

// These dependencies are deliberately test-only host trust boundaries. The
// worker transport cannot select the actor, policy, claim remote or proof cache.
type deliveryV1Fixture struct {
	ctx                           context.Context
	db                            testutil.DatabaseFixture
	manual                        *clock.Manual
	store                         *plantasks.Store
	adapter                       *plantasks.Adapter
	provider                      *plantasks.Provider
	delegations                   *plantasks.Delegations
	projector                     *plantasks.DeliveryV1Projector
	server                        *httptest.Server
	client                        *http.Client
	caller                        string
	request                       plantasks.DeliveryV1Request
	raw                           []byte
	lifecycleID                   string
	author, reviewer, fixer, host plantasks.Provenance
	actors                        *deliveryV1Actors
	claims                        *deliveryV1Claims
	policy                        *deliveryV1RuntimePolicy
	outcomes                      *deliveryV1Outcomes
	factory                       *deliveryV1InitialFactory
	capture                       *deliveryV1CapturedService
	delegated                     bool
}

func newDeliveryV1Fixture(t *testing.T, delegated bool) *deliveryV1Fixture {
	t.Helper()
	db := testutil.RequireDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate composed fixture: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	suffix := deliveryV1NewID(t)
	f := &deliveryV1Fixture{ctx: ctx, db: db, manual: clock.NewManual(now), caller: "caller-" + suffix, delegated: delegated}
	f.request = plantasks.DeliveryV1Request{Version: plantasks.DeliveryV1ProtocolVersion, DelegationID: "delegation-" + suffix, CallerID: f.caller, ProjectID: "fixture-project", JobID: "fixture-job", TaskID: "fixture-task", PlanRevision: 1, PlanDigest: strings.Repeat("a", 64), Spec: plantasks.DeliveryV1Spec{Repository: "https://github.com/example/project", TargetBranch: "main", SourceCommit: strings.Repeat("a", 40), Acceptance: []string{"independent reviewed source lands"}, PolicyRevision: "fixture-policy-v1", ProfileRevision: "fixture-profile-v1", ExecutionMode: "subscription_only", Envelope: plantasks.DeliveryV1Envelope{MaxAttempts: 4, MaxConcurrent: 2, MaxCostCents: 0, ExpiresAt: now.Add(10 * time.Minute)}}}
	actor := func(id string) plantasks.Provenance {
		return plantasks.Provenance{ActorID: id + "-" + suffix, ActorKind: "agent", AuthoredAt: now, SourceRevision: f.request.Spec.SourceCommit}
	}
	f.author, f.reviewer, f.fixer, f.host = actor("author"), actor("reviewer"), actor("fixer"), actor("host")
	f.actors = &deliveryV1Actors{actor: f.author}
	f.claims = &deliveryV1Claims{}
	f.policy = &deliveryV1RuntimePolicy{allowed: true, actors: map[string]bool{f.author.ActorID: true, f.reviewer.ActorID: true, f.fixer.ActorID: true}}
	f.outcomes = &deliveryV1Outcomes{allowed: map[string]plantasks.Receipt{}, proofs: map[string]plantasks.LandedEvidence{}}
	f.factory = &deliveryV1InitialFactory{author: f.author}
	var err error
	f.raw, err = plantasks.EncodeDeliveryV1Request(f.request)
	if err != nil {
		t.Fatalf("encode initial request: %v", err)
	}
	f.store, err = plantasks.NewStore(db.Pool, f.manual)
	if err != nil {
		t.Fatalf("construct composed store: %v", err)
	}
	f.adapter, err = plantasks.NewAdapter(f.store, f.claims, f.actors, f.policy, f.outcomes)
	if err != nil {
		t.Fatalf("construct composed adapter: %v", err)
	}
	f.provider, err = plantasks.NewProvider(f.adapter)
	if err != nil {
		t.Fatalf("construct actual provider: %v", err)
	}
	f.delegations, err = plantasks.NewDelegations(f.store, plantasks.DeliveryV1HTTPCallerAuthenticator{}, deliveryV1AdmissionPolicy{}, f.factory)
	if err != nil {
		t.Fatalf("construct composed delegations: %v", err)
	}
	f.projector, err = plantasks.NewDeliveryV1Projector(f.store)
	if err != nil {
		t.Fatalf("construct composed projector: %v", err)
	}
	f.capture = &deliveryV1CapturedService{service: f.delegations, bindings: map[string]plantasks.DelegationBinding{}}
	handler, err := plantasks.NewDeliveryV1HTTPHandler(deliveryV1Bearer{}, f.capture, f.projector)
	if err != nil {
		t.Fatalf("construct composed HTTPS handler: %v", err)
	}
	f.server = httptest.NewTLSServer(handler)
	t.Cleanup(f.server.Close)
	f.client = f.server.Client()
	f.client.Timeout = 10 * time.Second
	return f
}

func deliveryV1NewID(t testing.TB) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("generate fixture ID: %v", err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (f *deliveryV1Fixture) submit(t *testing.T) {
	t.Helper()
	var err error
	f.raw, err = plantasks.EncodeDeliveryV1Request(f.request)
	if err != nil {
		t.Fatalf("encode fixture request: %v", err)
	}
	if !f.delegated {
		digest, digestErr := f.request.Digest()
		if digestErr != nil {
			t.Fatalf("digest standalone scope: %v", digestErr)
		}
		state, stateErr := f.factory.InitialState(f.request, plantasks.DeliveryV1Authorization{Allowed: true, CallerID: f.caller, RequestDigest: digest, PolicyRevision: f.request.Spec.PolicyRevision, GrantRevision: "fixture-grant-v1"}, f.manual.Now())
		if stateErr != nil {
			t.Fatalf("build standalone state: %v", stateErr)
		}
		if err := f.store.Create(f.ctx, state); err != nil {
			t.Fatalf("create standalone state: %v", err)
		}
		f.lifecycleID = state.Lifecycle.ID
		return
	}
	status, body := f.httpRequest(t, http.MethodPut, "/v1/delegations/"+f.request.DelegationID, f.caller, f.raw)
	if status != http.StatusOK && status != http.StatusCreated {
		t.Fatalf("submit delegation: status=%d body=%s", status, body)
	}
	observation, err := plantasks.DecodeDeliveryV1Observation(body, f.request)
	if err != nil {
		t.Fatalf("decode submission observation: %v", err)
	}
	f.lifecycleID = observation.LifecycleID
}

func (f *deliveryV1Fixture) httpRequest(t *testing.T, method, path, caller string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(f.ctx, method, f.server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create fixture HTTP request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+caller)
	if method == http.MethodPut || method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("execute fixture HTTPS request: %v", err)
	}
	payload, readErr := io.ReadAll(io.LimitReader(response.Body, plantasks.DeliveryV1MaxJSONBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read fixture HTTPS response: read=%v close=%v", readErr, closeErr)
	}
	return response.StatusCode, payload
}

func (f *deliveryV1Fixture) observation(t *testing.T) plantasks.DeliveryV1Observation {
	t.Helper()
	status, body := f.httpRequest(t, http.MethodGet, "/v1/delegations/"+f.request.DelegationID, f.caller, nil)
	if status != http.StatusOK {
		t.Fatalf("read fixture observation: status=%d body=%s", status, body)
	}
	observation, err := plantasks.DecodeDeliveryV1Observation(body, f.request)
	if err != nil {
		t.Fatalf("decode fixture observation: %v", err)
	}
	return observation
}

func (f *deliveryV1Fixture) binding(t *testing.T) plantasks.DelegationBinding {
	t.Helper()
	if !f.delegated {
		t.Fatal("standalone lifecycle has no delegated binding")
	}
	_ = f.observation(t)
	f.capture.mu.Lock()
	binding, ok := f.capture.bindings[f.caller+"/"+f.request.DelegationID]
	f.capture.mu.Unlock()
	if !ok {
		t.Fatal("actual service did not capture caller-scoped binding")
	}
	return binding
}
func (f *deliveryV1Fixture) load(t *testing.T) plantasks.State {
	t.Helper()
	state, err := f.store.Load(f.ctx, f.lifecycleID)
	if err != nil {
		t.Fatalf("load fixture lifecycle: %v", err)
	}
	return state
}
func (f *deliveryV1Fixture) setActor(actor plantasks.Provenance) {
	f.actors.mu.Lock()
	defer f.actors.mu.Unlock()
	f.actors.actor = actor
}
func (f *deliveryV1Fixture) pr() plantasks.PRBinding {
	return plantasks.PRBinding{Number: 1, URL: f.request.Spec.Repository + "/pull/1", HeadSHA: f.request.Spec.SourceCommit, BaseSHA: strings.Repeat("b", 40), PolicyRevision: f.request.Spec.PolicyRevision}
}
func (f *deliveryV1Fixture) task(t *testing.T, stage plantasks.Stage) plantasks.Task {
	t.Helper()
	state := f.load(t)
	var selected plantasks.Task
	for _, task := range state.Tasks {
		if task.Stage == stage && (selected.ID == "" || task.Correction > selected.Correction) {
			selected = task
		}
	}
	if selected.ID == "" {
		t.Fatalf("no fixture task at stage %s", stage)
	}
	return selected
}
func (f *deliveryV1Fixture) receipt(t *testing.T, taskID string, outcome plantasks.Outcome, pr *plantasks.PRBinding, findings []string) plantasks.Receipt {
	t.Helper()
	f.actors.mu.RLock()
	actor := f.actors.actor
	f.actors.mu.RUnlock()
	receipt := plantasks.Receipt{Version: plantasks.VersionV1, ID: deliveryV1NewID(t), LifecycleID: f.lifecycleID, TaskID: taskID, Outcome: outcome, Actor: actor, PolicyRevision: f.request.Spec.PolicyRevision, PR: pr, FindingIDs: append([]string(nil), findings...), CreatedAt: f.manual.Now().UTC(), Detail: "explicit composed host fixture"}
	if outcome == plantasks.OutcomeMerged || outcome == plantasks.OutcomeLanded {
		receipt.MergeCommit = strings.Repeat("d", 40)
	}
	if outcome == plantasks.OutcomeLanded {
		receipt.LandedCommit = receipt.MergeCommit
	}
	f.outcomes.mu.Lock()
	f.outcomes.allowed[receipt.ID] = receipt
	f.outcomes.mu.Unlock()
	return receipt
}
func (f *deliveryV1Fixture) trustLanding(t *testing.T, receipt plantasks.Receipt) {
	t.Helper()
	state := f.load(t)
	task, ok := state.Tasks[receipt.TaskID]
	if !ok || task.PR == nil {
		t.Fatal("landing fixture lacks current review PR")
	}
	proof := plantasks.LandedEvidence{LifecycleID: f.lifecycleID, TaskID: receipt.TaskID, ReceiptID: receipt.ID, Revision: state.Revision, PRNumber: task.PR.Number, MergeCommit: receipt.MergeCommit, Receipt: plantasks.DeliveryV1LandedReceipt{Repository: f.request.Spec.Repository, TargetBranch: f.request.Spec.TargetBranch, PRURL: task.PR.URL, ReviewedHead: task.PR.HeadSHA, ReviewedBase: task.PR.BaseSHA, PolicyRevision: f.request.Spec.PolicyRevision, LandedCommit: receipt.LandedCommit, SourceDigest: strings.Repeat("a", 64), Reviewer: receipt.Actor.ActorID, Author: f.author.ActorID, Verifier: f.host.ActorID, VerifiedAt: f.manual.Now().UTC()}}
	f.outcomes.mu.Lock()
	f.outcomes.proofs[receipt.ID] = proof
	f.outcomes.mu.Unlock()
}

type deliveryV1Actors struct {
	mu    sync.RWMutex
	actor plantasks.Provenance
}

func (a *deliveryV1Actors) Authenticate(ctx context.Context) (plantasks.Provenance, error) {
	if err := ctx.Err(); err != nil {
		return plantasks.Provenance{}, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.actor, nil
}

type deliveryV1RuntimePolicy struct {
	mu      sync.RWMutex
	allowed bool
	actors  map[string]bool
}

func (p *deliveryV1RuntimePolicy) setAllowed(value bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allowed = value
}
func (p *deliveryV1RuntimePolicy) Authorize(ctx context.Context, life plantasks.Lifecycle, task plantasks.Task, actor plantasks.Provenance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.allowed || !p.actors[actor.ActorID] || task.LifecycleID != life.ID || life.PolicyRevision != "fixture-policy-v1" {
		return plantasks.ErrOutcomeDenied
	}
	return nil
}

type deliveryV1Claims struct {
	mu     sync.RWMutex
	remote string
}

func (c *deliveryV1Claims) setRemote(remote string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.remote = remote
}
func (c *deliveryV1Claims) Verify(ctx context.Context, taskID, sha, actor string) error {
	c.mu.RLock()
	remote := c.remote
	c.mu.RUnlock()
	if remote == "" {
		return plantasks.ErrClaimDenied
	}
	claimID, err := plantasks.ClaimTaskID(taskID)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	output, err := exec.CommandContext(bounded, "git", "--git-dir", remote, "rev-parse", "refs/claims/"+claimID).Output()
	if err != nil || strings.TrimSpace(string(output)) != sha {
		return plantasks.ErrClaimDenied
	}
	message, err := exec.CommandContext(bounded, "git", "--git-dir", remote, "show", "-s", "--format=%B", sha).Output()
	if err != nil || !strings.Contains(string(message), "["+actor+"]") {
		return plantasks.ErrClaimDenied
	}
	return nil
}

type deliveryV1Outcomes struct {
	mu      sync.RWMutex
	allowed map[string]plantasks.Receipt
	proofs  map[string]plantasks.LandedEvidence
}

func (o *deliveryV1Outcomes) Verify(ctx context.Context, _ plantasks.Lifecycle, _ plantasks.Task, receipt plantasks.Receipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	expected, ok := o.allowed[receipt.ID]
	receipt.LandedEvidence = nil
	if !ok || !reflect.DeepEqual(expected, receipt) {
		return plantasks.ErrOutcomeDenied
	}
	return nil
}
func (o *deliveryV1Outcomes) FetchLanding(ctx context.Context, _ plantasks.State, _ plantasks.Task, receipt plantasks.Receipt) (plantasks.LandedEvidence, error) {
	if err := ctx.Err(); err != nil {
		return plantasks.LandedEvidence{}, err
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	proof, ok := o.proofs[receipt.ID]
	if !ok {
		return plantasks.LandedEvidence{}, plantasks.ErrOutcomeDenied
	}
	return proof, nil
}
func (o *deliveryV1Outcomes) VerifyLanding(ctx context.Context, _ plantasks.State, _ plantasks.Task, receipt plantasks.Receipt, proof plantasks.LandedEvidence, _ time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	expected, ok := o.proofs[receipt.ID]
	if !ok || !reflect.DeepEqual(proof, expected) {
		return plantasks.ErrOutcomeDenied
	}
	return nil
}

type deliveryV1Bearer struct{}

func (deliveryV1Bearer) Authenticate(ctx context.Context, token string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !strings.HasPrefix(token, "caller-") {
		return "", errors.New("unknown fixture principal")
	}
	return token, nil
}

type deliveryV1AdmissionPolicy struct{}

func (deliveryV1AdmissionPolicy) Authorize(ctx context.Context, caller string, request plantasks.DeliveryV1Request) (plantasks.DeliveryV1Authorization, error) {
	if err := ctx.Err(); err != nil {
		return plantasks.DeliveryV1Authorization{}, err
	}
	digest, err := request.Digest()
	if err != nil {
		return plantasks.DeliveryV1Authorization{}, err
	}
	return plantasks.DeliveryV1Authorization{Allowed: caller == request.CallerID && request.Spec.ExecutionMode == "subscription_only" && request.Spec.Envelope.MaxCostCents == 0, CallerID: caller, RequestDigest: digest, PolicyRevision: request.Spec.PolicyRevision, GrantRevision: "fixture-grant-v1"}, nil
}
func (deliveryV1AdmissionPolicy) Verify(ctx context.Context, request plantasks.DeliveryV1Request, auth plantasks.DeliveryV1Authorization, _ time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	if !auth.Allowed || auth.CallerID != request.CallerID || auth.RequestDigest != digest || auth.PolicyRevision != request.Spec.PolicyRevision || auth.GrantRevision != "fixture-grant-v1" {
		return plantasks.ErrDelegationConflict
	}
	return nil
}

type deliveryV1InitialFactory struct{ author plantasks.Provenance }

func (f *deliveryV1InitialFactory) InitialState(request plantasks.DeliveryV1Request, authorization plantasks.DeliveryV1Authorization, now time.Time) (plantasks.State, error) {
	// IDs are host-generated. Creation remains atomic in the real delegation store.
	id, authorID, reviewID := deliveryV1FactoryID(), deliveryV1FactoryID(), deliveryV1FactoryID()
	parsed, err := url.Parse(request.Spec.Repository)
	if err != nil {
		return plantasks.State{}, err
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 2 {
		return plantasks.State{}, errors.New("fixture repository shape")
	}
	author := f.author
	author.AuthoredAt = now.UTC()
	author.SourceRevision = request.Spec.SourceCommit
	seconds := int((request.Spec.Envelope.ExpiresAt.Sub(now) + time.Second - 1) / time.Second)
	life := plantasks.Lifecycle{Version: plantasks.VersionV1, ID: id, DeliveryGateID: reviewID, Repository: plantasks.Repository{Owner: parts[0], Name: parts[1], Target: request.Spec.TargetBranch}, PolicyRevision: authorization.PolicyRevision, Authored: author, Limits: plantasks.CorrectionLimits{MaxCorrections: 1, MaxAttempts: request.Spec.Envelope.MaxAttempts, MaxConcurrentTasks: request.Spec.Envelope.MaxConcurrent, MaxDurationSeconds: seconds}, CreatedAt: now.UTC()}
	task := plantasks.Task{Version: plantasks.VersionV1, ID: authorID, LifecycleID: id, Stage: plantasks.StageAuthor, CreatedAt: now.UTC()}
	review := plantasks.Task{Version: plantasks.VersionV1, ID: reviewID, LifecycleID: id, Stage: plantasks.StageReview, Dependencies: []plantasks.Dependency{{TaskID: authorID, Kind: plantasks.DependencyHandoff}}, CreatedAt: now.UTC()}
	state := plantasks.State{Lifecycle: life, Tasks: map[string]plantasks.Task{authorID: task, reviewID: review}, Receipts: map[string]plantasks.Receipt{}, Completed: map[string]plantasks.Outcome{}, Claims: map[string]plantasks.Admission{}, Attempts: map[string]int{}, StartedAt: now.UTC()}
	return state, state.Validate()
}
func deliveryV1FactoryID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("fixture random ID generation failed")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type deliveryV1CapturedService struct {
	service  *plantasks.Delegations
	mu       sync.Mutex
	bindings map[string]plantasks.DelegationBinding
}

func (s *deliveryV1CapturedService) capture(binding plantasks.DelegationBinding) {
	if binding.CallerID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings[binding.CallerID+"/"+binding.DelegationID] = binding
}
func (s *deliveryV1CapturedService) Submit(ctx context.Context, raw []byte) (plantasks.DelegationBinding, error) {
	binding, err := s.service.Submit(ctx, raw)
	if err == nil {
		s.capture(binding)
	}
	return binding, err
}
func (s *deliveryV1CapturedService) Get(ctx context.Context, id string) (plantasks.DelegationBinding, error) {
	binding, err := s.service.Get(ctx, id)
	if err == nil {
		s.capture(binding)
	}
	return binding, err
}
func (s *deliveryV1CapturedService) Cancel(ctx context.Context, id string) (plantasks.DelegationBinding, error) {
	binding, err := s.service.Cancel(ctx, id)
	if err == nil {
		s.capture(binding)
	}
	return binding, err
}
