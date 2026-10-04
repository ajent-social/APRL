package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/plantasks"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

func TestPlanProviderPostgresLifecycleRoundTrip(t *testing.T) {
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate plan task fixture: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 11, 30, 0, 0, time.UTC))
	store, err := plantasks.NewStore(db.Pool, manual)
	if err != nil {
		t.Fatalf("construct plan task store: %v", err)
	}
	state := plantasksStoreState("87000000-0000-4000-8000-000000000001")
	initialAuthor := state.Tasks[plantasksStoreTaskID]
	initialAuthor.Authors = []plantasks.Provenance{{ActorID: "parallel-author", ActorKind: "agent", AuthoredAt: manual.Now(), SourceRevision: plantasksStorePR().HeadSHA}}
	state.Tasks[initialAuthor.ID] = initialAuthor
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("create lifecycle: %v", err)
	}
	pr := plantasksStorePR()
	author := plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: state.StartedAt}
	reviewer := plantasks.Provenance{ActorID: "reviewer-1", ActorKind: "human", AuthoredAt: manual.Now()}
	actors := &plantasksTestAuthenticator{actor: author}
	claims := &plantasksTestClaimVerifier{allowed: map[string]plantasksTestClaim{}}
	policy := &plantasksTestRuntimePolicy{lifecycleID: state.Lifecycle.ID,
		taskIDs:  map[string]bool{plantasksStoreTaskID: true, plantasksStoreReviewID: true},
		actorIDs: map[string]bool{author.ActorID: true, reviewer.ActorID: true}}
	outcomes := &plantasksTestOutcomeVerifier{allowed: map[string]plantasks.Receipt{}}
	adapter, err := plantasks.NewAdapter(store, claims, actors, policy, outcomes)
	if err != nil {
		t.Fatalf("construct adapter with required host dependencies: %v", err)
	}
	provider, err := plantasks.NewProvider(adapter)
	if err != nil {
		t.Fatalf("construct provider: %v", err)
	}

	claimAuthor := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	claims.allow(plantasksStoreTaskID, claimAuthor, author.ActorID)
	listed := providerCall(t, provider, map[string]any{"version": 1, "action": "list", "lifecycle_id": state.Lifecycle.ID})
	if listed["lifecycle_id"] != state.Lifecycle.ID || listed["revision"] != float64(0) {
		t.Fatalf("initial provider snapshot = %#v", listed)
	}

	rows, ok := listed["tasks"].([]any)
	if !ok {
		t.Fatalf("provider tasks must be an array: %#v", listed["tasks"])
	}
	foundAuthor := false
	for _, value := range rows {
		row, ok := value.(map[string]any)
		if !ok || row["task_id"] != plantasksStoreTaskID {
			continue
		}
		foundAuthor = true
		dependencies, ok := row["dependencies"].([]any)
		if !ok || len(dependencies) != 0 {
			t.Fatalf("leaf provider task dependencies must be an empty array: %#v", row["dependencies"])
		}
	}
	if !foundAuthor {
		t.Fatal("provider snapshot omitted the author task")
	}

	expires := manual.Now().Add(10 * time.Minute)
	providerCall(t, provider, map[string]any{"version": 1, "action": "admit", "lifecycle_id": state.Lifecycle.ID,
		"task_id": plantasksStoreTaskID, "expected_revision": 0, "claim_sha": claimAuthor, "expires_at": expires})
	// The host binds the authenticated author to the exact source revision
	// before it accepts a coding handoff result.
	author.SourceRevision = pr.HeadSHA
	actors.actor = author
	handoff := plantasks.Receipt{Version: plantasks.VersionV1, ID: "87000000-0000-4000-8000-000000000002",
		LifecycleID: state.Lifecycle.ID, TaskID: plantasksStoreTaskID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor: author, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &pr, CreatedAt: manual.Now()}
	providerCall(t, provider, map[string]any{"version": 1, "action": "result", "lifecycle_id": state.Lifecycle.ID,
		"task_id": plantasksStoreTaskID, "expected_revision": 1, "claim_sha": claimAuthor, "receipt": handoff})

	actors.actor = reviewer
	claimReview := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	claims.allow(plantasksStoreReviewID, claimReview, reviewer.ActorID)
	listed = providerCall(t, provider, map[string]any{"version": 1, "action": "list", "lifecycle_id": state.Lifecycle.ID})
	var snapshot plantasks.ProviderSnapshot
	if err := json.Unmarshal(mustJSON(t, listed), &snapshot); err != nil {
		t.Fatalf("decode provider snapshot: %v", err)
	}
	if snapshot.Revision != 2 {
		t.Fatalf("review-ready snapshot revision = %d, want 2", snapshot.Revision)
	}
	var reviewVisible *plantasks.VisibleTask
	var authorVisible *plantasks.VisibleTask
	for i := range snapshot.Tasks {
		if snapshot.Tasks[i].TaskID == plantasksStoreReviewID {
			reviewVisible = &snapshot.Tasks[i]
		}
		if snapshot.Tasks[i].TaskID == plantasksStoreTaskID {
			authorVisible = &snapshot.Tasks[i]
		}
	}
	if reviewVisible == nil || !reviewVisible.Ready || reviewVisible.DeliveryStatus != "pending" {
		t.Fatalf("review was not published as ready after handoff: %+v", reviewVisible)
	}
	if authorVisible == nil || authorVisible.DeliveryStatus != string(plantasks.OutcomeCodingHandoff) {
		t.Fatalf("author handoff was not visible as task-owned completion: %+v", authorVisible)
	}

	// A stale provider revision must not mutate durable state.
	before, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load before stale admission: %v", err)
	}
	denied := providerCall(t, provider, map[string]any{"version": 1, "action": "admit", "lifecycle_id": state.Lifecycle.ID,
		"task_id": plantasksStoreReviewID, "expected_revision": 1, "claim_sha": claimReview, "expires_at": expires})
	if denied["error"] != "admission_denied" {
		t.Fatalf("stale admission reply = %#v", denied)
	}
	after, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("stale admission changed state: before revision %d, after=%+v, err=%v", before.Revision, after, err)
	}

	providerCall(t, provider, map[string]any{"version": 1, "action": "admit", "lifecycle_id": state.Lifecycle.ID,
		"task_id": plantasksStoreReviewID, "expected_revision": 2, "claim_sha": claimReview, "expires_at": expires})
	// The reviewer session is likewise bound to the PR head before result
	// verification. Admission itself only needs the authenticated actor ID.
	reviewer.SourceRevision = pr.HeadSHA
	actors.actor = reviewer
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load before forged landing: %v", err)
	}
	forgedLanding := plantasks.Receipt{Version: plantasks.VersionV1, ID: "87000000-0000-4000-8000-000000000003",
		LifecycleID: state.Lifecycle.ID, TaskID: plantasksStoreReviewID, Outcome: plantasks.OutcomeLanded,
		Actor:          plantasks.Provenance{ActorID: reviewer.ActorID, ActorKind: reviewer.ActorKind, AuthoredAt: reviewer.AuthoredAt, SourceRevision: pr.HeadSHA},
		PolicyRevision: state.Lifecycle.PolicyRevision, PR: &pr, MergeCommit: "cccccccccccccccccccccccccccccccccccccccc",
		LandedCommit: "dddddddddddddddddddddddddddddddddddddddd", CreatedAt: manual.Now()}
	denied = providerCall(t, provider, map[string]any{"version": 1, "action": "result", "lifecycle_id": state.Lifecycle.ID,
		"task_id": plantasksStoreReviewID, "expected_revision": 3, "claim_sha": claimReview, "receipt": forgedLanding})
	if denied["error"] != "admission_denied" {
		t.Fatalf("forged landing reply = %#v", denied)
	}
	after, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("forged landing changed state: before revision %d, after=%+v, err=%v", before.Revision, after, err)
	}

	// Receipt actor identity comes from the authenticated host session. A
	// caller-supplied actor cannot record even an otherwise valid negative review.
	findings := []string{"87000000-0000-4000-8000-000000000004"}
	negative := plantasks.Receipt{Version: plantasks.VersionV1, ID: "87000000-0000-4000-8000-000000000005",
		LifecycleID: state.Lifecycle.ID, TaskID: plantasksStoreReviewID, Outcome: plantasks.OutcomeChangesRequest,
		Actor: reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &pr, FindingIDs: findings, CreatedAt: manual.Now()}
	spoofed := negative
	spoofed.Actor.ActorID = "caller-controlled"
	denied = providerCall(t, provider, map[string]any{"version": 1, "action": "result", "lifecycle_id": state.Lifecycle.ID,
		"task_id": plantasksStoreReviewID, "expected_revision": 3, "claim_sha": claimReview, "receipt": spoofed})
	if denied["error"] != "admission_denied" {
		t.Fatalf("caller-controlled actor reply = %#v", denied)
	}
	after, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("spoofed actor changed state: before revision %d, after=%+v, err=%v", before.Revision, after, err)
	}

	providerCall(t, provider, map[string]any{"version": 1, "action": "result", "lifecycle_id": state.Lifecycle.ID,
		"task_id": plantasksStoreReviewID, "expected_revision": 3, "claim_sha": claimReview, "receipt": negative})
	final, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load state after negative review: %v", err)
	}
	if final.Revision != 4 || final.Completed[plantasksStoreReviewID] != plantasks.OutcomeChangesRequest {
		t.Fatalf("negative review outcome not committed: revision=%d completed=%q", final.Revision, final.Completed[plantasksStoreReviewID])
	}
	var fix, rereview *plantasks.Task
	for _, task := range final.Tasks {
		switch task.Stage {
		case plantasks.StageFix:
			taskCopy := task
			fix = &taskCopy
		case plantasks.StageRereview:
			taskCopy := task
			rereview = &taskCopy
		}
	}
	if fix == nil || rereview == nil || !reflect.DeepEqual(fix.FindingIDs, findings) || len(rereview.Dependencies) != 1 || rereview.Dependencies[0].TaskID != fix.ID {
		t.Fatalf("negative review did not publish actionable fix and re-review rows: fix=%+v rereview=%+v", fix, rereview)
	}
}

func providerCall(t *testing.T, provider *plantasks.Provider, request map[string]any) map[string]any {
	t.Helper()
	var output bytes.Buffer
	if err := provider.Handle(context.Background(), bytes.NewReader(mustJSON(t, request)), &output); err != nil {
		t.Fatalf("provider handle %q: %v", request["action"], err)
	}
	var response map[string]any
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatalf("decode provider response %q: %v (%s)", request["action"], err, output.String())
	}
	return response
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode test JSON: %v", err)
	}
	return encoded
}

func TestPlanTaskStoreNoopCannotChurnRevision(t *testing.T) {
	ctx := context.Background()
	pool := testutil.RequireDatabase(t)
	if err := storage.Migrate(ctx, pool.Pool); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 3, 11, 30, 0, 0, time.UTC)
	store, err := plantasks.NewStore(pool.Pool, clock.NewManual(now))
	if err != nil {
		t.Fatal(err)
	}
	state := plantasksStoreState("87000000-0000-4000-8000-000000000002")
	if err := store.Create(ctx, state); err != nil {
		t.Fatal(err)
	}
	err = store.Update(ctx, state.Lifecycle.ID, state.Revision, func(*plantasks.State) error { return nil })
	if !errors.Is(err, plantasks.ErrConflict) {
		t.Fatalf("no-op accepted: %v", err)
	}
	loaded, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state, loaded) {
		t.Fatal("no-op changed persisted state")
	}
}
