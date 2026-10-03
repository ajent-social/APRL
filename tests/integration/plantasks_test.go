package integration

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/plantasks"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

func TestPlanTaskStore(t *testing.T) {
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate plan task fixture: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	store, err := plantasks.NewStore(db.Pool, manual)
	if err != nil {
		t.Fatalf("construct plan task store: %v", err)
	}

	state := plantasksStoreState("81000000-0000-4000-8000-000000000001")
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("create lifecycle: %v", err)
	}
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("replay exact lifecycle create: %v", err)
	}
	conflicting := state
	conflicting.Cancelled = true
	if err := store.Create(ctx, conflicting); !errors.Is(err, plantasks.ErrConflict) {
		t.Fatalf("create with reused lifecycle ID error = %v, want ErrConflict", err)
	}

	loaded, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle: %v", err)
	}
	if loaded.Revision != 0 || loaded.Lifecycle.ID != state.Lifecycle.ID || loaded.Tasks[plantasksStoreTaskID].Stage != plantasks.StageAuthor {
		t.Fatalf("loaded lifecycle did not preserve initial state: %+v", loaded)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 0, func(current *plantasks.State) error {
		current.Cancelled = true
		return nil
	}); err != nil {
		t.Fatalf("update lifecycle at revision zero: %v", err)
	}
	loaded, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load updated lifecycle: %v", err)
	}
	if loaded.Revision != 1 || !loaded.Cancelled {
		t.Fatalf("updated state = revision %d, cancelled %t; want revision 1 and cancelled", loaded.Revision, loaded.Cancelled)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 0, func(*plantasks.State) error { return nil }); !errors.Is(err, plantasks.ErrConflict) {
		t.Fatalf("stale revision update error = %v, want ErrConflict", err)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 1, func(current *plantasks.State) error {
		current.Lifecycle.PolicyRevision = "unapproved-policy"
		return nil
	}); !errors.Is(err, plantasks.ErrImmutable) {
		t.Fatalf("lifecycle definition rewrite error = %v, want ErrImmutable", err)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 1, func(current *plantasks.State) error {
		task := current.Tasks[plantasksStoreTaskID]
		task.CreatedAt = task.CreatedAt.Add(time.Minute)
		current.Tasks[task.ID] = task
		return nil
	}); !errors.Is(err, plantasks.ErrImmutable) {
		t.Fatalf("task definition rewrite error = %v, want ErrImmutable", err)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 1, func(current *plantasks.State) error {
		current.Cancelled = false
		return nil
	}); !errors.Is(err, plantasks.ErrImmutable) {
		t.Fatalf("cancellation reversal error = %v, want ErrImmutable", err)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 1, func(current *plantasks.State) error {
		current.StartedAt = current.StartedAt.Add(time.Second)
		return nil
	}); !errors.Is(err, plantasks.ErrImmutable) {
		t.Fatalf("start time rewrite error = %v, want ErrImmutable", err)
	}
}

func TestPlanTaskStoreReceiptAuditIsAppendOnly(t *testing.T) {
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate plan task fixture: %v", err)
	}
	store, err := plantasks.NewStore(db.Pool, clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("construct plan task store: %v", err)
	}
	state := plantasksStoreState("82000000-0000-4000-8000-000000000001")
	state.Receipts = map[string]plantasks.Receipt{plantasksStoreReceiptID: plantasksStoreReceipt(state.Lifecycle.ID)}
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("create lifecycle with historical receipt: %v", err)
	}

	if err := store.Update(ctx, state.Lifecycle.ID, 0, func(current *plantasks.State) error {
		current.Cancelled = true
		old := current.Receipts[plantasksStoreReceiptID]
		old.Detail = "rewritten historical outcome"
		current.Receipts[plantasksStoreReceiptID] = old
		return nil
	}); !errors.Is(err, plantasks.ErrReceiptAudit) {
		t.Fatalf("rewrite historical receipt error = %v, want ErrReceiptAudit", err)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 0, func(current *plantasks.State) error {
		current.Cancelled = true
		delete(current.Receipts, plantasksStoreReceiptID)
		return nil
	}); !errors.Is(err, plantasks.ErrReceiptAudit) {
		t.Fatalf("remove historical receipt error = %v, want ErrReceiptAudit", err)
	}

	newReceiptID := "82000000-0000-4000-8000-000000000003"
	if err := store.Update(ctx, state.Lifecycle.ID, 0, func(current *plantasks.State) error {
		current.Cancelled = true
		current.Receipts[newReceiptID] = plantasksStoreReceiptWithID(state.Lifecycle.ID, newReceiptID)
		return nil
	}); err != nil {
		t.Fatalf("append receipt in lifecycle update: %v", err)
	}
	var auditCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM plan_task_receipts WHERE lifecycle_id=$1::uuid`, state.Lifecycle.ID).Scan(&auditCount); err != nil {
		t.Fatalf("count durable receipt audit: %v", err)
	}
	if auditCount != 2 {
		t.Fatalf("durable receipt audit count = %d, want 2", auditCount)
	}
	loaded, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle after receipt append: %v", err)
	}
	if loaded.Revision != 1 || len(loaded.Receipts) != 2 {
		t.Fatalf("receipt append state revision=%d receipts=%d, want 1 and 2", loaded.Revision, len(loaded.Receipts))
	}
}

func TestPlanTaskStorePublishesHandoffAtomically(t *testing.T) {
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
	state := plantasksStoreState("83000000-0000-4000-8000-000000000001")
	review := state.Tasks[plantasksStoreReviewID]
	review.PR = nil
	state.Tasks[review.ID] = review
	state.CurrentPR = nil
	state.Claims = map[string]plantasks.Admission{}
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("create pre-handoff lifecycle: %v", err)
	}
	pr := plantasksStorePR()
	receipt := plantasks.Receipt{Version: plantasks.VersionV1, ID: "83000000-0000-4000-8000-000000000003",
		LifecycleID: state.Lifecycle.ID, TaskID: plantasksStoreTaskID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor:          plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: state.StartedAt, SourceRevision: pr.HeadSHA},
		PolicyRevision: state.Lifecycle.PolicyRevision, PR: &pr, CreatedAt: manual.Now()}
	if err := store.Update(ctx, state.Lifecycle.ID, 0, func(current *plantasks.State) error {
		now := manual.Now()
		admission := plantasks.Admission{TaskID: plantasksStoreTaskID, ClaimSHA: "author-claim", ActorID: "author-1",
			Revision: current.Revision, ExpiresAt: now.Add(15 * time.Minute)}
		return current.Admit(admission, now)
	}); err != nil {
		t.Fatalf("persist author admission: %v", err)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 1, func(current *plantasks.State) error {
		return current.Record(receipt, "author-claim", manual.Now())
	}); err != nil {
		t.Fatalf("persist author handoff and publish review: %v", err)
	}
	loaded, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load published review: %v", err)
	}
	published := loaded.Tasks[plantasksStoreReviewID]
	if published.PR == nil || *published.PR != pr || len(published.Authors) != 1 || loaded.CurrentPR == nil || *loaded.CurrentPR != pr {
		t.Fatalf("review handoff publication was not atomic: review=%+v current_pr=%+v", published, loaded.CurrentPR)
	}
}

func TestPlanTaskStoreConcurrentExpectedRevisionHasOneWinner(t *testing.T) {
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate plan task fixture: %v", err)
	}
	store, err := plantasks.NewStore(db.Pool, clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("construct plan task store: %v", err)
	}
	state := plantasksStoreState("84000000-0000-4000-8000-000000000001")
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("create lifecycle: %v", err)
	}
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ready <- struct{}{}
			<-start
			results <- store.Update(ctx, state.Lifecycle.ID, 0, func(current *plantasks.State) error {
				current.Cancelled = true
				return nil
			})
		}()
	}
	<-ready
	<-ready
	close(start)
	workers.Wait()
	close(results)
	wins, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, plantasks.ErrConflict):
			conflicts++
		default:
			t.Errorf("concurrent update returned unexpected error: %v", err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("concurrent revision outcomes: %d winner(s), %d conflict(s); want one each", wins, conflicts)
	}
}

func TestPlanTaskStoreDeferredCommitFailureRollsBackReceiptAndState(t *testing.T) {
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate plan task fixture: %v", err)
	}
	store, err := plantasks.NewStore(db.Pool, clock.NewManual(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("construct plan task store: %v", err)
	}
	state := plantasksStoreState("85000000-0000-4000-8000-000000000001")
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("create lifecycle: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `CREATE FUNCTION reject_plan_lifecycle_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced deferred commit failure'; END; $$`); err != nil {
		t.Fatalf("create deferred commit failure function: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `CREATE CONSTRAINT TRIGGER reject_plan_lifecycle_commit AFTER UPDATE ON plan_lifecycles
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_plan_lifecycle_commit()`); err != nil {
		t.Fatalf("create deferred commit failure trigger: %v", err)
	}
	if err := store.Update(ctx, state.Lifecycle.ID, 0, func(current *plantasks.State) error {
		current.Cancelled = true
		id := "85000000-0000-4000-8000-000000000002"
		current.Receipts[id] = plantasksStoreReceiptWithID(state.Lifecycle.ID, id)
		return nil
	}); err == nil {
		t.Fatal("update error = nil, want deferred commit failure")
	}
	if _, err := db.Pool.Exec(ctx, `DROP TRIGGER reject_plan_lifecycle_commit ON plan_lifecycles`); err != nil {
		t.Fatalf("drop deferred commit failure trigger: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `DROP FUNCTION reject_plan_lifecycle_commit()`); err != nil {
		t.Fatalf("drop deferred commit failure function: %v", err)
	}
	loaded, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle after failed commit: %v", err)
	}
	var receiptCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM plan_task_receipts WHERE lifecycle_id=$1::uuid`, state.Lifecycle.ID).Scan(&receiptCount); err != nil {
		t.Fatalf("count receipts after failed commit: %v", err)
	}
	if loaded.Revision != 0 || loaded.Cancelled || len(loaded.Receipts) != 0 || receiptCount != 0 {
		t.Fatalf("failed commit persisted partial state: revision=%d cancelled=%t receipts=%d audit=%d", loaded.Revision, loaded.Cancelled, len(loaded.Receipts), receiptCount)
	}
}

func TestPlanAdapterAuthorHandoffAndIndependentReviewClaims(t *testing.T) {
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
	state := plantasksStoreState("86000000-0000-4000-8000-000000000001")
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("create adapter lifecycle: %v", err)
	}
	author := plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: state.StartedAt}
	reviewer := plantasks.Provenance{ActorID: "reviewer-1", ActorKind: "human", AuthoredAt: manual.Now()}
	actors := &plantasksTestAuthenticator{actor: author}
	claims := &plantasksTestClaimVerifier{allowed: map[string]plantasksTestClaim{}}
	outcomes := &plantasksTestOutcomeVerifier{allowed: map[string]plantasks.Receipt{}}
	policy := &plantasksTestRuntimePolicy{lifecycleID: state.Lifecycle.ID, taskIDs: map[string]bool{
		plantasksStoreTaskID: true, plantasksStoreReviewID: true,
	}, actorIDs: map[string]bool{author.ActorID: true, reviewer.ActorID: true}}
	adapter, err := plantasks.NewAdapter(store, claims, actors, policy, outcomes)
	if err != nil {
		t.Fatalf("construct plan task adapter: %v", err)
	}
	claims.allow(plantasksStoreTaskID, "author-claim", author.ActorID)
	assertNoMutation := func(before plantasks.State, label string) {
		t.Helper()
		after, err := store.Load(ctx, state.Lifecycle.ID)
		if err != nil {
			t.Fatalf("load state after %s denial: %v", label, err)
		}
		if before.Revision != after.Revision || !reflect.DeepEqual(before.Tasks, after.Tasks) || !reflect.DeepEqual(before.Attempts, after.Attempts) ||
			!reflect.DeepEqual(before.Claims, after.Claims) || !reflect.DeepEqual(before.Receipts, after.Receipts) ||
			!reflect.DeepEqual(before.Completed, after.Completed) || !reflect.DeepEqual(before.CurrentPR, after.CurrentPR) ||
			before.DeliveryReceiptID != after.DeliveryReceiptID {
			t.Fatalf("%s denial mutated lifecycle", label)
		}
	}
	before, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load initial adapter state: %v", err)
	}
	if err := adapter.Admit(ctx, state.Lifecycle.ID, plantasksStoreTaskID, "wrong-claim", 0, manual.Now().Add(20*time.Minute)); !errors.Is(err, plantasks.ErrClaimDenied) {
		t.Fatalf("wrong claim admission error = %v, want ErrClaimDenied", err)
	}
	assertNoMutation(before, "wrong claim")
	actors.err = errors.New("no authenticated session")
	if err := adapter.Admit(ctx, state.Lifecycle.ID, plantasksStoreTaskID, "author-claim", 0, manual.Now().Add(20*time.Minute)); !errors.Is(err, plantasks.ErrActorDenied) {
		t.Fatalf("unauthenticated admission error = %v, want ErrActorDenied", err)
	}
	assertNoMutation(before, "unauthenticated actor")
	actors.err = nil
	policy.err = errors.New("host lifecycle admission disabled")
	if err := adapter.Admit(ctx, state.Lifecycle.ID, plantasksStoreTaskID, "author-claim", 0, manual.Now().Add(20*time.Minute)); err == nil {
		t.Fatal("host-policy denial error = nil, want denial")
	}
	assertNoMutation(before, "host policy")
	policy.err = nil
	if err := adapter.Admit(ctx, state.Lifecycle.ID, plantasksStoreTaskID, "author-claim", 0, manual.Now().Add(20*time.Minute)); err != nil {
		t.Fatalf("admit author claim: %v", err)
	}
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load state after author admission: %v", err)
	}
	pr := plantasksStorePR()
	author.SourceRevision = pr.HeadSHA
	actors.actor = author
	receipt := plantasks.Receipt{Version: plantasks.VersionV1, ID: "86000000-0000-4000-8000-000000000002",
		LifecycleID: state.Lifecycle.ID, TaskID: plantasksStoreTaskID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor: author, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &pr, CreatedAt: manual.Now()}
	spoofed := receipt
	spoofed.ID = "86000000-0000-4000-8000-000000000003"
	spoofed.Actor = reviewer
	if err := adapter.Result(ctx, state.Lifecycle.ID, spoofed, "author-claim", before.Revision); !errors.Is(err, plantasks.ErrActorDenied) {
		t.Fatalf("spoofed receipt actor error = %v, want ErrActorDenied", err)
	}
	assertNoMutation(before, "spoofed receipt actor")
	if err := adapter.Result(ctx, state.Lifecycle.ID, receipt, "author-claim", before.Revision); err != nil {
		t.Fatalf("record authoritative author handoff: %v", err)
	}
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load state after author handoff: %v", err)
	}
	claims.allow(plantasksStoreReviewID, "self-review-claim", author.ActorID)
	if err := adapter.Admit(ctx, state.Lifecycle.ID, plantasksStoreReviewID, "self-review-claim", before.Revision, manual.Now().Add(20*time.Minute)); err == nil {
		t.Fatal("self-review admission error = nil, want denial")
	}
	assertNoMutation(before, "self-review")
	actors.actor = reviewer
	claims.allow(plantasksStoreReviewID, "reviewer-claim", reviewer.ActorID)
	if err := adapter.Admit(ctx, state.Lifecycle.ID, plantasksStoreReviewID, "wrong-review-claim", before.Revision, manual.Now().Add(20*time.Minute)); !errors.Is(err, plantasks.ErrClaimDenied) {
		t.Fatalf("reviewer wrong-claim admission error = %v, want ErrClaimDenied", err)
	}
	assertNoMutation(before, "reviewer wrong claim")
	if err := adapter.Admit(ctx, state.Lifecycle.ID, plantasksStoreReviewID, "reviewer-claim", before.Revision, manual.Now().Add(20*time.Minute)); err != nil {
		t.Fatalf("admit independent reviewer: %v", err)
	}
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load state after reviewer admission: %v", err)
	}
	if err := adapter.Admit(ctx, state.Lifecycle.ID, plantasksStoreReviewID, "reviewer-claim", before.Revision, manual.Now().Add(20*time.Minute)); err == nil {
		t.Fatal("duplicate active claim admission error = nil, want denial")
	}
	assertNoMutation(before, "duplicate active claim")

	reviewPR := plantasksStorePR()
	reviewer.SourceRevision = reviewPR.HeadSHA
	actors.actor = reviewer
	changes := plantasks.Receipt{Version: plantasks.VersionV1, ID: "86000000-0000-4000-8000-000000000004",
		LifecycleID: state.Lifecycle.ID, TaskID: plantasksStoreReviewID, Outcome: plantasks.OutcomeChangesRequest,
		Actor: reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &reviewPR,
		FindingIDs: []string{"86000000-0000-4000-8000-000000000005"}, CreatedAt: manual.Now(), Detail: "one bounded fix is required"}
	if err := adapter.Result(ctx, state.Lifecycle.ID, changes, "reviewer-claim", before.Revision); err != nil {
		t.Fatalf("record changes requested and create correction tasks: %v", err)
	}
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle after negative review: %v", err)
	}
	var fixTask, rereviewTask plantasks.Task
	for _, task := range before.Tasks {
		if task.Correction == 1 && task.Stage == plantasks.StageFix {
			fixTask = task
		}
		if task.Correction == 1 && task.Stage == plantasks.StageRereview {
			rereviewTask = task
		}
	}
	if fixTask.ID == "" || rereviewTask.ID == "" {
		t.Fatalf("negative review did not materialize bounded fix/re-review tasks: fix=%+v review=%+v", fixTask, rereviewTask)
	}
	policy.taskIDs[fixTask.ID] = true
	policy.taskIDs[rereviewTask.ID] = true
	fixer := plantasks.Provenance{ActorID: "fixer-1", ActorKind: "agent", AuthoredAt: manual.Now()}
	policy.actorIDs[fixer.ActorID] = true
	claims.allow(fixTask.ID, "fixer-claim", fixer.ActorID)
	actors.actor = fixer
	if err := adapter.Admit(ctx, state.Lifecycle.ID, fixTask.ID, "fixer-claim", before.Revision, manual.Now().Add(20*time.Minute)); err != nil {
		t.Fatalf("admit bounded fix task: %v", err)
	}
	newPR := plantasksStorePR()
	newPR.HeadSHA = "cccccccccccccccccccccccccccccccccccccccc"
	fixer.SourceRevision = newPR.HeadSHA
	actors.actor = fixer
	fixReceipt := plantasks.Receipt{Version: plantasks.VersionV1, ID: "86000000-0000-4000-8000-000000000006",
		LifecycleID: state.Lifecycle.ID, TaskID: fixTask.ID, Outcome: plantasks.OutcomeCodingHandoff,
		Actor: fixer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &newPR, CreatedAt: manual.Now()}
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle after fix admission: %v", err)
	}
	if err := adapter.Result(ctx, state.Lifecycle.ID, fixReceipt, "fixer-claim", before.Revision); err != nil {
		t.Fatalf("publish fix handoff with new head: %v", err)
	}
	afterFix, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle after fix handoff: %v", err)
	}
	publishedRereview := afterFix.Tasks[rereviewTask.ID]
	if afterFix.CurrentPR == nil || *afterFix.CurrentPR != newPR || publishedRereview.PR == nil || *publishedRereview.PR != newPR || len(publishedRereview.Authors) != 2 ||
		publishedRereview.Authors[0].ActorID != author.ActorID || publishedRereview.Authors[1].ActorID != fixer.ActorID {
		t.Fatalf("fix handoff did not publish new snapshot and authors: current=%+v rereview=%+v", afterFix.CurrentPR, publishedRereview)
	}

	claims.allow(rereviewTask.ID, "author-rereview-claim", author.ActorID)
	actors.actor = author
	if err := adapter.Admit(ctx, state.Lifecycle.ID, rereviewTask.ID, "author-rereview-claim", afterFix.Revision, manual.Now().Add(20*time.Minute)); err == nil {
		t.Fatal("original author self-review admission error = nil, want denial")
	}
	assertNoMutation(afterFix, "original author self-review")
	claims.allow(rereviewTask.ID, "fixer-rereview-claim", fixer.ActorID)
	actors.actor = fixer
	if err := adapter.Admit(ctx, state.Lifecycle.ID, rereviewTask.ID, "fixer-rereview-claim", afterFix.Revision, manual.Now().Add(20*time.Minute)); err == nil {
		t.Fatal("fix author self-review admission error = nil, want denial")
	}
	assertNoMutation(afterFix, "fix author self-review")
	reviewer.SourceRevision = newPR.HeadSHA
	actors.actor = reviewer
	claims.allow(rereviewTask.ID, "rereviewer-claim", reviewer.ActorID)
	if err := adapter.Admit(ctx, state.Lifecycle.ID, rereviewTask.ID, "rereviewer-claim", afterFix.Revision, manual.Now().Add(20*time.Minute)); err != nil {
		t.Fatalf("admit independent reviewer for new head: %v", err)
	}
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle before verified delivery outcomes: %v", err)
	}
	approved := plantasks.Receipt{Version: plantasks.VersionV1, ID: "86000000-0000-4000-8000-000000000007",
		LifecycleID: state.Lifecycle.ID, TaskID: rereviewTask.ID, Outcome: plantasks.OutcomeApproved,
		Actor: reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &newPR, CreatedAt: manual.Now()}
	if err := adapter.Result(ctx, state.Lifecycle.ID, approved, "rereviewer-claim", before.Revision); err != nil {
		t.Fatalf("record independent approval: %v", err)
	}
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle after approval: %v", err)
	}
	merged := plantasks.Receipt{Version: plantasks.VersionV1, ID: "86000000-0000-4000-8000-000000000008",
		LifecycleID: state.Lifecycle.ID, TaskID: rereviewTask.ID, Outcome: plantasks.OutcomeMerged,
		Actor: reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &newPR,
		MergeCommit: "dddddddddddddddddddddddddddddddddddddddd", CreatedAt: manual.Now()}
	if err := adapter.Result(ctx, state.Lifecycle.ID, merged, "rereviewer-claim", before.Revision); !errors.Is(err, plantasks.ErrOutcomeDenied) {
		t.Fatalf("unverified merge outcome error = %v, want ErrOutcomeDenied", err)
	}
	assertNoMutation(before, "unverified merge")
	outcomes.allow(state.Lifecycle, rereviewTask, merged)
	if err := adapter.Result(ctx, state.Lifecycle.ID, merged, "rereviewer-claim", before.Revision); err != nil {
		t.Fatalf("record host-verified merge: %v", err)
	}
	if len(outcomes.seen) == 0 || outcomes.seen[len(outcomes.seen)-1].lifecycle.ID != state.Lifecycle.ID ||
		!reflect.DeepEqual(outcomes.seen[len(outcomes.seen)-1].task, publishedRereview) ||
		!reflect.DeepEqual(outcomes.seen[len(outcomes.seen)-1].receipt, merged) {
		t.Fatal("outcome verifier did not receive the locked lifecycle, task, and complete receipt")
	}
	before, err = store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle after merge: %v", err)
	}
	landed := plantasks.Receipt{Version: plantasks.VersionV1, ID: "86000000-0000-4000-8000-000000000009",
		LifecycleID: state.Lifecycle.ID, TaskID: rereviewTask.ID, Outcome: plantasks.OutcomeLanded,
		Actor: reviewer, PolicyRevision: state.Lifecycle.PolicyRevision, PR: &newPR,
		MergeCommit: merged.MergeCommit, LandedCommit: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", CreatedAt: manual.Now()}
	if err := adapter.Result(ctx, state.Lifecycle.ID, landed, "rereviewer-claim", before.Revision); !errors.Is(err, plantasks.ErrOutcomeDenied) {
		t.Fatalf("unverified landing outcome error = %v, want ErrOutcomeDenied", err)
	}
	assertNoMutation(before, "unverified landing")
	outcomes.allow(state.Lifecycle, rereviewTask, landed)
	if err := adapter.Result(ctx, state.Lifecycle.ID, landed, "rereviewer-claim", before.Revision); err != nil {
		t.Fatalf("record host-verified landing: %v", err)
	}
	afterLanding, err := store.Load(ctx, state.Lifecycle.ID)
	if err != nil {
		t.Fatalf("load lifecycle after landing: %v", err)
	}
	if afterLanding.DeliveryReceiptID != landed.ID || afterLanding.Completed[rereviewTask.ID] != plantasks.OutcomeLanded {
		t.Fatalf("verified landing was not committed: delivery=%q completed=%q", afterLanding.DeliveryReceiptID, afterLanding.Completed[rereviewTask.ID])
	}
}

type plantasksTestClaim struct {
	taskID  string
	actorID string
}

type plantasksTestClaimVerifier struct {
	allowed map[string]plantasksTestClaim
}

func (v *plantasksTestClaimVerifier) allow(taskID, claimSHA, actorID string) {
	v.allowed[claimSHA] = plantasksTestClaim{taskID: taskID, actorID: actorID}
}

func (v *plantasksTestClaimVerifier) Verify(_ context.Context, taskID, claimSHA, actorID string) error {
	claim, ok := v.allowed[claimSHA]
	if !ok || claim.taskID != taskID || claim.actorID != actorID {
		return errors.New("claim owner mismatch")
	}
	return nil
}

type plantasksTestAuthenticator struct {
	actor plantasks.Provenance
	err   error
}

func (a *plantasksTestAuthenticator) Authenticate(context.Context) (plantasks.Provenance, error) {
	return a.actor, a.err
}

type plantasksTestRuntimePolicy struct {
	lifecycleID string
	taskIDs     map[string]bool
	actorIDs    map[string]bool
	err         error
}

type plantasksTestOutcomeObservation struct {
	lifecycle plantasks.Lifecycle
	task      plantasks.Task
	receipt   plantasks.Receipt
}

type plantasksTestOutcomeVerifier struct {
	allowed map[string]plantasks.Receipt
	seen    []plantasksTestOutcomeObservation
}

func (v *plantasksTestOutcomeVerifier) allow(lifecycle plantasks.Lifecycle, task plantasks.Task, receipt plantasks.Receipt) {
	v.allowed[receipt.ID] = plantasks.Receipt{Version: receipt.Version, ID: receipt.ID, LifecycleID: lifecycle.ID, TaskID: task.ID,
		Outcome: receipt.Outcome, Actor: receipt.Actor, PolicyRevision: lifecycle.PolicyRevision, PR: receipt.PR,
		MergeCommit: receipt.MergeCommit, LandedCommit: receipt.LandedCommit, ObservedOutcome: receipt.ObservedOutcome,
		Detail: receipt.Detail, CreatedAt: receipt.CreatedAt}
}

func (v *plantasksTestOutcomeVerifier) Verify(_ context.Context, lifecycle plantasks.Lifecycle, task plantasks.Task, receipt plantasks.Receipt) error {
	v.seen = append(v.seen, plantasksTestOutcomeObservation{lifecycle: lifecycle, task: task, receipt: receipt})
	if receipt.Outcome != plantasks.OutcomeMerged && receipt.Outcome != plantasks.OutcomeLanded {
		return nil
	}
	expected, ok := v.allowed[receipt.ID]
	if !ok || lifecycle.ID != receipt.LifecycleID || task.ID != receipt.TaskID || !reflect.DeepEqual(expected, receipt) {
		return errors.New("host outcome evidence is not authorized")
	}
	return nil
}

func (p *plantasksTestRuntimePolicy) Authorize(_ context.Context, lifecycle plantasks.Lifecycle, task plantasks.Task, actor plantasks.Provenance) error {
	if p.err != nil {
		return p.err
	}
	if lifecycle.ID != p.lifecycleID || !p.taskIDs[task.ID] || !p.actorIDs[actor.ActorID] {
		return errors.New("host test policy denied lifecycle, task, or actor")
	}
	return nil
}

const plantasksStoreTaskID = "81000000-0000-4000-8000-000000000002"
const plantasksStoreReceiptID = "82000000-0000-4000-8000-000000000002"

func plantasksStoreState(lifecycleID string) plantasks.State {
	created := time.Date(2026, 10, 3, 11, 0, 0, 0, time.UTC)
	lifecycle := plantasks.Lifecycle{
		Version:        plantasks.VersionV1,
		ID:             lifecycleID,
		DeliveryGateID: plantasksStoreReviewID,
		Repository:     plantasks.Repository{Owner: "example", Name: "repo", Target: "main"},
		PolicyRevision: "policy-v1",
		Authored:       plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: created},
		Limits:         plantasks.CorrectionLimits{MaxCorrections: 2, MaxAttempts: 3, MaxDurationSeconds: 3600, MaxConcurrentTasks: 2},
		CreatedAt:      created,
	}
	task := plantasks.Task{Version: plantasks.VersionV1, ID: plantasksStoreTaskID, LifecycleID: lifecycleID,
		Stage: plantasks.StageAuthor, CreatedAt: created}
	review := plantasks.Task{Version: plantasks.VersionV1, ID: plantasksStoreReviewID, LifecycleID: lifecycleID,
		Stage: plantasks.StageReview, Dependencies: []plantasks.Dependency{{TaskID: task.ID, Kind: plantasks.DependencyHandoff}},
		CreatedAt: created}
	return plantasks.State{Lifecycle: lifecycle, Tasks: map[string]plantasks.Task{task.ID: task, review.ID: review},
		Receipts: map[string]plantasks.Receipt{}, Completed: map[string]plantasks.Outcome{}, Claims: map[string]plantasks.Admission{},
		Attempts: map[string]int{}, StartedAt: created}
}

const plantasksStoreReviewID = "81000000-0000-4000-8000-000000000003"

func plantasksStorePR() plantasks.PRBinding {
	return plantasks.PRBinding{Number: 1, URL: "https://github.com/example/repo/pull/1",
		HeadSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BaseSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PolicyRevision: "policy-v1"}
}

func plantasksStoreReceipt(lifecycleID string) plantasks.Receipt {
	return plantasksStoreReceiptWithID(lifecycleID, plantasksStoreReceiptID)
}

func plantasksStoreReceiptWithID(lifecycleID, receiptID string) plantasks.Receipt {
	return plantasks.Receipt{Version: plantasks.VersionV1, ID: receiptID, LifecycleID: lifecycleID,
		TaskID: plantasksStoreTaskID, Outcome: plantasks.OutcomeUnknown,
		Actor:          plantasks.Provenance{ActorID: "author-1", ActorKind: "human", AuthoredAt: time.Date(2026, 10, 3, 11, 30, 0, 0, time.UTC)},
		PolicyRevision: "policy-v1", CreatedAt: time.Date(2026, 10, 3, 11, 30, 0, 0, time.UTC), Detail: "provider outcome is unknown"}
}
