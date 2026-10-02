package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

func TestUnitOfWork(t *testing.T) {
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatal(err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	if got := manual.Advance(2 * time.Second); !got.Equal(time.Date(2026, 10, 1, 12, 0, 2, 0, time.UTC)) {
		t.Fatalf("manual clock advance: %s", got)
	}
	const orgID = "uow-org"
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets (org_id) VALUES ($1)`, orgID); err != nil {
		t.Fatal(err)
	}
	var taskID string
	err := db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,policy_version) VALUES($1,'owner/repo','source','owner','v1') RETURNING id::text`, orgID).Scan(&taskID)
	if err != nil {
		t.Fatal(err)
	}
	const deliveryID = "delivery-uow"
	if err := storage.WithUnitOfWork(ctx, db.Pool, manual, func(ctx context.Context, repos *storage.Repositories) error {
		_, err := repos.StoreWebhookDelivery(ctx, storage.DeliveryReceipt{DeliveryID: deliveryID, EventType: "pull_request", Payload: json.RawMessage(`{"action":"opened"}`)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WithUnitOfWork(ctx, db.Pool, manual, func(ctx context.Context, repos *storage.Repositories) error {
		inserted, err := repos.StoreWebhookDelivery(ctx, storage.DeliveryReceipt{DeliveryID: deliveryID, EventType: "pull_request", Payload: json.RawMessage(`{"action":"opened"}`)})
		if err != nil {
			return err
		}
		if inserted {
			return errors.New("identical receipt replay was reported as inserted")
		}
		_, err = repos.StoreWebhookDelivery(ctx, storage.DeliveryReceipt{DeliveryID: deliveryID, EventType: "pull_request", Payload: json.RawMessage(`{"action":"closed"}`)})
		if !errors.Is(err, storage.ErrDeliveryConflict) {
			return errors.New("conflicting receipt replay was accepted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	jobID := "11111111-1111-4111-8111-111111111111"
	operationID := "22222222-2222-4222-8222-222222222222"
	correlationID := "33333333-3333-4333-8333-333333333333"
	jobPayload := func(snapshot contracts.Snapshot, gen int64) []byte {
		b, err := json.Marshal(contracts.Job{Version: 1, TaskID: taskID, JobID: jobID, Generation: gen,
			Snapshot: snapshot, Attempt: 1, OperationID: operationID, CorrelationID: correlationID, Operation: "author"})
		if err != nil {
			t.Fatal(err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(b, &object); err != nil {
			t.Fatal(err)
		}
		delete(object, "budget_envelope") // Non-inference jobs have no price envelope.
		b, err = json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	input := func(payload []byte) storage.JobInput {
		return storage.JobInput{ID: jobID, LogicalKey: "task:" + taskID + ":generation:0:author", OperationType: "author", Payload: payload}
	}
	boom := errors.New("injected failure after job insert")
	err = storage.WithUnitOfWork(ctx, db.Pool, manual, func(ctx context.Context, repos *storage.Repositories) error {
		locked, err := repos.LockTask(ctx, taskID)
		if err != nil {
			return err
		}
		if err := repos.SetDeliveryDisposition(ctx, deliveryID, "PROCESSED"); err != nil {
			return err
		}
		if _, err := repos.UpdateTaskState(ctx, locked, 0, 1, "WAITING_CI"); err != nil {
			return err
		}
		locked, err = repos.LockTask(ctx, taskID)
		if err != nil {
			return err
		}
		if _, _, err := repos.InsertJobAndOutbox(ctx, locked, input(jobPayload(contracts.Snapshot{}, 1))); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected injected rollback error, got %v", err)
	}
	var state, disposition string
	var generation int64
	if err := db.Pool.QueryRow(ctx, `SELECT state,generation FROM tasks WHERE id=$1::uuid`, taskID).Scan(&state, &generation); err != nil {
		t.Fatal(err)
	}
	if state != "AUTHORING" || generation != 0 {
		t.Fatalf("partial task mutation: state=%s generation=%d", state, generation)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT disposition FROM webhook_deliveries WHERE delivery_id=$1`, deliveryID).Scan(&disposition); err != nil {
		t.Fatal(err)
	}
	if disposition != "INBOX" {
		t.Fatalf("partial delivery disposition: %s", disposition)
	}
	var jobs, outbox int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid`, taskID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid`, taskID).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || outbox != 0 {
		t.Fatalf("partial job/outbox writes: jobs=%d outbox=%d", jobs, outbox)
	}

	if err := storage.WithUnitOfWork(ctx, db.Pool, manual, func(ctx context.Context, repos *storage.Repositories) error {
		locked, err := repos.LockTask(ctx, taskID)
		if err != nil {
			return err
		}
		if err := repos.SetDeliveryDisposition(ctx, deliveryID, "PROCESSED"); err != nil {
			return err
		}
		if err := repos.SetDeliveryDisposition(ctx, deliveryID, "PROCESSED"); err != nil {
			return err
		}
		if err := repos.SetDeliveryDisposition(ctx, deliveryID, "IGNORED"); !errors.Is(err, storage.ErrDeliveryDisposition) {
			return errors.New("conflicting final disposition was accepted")
		}
		if _, _, err := repos.InsertJobAndOutbox(ctx, locked, input(jobPayload(contracts.Snapshot{}, 0))); err != nil {
			return err
		}
		_, _, err = repos.InsertJobAndOutbox(ctx, locked, input(jobPayload(contracts.Snapshot{}, 0)))
		if err != nil {
			return err
		}
		conflicting := "55555555-5555-4555-8555-555555555555"
		changed, e := json.Marshal(contracts.Job{Version: 1, TaskID: taskID, JobID: conflicting, Generation: 0, Snapshot: contracts.Snapshot{}, Attempt: 1, OperationID: operationID, CorrelationID: correlationID, Operation: "author"})
		if e != nil {
			return e
		}
		var object map[string]json.RawMessage
		if e = json.Unmarshal(changed, &object); e != nil {
			return e
		}
		delete(object, "budget_envelope")
		changed, e = json.Marshal(object)
		if e != nil {
			return e
		}
		conflictingInput := input(changed)
		conflictingInput.ID = conflicting
		if _, _, e = repos.InsertJobAndOutbox(ctx, locked, conflictingInput); !errors.Is(e, storage.ErrJobConflict) {
			return errors.New("conflicting logical job replay was accepted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid`, taskID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid`, taskID).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || outbox != 1 {
		t.Fatalf("idempotent insert counts: jobs=%d outbox=%d", jobs, outbox)
	}
	if err := storage.WithUnitOfWork(ctx, db.Pool, manual, func(ctx context.Context, repos *storage.Repositories) error {
		locked, err := repos.LockTask(ctx, taskID)
		if err != nil {
			return err
		}
		if _, _, err := repos.LockOrgBudgetAndTask(ctx, orgID, taskID); !errors.Is(err, storage.ErrLockOrder) {
			return errors.New("org budget lock after task lock was allowed")
		}
		updated, err := repos.UpdateTaskState(ctx, locked, 0, 1, "WAITING_CI")
		if err != nil {
			return err
		}
		if err := repos.RequireTaskGeneration(ctx, locked, 0); !errors.Is(err, storage.ErrLockHandle) {
			return errors.New("pre-update lock handle remained valid")
		}
		if _, _, err := repos.InsertJobAndOutbox(ctx, locked, input(jobPayload(contracts.Snapshot{}, 0))); !errors.Is(err, storage.ErrLockHandle) {
			return errors.New("stale lock handle created a job")
		}
		if err := repos.RequireTaskGeneration(ctx, updated, 1); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Attached PR jobs must carry the persisted snapshot; mismatched snapshots fail.
	foreignPRHead := "dddddddddddddddddddddddddddddddddddddddd"
	foreignPRBase := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	var foreignPRID int64
	if err := db.Pool.QueryRow(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha) VALUES($1::uuid,'owner/repo',2,$2,'main',$3) RETURNING id`, taskID, foreignPRHead, foreignPRBase).Scan(&foreignPRID); err != nil {
		t.Fatal(err)
	}
	var prTaskID string
	err = db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,policy_version) VALUES($1,'owner/repo','pr-source','owner','v1') RETURNING id::text`, orgID).Scan(&prTaskID)
	if err != nil {
		t.Fatal(err)
	}
	heading := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	base := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	integration := "cccccccccccccccccccccccccccccccccccccccc"
	var prID int64
	if err := db.Pool.QueryRow(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha,integration_sha) VALUES($1::uuid,'owner/repo',1,$2,'main',$3,$4) RETURNING id`, prTaskID, heading, base, integration).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	prJobID := "44444444-4444-4444-8444-444444444444"
	prPayload := func(s contracts.Snapshot) []byte {
		b, e := json.Marshal(contracts.Job{Version: 1, TaskID: prTaskID, JobID: prJobID, Generation: 0, Snapshot: s, Attempt: 1, OperationID: operationID, CorrelationID: correlationID, Operation: "author"})
		if e != nil {
			t.Fatal(e)
		}
		var object map[string]json.RawMessage
		if e := json.Unmarshal(b, &object); e != nil {
			t.Fatal(e)
		}
		delete(object, "budget_envelope")
		b, e = json.Marshal(object)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	correct := contracts.Snapshot{HeadSHA: heading, BaseSHA: base, IntegrationSHA: integration}
	err = storage.WithUnitOfWork(ctx, db.Pool, manual, func(ctx context.Context, repos *storage.Repositories) error {
		locked, e := repos.LockTask(ctx, prTaskID)
		if e != nil {
			return e
		}
		bad := storage.JobInput{LogicalKey: "pr:bad", OperationType: "author", Payload: prPayload(contracts.Snapshot{HeadSHA: heading, BaseSHA: base})}
		if _, _, e = repos.InsertJobAndOutbox(ctx, locked, bad); !errors.Is(e, storage.ErrInvalidJob) {
			return errors.New("stale/incomplete PR snapshot was accepted")
		}
		misbound := storage.JobInput{LogicalKey: "pr:misbound", OperationType: "author", PRID: &foreignPRID, Payload: prPayload(correct)}
		if _, _, e = repos.InsertJobAndOutbox(ctx, locked, misbound); !errors.Is(e, storage.ErrInvalidJob) {
			return errors.New("job referencing another task's PR was accepted")
		}
		good := storage.JobInput{LogicalKey: "pr:good", OperationType: "author", Payload: prPayload(correct)}
		remediationAttempt := int32(2)
		good.RemediationAttempt = &remediationAttempt
		_, _, e = repos.InsertJobAndOutbox(ctx, locked, good)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
}
