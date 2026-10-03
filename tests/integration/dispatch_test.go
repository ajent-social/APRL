package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/dispatch"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
	"github.com/redis/go-redis/v9"
)

const dispatchTestPromptHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

type dispatchTestFixture struct {
	db      testutil.DatabaseFixture
	redis   testutil.RedisFixture
	ctx     context.Context
	clock   *clock.Manual
	streams *queue.Streams
	taskID  string
	prID    int64
}

func TestDispatch(t *testing.T) {
	t.Run("publication_redelivery_preserves_job_identity", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		jobID := f.addJob(t, "author")
		outboxID := f.addOutbox(t, jobID, "DISPATCH", `{"job_id":"`+jobID+`"}`, f.clock.Now())
		if _, err := f.db.Pool.Exec(f.ctx, `CREATE FUNCTION dispatch_test_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit failure after Redis publication'; END $$`); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool.Exec(f.ctx, `CREATE CONSTRAINT TRIGGER dispatch_test_fail_commit AFTER UPDATE ON outbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_test_fail_commit()`); err != nil {
			t.Fatal(err)
		}
		d := dispatchTestNewDispatcher(t, f, f.streams)
		if count, err := d.DispatchDue(f.ctx, 1); err == nil || count != 0 {
			t.Fatalf("faulted dispatch = (%d,%v), want commit failure with zero committed rows", count, err)
		}
		var published *time.Time
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if published != nil {
			t.Fatal("outbox publication marker survived the injected commit failure")
		}
		if _, err := f.db.Pool.Exec(f.ctx, `DROP TRIGGER dispatch_test_fail_commit ON outbox`); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool.Exec(f.ctx, `DROP FUNCTION dispatch_test_fail_commit()`); err != nil {
			t.Fatal(err)
		}
		if count, err := d.DispatchDue(f.ctx, 1); err != nil || count != 1 {
			t.Fatalf("retry dispatch = (%d,%v), want one successful publication", count, err)
		}
		entries, err := f.redis.Client.XRange(f.ctx, f.streamKey(), "-", "+").Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 {
			t.Fatalf("transport entries = %d, want original plus retry", len(entries))
		}
		for _, entry := range entries {
			if entry.Values["job_id"] != jobID {
				t.Fatalf("transport entry logical id = %v, want stable job UUID %s", entry.Values["job_id"], jobID)
			}
		}
	})

	t.Run("delayed_due_rows_and_unknown_kind_fail_closed", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		jobID := f.addJob(t, "author")
		future := f.clock.Now().Add(10 * time.Minute)
		f.addOutbox(t, jobID, "DISPATCH", `{"job_id":"`+jobID+`"}`, future)
		d := dispatchTestNewDispatcher(t, f, f.streams)
		if count, err := d.DispatchDue(f.ctx, 1); err != nil || count != 0 {
			t.Fatalf("future row dispatched early: count=%d err=%v", count, err)
		}
		f.clock.Advance(10 * time.Minute)
		if count, err := d.DispatchDue(f.ctx, 1); err != nil || count != 1 {
			t.Fatalf("due row dispatch = (%d,%v), want one", count, err)
		}
		for _, kind := range []string{"CANCEL", "NOTIFY"} {
			jobID := f.addJob(t, "author")
			outboxID := f.addOutbox(t, jobID, kind, `{"job_id":"`+jobID+`"}`, f.clock.Now())
			if count, err := d.DispatchDue(f.ctx, 1); count != 1 || !errors.Is(err, dispatch.ErrNoHandler) {
				t.Fatalf("unhandled %s result = (%d,%v), want one persisted retry and ErrNoHandler", kind, count, err)
			}
			var attempt int32
			var published *time.Time
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT attempt_count,published_at FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&attempt, &published); err != nil {
				t.Fatal(err)
			}
			if attempt != 1 || published != nil {
				t.Fatalf("unhandled %s durable state=(attempt %d,published %v)", kind, attempt, published)
			}
			if _, err := f.db.Pool.Exec(f.ctx, `UPDATE outbox SET next_attempt_at=$2 WHERE id=$1::uuid`, outboxID, f.clock.Now().Add(24*time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("label_sync_remains_unclaimed_for_reconciliation", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		jobID := f.addJob(t, "author")
		labelID := f.addOutbox(t, jobID, "LABEL_SYNC", `{"task_id":"`+f.taskID+`"}`, f.clock.Now())
		dispatchID := f.addOutbox(t, jobID, "DISPATCH", `{"job_id":"`+jobID+`"}`, f.clock.Now())
		d := dispatchTestNewDispatcher(t, f, f.streams)
		if count, err := d.DispatchDue(f.ctx, 1); err != nil || count != 1 {
			t.Fatalf("eligible dispatch with label pending=(%d,%v), want one published dispatch", count, err)
		}
		var labelAttempts int32
		var labelPublished *time.Time
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT attempt_count,published_at FROM outbox WHERE id=$1::uuid`, labelID).Scan(&labelAttempts, &labelPublished); err != nil {
			t.Fatal(err)
		}
		var dispatchPublished *time.Time
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, dispatchID).Scan(&dispatchPublished); err != nil {
			t.Fatal(err)
		}
		if labelAttempts != 0 || labelPublished != nil || dispatchPublished == nil {
			t.Fatalf("dispatcher label=(attempts %d,published %v), eligible dispatch published=%v", labelAttempts, labelPublished, dispatchPublished)
		}
		if _, err := dispatch.NewDispatcher(f.db.Pool, f.clock, f.streams, map[string]dispatch.Handler{
			"LABEL_SYNC": func(context.Context, dispatch.OutboxItem) error { return nil },
		}); !errors.Is(err, dispatch.ErrInvalid) {
			t.Fatalf("LABEL_SYNC callback registration error=%v, want ErrInvalid", err)
		}
	})

	t.Run("live_lease_survives_duplicate_and_reclaim", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		streams := f.newStreams(t, "lease")
		jobID := f.addJob(t, "ci_reconcile")
		if _, err := streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
			t.Fatal(err)
		}
		if _, err := streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
			t.Fatal(err)
		}
		if err := streams.EnsureGroup(f.ctx); err != nil {
			t.Fatal(err)
		}
		messages, err := streams.Read(f.ctx, "dispatch-owner-a", 2*time.Millisecond, 2)
		if err != nil || len(messages) != 2 {
			t.Fatalf("read duplicate deliveries = (%d,%v)", len(messages), err)
		}
		var admitted leases.Lease
		consumer, err := queue.NewConsumer(f.db.Pool, f.clock, streams, queue.ConsumerConfig{
			LeaseTTL: time.Minute, ReclaimIdle: 0, PromptHash: dispatchTestPromptHash,
			SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test",
			BatchSize: 2, Block: time.Millisecond, OperationTimeout: 3 * time.Second,
		}, func(_ context.Context, lease leases.Lease) error { admitted = lease; return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := consumer.Handle(f.ctx, "dispatch-owner-a", messages[0]); !errors.Is(err, queue.ErrNotDisposed) {
			t.Fatalf("uncompleted execution handoff = %v, want no XACK", err)
		}
		reclaimed, _, err := streams.Reclaim(f.ctx, "dispatch-owner-b", 0, "0-0", 2)
		if err != nil || len(reclaimed) != 2 {
			t.Fatalf("XAUTOCLAIM = (%d,%v), want both pending duplicates", len(reclaimed), err)
		}
		if err := consumer.Handle(f.ctx, "dispatch-owner-b", reclaimed[0]); !errors.Is(err, queue.ErrLeaseActive) {
			t.Fatalf("reclaimed delivery stole a live lease: %v", err)
		}
		var storedToken string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT lease_token::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&storedToken); err != nil {
			t.Fatal(err)
		}
		if storedToken != admitted.Token {
			t.Fatalf("reclaim changed durable lease token from %s to %s", admitted.Token, storedToken)
		}
		if err := leases.Complete(f.ctx, f.db.Pool, f.clock, admitted, "SUCCESS"); err != nil {
			t.Fatal(err)
		}
		for _, message := range reclaimed {
			if err := consumer.Handle(f.ctx, "dispatch-owner-b", message); err != nil {
				t.Fatalf("ack duplicate after durable completion: %v", err)
			}
		}
		pending, err := f.redis.Client.XPending(f.ctx, f.streamKey()+"-lease", "dispatch-test-group-lease").Result()
		if err != nil || pending.Count != 0 {
			t.Fatalf("pending entries after durable completion = (%d,%v), want zero", pending.Count, err)
		}
	})

	t.Run("unknown_job_entry_remains_pending", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		streams := f.newStreams(t, "unknown")
		if err := streams.EnsureGroup(f.ctx); err != nil {
			t.Fatal(err)
		}
		missingID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		if _, err := streams.Publish(f.ctx, queue.DispatchKind, missingID); err != nil {
			t.Fatal(err)
		}
		messages, err := streams.Read(f.ctx, "dispatch-unknown", time.Millisecond, 1)
		if err != nil || len(messages) != 1 {
			t.Fatalf("read unknown job entry = (%d,%v)", len(messages), err)
		}
		consumer, err := queue.NewConsumer(f.db.Pool, f.clock, streams, queue.ConsumerConfig{
			LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: dispatchTestPromptHash,
			SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test",
			BatchSize: 1, Block: time.Millisecond, OperationTimeout: 3 * time.Second,
		}, func(context.Context, leases.Lease) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := consumer.Handle(f.ctx, "dispatch-unknown", messages[0]); !errors.Is(err, queue.ErrNotDisposed) {
			t.Fatalf("unknown job was acknowledged: %v", err)
		}
		pending, err := f.redis.Client.XPending(f.ctx, f.streamKey()+"-unknown", "dispatch-test-group-unknown").Result()
		if err != nil || pending.Count != 1 {
			t.Fatalf("unknown job pending count = (%d,%v), want one", pending.Count, err)
		}
	})

	t.Run("published_retry_hint_claims_fresh_run", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		streams := f.newStreams(t, "retry")
		jobID := f.addJob(t, "ci_reconcile")
		d := dispatchTestNewDispatcher(t, f, streams)
		if _, err := streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
			t.Fatal(err)
		}
		if err := streams.EnsureGroup(f.ctx); err != nil {
			t.Fatal(err)
		}
		messages, err := streams.Read(f.ctx, "dispatch-retry", time.Millisecond, 1)
		if err != nil || len(messages) != 1 {
			t.Fatalf("read initial retry-test delivery = (%d,%v)", len(messages), err)
		}
		executions := 0
		consumer, err := queue.NewConsumer(f.db.Pool, f.clock, streams, queue.ConsumerConfig{
			LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: dispatchTestPromptHash,
			SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test",
			BatchSize: 1, Block: time.Millisecond, OperationTimeout: 3 * time.Second,
		}, func(ctx context.Context, lease leases.Lease) error {
			executions++
			if lease.JobID != jobID || lease.RunAttempt != int32(executions) {
				return fmt.Errorf("lease identity/attempt = %s/%d, want %s/%d", lease.JobID, lease.RunAttempt, jobID, executions)
			}
			if executions == 1 {
				return leases.Retry(ctx, f.db.Pool, f.clock, lease, f.clock.Now().Add(time.Minute), "FAILED")
			}
			return leases.Complete(ctx, f.db.Pool, f.clock, lease, "SUCCESS")
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := consumer.Handle(f.ctx, "dispatch-retry", messages[0]); err != nil {
			t.Fatalf("durably defer first attempt: %v", err)
		}
		if count, err := d.DispatchDue(f.ctx, 1); err != nil || count != 0 {
			t.Fatalf("future retry outbox dispatched early: count=%d err=%v", count, err)
		}
		if _, err := streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
			t.Fatal(err)
		}
		early, err := streams.Read(f.ctx, "dispatch-retry", time.Millisecond, 1)
		if err != nil || len(early) != 1 {
			t.Fatalf("read early retry duplicate = (%d,%v)", len(early), err)
		}
		if err := consumer.Handle(f.ctx, "dispatch-retry", early[0]); !errors.Is(err, leases.ErrNotDue) {
			t.Fatalf("early queue hint bypassed Postgres retry deadline: %v", err)
		}
		f.clock.Advance(time.Minute)
		if count, err := d.DispatchDue(f.ctx, 1); err != nil || count != 1 {
			t.Fatalf("publish due retry = (%d,%v), want one", count, err)
		}
		fresh, err := streams.Read(f.ctx, "dispatch-retry", time.Millisecond, 1)
		if err != nil || len(fresh) != 1 {
			t.Fatalf("read marker-less fresh retry = (%d,%v)", len(fresh), err)
		}
		if fresh[0].JobID != jobID {
			t.Fatalf("retry logical job identity = %s, want stable %s", fresh[0].JobID, jobID)
		}
		if err := consumer.Handle(f.ctx, "dispatch-retry", fresh[0]); err != nil {
			t.Fatalf("execute due retry hint: %v", err)
		}
		pending, err := streams.ReadPending(f.ctx, "dispatch-retry", "0", 1)
		if err != nil || len(pending) != 1 {
			t.Fatalf("read early retry redelivery = (%d,%v)", len(pending), err)
		}
		if err := consumer.Handle(f.ctx, "dispatch-retry", pending[0]); err != nil {
			t.Fatalf("ack old duplicate after durable retry completion: %v", err)
		}
		if executions != 2 {
			t.Fatalf("executor calls = %d, want fresh second run", executions)
		}
		var status string
		var runCount int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM jobs WHERE id=$1::uuid`, jobID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM agent_runs WHERE job_id=$1::uuid`, jobID).Scan(&runCount); err != nil {
			t.Fatal(err)
		}
		if status != "COMPLETED" || runCount != 2 {
			t.Fatalf("retried job status/run count = %s/%d, want COMPLETED/2", status, runCount)
		}
	})

	t.Run("inference_fails_closed_without_durable_reservation", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		streams := f.newStreams(t, "budget")
		jobID := f.addJob(t, "author")
		if _, err := streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
			t.Fatal(err)
		}
		if err := streams.EnsureGroup(f.ctx); err != nil {
			t.Fatal(err)
		}
		messages, err := streams.Read(f.ctx, "dispatch-budget", time.Millisecond, 1)
		if err != nil || len(messages) != 1 {
			t.Fatalf("read inference entry = (%d,%v)", len(messages), err)
		}
		executed := false
		consumer, err := queue.NewConsumer(f.db.Pool, f.clock, streams, queue.ConsumerConfig{
			LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: dispatchTestPromptHash,
			SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test",
			BatchSize: 1, Block: time.Millisecond, OperationTimeout: 3 * time.Second,
		}, func(context.Context, leases.Lease) error { executed = true; return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := consumer.Handle(f.ctx, "dispatch-budget", messages[0]); !errors.Is(err, queue.ErrInferenceAdmissionRequired) {
			t.Fatalf("unbudgeted inference admission = %v, want fail-closed error", err)
		}
		var status string
		var runs int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM jobs WHERE id=$1::uuid`, jobID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM agent_runs WHERE job_id=$1::uuid`, jobID).Scan(&runs); err != nil {
			t.Fatal(err)
		}
		if status != "PENDING" || runs != 0 || executed {
			t.Fatalf("unbudgeted job state=(%s,runs=%d,executed=%t)", status, runs, executed)
		}

		proofStreams := f.newStreams(t, "budget-proof")
		proofJobID := f.addJob(t, "author")
		if _, err := proofStreams.Publish(f.ctx, queue.DispatchKind, proofJobID); err != nil {
			t.Fatal(err)
		}
		if err := proofStreams.EnsureGroup(f.ctx); err != nil {
			t.Fatal(err)
		}
		proofMessages, err := proofStreams.Read(f.ctx, "dispatch-budget-proof", time.Millisecond, 1)
		if err != nil || len(proofMessages) != 1 {
			t.Fatalf("read reservation-proof entry = (%d,%v)", len(proofMessages), err)
		}
		proofExecuted := false
		proofConsumer, err := queue.NewConsumer(f.db.Pool, f.clock, proofStreams, queue.ConsumerConfig{
			LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: dispatchTestPromptHash,
			SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test",
			BatchSize: 1, Block: time.Millisecond, OperationTimeout: 3 * time.Second,
			BudgetAdmission: func(context.Context, leases.Lease, contracts.Job) error { return nil },
		}, func(context.Context, leases.Lease) error { proofExecuted = true; return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := proofConsumer.Handle(f.ctx, "dispatch-budget-proof", proofMessages[0]); !errors.Is(err, queue.ErrReservationMissing) {
			t.Fatalf("adapter without durable reservation = %v, want fail closed", err)
		}
		var proofStatus, runStatus string
		var proofRuns int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM jobs WHERE id=$1::uuid`, proofJobID).Scan(&proofStatus); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*),min(execution_status) FROM agent_runs WHERE job_id=$1::uuid`, proofJobID).Scan(&proofRuns, &runStatus); err != nil {
			t.Fatal(err)
		}
		if proofStatus != "PENDING" || proofRuns != 1 || runStatus != "TERMINATED" || proofExecuted {
			t.Fatalf("missing reservation did not fence the run and preserve retry: job=%s runs=%d run=%s executed=%t", proofStatus, proofRuns, runStatus, proofExecuted)
		}
	})

	t.Run("entry_errors_do_not_starve_unrelated_work", func(t *testing.T) {
		for _, kind := range []string{"unknown_job", "early_retry", "missing_admission"} {
			t.Run(kind, func(t *testing.T) {
				f := newDispatchTestFixture(t)
				streams := f.newStreams(t, "run-"+kind)
				poisonID := "11111111-1111-4111-8111-111111111111"
				if kind == "missing_admission" {
					poisonID = f.addJob(t, "author")
				}
				if kind == "early_retry" {
					poisonID = f.addJob(t, "reply")
					lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: f.taskID, JobID: poisonID, TTL: time.Minute, AgentType: "C", PromptHash: dispatchTestPromptHash, SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test"})
					if err != nil {
						t.Fatal(err)
					}
					if err := leases.Retry(f.ctx, f.db.Pool, f.clock, lease, f.clock.Now().Add(time.Minute), "SUCCESS"); err != nil {
						t.Fatal(err)
					}
				}
				entryID, err := streams.Publish(f.ctx, queue.DispatchKind, poisonID)
				if err != nil {
					t.Fatal(err)
				}
				validID := f.addJob(t, "reply")
				if _, err := streams.Publish(f.ctx, queue.DispatchKind, validID); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(f.ctx)
				defer cancel()
				executed := make(chan string, 1)
				consumer, err := queue.NewConsumer(f.db.Pool, f.clock, streams, queue.ConsumerConfig{LeaseTTL: time.Minute, ReclaimIdle: time.Hour, PromptHash: dispatchTestPromptHash, SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test", BatchSize: 2, Block: time.Millisecond, OperationTimeout: 3 * time.Second}, func(ctx context.Context, lease leases.Lease) error {
					if err := leases.Complete(ctx, f.db.Pool, f.clock, lease, "SUCCESS"); err != nil {
						return err
					}
					executed <- lease.JobID
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- consumer.Run(ctx, "dispatch-run") }()
				select {
				case got := <-executed:
					if got != validID {
						t.Fatalf("executed %s instead of valid job %s", got, validID)
					}
				case err := <-done:
					t.Fatalf("entry %s stopped consumer before unrelated work: %v", kind, err)
				case <-f.ctx.Done():
					t.Fatal("unrelated job starved")
				}
				cancel()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("consumer shutdown timed out")
				}
				pending, err := f.redis.Client.XPendingExt(f.ctx, &redis.XPendingExtArgs{Stream: f.streamKey() + "-run-" + kind, Group: "dispatch-test-group-run-" + kind, Start: "-", End: "+", Count: 10}).Result()
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, entry := range pending {
					if entry.ID == entryID {
						found = true
					}
				}
				if !found {
					t.Fatal("undisposed poison/delayed hint was acknowledged")
				}
			})
		}
	})

	t.Run("admission_failure_retries_without_losing_job", func(t *testing.T) {
		interrupted := errors.New("admission response interrupted")
		for _, scenario := range []string{"emergency", "missing_reservation", "reserved_then_error", "context_cancelled"} {
			t.Run(scenario, func(t *testing.T) {
				f := newDispatchTestFixture(t)
				handleCtx, cancel := context.WithCancel(f.ctx)
				t.Cleanup(cancel)
				jobID := f.addJob(t, "author")
				if err := f.streams.EnsureGroup(f.ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := f.streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
					t.Fatal(err)
				}
				messages, err := f.streams.Read(f.ctx, "admission-retry", time.Millisecond, 1)
				if err != nil || len(messages) != 1 {
					t.Fatalf("read delivery = (%v,%v)", messages, err)
				}
				if scenario == "emergency" {
					if _, err := f.db.Pool.Exec(f.ctx, `UPDATE org_budgets SET emergency_mode=true`); err != nil {
						t.Fatal(err)
					}
				}
				envelope := contracts.BudgetEnvelope{MaxCostMicroUSD: 1000, MaxInputTokens: 10, MaxOutputTokens: 10, MaxCalls: 1, PricingVersion: "test-v1"}
				admissions, executions := 0, 0
				cfg := queue.ConsumerConfig{LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: dispatchTestPromptHash, SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test", BatchSize: 1, Block: time.Millisecond, OperationTimeout: 3 * time.Second}
				var reservationID string
				pendingCount := func() int64 {
					t.Helper()
					value, err := f.redis.Client.XPending(f.ctx, f.streamKey(), "dispatch-test-group").Result()
					if err != nil {
						t.Fatal(err)
					}
					return value.Count
				}
				cfg.BudgetAdmission = func(ctx context.Context, lease leases.Lease, _ contracts.Job) error {
					admissions++
					if admissions == 1 && scenario == "context_cancelled" {
						cancel()
						return ctx.Err()
					}
					if admissions == 1 && scenario == "missing_reservation" {
						return nil
					}
					reservation, err := budget.Reserve(ctx, f.db.Pool, f.clock, lease, envelope)
					if err != nil {
						return err
					}
					reservationID = reservation.ID
					if admissions == 1 && scenario == "reserved_then_error" {
						return interrupted
					}
					return nil
				}
				consumer, err := queue.NewConsumer(f.db.Pool, f.clock, f.streams, cfg, func(ctx context.Context, lease leases.Lease) error {
					executions++
					if err := leases.Complete(ctx, f.db.Pool, f.clock, lease, "SUCCESS"); err != nil {
						return err
					}
					return budget.SettleReservation(ctx, f.db.Pool, f.clock, budget.SettlementRequest{ReservationID: reservationID, RunID: lease.RunID, FinalUsageKnown: true})
				})
				if err != nil {
					t.Fatal(err)
				}
				wantErr := interrupted
				switch scenario {
				case "emergency":
					wantErr = budget.ErrEmergency
				case "missing_reservation":
					wantErr = queue.ErrReservationMissing
				case "context_cancelled":
					wantErr = context.Canceled
				}
				if err := consumer.Handle(handleCtx, "admission-retry", messages[0]); !errors.Is(err, wantErr) {
					t.Fatalf("first admission error = %v, want %v", err, wantErr)
				}
				var jobStatus, runID, runStatus string
				var reserved int64
				if err := f.db.Pool.QueryRow(f.ctx, `SELECT j.status,r.id::text,r.execution_status,t.reserved_micro_usd FROM jobs j JOIN agent_runs r ON r.job_id=j.id JOIN tasks t ON t.id=j.task_id WHERE j.id=$1::uuid`, jobID).Scan(&jobStatus, &runID, &runStatus, &reserved); err != nil {
					t.Fatal(err)
				}
				if jobStatus != "PENDING" || runStatus != "TERMINATED" || executions != 0 || reserved != 0 || pendingCount() != 1 {
					t.Fatalf("unstarted failed admission lost work: job=%s run=%s executions=%d reserved=%d", jobStatus, runStatus, executions, reserved)
				}
				var due time.Time
				if err := f.db.Pool.QueryRow(f.ctx, `SELECT next_attempt_at FROM outbox WHERE job_id=$1::uuid AND kind='DISPATCH' AND payload->>'retry_from_run_id'=$2`, jobID, runID).Scan(&due); err != nil || !due.After(f.clock.Now()) {
					t.Fatalf("durable future retry missing: due=%s err=%v", due, err)
				}
				if err := consumer.Handle(f.ctx, "admission-retry", messages[0]); !errors.Is(err, leases.ErrNotDue) {
					t.Fatalf("early retry = %v, want ErrNotDue", err)
				}
				if admissions != 1 || executions != 0 {
					t.Fatal("early transport redelivery bypassed durable retry time")
				}
				if _, err := f.db.Pool.Exec(f.ctx, `UPDATE org_budgets SET emergency_mode=false`); err != nil {
					t.Fatal(err)
				}
				f.clock.Set(due)
				if err := consumer.Handle(f.ctx, "admission-retry", messages[0]); err != nil {
					t.Fatalf("recovered admission retry: %v", err)
				}
				if executions != 1 || admissions != 2 || pendingCount() != 0 {
					t.Fatalf("recovered execution/ack = %d/%d/%d", executions, admissions, pendingCount())
				}
			})
		}
	})

	t.Run("slow_admission_cannot_handoff_expired_lease", func(t *testing.T) {
		for _, charged := range []bool{false, true} {
			t.Run(fmt.Sprintf("metered_%t", charged), func(t *testing.T) {
				f := newDispatchTestFixture(t)
				streams := f.newStreams(t, "slow-admission")
				jobID := f.addJob(t, "author")
				if _, err := streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
					t.Fatal(err)
				}
				if err := streams.EnsureGroup(f.ctx); err != nil {
					t.Fatal(err)
				}
				messages, err := streams.Read(f.ctx, "dispatch-slow", time.Millisecond, 1)
				if err != nil || len(messages) != 1 {
					t.Fatalf("read slow entry: %v", err)
				}
				executed := false
				envelope := contracts.BudgetEnvelope{MaxCostMicroUSD: 1000, MaxInputTokens: 10, MaxOutputTokens: 10, MaxCalls: 1, PricingVersion: "test-v1"}
				consumer, err := queue.NewConsumer(f.db.Pool, f.clock, streams, queue.ConsumerConfig{LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: dispatchTestPromptHash, SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test", BatchSize: 1, Block: time.Millisecond, OperationTimeout: 3 * time.Second, BudgetAdmission: func(ctx context.Context, lease leases.Lease, _ contracts.Job) error {
					reservation, err := budget.Reserve(ctx, f.db.Pool, f.clock, lease, envelope)
					if err != nil {
						return err
					}
					if charged {
						if _, err := budget.RecordCharge(ctx, f.db.Pool, f.clock, budget.ChargeRequest{RequestID: "slow-test", ReservationID: reservation.ID, RunID: lease.RunID, Model: "test", Usage: json.RawMessage(`{}`), CostMicroUSD: 50}); err != nil {
							return err
						}
					}
					f.clock.Advance(time.Minute)
					return nil
				}}, func(context.Context, leases.Lease) error { executed = true; return nil })
				if err != nil {
					t.Fatal(err)
				}
				if err := consumer.Handle(f.ctx, "dispatch-slow", messages[0]); !errors.Is(err, leases.ErrStale) || executed {
					t.Fatalf("expired handoff: err=%v executed=%v", err, executed)
				}
				var runStatus, reservationStatus, jobStatus string
				var remaining, reserved int64
				if err := f.db.Pool.QueryRow(f.ctx, `SELECT r.execution_status,b.status,b.remaining_micro_usd,t.reserved_micro_usd,j.status FROM agent_runs r JOIN budget_reservations b ON b.run_id=r.id JOIN tasks t ON t.id=r.task_id JOIN jobs j ON j.id=r.job_id WHERE r.job_id=$1::uuid`, jobID).Scan(&runStatus, &reservationStatus, &remaining, &reserved, &jobStatus); err != nil {
					t.Fatal(err)
				}
				wantStatus, wantRemaining := "SETTLED", int64(0)
				if charged {
					wantStatus, wantRemaining = "UNKNOWN", 950
				}
				if runStatus != "TERMINATED" || reservationStatus != wantStatus || remaining != wantRemaining || reserved != wantRemaining || jobStatus != "PENDING" {
					t.Fatalf("unstarted cleanup run=%s reservation=%s remaining=%d reserved=%d job=%s", runStatus, reservationStatus, remaining, reserved, jobStatus)
				}
				var retry bool
				if err := f.db.Pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM outbox o JOIN agent_runs r ON r.id::text=o.payload->>'retry_from_run_id' WHERE o.job_id=$1::uuid AND o.kind='DISPATCH' AND o.next_attempt_at>$2)`, jobID, f.clock.Now()).Scan(&retry); err != nil || !retry {
					t.Fatalf("missing durable retry: %v", err)
				}
			})
		}
	})

	t.Run("blocked_read_honors_context_cancellation", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		options := *f.redis.Client.Options()
		options.ContextTimeoutEnabled = true
		client := redis.NewClient(&options)
		hook := &dispatchTestBlockingReadHook{started: make(chan struct{})}
		client.AddHook(hook)
		streams, err := queue.NewStreams(client, f.streamKey()+"-cancel", "dispatch-test-group-cancel")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := streams.Close(); err != nil {
				t.Errorf("close cancellation-probe streams: %v", err)
			}
		})
		if err := streams.EnsureGroup(f.ctx); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(f.ctx)
		result := make(chan error, 1)
		go func() {
			_, err := streams.Read(ctx, "dispatch-cancel", 30*time.Second, 1)
			result <- err
		}()
		<-hook.started
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("blocked read after cancel = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("blocked Redis read did not return after context cancellation")
		}
	})
}

func newDispatchTestFixture(t *testing.T) *dispatchTestFixture {
	t.Helper()
	db, redisFixture := testutil.RequireServices(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate dispatch fixture: %v", err)
	}
	manual := clock.NewManual(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES('dispatch-test-org')`); err != nil {
		t.Fatal(err)
	}
	var taskID string
	if err := db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,policy_version) VALUES('dispatch-test-org','owner/repo','dispatch-source','owner','v1') RETURNING id::text`).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	var prID int64
	if err := db.Pool.QueryRow(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha) VALUES($1,'owner/repo',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','main','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') RETURNING id`, taskID).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	clientOptions := *redisFixture.Client.Options()
	clientOptions.ContextTimeoutEnabled = true
	client := redis.NewClient(&clientOptions)
	streams, err := queue.NewStreams(client, redisFixture.KeyPrefix+"dispatch", "dispatch-test-group")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := streams.Close(); err != nil {
			t.Errorf("close dispatch streams: %v", err)
		}
	})
	return &dispatchTestFixture{db: db, redis: redisFixture, ctx: ctx, clock: manual, streams: streams, taskID: taskID, prID: prID}
}

func (f *dispatchTestFixture) addJob(t *testing.T, operation string) string {
	t.Helper()
	var jobID, operationID, correlationID string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT gen_random_uuid()::text,gen_random_uuid()::text,gen_random_uuid()::text`).Scan(&jobID, &operationID, &correlationID); err != nil {
		t.Fatal(err)
	}
	job := contracts.Job{Version: 1, TaskID: f.taskID, JobID: jobID, Generation: 0,
		Snapshot: contracts.Snapshot{HeadSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BaseSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		Attempt:  1, OperationID: operationID, CorrelationID: correlationID, Operation: operation}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "budget_envelope")
	payload, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO jobs(id,task_id,pr_id,logical_key,operation_type,generation,expected_head_sha,expected_base_sha,remediation_attempt,payload)
		VALUES($1::uuid,$2::uuid,$3,$4,$5,0,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',1,$6::jsonb)`, jobID, f.taskID, f.prID, "dispatch:"+jobID, operation, payload); err != nil {
		t.Fatal(err)
	}
	return jobID
}

func (f *dispatchTestFixture) addOutbox(t *testing.T, jobID, kind, payload string, due time.Time) string {
	t.Helper()
	var id string
	if err := f.db.Pool.QueryRow(f.ctx, `INSERT INTO outbox(task_id,job_id,kind,payload,next_attempt_at) VALUES($1::uuid,$2::uuid,$3,$4::jsonb,$5) RETURNING id::text`, f.taskID, jobID, kind, payload, due).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func (f *dispatchTestFixture) streamKey() string { return f.redis.KeyPrefix + "dispatch" }

func (f *dispatchTestFixture) newStreams(t *testing.T, suffix string) *queue.Streams {
	t.Helper()
	key, group := f.streamKey()+"-"+suffix, "dispatch-test-group-"+suffix
	clientOptions := *f.redis.Client.Options()
	clientOptions.ContextTimeoutEnabled = true
	client := redis.NewClient(&clientOptions)
	streams, err := queue.NewStreams(client, key, group)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := streams.Close(); err != nil {
			t.Errorf("close %s dispatch streams: %v", suffix, err)
		}
	})
	return streams
}

func dispatchTestNewDispatcher(t *testing.T, f *dispatchTestFixture, streams *queue.Streams) *dispatch.Dispatcher {
	t.Helper()
	d, err := dispatch.NewDispatcher(f.db.Pool, f.clock, streams, nil)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

type dispatchTestBlockingReadHook struct {
	started chan struct{}
}

func (h *dispatchTestBlockingReadHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *dispatchTestBlockingReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		if command.Name() == "xreadgroup" {
			close(h.started)
			<-ctx.Done()
			return ctx.Err()
		}
		return next(ctx, command)
	}
}

func (h *dispatchTestBlockingReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
