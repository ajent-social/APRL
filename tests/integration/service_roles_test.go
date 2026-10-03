package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/app"
	"github.com/ajent-social/APRL/internal/broker"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/dispatch"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/ajent-social/APRL/internal/reconcile"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/supervisor"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

func TestServiceRoles(t *testing.T) {
	t.Run("startup_recovery_failure_keeps_mutations_and_groups_unstarted", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		jobID := f.addJob(t, "author")
		outboxID := f.addOutbox(t, jobID, queue.DispatchKind, `{"job_id":"`+jobID+`"}`, f.clock.Now())
		if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO webhook_deliveries(delivery_id,event_type,payload,disposition) VALUES('service-recovery-failure','issues','{}'::jsonb,'INBOX')`); err != nil {
			t.Fatalf("seed durable inbox delivery: %v", err)
		}
		recovery := &serviceRoleRecovery{recoverErr: errors.New("inventory denied")}
		service := serviceRoleNewControl(t, f, recovery, nil)
		ctx, cancel := context.WithCancel(context.Background())
		done := serviceRoleRun(ctx, t, service)
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("control Run returned nil after startup recovery failure")
			}
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("control Run did not fail closed after startup recovery failure")
		}
		cancel()

		serviceRoleRequireUnchangedWork(t, f, jobID, outboxID, "service-recovery-failure")
		if length, err := f.redis.Client.XLen(f.ctx, f.streamKey()).Result(); err != nil || length != 0 {
			t.Fatalf("queue stream after failed startup recovery=(%d,%v), want no stream writes", length, err)
		}
		if exists, err := f.redis.Client.Exists(f.ctx, f.streamKey()).Result(); err != nil || exists != 0 {
			t.Fatalf("startup recovery failure created Redis stream/group: exists=%d err=%v", exists, err)
		}
	})

	t.Run("control_rechecks_inventory_before_inbox_routing_and_dispatch", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		jobID := f.addJob(t, "author")
		outboxID := f.addOutbox(t, jobID, queue.DispatchKind, `{"job_id":"`+jobID+`"}`, f.clock.Now())
		if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO webhook_deliveries(delivery_id,event_type,payload,disposition) VALUES('service-stale-inventory','issues','{}'::jsonb,'INBOX')`); err != nil {
			t.Fatalf("seed durable inbox delivery: %v", err)
		}
		recovery := &serviceRoleRecovery{unavailableFromReadyCall: 2}
		service := serviceRoleNewControl(t, f, recovery, nil)
		ctx, cancel := context.WithCancel(context.Background())
		done := serviceRoleRun(ctx, t, service)
		serviceRoleWaitReadyCall(t, recovery, 2)
		serviceRoleRequireUnchangedWork(t, f, jobID, outboxID, "service-stale-inventory")
		if length, err := f.redis.Client.XLen(f.ctx, f.streamKey()).Result(); err != nil || length != 0 {
			t.Fatalf("queue stream while inventory stale=(%d,%v), want no dispatch", length, err)
		}
		cancel()
		serviceRoleStop(t, service, done)
	})

	t.Run("control_publishes_hints_without_consuming_or_closing_callers", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		jobID := f.addJob(t, "author")
		outboxID := f.addOutbox(t, jobID, queue.DispatchKind, `{"job_id":"`+jobID+`"}`, f.clock.Now())
		const deliveryID = "service-successful-control-route"
		payload := `{"action":"labeled","repository":{"full_name":"owner/repo"},"sender":{"id":17,"type":"User"},"issue":{"id":1001,"number":31,"user":{"id":44,"type":"User"}},"label":{"name":"aprl:implement"}}`
		if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO webhook_deliveries(delivery_id,event_type,payload,disposition,received_at) VALUES($1,'issues',$2::jsonb,'INBOX',$3)`, deliveryID, payload, f.clock.Now()); err != nil {
			t.Fatalf("seed routable issue delivery: %v", err)
		}
		service := serviceRoleNewControl(t, f, &serviceRoleRecovery{}, nil)
		serviceRoleCheckHealthRoutes(t, service)
		ctx, cancel := context.WithCancel(context.Background())
		done := serviceRoleRun(ctx, t, service)
		stopped := false
		defer func() {
			if !stopped {
				cancel()
				serviceRoleStopAllowError(t, service, done)
			}
		}()
		serviceRoleWaitUntilDiagnostic(t, "control routes inbox and publishes due hint", func() (bool, error) {
			published, err := f.redis.Client.XLen(f.ctx, f.streamKey()).Result()
			if err != nil {
				return false, err
			}
			var disposition string
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT disposition FROM webhook_deliveries WHERE delivery_id=$1`, deliveryID).Scan(&disposition); err != nil {
				return false, err
			}
			return published >= 1 && disposition == "PROCESSED", nil
		}, func() string {
			var disposition string
			_ = f.db.Pool.QueryRow(f.ctx, `SELECT disposition FROM webhook_deliveries WHERE delivery_id=$1`, deliveryID).Scan(&disposition)
			var routedTasks, outboxes int
			_ = f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM tasks WHERE repo_full_name='owner/repo' AND source_key='issue:31'`).Scan(&routedTasks)
			_ = f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid`, f.taskID).Scan(&outboxes)
			length, lengthErr := f.redis.Client.XLen(f.ctx, f.streamKey()).Result()
			runState := "running"
			select {
			case runErr := <-done:
				runState = fmt.Sprintf("exited: %v", runErr)
				done <- runErr
			default:
			}
			var attempts int32
			var published *time.Time
			_ = f.db.Pool.QueryRow(f.ctx, `SELECT attempt_count,published_at FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&attempts, &published)
			return fmt.Sprintf("run=%s inbox=%s routedTasks=%d taskOutboxes=%d streamLen=%d streamErr=%v initialOutbox=(attempts=%d published=%v)", runState, disposition, routedTasks, outboxes, length, lengthErr, attempts, published)
		})
		var routedTasks int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM tasks WHERE repo_full_name='owner/repo' AND source_key='issue:31' AND state='AUTHORING'`).Scan(&routedTasks); err != nil {
			t.Fatal(err)
		}
		if routedTasks != 1 {
			t.Fatalf("control role routed inbox into %d durable tasks, want one", routedTasks)
		}
		var jobStatus string
		var leaseToken *string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&jobStatus, &leaseToken); err != nil {
			t.Fatalf("read job after control publication: %v", err)
		}
		var runs int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM agent_runs WHERE job_id=$1::uuid`, jobID).Scan(&runs); err != nil {
			t.Fatalf("count inference runs after control publication: %v", err)
		}
		var publishedAt *time.Time
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&publishedAt); err != nil {
			t.Fatalf("read control outbox acknowledgement: %v", err)
		}
		if jobStatus != "PENDING" || leaseToken != nil || runs != 0 || publishedAt == nil {
			t.Fatalf("control role crossed the consumer boundary: job=%s lease=%v runs=%d published=%v", jobStatus, leaseToken, runs, publishedAt)
		}
		pending, err := f.redis.Client.XPending(f.ctx, f.streamKey(), "dispatch-test-group").Result()
		if err != nil || pending.Count != 0 {
			t.Fatalf("control role consumed its own dispatch hint: pending=%d err=%v", pending.Count, err)
		}
		cancel()
		serviceRoleStop(t, service, done)
		stopped = true
		if err := f.db.Pool.Ping(f.ctx); err != nil {
			t.Fatalf("control shutdown closed caller-owned PostgreSQL pool: %v", err)
		}
		if err := f.redis.Client.Ping(f.ctx).Err(); err != nil {
			t.Fatalf("control shutdown closed caller-owned Redis client: %v", err)
		}
	})

	t.Run("control_rechecks_inventory_between_rows_in_one_dispatch_batch", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		firstJob := f.addJob(t, "author")
		secondJob := f.addJob(t, "author")
		firstOutbox := f.addOutbox(t, firstJob, "NOTIFY", `{"job_id":"`+firstJob+`"}`, f.clock.Now())
		secondOutbox := f.addOutbox(t, secondJob, "NOTIFY", `{"job_id":"`+secondJob+`"}`, f.clock.Now())
		recovery := &serviceRoleRecovery{}
		var callbackCalls atomic.Int32
		ack := func(context.Context, dispatch.OutboxItem) error {
			if callbackCalls.Add(1) == 1 {
				recovery.forceUnavailable.Store(true)
			}
			return nil
		}
		service := serviceRoleNewControlBatch(t, f, recovery, ack, 2)
		server := httptest.NewServer(service.Handler())
		t.Cleanup(server.Close)
		ctx, cancel := context.WithCancel(context.Background())
		done := serviceRoleRun(ctx, t, service)
		stopped := false
		defer func() {
			if !stopped {
				cancel()
				serviceRoleStopAllowError(t, service, done)
			}
		}()
		serviceRoleWaitUntil(t, "first outbox callback", func() (bool, error) {
			return callbackCalls.Load() == 1, nil
		})
		serviceRoleWaitUntil(t, "control observes revoked inventory", func() (bool, error) {
			return recovery.readyCalls.Load() >= 2, nil
		})
		var secondAttempts int32
		var secondPublished *time.Time
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT attempt_count,published_at FROM outbox WHERE id=$1::uuid`, secondOutbox).Scan(&secondAttempts, &secondPublished); err != nil {
			t.Fatal(err)
		}
		if callbackCalls.Load() != 1 || secondAttempts != 0 || secondPublished != nil {
			t.Fatalf("second row crossed revoked inventory fence: callbackCalls=%d attempts=%d published=%v", callbackCalls.Load(), secondAttempts, secondPublished)
		}
		serviceRoleRequireStatus(t, server.Client(), server.URL+"/readyz", http.StatusServiceUnavailable, "not_ready", []string{"startup_recovery"})
		recovery.forceUnavailable.Store(false)
		serviceRoleWaitUntil(t, "second outbox callback after inventory recovery", func() (bool, error) {
			return callbackCalls.Load() == 2, nil
		})
		var firstPublished, secondPublishedAfter *time.Time
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, firstOutbox).Scan(&firstPublished); err != nil {
			t.Fatal(err)
		}
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, secondOutbox).Scan(&secondPublishedAfter); err != nil {
			t.Fatal(err)
		}
		if firstPublished == nil || secondPublishedAfter == nil {
			t.Fatalf("outbox completion after restored inventory: first=%v second=%v", firstPublished, secondPublishedAfter)
		}
		serviceRoleWaitUntil(t, "control readiness after loop recovery", func() (bool, error) {
			response, err := server.Client().Get(server.URL + "/readyz")
			if err != nil {
				return false, err
			}
			bodyRaw, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				return false, fmt.Errorf("read/close readyz response: read=%v close=%v", readErr, closeErr)
			}
			var body struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal(bodyRaw, &body); err != nil {
				return false, err
			}
			return response.StatusCode == http.StatusOK && body.Status == "ready", nil
		})
		cancel()
		serviceRoleStop(t, service, done)
		stopped = true
	})

	t.Run("running_control_keeps_liveness_up_during_redis_readiness_loss", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		probeRedis, probeClosed := serviceRoleRedisProbeClient(t, f)
		recovery := &serviceRoleRecovery{}
		service := serviceRoleNewControlWithRedis(t, f, recovery, nil, probeRedis, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := serviceRoleRun(ctx, t, service)
		stopped := false
		defer func() {
			if !stopped {
				cancel()
				serviceRoleStopAllowError(t, service, done)
			}
		}()
		serviceRoleWaitForGroup(t, f)
		if err := probeRedis.Close(); err != nil {
			t.Fatalf("close isolated Redis readiness client: %v", err)
		}
		probeClosed.Store(true)
		jobID := f.addJob(t, "author")
		outboxID := f.addOutbox(t, jobID, queue.DispatchKind, `{"job_id":"`+jobID+`"}`, f.clock.Now())
		server := httptest.NewServer(service.Handler())
		t.Cleanup(server.Close)
		serviceRoleRequireStatus(t, server.Client(), server.URL+"/healthz", http.StatusOK, "alive", nil)
		serviceRoleRequireStatus(t, server.Client(), server.URL+"/readyz", http.StatusServiceUnavailable, "not_ready", []string{"redis"})
		readyCallsBeforeOutage := recovery.readyCalls.Load()
		serviceRoleWaitUntil(t, "running control loop rechecks after Redis readiness loss", func() (bool, error) {
			return recovery.readyCalls.Load() > readyCallsBeforeOutage, nil
		})
		select {
		case err := <-done:
			done <- err
			t.Fatalf("control process exited while Redis readiness was unavailable: %v", err)
		default:
		}
		var published *time.Time
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if published != nil {
			t.Fatalf("control dispatched while Redis readiness was unavailable: published_at=%v", published)
		}
		if length, err := f.redis.Client.XLen(f.ctx, f.streamKey()).Result(); err != nil || length != 0 {
			t.Fatalf("control published during Redis readiness loss: streamLen=%d err=%v", length, err)
		}
		cancel()
		serviceRoleStopAllowError(t, service, done)
		stopped = true
	})

	t.Run("cancel_ack_requires_exact_durable_target_and_empty_run_is_ack_only", func(t *testing.T) {
		cases := []struct {
			name      string
			payload   string
			wantCalls int32
			wantAck   bool
		}{
			{name: "empty_run_id_is_ack_only", payload: `{"task_id":"%s","job_id":"%s","run_id":""}`, wantAck: true},
			{name: "matching_run_calls_host_ack", payload: `{"task_id":"%s","job_id":"%s","run_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}`, wantCalls: 1, wantAck: true},
			{name: "missing_run_id_retries", payload: `{"task_id":"%s","job_id":"%s"}`},
			{name: "mismatched_job_retries", payload: `{"task_id":"%s","job_id":"%s","run_id":""}`},
			{name: "unknown_field_retries", payload: `{"task_id":"%s","job_id":"%s","run_id":"","unexpected":true}`},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				f := newDispatchTestFixture(t)
				jobID := f.addJob(t, "author")
				storedTaskID, storedJobID := f.taskID, jobID
				rowJobID := storedJobID
				payloadJobID := storedJobID
				if test.name == "mismatched_job_retries" {
					payloadJobID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
				}
				payload := fmt.Sprintf(test.payload, storedTaskID, payloadJobID)
				outboxID := f.addOutbox(t, rowJobID, "CANCEL", payload, f.clock.Now())
				var ackCalls atomic.Int32
				ack := func(context.Context, dispatch.OutboxItem) error { ackCalls.Add(1); return nil }
				service := serviceRoleNewControl(t, f, &serviceRoleRecovery{}, ack)
				ctx, cancel := context.WithCancel(context.Background())
				done := serviceRoleRun(ctx, t, service)
				serviceRoleWaitUntil(t, "cancel outbox attempt", func() (bool, error) {
					var attempts int32
					var published *time.Time
					if err := f.db.Pool.QueryRow(f.ctx, `SELECT attempt_count,published_at FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&attempts, &published); err != nil {
						return false, err
					}
					return (test.wantAck && published != nil) || (!test.wantAck && attempts > 0), nil
				})
				cancel()
				serviceRoleStopAllowError(t, service, done)
				if got := ackCalls.Load(); got != test.wantCalls {
					t.Fatalf("host cancellation callback calls=%d, want %d", got, test.wantCalls)
				}
				var attempts int32
				var published *time.Time
				if err := f.db.Pool.QueryRow(f.ctx, `SELECT attempt_count,published_at FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&attempts, &published); err != nil {
					t.Fatal(err)
				}
				if test.wantAck && published == nil {
					t.Fatal("valid cancellation was not durably acknowledged")
				}
				if !test.wantAck && (published != nil || attempts == 0) {
					t.Fatalf("invalid cancellation was not retained for retry: attempts=%d published=%v", attempts, published)
				}
			})
		}
	})

	t.Run("worker_rechecks_inventory_before_reclaim_or_new_claim", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		jobID := f.addJob(t, "ci_reconcile")
		if _, err := f.streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
			t.Fatalf("seed inference queue hint: %v", err)
		}
		recovery := &serviceRoleRecovery{unavailableFromReadyCall: 2}
		var runnerStarts atomic.Int32
		deps := serviceRoleWorkerDependencies(t, f, recovery)
		deps.Runner = serviceRoleRunner{starts: &runnerStarts}
		service, err := app.NewWorker(serviceRoleConfig(app.RoleWorker), deps)
		if err != nil {
			t.Fatalf("construct worker service: %v", err)
		}
		serviceRoleCheckHealthRoutes(t, service)
		ctx, cancel := context.WithCancel(context.Background())
		done := serviceRoleRun(ctx, t, service)
		serviceRoleWaitReadyCall(t, recovery, 2)
		var jobStatus string
		var leaseToken *string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&jobStatus, &leaseToken); err != nil {
			t.Fatal(err)
		}
		var runs int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM agent_runs WHERE job_id=$1::uuid`, jobID).Scan(&runs); err != nil {
			t.Fatal(err)
		}
		pending, err := f.redis.Client.XPending(f.ctx, f.streamKey(), "dispatch-test-group").Result()
		if err != nil {
			t.Fatal(err)
		}
		if jobStatus != "PENDING" || leaseToken != nil || runs != 0 || pending.Count != 0 || runnerStarts.Load() != 0 {
			t.Fatalf("worker acted after inventory became unavailable: job=%s lease=%v runs=%d pending=%d runnerStarts=%d", jobStatus, leaseToken, runs, pending.Count, runnerStarts.Load())
		}
		cancel()
		serviceRoleStop(t, service, done)
	})

	t.Run("running_worker_keeps_liveness_up_and_defers_claim_during_redis_readiness_loss", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		probeRedis, probeClosed := serviceRoleRedisProbeClient(t, f)
		recovery := &serviceRoleRecovery{}
		deps := serviceRoleWorkerDependencies(t, f, recovery)
		deps.Redis = probeRedis
		var runnerStarts atomic.Int32
		deps.Runner = serviceRoleRunner{starts: &runnerStarts}
		service, err := app.NewWorker(serviceRoleConfig(app.RoleWorker), deps)
		if err != nil {
			t.Fatalf("construct worker service: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := serviceRoleRun(ctx, t, service)
		stopped := false
		defer func() {
			if !stopped {
				cancel()
				serviceRoleStopAllowError(t, service, done)
			}
		}()
		serviceRoleWaitForGroup(t, f)
		if err := probeRedis.Close(); err != nil {
			t.Fatalf("close isolated Redis readiness client: %v", err)
		}
		probeClosed.Store(true)
		jobID := f.addJob(t, "ci_reconcile")
		if _, err := f.streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
			t.Fatalf("publish worker hint after Redis readiness loss: %v", err)
		}
		server := httptest.NewServer(service.Handler())
		t.Cleanup(server.Close)
		serviceRoleRequireStatus(t, server.Client(), server.URL+"/healthz", http.StatusOK, "alive", nil)
		serviceRoleRequireStatus(t, server.Client(), server.URL+"/readyz", http.StatusServiceUnavailable, "not_ready", []string{"redis"})
		readyCallsBeforeOutage := recovery.readyCalls.Load()
		serviceRoleWaitUntil(t, "running worker rechecks after Redis readiness loss", func() (bool, error) {
			return recovery.readyCalls.Load() > readyCallsBeforeOutage, nil
		})
		select {
		case err := <-done:
			done <- err
			t.Fatalf("worker process exited while Redis readiness was unavailable: %v", err)
		default:
		}
		var jobStatus string
		var leaseToken *string
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&jobStatus, &leaseToken); err != nil {
			t.Fatal(err)
		}
		var runs int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM agent_runs WHERE job_id=$1::uuid`, jobID).Scan(&runs); err != nil {
			t.Fatal(err)
		}
		pending, err := f.redis.Client.XPending(f.ctx, f.streamKey(), "dispatch-test-group").Result()
		if err != nil {
			t.Fatal(err)
		}
		if jobStatus != "PENDING" || leaseToken != nil || runs != 0 || pending.Count != 0 || runnerStarts.Load() != 0 {
			t.Fatalf("worker acted during Redis readiness loss: job=%s lease=%v runs=%d pending=%d starts=%d", jobStatus, leaseToken, runs, pending.Count, runnerStarts.Load())
		}
		cancel()
		serviceRoleStopAllowError(t, service, done)
		stopped = true
	})

	t.Run("worker_budget_gate_disposes_unstarted_claim_and_recovers_for_retry", func(t *testing.T) {
		f := newDispatchTestFixture(t)
		jobID := f.addJob(t, "author")
		deps := serviceRoleWorkerDependencies(t, f, &serviceRoleRecovery{})
		if _, err := f.db.Pool.Exec(f.ctx, `UPDATE jobs SET payload=jsonb_set(payload,'{budget_envelope}',$2::jsonb,true) WHERE id=$1::uuid`, jobID,
			`{"max_cost_micro_usd":1000,"max_input_tokens":10,"max_output_tokens":10,"max_calls":1,"pricing_version":"service-role-test-v1"}`); err != nil {
			t.Fatalf("pin worker fixture budget envelope: %v", err)
		}
		if _, err := f.streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
			t.Fatalf("seed inference queue hint: %v", err)
		}
		recovery := &serviceRoleRecovery{}
		recovery.afterClaimCheck = func(ctx context.Context) (bool, error) {
			var running int
			err := f.db.Pool.QueryRow(ctx, `SELECT count(*) FROM agent_runs WHERE job_id=$1::uuid AND execution_status='RUNNING'`, jobID).Scan(&running)
			return running > 0, err
		}
		recovery.afterClaimEnabled.Store(true)
		deps.StartupRecovery = recovery
		deps.ConsumerConfig.ReclaimIdle = 0
		var runnerStarts atomic.Int32
		deps.Runner = serviceRoleRunner{starts: &runnerStarts}
		service, err := app.NewWorker(serviceRoleConfig(app.RoleWorker), deps)
		if err != nil {
			t.Fatalf("construct worker service: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := serviceRoleRun(ctx, t, service)
		serviceRoleWaitUntil(t, "unstarted admission cleanup", func() (bool, error) {
			var status string
			var leaseToken *string
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&status, &leaseToken); err != nil {
				return false, err
			}
			var runStatus string
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT execution_status FROM agent_runs WHERE job_id=$1::uuid ORDER BY attempt_number DESC LIMIT 1`, jobID).Scan(&runStatus); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return false, nil
				}
				return false, err
			}
			return recovery.forceUnavailable.Load() && status == "PENDING" && leaseToken == nil && runStatus == "TERMINATED", nil
		})
		var reservations int
		if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM budget_reservations WHERE task_id=$1::uuid`, f.taskID).Scan(&reservations); err != nil {
			t.Fatal(err)
		}
		pending, err := f.redis.Client.XPending(f.ctx, f.streamKey(), "dispatch-test-group").Result()
		if err != nil {
			t.Fatal(err)
		}
		if reservations != 0 || pending.Count != 1 || runnerStarts.Load() != 0 {
			t.Fatalf("revoked admission was charged, acknowledged, or started: reservations=%d pending=%d runnerStarts=%d", reservations, pending.Count, runnerStarts.Load())
		}
		select {
		case runErr := <-done:
			cancel()
			t.Fatalf("worker exited instead of retaining retryable work after recovery loss: %v", runErr)
		default:
		}

		recovery.afterClaimEnabled.Store(false)
		recovery.forceUnavailable.Store(false)
		f.clock.Advance(time.Second)
		serviceRoleWaitUntil(t, "worker retries after inventory recovery", func() (bool, error) {
			return runnerStarts.Load() > 0, nil
		})
		cancel()
		serviceRoleStopAllowError(t, service, done)
	})

	t.Run("worker_constructor_rejects_missing_oci_or_provider_probe", func(t *testing.T) {
		for _, missing := range []string{"oci", "provider"} {
			t.Run(missing, func(t *testing.T) {
				f := newDispatchTestFixture(t)
				deps := serviceRoleWorkerDependencies(t, f, &serviceRoleRecovery{})
				if missing == "oci" {
					deps.OCIReadiness = nil
				} else {
					deps.ProviderReadiness = nil
				}
				if _, err := app.NewWorker(serviceRoleConfig(app.RoleWorker), deps); err == nil {
					t.Fatalf("NewWorker accepted missing %s readiness probe", missing)
				}
			})
		}
	})
}

type serviceRoleRecovery struct {
	readyCalls               atomic.Int32
	unavailableFromReadyCall int32
	forceUnavailable         atomic.Bool
	afterClaimEnabled        atomic.Bool
	afterClaimCheck          func(context.Context) (bool, error)
	recoverErr               error
}

func (r *serviceRoleRecovery) Recover(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.recoverErr
}

func (r *serviceRoleRecovery) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	call := r.readyCalls.Add(1)
	if r.afterClaimEnabled.Load() && r.afterClaimCheck != nil {
		claimed, err := r.afterClaimCheck(ctx)
		if err != nil {
			return err
		}
		if claimed {
			r.forceUnavailable.Store(true)
		}
	}
	if r.forceUnavailable.Load() || (r.unavailableFromReadyCall > 0 && call >= r.unavailableFromReadyCall) {
		return errors.New("inventory unavailable")
	}
	return nil
}

type serviceRoleOperations struct{}

func (serviceRoleOperations) Reconcile(context.Context, string) (broker.Operation, error) {
	return broker.Operation{}, nil
}

type serviceRoleLabels struct{}

func (serviceRoleLabels) ApplyLifecycleLabel(context.Context, reconcile.LabelIntent) error {
	return nil
}

type serviceRoleProbe struct{}

func (serviceRoleProbe) Check(ctx context.Context) error { return ctx.Err() }

type serviceRoleRunner struct{ starts *atomic.Int32 }

func (r serviceRoleRunner) Start(context.Context, supervisor.ProcessSpec) (supervisor.Process, error) {
	r.starts.Add(1)
	return nil, errors.New("unexpected test worker process start")
}

type serviceRoleResultSubmitter struct{}

func (serviceRoleResultSubmitter) Submit(context.Context, results.Principal, contracts.Result) (results.Accepted, error) {
	return results.Accepted{}, errors.New("unexpected test result submission")
}

func serviceRoleNewControl(t *testing.T, f *dispatchTestFixture, recovery *serviceRoleRecovery, ack dispatch.Handler) *app.App {
	return serviceRoleNewControlBatch(t, f, recovery, ack, 1)
}

func serviceRoleNewControlBatch(t *testing.T, f *dispatchTestFixture, recovery *serviceRoleRecovery, ack dispatch.Handler, batch int) *app.App {
	return serviceRoleNewControlWithRedis(t, f, recovery, ack, f.redis.Client, batch)
}

func serviceRoleNewControlWithRedis(t *testing.T, f *dispatchTestFixture, recovery *serviceRoleRecovery, ack dispatch.Handler, redisClient *redis.Client, batch int) *app.App {
	t.Helper()
	if ack == nil {
		ack = func(context.Context, dispatch.OutboxItem) error { return nil }
	}
	config := serviceRoleConfig(app.RoleControl)
	config.DispatchBatch = batch
	service, err := app.NewControl(config, app.ControlDependencies{
		Pool: f.db.Pool, Redis: redisClient, Clock: f.clock, Streams: f.streams,
		RouterConfig: serviceRoleRouterConfig(), Operations: serviceRoleOperations{}, Labels: serviceRoleLabels{},
		ReconcileConfig: reconcile.Config{BatchSize: 1, RPCTimeout: 100 * time.Millisecond, MaxRetryDelay: time.Second},
		CancelAck:       ack, NotifyAck: ack, StartupRecovery: recovery,
	})
	if err != nil {
		t.Fatalf("construct control service: %v", err)
	}
	return service
}

func serviceRoleWorkerDependencies(t *testing.T, f *dispatchTestFixture, recovery *serviceRoleRecovery) app.WorkerDependencies {
	t.Helper()
	holds, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{ResourceScope: "service-role-test", MaxActive: 1, VerifierTimeout: time.Second}, processholdsFixtureVerifier{now: f.clock.Now})
	if err != nil {
		t.Fatalf("construct process-hold store: %v", err)
	}
	starts := &atomic.Int32{}
	return app.WorkerDependencies{
		Pool: f.db.Pool, Redis: f.redis.Client, Clock: f.clock, Streams: f.streams,
		Runner: serviceRoleRunner{starts: starts}, ResultSubmitter: serviceRoleResultSubmitter{}, Holds: holds,
		SupervisorConfig: supervisor.Config{SupervisorIdentity: "service-role-host", CredentialID: "test-host-credential", WorkspaceRoot: t.TempDir(), LeaseTTL: time.Minute, HeartbeatInterval: 100 * time.Millisecond, MaxExecution: 500 * time.Millisecond, StartTimeout: 100 * time.Millisecond, TermGrace: 100 * time.Millisecond, KillWait: 100 * time.Millisecond},
		ConsumerConfig:   queue.ConsumerConfig{LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: dispatchTestPromptHash, SupervisorIdentity: "service-role-host", CredentialID: "test-host-credential", BatchSize: 1, Block: time.Millisecond, OperationTimeout: 2 * time.Second},
		BudgetEnvelope:   contracts.BudgetEnvelope{MaxCostMicroUSD: 1000, MaxInputTokens: 10, MaxOutputTokens: 10, MaxCalls: 1, PricingVersion: "service-role-test-v1"},
		StartupRecovery:  recovery, OCIReadiness: serviceRoleProbe{}, ProviderReadiness: serviceRoleProbe{}, HostReadiness: serviceRoleProbe{},
	}
}

func serviceRoleRouterConfig() router.Config {
	return router.Config{
		Repositories: map[string]router.RepositoryPolicy{"owner/repo": {
			OrgID: "dispatch-test-org", PolicyVersion: "v1", TaskBudgetLimitMicroUSD: 1000,
			IssueEnrollmentLabel: "aprl:implement", AuthorizedIssueLabelerIDs: map[int64]struct{}{17: {}},
			AuthorizedHumanPRAuthorIDs: map[int64]struct{}{},
		}},
		TrustedAPRLActorIDs: map[int64]struct{}{81: {}}, TrustedAPRLAppIDs: map[int64]struct{}{901: {}},
		TrustedCIAppIDs: map[int64]struct{}{902: {}}, TrustedCIActorIDs: map[int64]struct{}{82: {}},
		TrustedCISenderLogins: map[string]struct{}{"github-actions[bot]": {}}, MaxTaskBudgetMicroUSD: 1000,
	}
}

func serviceRoleConfig(role app.Role) app.Config {
	return app.Config{Role: role, ListenAddr: "127.0.0.1:0", StartupTimeout: 2 * time.Second, RecoveryTimeout: time.Second,
		RPCDeadline: 250 * time.Millisecond, PollInterval: 20 * time.Millisecond, ShutdownTimeout: time.Second,
		ReadinessTimeout: time.Second, ReadinessPollInterval: 10 * time.Millisecond, MaxRequestBodyBytes: 1 << 20,
		DispatchBatch: 1, ConsumerName: "service-role-test"}
}

func serviceRoleRun(ctx context.Context, t *testing.T, service *app.App) chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- service.Run(ctx) }()
	return result
}

func serviceRoleStop(t *testing.T, service *app.App, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown service role: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("service role returned during shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("service role did not join before shutdown deadline")
	}
}

func serviceRoleStopAllowError(t *testing.T, service *app.App, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown service role: %v", err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("service role did not join before shutdown deadline")
	}
}

func serviceRoleWaitReadyCall(t *testing.T, recovery *serviceRoleRecovery, want int32) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for recovery.readyCalls.Load() < want {
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("startup recovery Ready called %d times, want at least %d", recovery.readyCalls.Load(), want)
		}
	}
}

func serviceRoleWaitUntil(t *testing.T, what string, probe func() (bool, error)) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := probe()
		if err != nil {
			t.Fatalf("probe %s: %v", what, err)
		}
		if ready {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func serviceRoleWaitUntilDiagnostic(t *testing.T, what string, probe func() (bool, error), diagnostic func() string) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := probe()
		if err != nil {
			t.Fatalf("probe %s: %v", what, err)
		}
		if ready {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s: %s", what, diagnostic())
		}
	}
}

func serviceRoleRedisProbeClient(t *testing.T, f *dispatchTestFixture) (*redis.Client, *atomic.Bool) {
	t.Helper()
	options := *f.redis.Client.Options()
	options.ContextTimeoutEnabled = true
	client := redis.NewClient(&options)
	closed := &atomic.Bool{}
	t.Cleanup(func() {
		if closed.CompareAndSwap(false, true) {
			if err := client.Close(); err != nil {
				t.Errorf("close isolated Redis readiness client: %v", err)
			}
		}
	})
	if err := client.Ping(f.ctx).Err(); err != nil {
		t.Fatalf("ping isolated Redis readiness client: %v", err)
	}
	return client, closed
}

func serviceRoleWaitForGroup(t *testing.T, f *dispatchTestFixture) {
	t.Helper()
	serviceRoleWaitUntil(t, "control/worker queue group creation", func() (bool, error) {
		groups, err := f.redis.Client.XInfoGroups(f.ctx, f.streamKey()).Result()
		if err != nil {
			return false, nil
		}
		return len(groups) != 0, nil
	})
}

func serviceRoleRequireStatus(t *testing.T, client *http.Client, endpoint string, wantCode int, wantStatus string, wantDependencies []string) {
	t.Helper()
	response, err := client.Get(endpoint)
	if err != nil {
		t.Fatalf("request %s: %v", endpoint, err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read/close %s response: read=%v close=%v", endpoint, readErr, closeErr)
	}
	var decoded struct {
		Status       string   `json:"status"`
		Dependencies []string `json:"dependencies"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode %s response %q: %v", endpoint, body, err)
	}
	dependenciesContainExpected := true
	for _, expected := range wantDependencies {
		found := false
		for _, actual := range decoded.Dependencies {
			if actual == expected {
				found = true
				break
			}
		}
		dependenciesContainExpected = dependenciesContainExpected && found
	}
	if response.StatusCode != wantCode || decoded.Status != wantStatus || !dependenciesContainExpected || !sort.StringsAreSorted(decoded.Dependencies) {
		t.Fatalf("%s response=(%d,%+v), want (%d,status=%s,dependencies=%v)", endpoint, response.StatusCode, decoded, wantCode, wantStatus, wantDependencies)
	}
}

func serviceRoleCheckHealthRoutes(t *testing.T, service *app.App) {
	t.Helper()
	server := httptest.NewServer(service.Handler())
	t.Cleanup(server.Close)
	response, err := server.Client().Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("request role liveness endpoint: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("close role liveness response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("role liveness status=%d, want 200", response.StatusCode)
	}
	response, err = server.Client().Get(server.URL + "/webhooks/github")
	if err != nil {
		t.Fatalf("request API-only route from non-API role: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("close non-API webhook response: %v", err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("non-API role exposed webhook route with status %d, want 404", response.StatusCode)
	}
	response, err = server.Client().Get(server.URL + "/internal/results")
	if err != nil {
		t.Fatalf("request API-only results route from non-API role: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("close non-API results response: %v", err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("non-API role exposed results route with status %d, want 404", response.StatusCode)
	}
}

func serviceRoleRequireUnchangedWork(t *testing.T, f *dispatchTestFixture, jobID, outboxID, deliveryID string) {
	t.Helper()
	var jobStatus string
	var token *string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status,lease_token::text FROM jobs WHERE id=$1::uuid`, jobID).Scan(&jobStatus, &token); err != nil {
		t.Fatal(err)
	}
	var runs int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM agent_runs WHERE job_id=$1::uuid`, jobID).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	var published *time.Time
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	var disposition string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT disposition FROM webhook_deliveries WHERE delivery_id=$1`, deliveryID).Scan(&disposition); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "PENDING" || token != nil || runs != 0 || published != nil || disposition != "INBOX" {
		t.Fatalf("work changed without current recovery evidence: job=%s lease=%v runs=%d published=%v disposition=%s", jobStatus, token, runs, published, disposition)
	}
}
