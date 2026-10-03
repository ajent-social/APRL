package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/broker"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/control"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/lifecycle"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/ajent-social/APRL/internal/reconcile"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

type reconcilePublisher struct {
	streams   *queue.Streams
	mu        sync.Mutex
	events    []string
	onPublish func(string)
	failures  int
	failErr   error
}

func (p *reconcilePublisher) Publish(ctx context.Context, kind, jobID string) (string, error) {
	p.mu.Lock()
	p.events = append(p.events, "publish:"+jobID)
	fail := p.failures > 0
	if fail {
		p.failures--
	}
	failErr := p.failErr
	p.mu.Unlock()
	if p.onPublish != nil {
		p.onPublish(jobID)
	}
	if fail {
		if failErr == nil {
			failErr = errors.New("injected Redis publication failure")
		}
		return "", failErr
	}
	return p.streams.Publish(ctx, kind, jobID)
}

func (p *reconcilePublisher) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

type reconcileOperations struct {
	mu      sync.Mutex
	results map[string]broker.Operation
	calls   []string
}

func (o *reconcileOperations) Reconcile(_ context.Context, id string) (broker.Operation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, id)
	return o.results[id], nil
}

func (o *reconcileOperations) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.calls...)
}

type reconcileLabels struct {
	mu      sync.Mutex
	intents []reconcile.LabelIntent
}

func (l *reconcileLabels) ApplyLifecycleLabel(_ context.Context, intent reconcile.LabelIntent) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.intents = append(l.intents, intent)
	return nil
}

func (l *reconcileLabels) snapshot() []reconcile.LabelIntent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]reconcile.LabelIntent(nil), l.intents...)
}

type interleavingReconcileLabels struct {
	mu           sync.Mutex
	intents      []reconcile.LabelIntent
	current      string
	firstCall    bool
	firstEntered chan struct{}
	releaseFirst chan struct{}
}

func (l *interleavingReconcileLabels) ApplyLifecycleLabel(ctx context.Context, intent reconcile.LabelIntent) error {
	l.mu.Lock()
	first := !l.firstCall
	l.firstCall = true
	l.mu.Unlock()
	if first {
		close(l.firstEntered)
		select {
		case <-l.releaseFirst:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	l.mu.Lock()
	l.intents = append(l.intents, intent)
	l.current = intent.Label
	l.mu.Unlock()
	return nil
}

func (l *interleavingReconcileLabels) snapshot() ([]reconcile.LabelIntent, string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]reconcile.LabelIntent(nil), l.intents...), l.current
}

func TestReconcileRepublishesPendingJobsAndFencesExpiredLeasesFirst(t *testing.T) {
	f := newDispatchTestFixture(t)
	if err := f.streams.EnsureGroup(f.ctx); err != nil {
		t.Fatal(err)
	}
	expiredJob := f.addJob(t, "author")
	lostQueueJob := f.addJob(t, "author")
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: f.taskID, JobID: expiredJob,
		TTL: time.Minute, AgentType: "A", PromptHash: dispatchTestPromptHash,
		SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test"})
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Minute)
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO outbox(task_id,job_id,kind,payload,published_at,next_attempt_at)
		VALUES($1::uuid,$2::uuid,'DISPATCH',jsonb_build_object('job_id',$2::text),$3,$3)`, f.taskID, lostQueueJob, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	var events []string
	var eventsMu sync.Mutex
	publisher := &reconcilePublisher{streams: f.streams, onPublish: func(jobID string) {
		eventsMu.Lock()
		events = append(events, "publish:"+jobID)
		eventsMu.Unlock()
	}}
	operations := &reconcileOperations{results: map[string]broker.Operation{}}
	labels := &reconcileLabels{}
	service, err := reconcile.New(f.db.Pool, f.clock, publisher, operations,
		func(context.Context) error { return nil },
		func(ctx context.Context, pool *pgxpool.Pool, _ clock.Clock, taskID string) error {
			eventsMu.Lock()
			events = append(events, "fence:"+taskID)
			eventsMu.Unlock()
			return leases.FenceExpiredTask(ctx, pool, f.clock, taskID)
		}, labels, reconcile.Config{BatchSize: 10, RPCTimeout: 2 * time.Second, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.RunOnce(f.ctx)
	if err != nil {
		t.Fatalf("reconcile pass: %v", err)
	}
	if report.ExpiredTasks != 1 || report.PublishedJobHints != 2 {
		t.Fatalf("reconcile report=%+v, want one expiry and two pending job hints", report)
	}
	var runState, jobState string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT r.execution_status,j.status FROM agent_runs r JOIN jobs j ON j.id=r.job_id WHERE r.id=$1::uuid`, lease.RunID).
		Scan(&runState, &jobState); err != nil {
		t.Fatal(err)
	}
	if runState != "TERMINATED" || jobState != "PENDING" {
		t.Fatalf("expired run/job=(%s,%s), want TERMINATED/PENDING", runState, jobState)
	}
	entries, err := f.streams.Read(f.ctx, "reconcile-loss", 10*time.Millisecond, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("recovered Redis entries=(%+v,%v), want both pending job IDs", entries, err)
	}
	want := map[string]bool{expiredJob: false, lostQueueJob: false}
	for _, entry := range entries {
		if entry.Kind != queue.DispatchKind {
			t.Fatalf("recovered queue kind=%q", entry.Kind)
		}
		if _, ok := want[entry.JobID]; !ok {
			t.Fatalf("recovered unexpected job %s", entry.JobID)
		}
		want[entry.JobID] = true
	}
	for jobID, seen := range want {
		if !seen {
			t.Errorf("durable pending job %s was not republished", jobID)
		}
	}
	if got := publisher.snapshot(); len(got) != 2 {
		t.Fatalf("publisher calls=%v, want exactly two", got)
	}
	eventsMu.Lock()
	fenceEvents := append([]string(nil), events...)
	eventsMu.Unlock()
	if len(fenceEvents) != 3 || fenceEvents[0] != "fence:"+f.taskID ||
		(fenceEvents[1] != "publish:"+expiredJob && fenceEvents[1] != "publish:"+lostQueueJob) ||
		(fenceEvents[2] != "publish:"+expiredJob && fenceEvents[2] != "publish:"+lostQueueJob) || fenceEvents[1] == fenceEvents[2] {
		t.Fatalf("expected expiry fencing before both queue publications, events=%v", fenceEvents)
	}
}

func TestReconcileRespectsDurableRetryDeadline(t *testing.T) {
	f := newDispatchTestFixture(t)
	jobID := f.addJob(t, "author")
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: f.taskID, JobID: jobID,
		TTL: time.Minute, AgentType: "A", PromptHash: dispatchTestPromptHash,
		SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test"})
	if err != nil {
		t.Fatal(err)
	}
	due := f.clock.Now().Add(time.Hour)
	if err := leases.Retry(f.ctx, f.db.Pool, f.clock, lease, due, "SUCCESS"); err != nil {
		t.Fatal(err)
	}
	publisher := &reconcilePublisher{streams: f.streams}
	service, err := reconcile.New(f.db.Pool, f.clock, publisher, &reconcileOperations{results: map[string]broker.Operation{}},
		func(context.Context) error { return nil }, leases.FenceExpiredTask, &reconcileLabels{},
		reconcile.Config{BatchSize: 10, RPCTimeout: 2 * time.Second, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if report, err := service.RunOnce(f.ctx); err != nil || report.PublishedJobHints != 0 {
		t.Fatalf("before retry deadline=(%+v,%v), want no early publication", report, err)
	}
	f.clock.Advance(time.Hour)
	if report, err := service.RunOnce(f.ctx); err != nil || report.PublishedJobHints != 1 {
		t.Fatalf("at retry deadline=(%+v,%v), want one publication", report, err)
	}
	if events := publisher.snapshot(); len(events) != 1 || events[0] != "publish:"+jobID {
		t.Fatalf("publisher calls=%v, want the due stable job ID", events)
	}
}

func TestReconcileDirectPublishFailurePersistsBackoffAcrossRestart(t *testing.T) {
	f := newDispatchTestFixture(t)
	jobID := f.addJob(t, "author")
	outboxID := f.addOutbox(t, jobID, "DISPATCH", `{"job_id":"`+jobID+`"}`, f.clock.Now())
	if _, err := f.streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `UPDATE outbox SET published_at=$2 WHERE id=$1::uuid`, outboxID, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := f.redis.Client.Del(f.ctx, f.streamKey()).Err(); err != nil {
		t.Fatalf("simulate Redis stream loss after publication: %v", err)
	}
	failedPublisher := &reconcilePublisher{streams: f.streams, failures: 1, failErr: errors.New("temporary Redis outage")}
	newService := func(publisher *reconcilePublisher) *reconcile.Service {
		t.Helper()
		service, err := reconcile.New(f.db.Pool, f.clock, publisher, &reconcileOperations{results: map[string]broker.Operation{}},
			func(context.Context) error { return nil }, leases.FenceExpiredTask, &reconcileLabels{},
			reconcile.Config{BatchSize: 10, RPCTimeout: 2 * time.Second, MaxRetryDelay: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	first := newService(failedPublisher)
	if _, err := first.RunOnce(f.ctx); err == nil {
		t.Fatal("direct Redis publication failure was not reported")
	}
	var repairOutbox, repairGeneration string
	var nextAttempt time.Time
	var attempts int32
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id::text,payload->>'repair_generation',next_attempt_at,attempt_count FROM outbox
		WHERE task_id=$1::uuid AND job_id=$2::uuid AND kind='DISPATCH' AND published_at IS NULL`, f.taskID, jobID).
		Scan(&repairOutbox, &repairGeneration, &nextAttempt, &attempts); err != nil {
		t.Fatalf("read persisted repair intent: %v", err)
	}
	if repairOutbox != outboxID || repairGeneration != "0" || !nextAttempt.After(f.clock.Now()) || attempts != 1 {
		t.Fatalf("repair intent=(%s,generation=%s,next=%s,attempts=%d), want original outbox, generation 0, bounded future deadline, attempt 1",
			repairOutbox, repairGeneration, nextAttempt, attempts)
	}
	if got := len(failedPublisher.snapshot()); got != 1 {
		t.Fatalf("failed service publish count=%d, want one", got)
	}

	// Reconstruct the reconciler to prove retry timing lives in PostgreSQL, not
	// in process memory. A pass before the persisted deadline must not call Redis.
	restartedPublisher := &reconcilePublisher{streams: f.streams}
	restarted := newService(restartedPublisher)
	if report, err := restarted.RunOnce(f.ctx); err != nil || report.PublishedJobHints != 0 {
		t.Fatalf("restarted pass before repair deadline=(%+v,%v), want zero publication", report, err)
	}
	if got := len(restartedPublisher.snapshot()); got != 0 {
		t.Fatalf("publisher was called %d times before persisted deadline", got)
	}
	f.clock.Advance(nextAttempt.Sub(f.clock.Now()))
	if report, err := restarted.RunOnce(f.ctx); err != nil || report.PublishedJobHints != 1 {
		t.Fatalf("restarted pass at repair deadline=(%+v,%v), want one stable hint", report, err)
	}
	entries, err := f.redis.Client.XRange(f.ctx, f.streamKey(), "-", "+").Result()
	if err != nil || len(entries) != 1 || entries[0].Values["job_id"] != jobID {
		t.Fatalf("Redis repair entries=(%+v,%v), want exactly the stable job ID %s", entries, err, jobID)
	}
	var published bool
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at IS NOT NULL FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatal("successful direct repair did not acknowledge its durable outbox intent")
	}
}

func TestReconcileDirectPublishFailureDoesNotRearmAfterConcurrentPause(t *testing.T) {
	f := newDispatchTestFixture(t)
	jobID := f.addJob(t, "author")
	outboxID := f.addOutbox(t, jobID, "DISPATCH", `{"job_id":"`+jobID+`"}`, f.clock.Now())
	if _, err := f.streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `UPDATE outbox SET published_at=$2 WHERE id=$1::uuid`, outboxID, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := f.redis.Client.Del(f.ctx, f.streamKey()).Err(); err != nil {
		t.Fatalf("simulate Redis stream loss before cancellation race: %v", err)
	}
	publisher := &reconcilePublisher{streams: f.streams, failures: 1, failErr: errors.New("temporary Redis outage")}
	publisher.onPublish = func(string) {
		err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
			_, locked, err := repos.LockOrgBudgetAndTask(ctx, "dispatch-test-org", f.taskID)
			if err != nil {
				return err
			}
			_, err = control.PauseLocked(ctx, repos, locked, f.clock, control.PauseRequest{Authority: control.Authority{
				ActorID: "dispatch-test-owner", Repository: "owner/repo", Permission: control.PermissionWrite, VerifiedAt: f.clock.Now()}, Reason: "pause during queue repair"})
			return err
		})
		if err != nil {
			t.Errorf("pause task during Redis publication: %v", err)
		}
	}
	service, err := reconcile.New(f.db.Pool, f.clock, publisher, &reconcileOperations{results: map[string]broker.Operation{}},
		func(context.Context) error { return nil }, leases.FenceExpiredTask, &reconcileLabels{},
		reconcile.Config{BatchSize: 10, RPCTimeout: 2 * time.Second, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunOnce(f.ctx); err == nil {
		t.Fatal("direct Redis publication failure was not reported")
	}
	var taskState, jobStatus string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT t.state,j.status FROM tasks t JOIN jobs j ON j.task_id=t.id WHERE j.id=$1::uuid`, jobID).
		Scan(&taskState, &jobStatus); err != nil {
		t.Fatal(err)
	}
	if taskState != "PAUSED" || jobStatus != "CANCELLED" {
		t.Fatalf("concurrent pause/job state=(%s,%s), want PAUSED/CANCELLED", taskState, jobStatus)
	}
	var pendingRepairs int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND job_id=$2::uuid AND kind='DISPATCH'
		AND published_at IS NULL AND payload ? 'repair_generation'`, f.taskID, jobID).Scan(&pendingRepairs); err != nil {
		t.Fatal(err)
	}
	if pendingRepairs != 0 {
		t.Fatalf("pause race persisted %d dispatch repair intents for cancelled work", pendingRepairs)
	}
	var originalPublished bool
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at IS NOT NULL FROM outbox WHERE id=$1::uuid`, outboxID).Scan(&originalPublished); err != nil {
		t.Fatal(err)
	}
	if !originalPublished {
		t.Fatal("pause race rearmed the previously published dispatch outbox")
	}
}

func TestReconcileUsesFairRemoteLookupOnlyForUncertainOperations(t *testing.T) {
	f := newDispatchTestFixture(t)
	var operationIDs []string
	for i := 0; i < 2; i++ {
		var id string
		if err := f.db.Pool.QueryRow(f.ctx, `INSERT INTO github_operations(task_id,generation,operation_type,identity,request,status)
			VALUES($1::uuid,0,'push','A','{}'::jsonb,'UNKNOWN') RETURNING id::text`, f.taskID).Scan(&id); err != nil {
			t.Fatal(err)
		}
		operationIDs = append(operationIDs, id)
	}
	operations := &reconcileOperations{results: map[string]broker.Operation{}}
	service, err := reconcile.New(f.db.Pool, f.clock, &reconcilePublisher{streams: f.streams}, operations,
		func(context.Context) error { return nil }, leases.FenceExpiredTask, &reconcileLabels{},
		reconcile.Config{BatchSize: 1, RPCTimeout: 2 * time.Second, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := service.RunOnce(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	calls := operations.snapshot()
	if len(calls) != 2 {
		t.Fatalf("remote reconcile calls=%v, want two bounded lookup attempts", calls)
	}
	seen := map[string]bool{calls[0]: true, calls[1]: true}
	if calls[0] == calls[1] || !seen[operationIDs[0]] || !seen[operationIDs[1]] {
		t.Fatalf("persistent UNKNOWN operations starved a later row: ids=%v calls=%v", operationIDs, calls)
	}
}

func TestReconcileProjectsOnlyCurrentConfirmedMergeAndRepairsLabelsWithoutStateInput(t *testing.T) {
	for _, tc := range []struct {
		name             string
		merged           bool
		wrongHead        bool
		wrongReceiptHead bool
		emptyReceiptHead bool
		wantMerged       bool
	}{
		{name: "confirmed_current_snapshot", merged: true, wantMerged: true},
		{name: "receipt_not_merged", merged: false},
		{name: "stale_head", merged: true, wrongHead: true},
		{name: "receipt_confirms_different_source_head", merged: true, wrongReceiptHead: true},
		{name: "receipt_omits_source_head", merged: true, emptyReceiptHead: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDispatchTestFixture(t)
			head := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			base := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			integration := "cccccccccccccccccccccccccccccccccccccccc"
			if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET state='READY_TO_MERGE' WHERE id=$1::uuid`, f.taskID); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.Pool.Exec(f.ctx, `UPDATE prs SET integration_sha=$2 WHERE task_id=$1::uuid`, f.taskID, integration); err != nil {
				t.Fatal(err)
			}
			expectedHead := head
			if tc.wrongHead {
				expectedHead = "dddddddddddddddddddddddddddddddddddddddd"
			}
			receiptHead := head
			if tc.wrongReceiptHead {
				receiptHead = "dddddddddddddddddddddddddddddddddddddddd"
			} else if tc.emptyReceiptHead {
				receiptHead = ""
			}
			request, _ := json.Marshal(map[string]string{"expected_head_sha": expectedHead, "base_sha": base,
				"integration_sha": integration, "target_branch": "main"})
			result, _ := json.Marshal(broker.RemoteReceipt{RemoteID: "host-confirmed-merge", HeadSHA: receiptHead, Merged: tc.merged})
			if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO github_operations(task_id,generation,operation_type,identity,request,result,status)
				VALUES($1::uuid,0,'merge','A',$2::jsonb,$3::jsonb,'CONFIRMED')`, f.taskID, string(request), string(result)); err != nil {
				t.Fatal(err)
			}
			labels := &reconcileLabels{}
			service, err := reconcile.New(f.db.Pool, f.clock, &reconcilePublisher{streams: f.streams},
				&reconcileOperations{results: map[string]broker.Operation{}}, func(context.Context) error { return nil },
				leases.FenceExpiredTask, labels, reconcile.Config{BatchSize: 10, RPCTimeout: 2 * time.Second, MaxRetryDelay: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			report, err := service.RunOnce(f.ctx)
			if err != nil {
				t.Fatalf("reconcile confirmed merge: %v", err)
			}
			var state string
			var generation int64
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,generation FROM tasks WHERE id=$1::uuid`, f.taskID).Scan(&state, &generation); err != nil {
				t.Fatal(err)
			}
			if tc.wantMerged {
				if state != string(lifecycle.Merged) || generation != 0 || report.MergedTasks != 1 {
					t.Fatalf("confirmed merge projection=(state %s gen %d report %+v)", state, generation, report)
				}
				intents := labels.snapshot()
				if len(intents) != 1 || intents[0].State != string(lifecycle.Merged) || intents[0].Label != "" {
					t.Fatalf("terminal label repair intents=%+v", intents)
				}
			} else if state != string(lifecycle.ReadyToMerge) || generation != 0 || report.MergedTasks != 0 || len(labels.snapshot()) != 0 {
				t.Fatalf("unconfirmed/stale merge advanced durable state: state=%s gen=%d report=%+v labels=%+v", state, generation, report, labels.snapshot())
			}
		})
	}
}

func TestReconcileStartupHoldGatePrecedesEverySideEffect(t *testing.T) {
	f := newDispatchTestFixture(t)
	jobID := f.addJob(t, "author")
	if err := f.streams.EnsureGroup(f.ctx); err != nil {
		t.Fatal(err)
	}
	publisher := &reconcilePublisher{streams: f.streams}
	operations := &reconcileOperations{results: map[string]broker.Operation{}}
	labels := &reconcileLabels{}
	fenceCalls := 0
	service, err := reconcile.New(f.db.Pool, f.clock, publisher, operations,
		func(context.Context) error { return errors.New("inventory incomplete") },
		func(context.Context, *pgxpool.Pool, clock.Clock, string) error { fenceCalls++; return nil },
		labels, reconcile.Config{BatchSize: 10, RPCTimeout: 2 * time.Second, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if report, err := service.RunOnce(f.ctx); !errors.Is(err, reconcile.ErrNotReady) || report != (reconcile.Report{}) {
		t.Fatalf("startup-gated pass=(%+v,%v), want no work and not-ready", report, err)
	}
	entries, err := f.streams.Read(f.ctx, "reconcile-gated", time.Millisecond, 1)
	if err != nil || len(entries) != 0 {
		t.Fatalf("startup-gated queue entries=(%+v,%v), want none for job %s", entries, err, jobID)
	}
	if fenceCalls != 0 || len(operations.snapshot()) != 0 || len(publisher.snapshot()) != 0 || len(labels.snapshot()) != 0 {
		t.Fatalf("startup gate allowed side effects: fences=%d ops=%v publishes=%v labels=%v", fenceCalls, operations.snapshot(), publisher.snapshot(), labels.snapshot())
	}
}

func TestReconcileRejectsLateNilStartupHoldInventoryResult(t *testing.T) {
	f := newDispatchTestFixture(t)
	f.addJob(t, "author")
	var operationID string
	if err := f.db.Pool.QueryRow(f.ctx, `INSERT INTO github_operations(task_id,generation,operation_type,identity,request,status)
		VALUES($1::uuid,0,'push','A','{}'::jsonb,'UNKNOWN') RETURNING id::text`, f.taskID).Scan(&operationID); err != nil {
		t.Fatal(err)
	}
	var prID int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id FROM prs WHERE task_id=$1::uuid`, f.taskID).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO outbox(task_id,kind,payload,next_attempt_at)
		VALUES($1::uuid,'LABEL_SYNC',jsonb_build_object('task_id',$1::uuid,'pr_id',$2::bigint,'generation',0,'state','AUTHORING'),$3)`,
		f.taskID, prID, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	publisher := &reconcilePublisher{streams: f.streams}
	operations := &reconcileOperations{results: map[string]broker.Operation{}}
	labels := &reconcileLabels{}
	service, err := reconcile.New(f.db.Pool, f.clock, publisher, operations,
		func(ctx context.Context) error { <-ctx.Done(); return nil }, leases.FenceExpiredTask, labels,
		reconcile.Config{BatchSize: 10, RPCTimeout: 10 * time.Millisecond, MaxRetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if report, err := service.RunOnce(f.ctx); !errors.Is(err, reconcile.ErrNotReady) || report != (reconcile.Report{}) {
		t.Fatalf("late nil startup inventory=(%+v,%v), want no work and not-ready", report, err)
	}
	if len(operations.snapshot()) != 0 || len(publisher.snapshot()) != 0 || len(labels.snapshot()) != 0 {
		t.Fatalf("late startup proof admitted side effects: ops=%v publishes=%v labels=%v", operations.snapshot(), publisher.snapshot(), labels.snapshot())
	}
}

func TestLabelRepairRearmsStaleCallbackAfterNewerProjection(t *testing.T) {
	f := newDispatchTestFixture(t)
	var prID int64
	var prNumber int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT id,pr_number FROM prs WHERE task_id=$1::uuid`, f.taskID).Scan(&prID, &prNumber); err != nil {
		t.Fatal(err)
	}
	oldOutbox := "10000000-0000-4000-8000-000000000001"
	newOutbox := "10000000-0000-4000-8000-000000000002"
	insertIntent := func(id, state, label string) {
		t.Helper()
		payload, err := json.Marshal(map[string]any{"task_id": f.taskID, "pr_id": prID, "generation": 0, "state": state, "label": label})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO outbox(id,task_id,kind,payload,next_attempt_at)
			VALUES($1::uuid,$2::uuid,'LABEL_SYNC',$3::jsonb,$4)`, id, f.taskID, string(payload), f.clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	var oldState string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state FROM tasks WHERE id=$1::uuid`, f.taskID).Scan(&oldState); err != nil {
		t.Fatal(err)
	}
	insertIntent(oldOutbox, oldState, "old-label")
	labels := &interleavingReconcileLabels{firstEntered: make(chan struct{}), releaseFirst: make(chan struct{})}
	newService := func() *reconcile.Service {
		t.Helper()
		service, err := reconcile.New(f.db.Pool, f.clock, &reconcilePublisher{streams: f.streams},
			&reconcileOperations{results: map[string]broker.Operation{}}, func(context.Context) error { return nil },
			leases.FenceExpiredTask, labels, reconcile.Config{BatchSize: 1, RPCTimeout: 2 * time.Second, MaxRetryDelay: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	first := newService()
	firstResult := make(chan error, 1)
	go func() {
		_, err := first.RunOnce(f.ctx)
		firstResult <- err
	}()
	select {
	case <-labels.firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first stale label callback did not start")
	}
	if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET state='IN_REVIEW' WHERE id=$1::uuid`, f.taskID); err != nil {
		t.Fatal(err)
	}
	insertIntent(newOutbox, string(lifecycle.InReview), "new-label")
	second := newService()
	if _, err := second.RunOnce(f.ctx); err != nil {
		t.Fatalf("new projection label pass: %v", err)
	}
	close(labels.releaseFirst)
	if err := <-firstResult; err != nil {
		t.Fatalf("stale label pass: %v", err)
	}
	var oldPublished bool
	var pending int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT published_at IS NOT NULL FROM outbox WHERE id=$1::uuid`, oldOutbox).Scan(&oldPublished); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND kind='LABEL_SYNC' AND published_at IS NULL`, f.taskID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if oldPublished || pending == 0 {
		t.Fatalf("stale callback was acknowledged instead of rearmed: oldPublished=%v pending=%d", oldPublished, pending)
	}
	for i := 0; i < 3; i++ {
		if _, err := second.RunOnce(f.ctx); err != nil {
			t.Fatalf("converge current label pass %d: %v", i, err)
		}
	}
	intents, current := labels.snapshot()
	if current != "agent:reviewing" || len(intents) < 3 {
		t.Fatalf("label repair did not converge after stale completion: current=%q intents=%+v", current, intents)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND kind='LABEL_SYNC' AND published_at IS NULL`, f.taskID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("current durable label intents remain pending after convergence: %d", pending)
	}
}

var _ reconcile.Publisher = (*reconcilePublisher)(nil)
var _ reconcile.OperationReconciler = (*reconcileOperations)(nil)
var _ reconcile.LabelOutputter = (*reconcileLabels)(nil)
var _ reconcile.LabelOutputter = (*interleavingReconcileLabels)(nil)
