package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/ci"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/lifecycle"
	"github.com/ajent-social/APRL/internal/policy"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

const (
	ciHead        = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ciBase        = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	ciIntegration = "cccccccccccccccccccccccccccccccccccccccc"
	ciOldHead     = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

type ciFixture struct {
	db     *testutil.DatabaseFixture
	clock  *clock.Manual
	ctx    context.Context
	orgID  string
	taskID string
	prID   int64
}

func ciRequireFixture(t *testing.T, integration string, deadline time.Time) ciFixture {
	t.Helper()
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatal(err)
	}
	clockNow := clock.NewManual(time.Date(2026, 10, 1, 18, 0, 0, 0, time.UTC))
	orgID := "ci-test-org"
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, orgID); err != nil {
		t.Fatal(err)
	}
	var taskID string
	if err := db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version) VALUES($1,'ci/test','ci-test','44','WAITING_CI','ci-test-v1') RETURNING id::text`, orgID).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	var deadlineValue any
	if !deadline.IsZero() {
		deadlineValue = deadline
	}
	var prID int64
	if err := db.Pool.QueryRow(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha,integration_sha,approved_head_sha,approved_base_sha,human_approval_id,ci_status,ci_deadline_at) VALUES($1::uuid,'ci/test',17,$2,'main',$3,$4,$2,$3,'human-approval','PENDING',$5) RETURNING id`, taskID, ciHead, ciBase, nullableCISHA(integration), deadlineValue).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	return ciFixture{db: &db, clock: clockNow, ctx: ctx, orgID: orgID, taskID: taskID, prID: prID}
}

func nullableCISHA(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func ciTestConfig() policy.CIConfig {
	producer := policy.Producer{Kind: policy.ProducerApp, ID: "902"}
	return policy.CIConfig{
		TrustedProducers: []policy.Producer{producer},
		RequiredChecks:   []policy.RequiredCheck{{Name: "build", Producer: producer}, {Name: "lint", Producer: producer}},
		WaitTimeout:      30 * time.Minute,
		PollInterval:     5 * time.Minute,
	}
}

func ciSnapshot(integration string) contracts.Snapshot {
	return contracts.Snapshot{HeadSHA: ciHead, BaseSHA: ciBase, IntegrationSHA: integration}
}

func ciPassingObservations(snapshot contracts.Snapshot, config policy.CIConfig, now time.Time) []ci.Observation {
	observations := make([]ci.Observation, 0, len(config.RequiredChecks))
	for _, check := range config.RequiredChecks {
		observations = append(observations, ci.Observation{Name: check.Name, Producer: check.Producer, HeadSHA: snapshot.HeadSHA, BaseSHA: snapshot.BaseSHA, IntegrationSHA: snapshot.IntegrationSHA, Status: "completed", Conclusion: "success", Attempt: 1, UpdatedAt: now})
	}
	return observations
}

func ciCreateJob(t *testing.T, f ciFixture, snapshot contracts.Snapshot, generation int64) string {
	t.Helper()
	jobID := fmt.Sprintf("c1000000-0000-4000-8000-%012x", generation+1)
	job := contracts.Job{Version: 1, TaskID: f.taskID, JobID: jobID, Generation: generation, Snapshot: snapshot, Attempt: 1, OperationID: fmt.Sprintf("c2000000-0000-4000-8000-%012x", generation+1), CorrelationID: fmt.Sprintf("c3000000-0000-4000-8000-%012x", generation+1), Operation: ci.OperationCIReconcile}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		_, locked, err := repos.LockOrgBudgetAndTask(ctx, f.orgID, f.taskID)
		if err != nil {
			return err
		}
		_, _, err = repos.InsertJobAndOutbox(ctx, locked, storage.JobInput{ID: jobID, LogicalKey: fmt.Sprintf("task:%s:generation:%d:ci_reconcile", f.taskID, generation), OperationType: ci.OperationCIReconcile, PRID: &f.prID, Payload: encoded})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return jobID
}

func ciClaim(t *testing.T, f ciFixture, jobID string) leases.Lease {
	t.Helper()
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: f.taskID, JobID: jobID, TTL: time.Hour, AgentType: "A", PromptHash: strings.Repeat("a", 64), SupervisorIdentity: "ci-test-supervisor", CredentialID: "ci-test-credential"})
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func ciWithLockedTask(t *testing.T, f ciFixture, callback func(context.Context, *storage.Repositories, storage.LockedTask) error) {
	t.Helper()
	err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		_, locked, err := repos.LockOrgBudgetAndTask(ctx, f.orgID, f.taskID)
		if err != nil {
			return err
		}
		return callback(ctx, repos, locked)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func ciState(t *testing.T, f ciFixture) (string, int64, int32) {
	t.Helper()
	var state string
	var generation int64
	var cycle int32
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT state,generation,cycle_count FROM tasks WHERE id=$1::uuid`, f.taskID).Scan(&state, &generation, &cycle); err != nil {
		t.Fatal(err)
	}
	return state, generation, cycle
}

func ciJobCounts(t *testing.T, f ciFixture) (int, int) {
	t.Helper()
	var jobs, outbox int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid`, f.taskID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid`, f.taskID).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	return jobs, outbox
}

func TestCIAggregateTrustedSnapshot(t *testing.T) {
	config := ciTestConfig()
	snapshot := ciSnapshot(ciIntegration)
	now := time.Date(2026, 10, 1, 18, 0, 0, 0, time.UTC)
	pass := ciPassingObservations(snapshot, config, now)
	passed, err := ci.Evaluate(snapshot, config, pass)
	if err != nil || passed.Outcome != ci.OutcomePassed || passed.Passed != 2 {
		t.Fatalf("current trusted checks did not pass: aggregate=%+v err=%v", passed, err)
	}
	for _, tc := range []struct {
		name     string
		observed []ci.Observation
		want     ci.Outcome
		stale    int
	}{
		{name: "old_head_pass_does_not_admit", observed: []ci.Observation{pass[0], {Name: "lint", Producer: pass[1].Producer, HeadSHA: ciOldHead, BaseSHA: ciBase, IntegrationSHA: ciIntegration, Status: "completed", Conclusion: "success", Attempt: 1, UpdatedAt: now}}, want: ci.OutcomePending, stale: 1},
		{name: "untrusted_producer_does_not_admit", observed: []ci.Observation{pass[0], {Name: "lint", Producer: policy.Producer{Kind: policy.ProducerApp, ID: "999"}, HeadSHA: ciHead, BaseSHA: ciBase, IntegrationSHA: ciIntegration, Status: "completed", Conclusion: "success", Attempt: 1, UpdatedAt: now}}, want: ci.OutcomePending},
		{name: "pending_does_not_pass", observed: []ci.Observation{pass[0], {Name: "lint", Producer: pass[1].Producer, HeadSHA: ciHead, BaseSHA: ciBase, IntegrationSHA: ciIntegration, Status: "in_progress", Attempt: 1, UpdatedAt: now}}, want: ci.OutcomePending},
		{name: "cancelled_does_not_pass", observed: []ci.Observation{pass[0], {Name: "lint", Producer: pass[1].Producer, HeadSHA: ciHead, BaseSHA: ciBase, IntegrationSHA: ciIntegration, Status: "completed", Conclusion: "cancelled", Attempt: 1, UpdatedAt: now}}, want: ci.OutcomeInconclusive},
		{name: "neutral_does_not_pass", observed: []ci.Observation{pass[0], {Name: "lint", Producer: pass[1].Producer, HeadSHA: ciHead, BaseSHA: ciBase, IntegrationSHA: ciIntegration, Status: "completed", Conclusion: "neutral", Attempt: 1, UpdatedAt: now}}, want: ci.OutcomeInconclusive},
		{name: "skipped_does_not_pass", observed: []ci.Observation{pass[0], {Name: "lint", Producer: pass[1].Producer, HeadSHA: ciHead, BaseSHA: ciBase, IntegrationSHA: ciIntegration, Status: "completed", Conclusion: "skipped", Attempt: 1, UpdatedAt: now}}, want: ci.OutcomeInconclusive},
		{name: "failure_is_explicit", observed: []ci.Observation{pass[0], {Name: "lint", Producer: pass[1].Producer, HeadSHA: ciHead, BaseSHA: ciBase, IntegrationSHA: ciIntegration, Status: "completed", Conclusion: "failure", Attempt: 1, UpdatedAt: now}}, want: ci.OutcomeFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ci.Evaluate(snapshot, config, tc.observed)
			if err != nil || got.Outcome != tc.want || got.StaleObservations != tc.stale {
				t.Fatalf("aggregate=%+v err=%v; want outcome=%s stale=%d", got, err, tc.want, tc.stale)
			}
			if policy.CanAdmitReview(policy.ReviewAdmission{TaskState: string(lifecycle.WaitingCI), CurrentSnapshot: snapshot, TestedSnapshot: got.Snapshot, RequiredChecks: got.Required, ChecksPassed: got.Outcome == ci.OutcomePassed}) != (tc.want == ci.OutcomePassed) {
				t.Fatalf("review admission did not follow aggregate %s", got.Outcome)
			}
		})
	}
	empty := config
	empty.RequiredChecks = nil
	got, err := ci.Evaluate(snapshot, empty, pass)
	if err != nil || got.Outcome != ci.OutcomeInconclusive || policy.CanAutonomouslyMerge(policy.MergeAdmission{TaskState: string(lifecycle.ReadyToMerge), CurrentSnapshot: snapshot, ReviewedSnapshot: snapshot, CISnapshot: snapshot, RequiredChecks: got.Required, ChecksPassed: got.Outcome == ci.OutcomePassed, ReviewApproved: true, AutonomousMergeEnabled: true, TargetBranch: "feature/topic", AllowedTargetBranches: []string{"feature/*"}}) {
		t.Fatalf("empty check policy was not fail-closed: aggregate=%+v err=%v", got, err)
	}
	merge := policy.MergeAdmission{TaskState: string(lifecycle.ReadyToMerge), CurrentSnapshot: snapshot, ReviewedSnapshot: snapshot, CISnapshot: snapshot, RequiredChecks: passed.Required, ChecksPassed: true, ReviewApproved: true, AutonomousMergeEnabled: true, TargetBranch: "feature/topic", AllowedTargetBranches: []string{"feature/*"}}
	if !policy.CanAutonomouslyMerge(merge) {
		t.Fatal("fully approved current nonprotected target was incorrectly denied")
	}
	merge.CISnapshot.IntegrationSHA = ciOldHead
	if policy.CanAutonomouslyMerge(merge) {
		t.Fatal("stale integration snapshot was allowed to merge")
	}
	merge.CISnapshot = snapshot
	merge.TargetBranch = "main"
	merge.AllowedTargetBranches = []string{"feature/*", "main"}
	merge.ProtectedTargetBranches = []string{"main"}
	if policy.CanAutonomouslyMerge(merge) {
		t.Fatal("protected target without current human approval was allowed to merge")
	}
	merge.HumanApprovalCurrent = true
	if !policy.CanAutonomouslyMerge(merge) {
		t.Fatal("protected target with current human approval was denied")
	}
}

func TestCIPinIntegrationFencesAndRequeues(t *testing.T) {
	f := ciRequireFixture(t, "", time.Time{})
	config := ciTestConfig()
	initial := contracts.Snapshot{HeadSHA: ciHead, BaseSHA: ciBase}
	jobID := ciCreateJob(t, f, initial, 0)
	lease := ciClaim(t, f, jobID)
	var pinned storage.LockedTask
	ciWithLockedTask(t, f, func(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask) error {
		var err error
		var changed bool
		pinned, changed, err = ci.PinIntegration(ctx, repos, locked, lease, f.clock, config, ciIntegration)
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("integration SHA was not pinned")
		}
		return nil
	})
	if pinned.Record().Generation != 1 || pinned.Record().Snapshot.IntegrationSHA != ciIntegration {
		t.Fatalf("returned task not refreshed after pin: %+v", pinned.Record())
	}
	state, generation, cycle := ciState(t, f)
	if state != string(lifecycle.WaitingCI) || generation != 1 || cycle != 0 {
		t.Fatalf("pin changed lifecycle unexpectedly: state=%s generation=%d cycle=%d", state, generation, cycle)
	}
	var oldStatus, ciStatus string
	var approvedHead *string
	var deadline *time.Time
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT j.status FROM jobs j WHERE j.id=$1::uuid`, jobID).Scan(&oldStatus); err != nil {
		t.Fatal(err)
	}
	if oldStatus != "COMPLETED" {
		t.Fatalf("old CI job lease was not completed before pin: %s", oldStatus)
	}
	var newJobPayload []byte
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT payload FROM jobs WHERE task_id=$1::uuid AND generation=1 AND operation_type='ci_reconcile'`, f.taskID).Scan(&newJobPayload); err != nil {
		t.Fatal(err)
	}
	newJob, err := contracts.DecodeJob(newJobPayload)
	if err != nil || newJob.Snapshot.IntegrationSHA != ciIntegration || newJob.Generation != 1 {
		t.Fatalf("successor job did not carry pinned snapshot: job=%+v err=%v", newJob, err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT ci_status,approved_head_sha,ci_deadline_at FROM prs WHERE id=$1`, f.prID).Scan(&ciStatus, &approvedHead, &deadline); err != nil {
		t.Fatal(err)
	}
	if ciStatus != "PENDING" || approvedHead != nil || deadline == nil {
		t.Fatalf("pin failed to invalidate gates/start deadline: ci=%s approval=%v deadline=%v", ciStatus, approvedHead, deadline)
	}
	jobs, outbox := ciJobCounts(t, f)
	if jobs != 2 || outbox != 2 {
		t.Fatalf("pin transaction did not queue one successor: jobs=%d outbox=%d", jobs, outbox)
	}
	routerInstance, err := router.New(f.db.Pool, f.clock, router.Config{
		Repositories: map[string]router.RepositoryPolicy{
			"ci/test": {OrgID: f.orgID, PolicyVersion: "ci-test-v1", TaskBudgetLimitMicroUSD: 5_000_000},
		},
		TrustedCIAppIDs: map[int64]struct{}{902: {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	statusHint := []byte(`{"repository":{"full_name":"ci/test"},"check_run":{"head_sha":"` + ciIntegration + `","app":{"id":902}}}`)
	if err := storage.WithUnitOfWork(f.ctx, f.db.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		_, err := repos.StoreWebhookDelivery(ctx, storage.DeliveryReceipt{DeliveryID: "ci-pinned-status-hint", EventType: "check_run", Payload: statusHint})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := routerInstance.RouteDelivery(f.ctx, "ci-pinned-status-hint"); err != nil {
		t.Fatalf("router status hint did not coalesce with pinned CI job: %v", err)
	}
	jobs, outbox = ciJobCounts(t, f)
	if jobs != 2 || outbox != 2 {
		t.Fatalf("router hint duplicated pinned reconciliation: jobs=%d outbox=%d", jobs, outbox)
	}
}

func TestCIReconcilePassQueuesReviewOnce(t *testing.T) {
	f := ciRequireFixture(t, ciIntegration, time.Date(2026, 10, 1, 18, 30, 0, 0, time.UTC))
	jobID := ciCreateJob(t, f, ciSnapshot(ciIntegration), 0)
	lease := ciClaim(t, f, jobID)
	observations := ciPassingObservations(ciSnapshot(ciIntegration), ciTestConfig(), f.clock.Now())
	var result ci.Aggregate
	ciWithLockedTask(t, f, func(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask) error {
		var err error
		result, err = ci.Reconcile(ctx, repos, locked, lease, f.clock, ciTestConfig(), observations)
		return err
	})
	if result.Outcome != ci.OutcomePassed {
		t.Fatalf("aggregate did not pass: %+v", result)
	}
	state, generation, _ := ciState(t, f)
	if state != string(lifecycle.InReview) || generation != 0 {
		t.Fatalf("CI pass did not enter review without changing snapshot generation: %s/%d", state, generation)
	}
	jobs, outbox := ciJobCounts(t, f)
	if jobs != 2 || outbox != 2 {
		t.Fatalf("expected one review job/outbox: jobs=%d outbox=%d", jobs, outbox)
	}
	var status string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT ci_status FROM prs WHERE id=$1`, f.prID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCESS" {
		t.Fatalf("passing aggregate status=%s", status)
	}
	ciWithLockedTask(t, f, func(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask) error {
		_, err := ci.Reconcile(ctx, repos, locked, lease, f.clock, ciTestConfig(), nil)
		return err
	})
	jobs, outbox = ciJobCounts(t, f)
	if jobs != 2 || outbox != 2 {
		t.Fatalf("duplicate completion queued B more than once: jobs=%d outbox=%d", jobs, outbox)
	}
}

func TestCIPendingRetryAndDeadlineEscalation(t *testing.T) {
	deadlineAt := time.Date(2026, 10, 1, 18, 10, 0, 0, time.UTC)
	f := ciRequireFixture(t, ciIntegration, deadlineAt)
	jobID := ciCreateJob(t, f, ciSnapshot(ciIntegration), 0)
	firstLease := ciClaim(t, f, jobID)
	config := ciTestConfig()
	var result ci.Aggregate
	ciWithLockedTask(t, f, func(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask) error {
		var err error
		result, err = ci.Reconcile(ctx, repos, locked, firstLease, f.clock, config, nil)
		return err
	})
	if result.Outcome != ci.OutcomePending {
		t.Fatalf("missing check was not pending: %+v", result)
	}
	if _, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{TaskID: f.taskID, JobID: jobID, TTL: time.Hour, AgentType: "A", PromptHash: strings.Repeat("a", 64), SupervisorIdentity: "ci-test-supervisor", CredentialID: "ci-test-credential"}); !errors.Is(err, leases.ErrNotDue) {
		t.Fatalf("early retry claim err=%v, want ErrNotDue", err)
	}
	f.clock.Advance(5 * time.Minute)
	secondLease := ciClaim(t, f, jobID)
	f.clock.Set(deadlineAt)
	latePass := ciPassingObservations(ciSnapshot(ciIntegration), config, deadlineAt)
	ciWithLockedTask(t, f, func(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask) error {
		var err error
		result, err = ci.Reconcile(ctx, repos, locked, secondLease, f.clock, config, latePass)
		return err
	})
	if result.Outcome != ci.OutcomeTimedOut {
		t.Fatalf("late passing checks were admitted after deadline: %+v", result)
	}
	state, generation, _ := ciState(t, f)
	if state != string(lifecycle.Escalated) || generation != 1 {
		t.Fatalf("deadline did not escalate and fence the previous generation: %s/%d", state, generation)
	}
	var reason, ciStatus string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT reason FROM escalations WHERE task_id=$1::uuid ORDER BY id DESC LIMIT 1`, f.taskID).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT ci_status FROM prs WHERE id=$1`, f.prID).Scan(&ciStatus); err != nil {
		t.Fatal(err)
	}
	if reason != "CI_TIMEOUT" || ciStatus != "ERROR" {
		t.Fatalf("wrong timeout record reason=%s ci_status=%s", reason, ciStatus)
	}
	jobs, _ := ciJobCounts(t, f)
	if jobs != 1 {
		t.Fatalf("late pass queued reviewer work after deadline: jobs=%d", jobs)
	}
}

func TestCIFailureQueuesBoundedRemediation(t *testing.T) {
	f := ciRequireFixture(t, ciIntegration, time.Date(2026, 10, 1, 18, 30, 0, 0, time.UTC))
	jobID := ciCreateJob(t, f, ciSnapshot(ciIntegration), 0)
	lease := ciClaim(t, f, jobID)
	config := ciTestConfig()
	observations := ciPassingObservations(ciSnapshot(ciIntegration), config, f.clock.Now())
	observations[1].Conclusion = "failure"
	ciWithLockedTask(t, f, func(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask) error {
		_, err := ci.Reconcile(ctx, repos, locked, lease, f.clock, config, observations)
		return err
	})
	state, generation, cycle := ciState(t, f)
	if state != string(lifecycle.ChangesRequested) || generation != 0 || cycle != 1 {
		t.Fatalf("CI failure did not count remediation cycle: state=%s generation=%d cycle=%d", state, generation, cycle)
	}
	var remediationAttempt int32
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT remediation_attempt FROM jobs WHERE task_id=$1::uuid AND operation_type='fix'`, f.taskID).Scan(&remediationAttempt); err != nil {
		t.Fatal(err)
	}
	if remediationAttempt != 1 {
		t.Fatalf("fixer remediation attempt=%d, want 1", remediationAttempt)
	}
	jobs, outbox := ciJobCounts(t, f)
	if jobs != 2 || outbox != 2 {
		t.Fatalf("failure should queue exactly one bounded C job: jobs=%d outbox=%d", jobs, outbox)
	}
}

func TestCIFailedAtLimitUsesControlFence(t *testing.T) {
	f := ciRequireFixture(t, ciIntegration, time.Date(2026, 10, 1, 18, 30, 0, 0, time.UTC))
	if _, err := f.db.Pool.Exec(f.ctx, `UPDATE tasks SET cycle_count=max_review_cycles WHERE id=$1::uuid`, f.taskID); err != nil {
		t.Fatal(err)
	}
	jobID := ciCreateJob(t, f, ciSnapshot(ciIntegration), 0)
	lease := ciClaim(t, f, jobID)
	observations := ciPassingObservations(ciSnapshot(ciIntegration), ciTestConfig(), f.clock.Now())
	observations[0].Conclusion = "failure"
	ciWithLockedTask(t, f, func(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask) error {
		_, err := ci.Reconcile(ctx, repos, locked, lease, f.clock, ciTestConfig(), observations)
		return err
	})
	state, generation, cycles := ciState(t, f)
	if state != string(lifecycle.Escalated) || generation != 1 || cycles != 3 {
		t.Fatalf("limit did not fence without consuming another attempt: %s/%d/%d", state, generation, cycles)
	}
	var actions, fixes int
	var approvalsCleared bool
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM control_actions WHERE task_id=$1::uuid AND action='escalate' AND reason='REMEDIATION_LIMIT'`, f.taskID).Scan(&actions); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid AND operation_type='fix'`, f.taskID).Scan(&fixes); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT approved_head_sha IS NULL AND approved_base_sha IS NULL AND human_approval_id IS NULL FROM prs WHERE id=$1`, f.prID).Scan(&approvalsCleared); err != nil {
		t.Fatal(err)
	}
	if actions != 1 || fixes != 0 || !approvalsCleared {
		t.Fatalf("fence effects: actions=%d fixes=%d approvalsCleared=%v", actions, fixes, approvalsCleared)
	}
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, lease); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("old generation retained authority: %v", err)
	}
}
