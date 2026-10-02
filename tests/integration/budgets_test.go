package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

const budgetPromptHash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

type budgetFixture struct {
	ctx      context.Context
	db       testutil.DatabaseFixture
	clock    *clock.Manual
	orgID    string
	sequence int
}

type budgetRun struct {
	taskID   string
	jobID    string
	lease    leases.Lease
	envelope contracts.BudgetEnvelope
}

func budgetNewFixture(t *testing.T, orgLimit int64) *budgetFixture {
	t.Helper()
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatalf("migrate budget fixture: %v", err)
	}
	const orgID = "budget-test-org"
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id,rolling_limit_micro_usd) VALUES($1,$2)`, orgID, orgLimit); err != nil {
		t.Fatalf("insert budget organization: %v", err)
	}
	return &budgetFixture{ctx: ctx, db: db, clock: clock.NewManual(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)), orgID: orgID}
}

func (f *budgetFixture) newRun(t *testing.T, taskLimit int64, envelope contracts.BudgetEnvelope, operation string) budgetRun {
	t.Helper()
	f.sequence++
	seq := f.sequence
	taskID := fmt.Sprintf("%08x-aaaa-4aaa-8aaa-%012x", seq, seq)
	jobID := fmt.Sprintf("%08x-bbbb-4bbb-8bbb-%012x", seq, seq)
	operationID := fmt.Sprintf("%08x-cccc-4ccc-8ccc-%012x", seq, seq)
	correlationID := fmt.Sprintf("%08x-dddd-4ddd-8ddd-%012x", seq, seq)
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO tasks(id,org_id,repo_full_name,source_key,owner_id,budget_limit_micro_usd,policy_version)
		VALUES($1::uuid,$2,$3,$4,'owner',$5,'budget-test-v1')`, taskID, f.orgID, "owner/repo", fmt.Sprintf("issue-%d", seq), taskLimit); err != nil {
		t.Fatalf("insert budget task: %v", err)
	}
	payload, err := json.Marshal(contracts.Job{
		Version: 1, TaskID: taskID, JobID: jobID, Generation: 0, Snapshot: contracts.Snapshot{}, Attempt: 1,
		OperationID: operationID, CorrelationID: correlationID, Operation: operation, Envelope: envelope,
	})
	if err != nil {
		t.Fatalf("marshal budget job: %v", err)
	}
	if _, err := f.db.Pool.Exec(f.ctx, `INSERT INTO jobs(id,task_id,logical_key,operation_type,generation,payload)
		VALUES($1::uuid,$2::uuid,$3,$4,0,$5::jsonb)`, jobID, taskID, fmt.Sprintf("budget:%s", jobID), operation, payload); err != nil {
		t.Fatalf("insert budget job: %v", err)
	}
	agentType, supported := leases.AgentTypeForOperation(operation)
	if !supported {
		t.Fatalf("test operation %q does not have an agent mapping", operation)
	}
	lease, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{
		TaskID: taskID, JobID: jobID, TTL: time.Minute, AgentType: agentType,
		PromptHash: budgetPromptHash, SupervisorIdentity: "budget-test-supervisor", CredentialID: "budget-test-credential-ref",
	})
	if err != nil {
		t.Fatalf("claim budget test run: %v", err)
	}
	return budgetRun{taskID: taskID, jobID: jobID, lease: lease, envelope: envelope}
}

func budgetEnvelope(maxMicroUSD int64) contracts.BudgetEnvelope {
	return contracts.BudgetEnvelope{MaxCostMicroUSD: maxMicroUSD, MaxInputTokens: 10, MaxOutputTokens: 10, MaxCalls: 2, PricingVersion: "fixture-rate-card-v1"}
}

func budgetCharge(reservation budget.Reservation, requestID string, cost int64, usage string) budget.ChargeRequest {
	return budget.ChargeRequest{RequestID: requestID, ReservationID: reservation.ID, RunID: reservation.RunID,
		Model: "fixture-model", Usage: json.RawMessage(usage), CostMicroUSD: cost}
}

func TestBudgetsReserveAndTaskLimit(t *testing.T) {
	f := budgetNewFixture(t, 100)
	run := f.newRun(t, 100, budgetEnvelope(60), "author")
	reservation, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope)
	if err != nil {
		t.Fatalf("reserve inference: %v", err)
	}
	if reservation.EnvelopeMicroUSD != 60 || reservation.RemainingMicroUSD != 60 || reservation.Status != "RESERVED" {
		t.Fatalf("unexpected reservation: %+v", reservation)
	}
	if reservation.Envelope != run.envelope {
		t.Fatalf("persisted admission envelope=%+v, want %+v", reservation.Envelope, run.envelope)
	}
	replay, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope)
	if err != nil || replay.ID != reservation.ID {
		t.Fatalf("idempotent reserve=(%+v,%v), want existing %s", replay, err, reservation.ID)
	}
	var taskSpent, taskReserved int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT spent_micro_usd,reserved_micro_usd FROM tasks WHERE id=$1::uuid`, run.taskID).Scan(&taskSpent, &taskReserved); err != nil {
		t.Fatal(err)
	}
	if taskSpent != 0 || taskReserved != 60 {
		t.Fatalf("task spent/reserved=(%d,%d), want 0/60", taskSpent, taskReserved)
	}
	tooLarge := f.newRun(t, 50, budgetEnvelope(51), "author")
	if _, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, tooLarge.lease, tooLarge.envelope); !errors.Is(err, budget.ErrTaskBudget) {
		t.Fatalf("reserve beyond task limit=%v, want ErrTaskBudget", err)
	}
	mismatched := f.newRun(t, 100, budgetEnvelope(10), "author")
	if _, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, mismatched.lease, budgetEnvelope(11)); !errors.Is(err, budget.ErrInvalidEnvelope) {
		t.Fatalf("reserve with host envelope differing from durable job=%v, want ErrInvalidEnvelope", err)
	}
}

func TestBudgetsConcurrentReservationsRespectOrganizationLimit(t *testing.T) {
	f := budgetNewFixture(t, 10)
	first := f.newRun(t, 10, budgetEnvelope(7), "author")
	second := f.newRun(t, 10, budgetEnvelope(7), "author")
	start := make(chan struct{})
	type reserveResult struct {
		reservation budget.Reservation
		err         error
	}
	results := make(chan reserveResult, 2)
	var wait sync.WaitGroup
	for _, run := range []budgetRun{first, second} {
		wait.Add(1)
		go func(run budgetRun) {
			defer wait.Done()
			<-start
			reservation, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope)
			results <- reserveResult{reservation: reservation, err: err}
		}(run)
	}
	close(start)
	wait.Wait()
	close(results)
	reserved, denied := 0, 0
	for result := range results {
		if result.err == nil {
			reserved++
		} else if errors.Is(result.err, budget.ErrOrganizationBudget) {
			denied++
		} else {
			t.Fatalf("concurrent reserve error=%v", result.err)
		}
	}
	if reserved != 1 || denied != 1 {
		t.Fatalf("concurrent org admissions: reserved=%d denied=%d, want 1/1", reserved, denied)
	}
	var sum int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT COALESCE(sum(remaining_micro_usd),0) FROM budget_reservations WHERE org_id=$1 AND status<>'SETTLED'`, f.orgID).Scan(&sum); err != nil {
		t.Fatal(err)
	}
	if sum > 10 {
		t.Fatalf("outstanding reservation sum=%d exceeds org cap 10", sum)
	}
}

func TestBudgetsConcurrentSettlementAndAdmission(t *testing.T) {
	f := budgetNewFixture(t, 100)
	oldRun := f.newRun(t, 100, budgetEnvelope(50), "author")
	oldReservation, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, oldRun.lease, oldRun.envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, budgetCharge(oldReservation, "settlement-race-charge", 10, `{"tokens":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := leases.Complete(f.ctx, f.db.Pool, f.clock, oldRun.lease, "SUCCESS"); err != nil {
		t.Fatal(err)
	}
	newRun := f.newRun(t, 100, budgetEnvelope(90), "author")
	start := make(chan struct{})
	settled := make(chan error, 1)
	reserved := make(chan error, 1)
	go func() {
		<-start
		settled <- budget.SettleReservation(f.ctx, f.db.Pool, f.clock, budget.SettlementRequest{ReservationID: oldReservation.ID, RunID: oldRun.lease.RunID, FinalUsageKnown: true})
	}()
	go func() {
		<-start
		_, reserveErr := budget.Reserve(f.ctx, f.db.Pool, f.clock, newRun.lease, newRun.envelope)
		reserved <- reserveErr
	}()
	close(start)
	if err := <-settled; err != nil {
		t.Fatalf("settle concurrently with admission: %v", err)
	}
	if err := <-reserved; err != nil && !errors.Is(err, budget.ErrOrganizationBudget) {
		t.Fatalf("concurrent admission error=%v, want success or serialized budget denial", err)
	} else if errors.Is(err, budget.ErrOrganizationBudget) {
		if _, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, newRun.lease, newRun.envelope); err != nil {
			t.Fatalf("admit after settlement releases unused coverage: %v", err)
		}
	}
	var total string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT (COALESCE((SELECT SUM(cost_micro_usd) FROM cost_entries ce JOIN budget_reservations br ON br.id=ce.reservation_id WHERE br.org_id=$1),0)+COALESCE((SELECT SUM(remaining_micro_usd) FROM budget_reservations WHERE org_id=$1 AND status IN ('RESERVED','SETTLING','UNKNOWN')),0))::text`, f.orgID).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != "100" {
		t.Fatalf("settled cost plus outstanding reservations=%s, want 100", total)
	}
}

func TestBudgetsChargeReplayUnknownAndRevokedBilling(t *testing.T) {
	f := budgetNewFixture(t, 100)
	run := f.newRun(t, 100, budgetEnvelope(50), "author")
	reservation, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.MarkUnknown(f.ctx, f.db.Pool, f.clock, reservation.ID); err != nil {
		t.Fatalf("mark provider usage unknown: %v", err)
	}
	var reservedBefore int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT remaining_micro_usd FROM budget_reservations WHERE id=$1::uuid`, reservation.ID).Scan(&reservedBefore); err != nil {
		t.Fatal(err)
	}
	if reservedBefore != 50 {
		t.Fatalf("unknown usage released coverage: remaining=%d, want 50", reservedBefore)
	}
	settlement := budget.SettlementRequest{ReservationID: reservation.ID, RunID: run.lease.RunID, FinalUsageKnown: true}
	if err := budget.SettleReservation(f.ctx, f.db.Pool, f.clock, settlement); !errors.Is(err, budget.ErrAdmission) {
		t.Fatalf("settle a still-running run=%v, want ErrAdmission", err)
	}
	f.clock.Advance(time.Minute)
	replacement, err := leases.Claim(f.ctx, f.db.Pool, f.clock, leases.ClaimRequest{
		TaskID: run.taskID, JobID: run.jobID, TTL: time.Minute, AgentType: "A", PromptHash: budgetPromptHash,
		SupervisorIdentity: "budget-test-supervisor", CredentialID: "replacement-credential-ref",
	})
	if err != nil {
		t.Fatalf("replace expired lease: %v", err)
	}
	if err := leases.Validate(f.ctx, f.db.Pool, f.clock, run.lease); !errors.Is(err, leases.ErrStale) {
		t.Fatalf("old lease validated after replacement: %v", err)
	}
	charged := budgetCharge(reservation, "metered-request-1", 30, `{"input_tokens":4,"output_tokens":2}`)
	chargeResult, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, charged)
	if err != nil {
		t.Fatalf("record historical charge after revocation: %v", err)
	}
	if chargeResult.Duplicate || chargeResult.OverEnvelope {
		t.Fatalf("unexpected initial charge result: %+v", chargeResult)
	}
	replay, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, charged)
	if err != nil || !replay.Duplicate {
		t.Fatalf("identical request replay=(%+v,%v), want duplicate", replay, err)
	}
	conflicting := charged
	conflicting.CostMicroUSD++
	if _, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, conflicting); !errors.Is(err, budget.ErrRequestConflict) {
		t.Fatalf("conflicting request ID=%v, want ErrRequestConflict", err)
	}
	var taskSpent, taskReserved int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT spent_micro_usd,reserved_micro_usd FROM tasks WHERE id=$1::uuid`, run.taskID).Scan(&taskSpent, &taskReserved); err != nil {
		t.Fatal(err)
	}
	if taskSpent != 30 || taskReserved != 20 {
		t.Fatalf("after charge spent/reserved=%d/%d, want 30/20", taskSpent, taskReserved)
	}
	if err := leases.Complete(f.ctx, f.db.Pool, f.clock, replacement, "TERMINATED"); err != nil {
		t.Fatalf("terminate replacement run: %v", err)
	}
	settlement.FinalUsageKnown = false
	if err := budget.SettleReservation(f.ctx, f.db.Pool, f.clock, settlement); !errors.Is(err, budget.ErrUsageIncomplete) {
		t.Fatalf("settle unknown usage without final reconciliation=%v, want ErrUsageIncomplete", err)
	}
	settlement.FinalUsageKnown = true
	if err := budget.SettleReservation(f.ctx, f.db.Pool, f.clock, settlement); err != nil {
		t.Fatalf("settle revoked historical run after reconciliation: %v", err)
	}
	if err := budget.SettleReservation(f.ctx, f.db.Pool, f.clock, settlement); err != nil {
		t.Fatalf("idempotent reservation settlement: %v", err)
	}
	var status string
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM budget_reservations WHERE id=$1::uuid`, reservation.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SETTLED" {
		t.Fatalf("reservation status=%q, want SETTLED", status)
	}
	settledReplay, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, charged)
	if err != nil || !settledReplay.Duplicate {
		t.Fatalf("identical charge replay after settlement=(%+v,%v), want duplicate", settledReplay, err)
	}
	lateCharge := budgetCharge(reservation, "late-metered-request", 5, `{"input_tokens":1,"output_tokens":1}`)
	late, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, lateCharge)
	if err != nil || !late.SettlementReopened {
		t.Fatalf("late authoritative charge=(%+v,%v), want persisted reopened usage", late, err)
	}
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT status FROM budget_reservations WHERE id=$1::uuid`, reservation.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "UNKNOWN" {
		t.Fatalf("late charge status=%q, want UNKNOWN", status)
	}
	var lateEmergency bool
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT emergency_mode FROM org_budgets WHERE org_id=$1`, f.orgID).Scan(&lateEmergency); err != nil {
		t.Fatal(err)
	}
	if !lateEmergency {
		t.Fatal("late authoritative charge did not fence organization")
	}
	settlement.FinalUsageKnown = false
	if err := budget.SettleReservation(f.ctx, f.db.Pool, f.clock, settlement); !errors.Is(err, budget.ErrUsageIncomplete) {
		t.Fatalf("settle reopened usage without reconciliation=%v, want ErrUsageIncomplete", err)
	}
}

func TestBudgetsRollingWindowBoundary(t *testing.T) {
	for _, item := range []struct {
		name      string
		elapsed   time.Duration
		wantError bool
	}{{"just-before", 24*time.Hour - time.Nanosecond, true}, {"exact-boundary", 24 * time.Hour, false}} {
		t.Run(item.name, func(t *testing.T) {
			f := budgetNewFixture(t, 100)
			run := f.newRun(t, 100, budgetEnvelope(60), "author")
			reservation, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, budgetCharge(reservation, "rolling-charge", 60, `{"tokens":1}`)); err != nil {
				t.Fatalf("record rolling-window charge: %v", err)
			}
			if err := leases.Complete(f.ctx, f.db.Pool, f.clock, run.lease, "SUCCESS"); err != nil {
				t.Fatalf("complete billed run: %v", err)
			}
			if err := budget.SettleReservation(f.ctx, f.db.Pool, f.clock, budget.SettlementRequest{ReservationID: reservation.ID, RunID: run.lease.RunID, FinalUsageKnown: true}); err != nil {
				t.Fatalf("settle billed run: %v", err)
			}
			f.clock.Advance(item.elapsed)
			next := f.newRun(t, 100, budgetEnvelope(100), "author")
			_, err = budget.Reserve(f.ctx, f.db.Pool, f.clock, next.lease, next.envelope)
			if item.wantError && !errors.Is(err, budget.ErrOrganizationBudget) {
				t.Fatalf("reserve at %s error=%v, want org budget denial", item.elapsed, err)
			}
			if !item.wantError && err != nil {
				t.Fatalf("reserve at exact 24-hour boundary: %v", err)
			}
		})
	}
}

func TestBudgetsOverrunFencesOrganizationAndRecordsExactCharge(t *testing.T) {
	f := budgetNewFixture(t, 100)
	run := f.newRun(t, 10, budgetEnvelope(10), "author")
	reservation, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, budgetCharge(reservation, "overrun-request", 15, `{"tokens":8}`))
	if err != nil {
		t.Fatalf("record provider overrun: %v", err)
	}
	if !result.OverEnvelope || !result.OverTaskBudget {
		t.Fatalf("overrun result=%+v, want envelope+task budget breach", result)
	}
	var emergency bool
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT emergency_mode FROM org_budgets WHERE org_id=$1`, f.orgID).Scan(&emergency); err != nil {
		t.Fatal(err)
	}
	if !emergency {
		t.Fatal("provider envelope overrun did not fence organization inference")
	}
	if _, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope); !errors.Is(err, budget.ErrEmergency) {
		t.Fatalf("replay reservation while org emergency=%v, want ErrEmergency", err)
	}
	var cost, spent int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT c.cost_micro_usd,t.spent_micro_usd FROM cost_entries c JOIN budget_reservations r ON r.id=c.reservation_id JOIN tasks t ON t.id=r.task_id WHERE c.request_id='overrun-request'`).Scan(&cost, &spent); err != nil {
		t.Fatal(err)
	}
	if cost != 15 || spent != 15 {
		t.Fatalf("exact persisted overrun cost/task spend=%d/%d, want 15/15", cost, spent)
	}
	blocked := f.newRun(t, 100, budgetEnvelope(1), "author")
	if _, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, blocked.lease, blocked.envelope); !errors.Is(err, budget.ErrEmergency) {
		t.Fatalf("reserve while org emergency=%v, want ErrEmergency", err)
	}
}

func TestBudgetsUnknownEnvelopeAndControlOperationDenied(t *testing.T) {
	f := budgetNewFixture(t, 100)
	noPricing := contracts.BudgetEnvelope{}
	run := f.newRun(t, 100, noPricing, "author")
	if _, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope); !errors.Is(err, budget.ErrInvalidEnvelope) {
		t.Fatalf("reserve missing pricing identity=%v, want ErrInvalidEnvelope", err)
	}
	control := f.newRun(t, 100, contracts.BudgetEnvelope{}, "ci_reconcile")
	if _, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, control.lease, control.envelope); !errors.Is(err, budget.ErrAdmission) {
		t.Fatalf("reserve control operation=%v, want ErrAdmission", err)
	}
}

func TestBudgetsOverflowSaturatesCounterButKeepsExactLedger(t *testing.T) {
	f := budgetNewFixture(t, math.MaxInt64)
	limit := int64(math.MaxInt64)
	run := f.newRun(t, limit, budgetEnvelope(limit), "author")
	reservation, err := budget.Reserve(f.ctx, f.db.Pool, f.clock, run.lease, run.envelope)
	if err != nil {
		t.Fatal(err)
	}
	first, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, budgetCharge(reservation, "huge-1", 6_000_000_000_000_000_000, `{"tokens":1}`))
	if err != nil || first.TaskSpendSaturated {
		t.Fatalf("first huge charge=(%+v,%v), want exact unsaturated charge", first, err)
	}
	second, err := budget.RecordCharge(f.ctx, f.db.Pool, f.clock, budgetCharge(reservation, "huge-2", 6_000_000_000_000_000_000, `{"tokens":2}`))
	if err != nil || !second.TaskSpendSaturated || !second.OverTaskBudget {
		t.Fatalf("overflowing huge charge=(%+v,%v), want saturated over-budget result", second, err)
	}
	var exact string
	var saturated int64
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT SUM(c.cost_micro_usd)::text,t.spent_micro_usd FROM cost_entries c JOIN budget_reservations r ON r.id=c.reservation_id JOIN tasks t ON t.id=r.task_id WHERE r.task_id=$1::uuid GROUP BY t.spent_micro_usd`, run.taskID).Scan(&exact, &saturated); err != nil {
		t.Fatal(err)
	}
	if exact != "12000000000000000000" || saturated != math.MaxInt64 {
		t.Fatalf("exact cost/saturated cache=%s/%d, want 12000000000000000000/%d", exact, saturated, int64(math.MaxInt64))
	}
}
