package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/queue"
)

type unresolvedAdmissionVerifier struct{}

func (unresolvedAdmissionVerifier) VerifyNeverStarted(context.Context, processholds.Hold) (processholds.ReapEvidence, error) {
	return processholds.ReapEvidence{}, errors.New("launch outcome is unresolved")
}

func (unresolvedAdmissionVerifier) VerifyGroupDrained(context.Context, processholds.Hold) (processholds.ReapEvidence, error) {
	return processholds.ReapEvidence{}, errors.New("process ownership is unresolved")
}

func TestAdmissionAmbiguousProcessHoldRetainsCoverage(t *testing.T) {
	for _, mode := range []string{"unknown", "group_drained", "never_started"} {
		t.Run(mode, func(t *testing.T) {
			f := newDispatchTestFixture(t)
			jobID := f.addJob(t, "author")
			if err := f.streams.EnsureGroup(f.ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := f.streams.Publish(f.ctx, queue.DispatchKind, jobID); err != nil {
				t.Fatal(err)
			}
			deliveries, err := f.streams.Read(f.ctx, "process-hold-admission", time.Millisecond, 1)
			if err != nil || len(deliveries) != 1 {
				t.Fatalf("read admission delivery: (%v, %v)", deliveries, err)
			}
			var verifier processholds.ProcessEvidenceVerifier = unresolvedAdmissionVerifier{}
			if mode != "unknown" {
				verifier = processholdsFixtureVerifier{now: f.clock.Now}
			}
			holds, err := processholds.NewStore(f.db.Pool, f.clock, processholds.Config{
				ResourceScope: "fixture-admission-pool", MaxActive: 1, VerifierTimeout: time.Second,
			}, verifier)
			if err != nil {
				t.Fatal(err)
			}
			interrupted := errors.New("host admission outcome ambiguous")
			var original leases.Lease
			executed := false
			consumer, err := queue.NewConsumer(f.db.Pool, f.clock, f.streams, queue.ConsumerConfig{
				LeaseTTL: time.Minute, ReclaimIdle: time.Second, PromptHash: dispatchTestPromptHash,
				SupervisorIdentity: "host-supervisor-test", CredentialID: "credential-ref-test",
				BatchSize: 1, Block: time.Millisecond, OperationTimeout: 3 * time.Second,
				BudgetAdmission: func(ctx context.Context, lease leases.Lease, _ contracts.Job) error {
					original = lease
					if _, err := budget.Reserve(ctx, f.db.Pool, f.clock, lease, contracts.BudgetEnvelope{
						MaxCostMicroUSD: 1000, MaxInputTokens: 10, MaxOutputTokens: 10, MaxCalls: 1, PricingVersion: "test-v1",
					}); err != nil {
						return err
					}
					// Durable host launch intent can outlive its response. Absence of a
					// process tuple cannot establish never-started cleanup authority.
					hold, err := holds.Reserve(ctx, lease, "/owned/ambiguous-fixture", "host-supervisor-test")
					if err != nil {
						return err
					}
					if mode == "group_drained" {
						if _, err := holds.Started(ctx, hold.RunID, processholds.ProcessIdentity{PID: 42, PGID: 40, StartIdentity: "fixture-owned-start"}); err != nil {
							return err
						}
					}
					if mode == "unknown" {
						if _, err := holds.Unknown(ctx, hold.RunID, processholds.ReasonStartAmbiguous); err != nil {
							return err
						}
					} else if _, err := holds.Reaped(ctx, hold.RunID); err != nil {
						return err
					}
					return interrupted
				},
			}, func(context.Context, leases.Lease) error { executed = true; return nil })
			if err != nil {
				t.Fatal(err)
			}
			if err := consumer.Handle(f.ctx, "process-hold-admission", deliveries[0]); !errors.Is(err, interrupted) || executed {
				t.Fatalf("ambiguous admission: err=%v executed=%v", err, executed)
			}
			var runStatus, reservationStatus, jobStatus string
			var remaining, reserved int64
			if err := f.db.Pool.QueryRow(f.ctx, `SELECT r.execution_status,b.status,b.remaining_micro_usd,t.reserved_micro_usd,j.status
		FROM agent_runs r JOIN budget_reservations b ON b.run_id=r.id JOIN tasks t ON t.id=r.task_id
		JOIN jobs j ON j.id=r.job_id WHERE r.id=$1::uuid`, original.RunID).
				Scan(&runStatus, &reservationStatus, &remaining, &reserved, &jobStatus); err != nil {
				t.Fatal(err)
			}
			wantStatus, wantRemaining := "UNKNOWN", int64(1000)
			if mode == "never_started" {
				wantStatus, wantRemaining = "SETTLED", 0
			}
			if runStatus != "TERMINATED" || reservationStatus != wantStatus || remaining != wantRemaining || reserved != wantRemaining || jobStatus != "PENDING" {
				t.Fatalf("ambiguous cleanup run=%s reservation=%s remaining=%d reserved=%d job=%s", runStatus, reservationStatus, remaining, reserved, jobStatus)
			}
			if _, err := holds.Reaped(f.ctx, original.RunID); mode == "unknown" && err == nil {
				t.Fatal("unresolved host proof released process capacity")
			}
			if unresolved, err := holds.HasUnresolvedTask(f.ctx, original.TaskID); err != nil || unresolved != (mode == "unknown") {
				t.Fatalf("unresolved launch reservation lost: held=%v err=%v", unresolved, err)
			}
		})
	}
}
