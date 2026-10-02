// Package budget reserves task and organization capacity before inference and
// records metered usage durably after each provider request.
package budget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const rollingWindow = 24 * time.Hour

var (
	// ErrInvalid reports malformed budget admission or settlement input.
	ErrInvalid = errors.New("invalid budget request")
	// ErrEmergency reports that organization inference is disabled by its kill switch.
	ErrEmergency = errors.New("organization budget is in emergency mode")
	// ErrTaskBudget reports that a task has insufficient remaining capacity.
	ErrTaskBudget = errors.New("insufficient task budget")
	// ErrOrganizationBudget reports that an organization has insufficient rolling capacity.
	ErrOrganizationBudget = errors.New("insufficient organization budget")
	// ErrReservationConflict reports incompatible reservation reuse for one run.
	ErrReservationConflict = errors.New("run already has a conflicting budget reservation")
	// ErrRequestConflict reports reuse of a provider request ID with different meaning.
	ErrRequestConflict = errors.New("provider request ID reused with conflicting usage")
	// ErrReservationClosed reports an attempt to add usage after final settlement.
	ErrReservationClosed = errors.New("budget reservation is already settled")
	// ErrUsageIncomplete reports that a caller has not reconciled outstanding request usage.
	ErrUsageIncomplete = errors.New("provider request usage is not final")
	// ErrAdmission reports a run that is not a current, supported inference admission.
	ErrAdmission = errors.New("run is not a current inference admission")
	// ErrInvalidEnvelope reports a missing, incomplete, or untrusted pricing envelope.
	ErrInvalidEnvelope = errors.New("invalid inference budget envelope")
)

// Reservation is the durable inference envelope attached to one agent run.
type Reservation struct {
	ID                string
	TaskID            string
	OrgID             string
	RunID             string
	EnvelopeMicroUSD  int64
	RemainingMicroUSD int64
	PricingVersion    string
	Envelope          contracts.BudgetEnvelope
	Status            string
	CreatedAt         time.Time
	SettledAt         *time.Time
}

// ChargeRequest is one authoritative metered provider response. Usage must be
// a JSON object. Request IDs are global idempotency keys, not worker operation IDs.
type ChargeRequest struct {
	RequestID     string
	ReservationID string
	RunID         string
	Model         string
	Usage         json.RawMessage
	CostMicroUSD  int64
}

// ChargeResult describes committed usage and any overrun requiring an operator
// response. Overruns still persist exact provider facts and fence organization work.
type ChargeResult struct {
	Duplicate                bool
	OverEnvelope             bool
	OverTaskBudget           bool
	OverOrganizationLimit    bool
	TaskSpendSaturated       bool
	TaskReservationSaturated bool
	TaskLedgerMismatch       bool
	SettlementReopened       bool
}

// SettlementRequest closes one run's reservation only after the trusted
// metered gateway or supervisor has reconciled every accepted provider request.
type SettlementRequest struct {
	ReservationID   string
	RunID           string
	FinalUsageKnown bool
}

// Reserve admits the immutable run's envelope in a dedicated transaction.
// The caller may start inference only after this method commits successfully.
func Reserve(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, lease leases.Lease, envelope contracts.BudgetEnvelope) (Reservation, error) {
	if ctx == nil || pool == nil || c == nil {
		return Reservation{}, fmt.Errorf("reserve inference budget: %w", ErrInvalid)
	}
	var result Reservation
	err := storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		var err error
		result, err = ReserveLocked(ctx, repos, c, lease, envelope)
		return err
	})
	if err != nil {
		return Reservation{}, err
	}
	return result, nil
}

// ReserveLocked composes admission with a caller-owned UOW. It must be the
// first lock-taking operation in that UOW (organization then task/PR, then job
// and run), and inference must remain stopped until the UOW commits.
func ReserveLocked(ctx context.Context, repos *storage.Repositories, c clock.Clock, lease leases.Lease, envelope contracts.BudgetEnvelope) (Reservation, error) {
	if repos == nil || c == nil || ctx == nil || !budgetID(lease.TaskID) || !budgetID(lease.JobID) || !budgetID(lease.RunID) || !budgetID(lease.Token) {
		return Reservation{}, fmt.Errorf("reserve inference budget: %w", ErrInvalid)
	}
	_, task, err := lockOrgTask(ctx, repos, lease.TaskID)
	if err != nil {
		return Reservation{}, err
	}
	if err := leases.ValidateLocked(ctx, repos, lease, c); err != nil {
		return Reservation{}, fmt.Errorf("validate inference lease: %w", ErrAdmission)
	}
	job, err := loadLeaseJob(ctx, repos, lease.TaskID, lease.JobID)
	if err != nil {
		return Reservation{}, err
	}
	if !leases.RequiresInference(job.Operation) {
		return Reservation{}, fmt.Errorf("operation %q does not use inference: %w", job.Operation, ErrAdmission)
	}
	if job.Envelope != (contracts.BudgetEnvelope{}) && job.Envelope != envelope {
		return Reservation{}, fmt.Errorf("host envelope differs from durable job envelope: %w", ErrInvalidEnvelope)
	}
	if err := envelope.Validate(); err != nil {
		return Reservation{}, fmt.Errorf("validate pinned inference envelope: %w", ErrInvalidEnvelope)
	}
	if task.OrgID == "" {
		return Reservation{}, fmt.Errorf("task has no organization budget: %w", ErrInvalid)
	}
	// A reservation is unique per immutable run. Exact admission retries return
	// the existing active reservation without spending capacity a second time.
	orgBudget, err := lockedOrgBudget(ctx, repos, task.OrgID)
	if err != nil {
		return Reservation{}, err
	}
	if orgBudget.EmergencyMode {
		return Reservation{}, ErrEmergency
	}
	existing, found, err := reservationForRun(ctx, repos, lease.TaskID, lease.RunID)
	if err != nil {
		return Reservation{}, err
	}
	if found {
		if existing.Envelope != envelope || existing.EnvelopeMicroUSD != envelope.MaxCostMicroUSD || existing.PricingVersion != envelope.PricingVersion || existing.Status != "RESERVED" {
			return Reservation{}, ErrReservationConflict
		}
		return existing, nil
	}
	if task.BudgetLimitMicroUSD < 0 || task.SpentMicroUSD < 0 || task.ReservedMicroUSD < 0 {
		return Reservation{}, ErrTaskBudget
	}
	var taskHasCapacity bool
	if err := repos.Queries().QueryRow(ctx, `SELECT $2::numeric <= $1::numeric
		- COALESCE((SELECT SUM(c.cost_micro_usd) FROM cost_entries c JOIN budget_reservations r ON r.id=c.reservation_id WHERE r.task_id=$3::uuid),0)
		- COALESCE((SELECT SUM(remaining_micro_usd) FROM budget_reservations WHERE task_id=$3::uuid AND status IN ('RESERVED','SETTLING','UNKNOWN')),0)`,
		task.BudgetLimitMicroUSD, envelope.MaxCostMicroUSD, task.ID).Scan(&taskHasCapacity); err != nil {
		return Reservation{}, fmt.Errorf("check authoritative task budget: %w", err)
	}
	if !taskHasCapacity {
		return Reservation{}, ErrTaskBudget
	}
	now := c.Now().UTC()
	if orgBudget.RollingLimitMicroUSD < 0 {
		return Reservation{}, fmt.Errorf("negative organization limit: %w", ErrInvalid)
	}
	var orgHasCapacity bool
	err = repos.Queries().QueryRow(ctx, `SELECT $1::numeric <= $2::numeric
		- COALESCE((SELECT SUM(c.cost_micro_usd) FROM cost_entries c
			JOIN budget_reservations r ON r.id=c.reservation_id WHERE r.org_id=$3
			AND c.charged_at > $4 AND c.charged_at <= $5),0)
		- COALESCE((SELECT SUM(remaining_micro_usd) FROM budget_reservations
			WHERE org_id=$3 AND status IN ('RESERVED','SETTLING','UNKNOWN')),0)`,
		envelope.MaxCostMicroUSD, orgBudget.RollingLimitMicroUSD, task.OrgID,
		now.Add(-rollingWindow), now).Scan(&orgHasCapacity)
	if err != nil {
		return Reservation{}, fmt.Errorf("check rolling organization budget: %w", err)
	}
	if !orgHasCapacity {
		return Reservation{}, ErrOrganizationBudget
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		return Reservation{}, fmt.Errorf("encode inference envelope: %w", err)
	}
	var result Reservation
	var savedEnvelope []byte
	err = repos.Queries().QueryRow(ctx, `INSERT INTO budget_reservations
		(task_id,org_id,run_id,envelope_micro_usd,remaining_micro_usd,pricing_version,admission_envelope,status,created_at)
		VALUES($1::uuid,$2,$3::uuid,$4,$4,$5,$6::jsonb,'RESERVED',$7)
		RETURNING id::text,task_id::text,org_id,run_id::text,envelope_micro_usd,remaining_micro_usd,
		pricing_version,admission_envelope,status,created_at,settled_at`, lease.TaskID, task.OrgID, lease.RunID,
		envelope.MaxCostMicroUSD, envelope.PricingVersion, string(envelopeJSON), now).Scan(
		&result.ID, &result.TaskID, &result.OrgID, &result.RunID, &result.EnvelopeMicroUSD,
		&result.RemainingMicroUSD, &result.PricingVersion, &savedEnvelope, &result.Status, &result.CreatedAt, &result.SettledAt)
	if err != nil {
		return Reservation{}, fmt.Errorf("create inference budget reservation: %w", err)
	}
	if err := json.Unmarshal(savedEnvelope, &result.Envelope); err != nil {
		return Reservation{}, fmt.Errorf("decode saved inference envelope: %w", err)
	}
	_, err = repos.Queries().Exec(ctx, `UPDATE tasks SET reserved_micro_usd=reserved_micro_usd+$2,updated_at=$3
		WHERE id=$1::uuid`, lease.TaskID, envelope.MaxCostMicroUSD, now)
	if err != nil {
		return Reservation{}, fmt.Errorf("apply task budget reservation: %w", err)
	}
	return result, nil
}

// RecordCharge writes one provider request's actual usage. It deliberately
// does not require a current worker lease: accepted billing remains valid after
// cancellation or revocation. The run and reservation identities are durable.
func RecordCharge(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, request ChargeRequest) (ChargeResult, error) {
	if ctx == nil || pool == nil || c == nil {
		return ChargeResult{}, fmt.Errorf("record provider charge: %w", ErrInvalid)
	}
	var result ChargeResult
	err := storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		var err error
		result, err = RecordChargeLocked(ctx, repos, c, request)
		return err
	})
	if err != nil {
		return ChargeResult{}, err
	}
	return result, nil
}

// RecordChargeLocked composes usage recording into a caller's UOW. It takes
// locks in organization/task/PR/job/run/reservation/request order.
func RecordChargeLocked(ctx context.Context, repos *storage.Repositories, c clock.Clock, request ChargeRequest) (ChargeResult, error) {
	if ctx == nil || repos == nil || c == nil || strings.TrimSpace(request.RequestID) == "" ||
		!budgetID(request.ReservationID) || !budgetID(request.RunID) || strings.TrimSpace(request.Model) == "" || request.CostMicroUSD < 0 || !usageObject(request.Usage) {
		return ChargeResult{}, fmt.Errorf("record provider charge: %w", ErrInvalid)
	}
	route, err := routeReservation(ctx, repos, request.ReservationID)
	if err != nil {
		return ChargeResult{}, err
	}
	if route.runID != request.RunID {
		return ChargeResult{}, ErrRequestConflict
	}
	_, task, err := lockOrgTask(ctx, repos, route.taskID)
	if err != nil {
		return ChargeResult{}, err
	}
	if _, err := lockHistoricalRun(ctx, repos, task.ID, route.runID); err != nil {
		return ChargeResult{}, err
	}
	reservation, err := lockReservation(ctx, repos, request.ReservationID)
	if err != nil {
		return ChargeResult{}, err
	}
	if reservation.TaskID != route.taskID || reservation.OrgID != task.OrgID || reservation.RunID != request.RunID {
		return ChargeResult{}, ErrRequestConflict
	}
	var exact bool
	err = repos.Queries().QueryRow(ctx, `SELECT reservation_id=$2::uuid AND model=$3 AND usage=$4::jsonb
		AND cost_micro_usd=$5 FROM cost_entries WHERE request_id=$1 FOR UPDATE`, request.RequestID,
		request.ReservationID, request.Model, string(request.Usage), request.CostMicroUSD).Scan(&exact)
	if err == nil {
		if !exact {
			return ChargeResult{}, ErrRequestConflict
		}
		return ChargeResult{Duplicate: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ChargeResult{}, fmt.Errorf("check provider request replay: %w", err)
	}
	wasSettled := reservation.Status == "SETTLED"
	now := c.Now().UTC()
	inserted := ""
	err = repos.Queries().QueryRow(ctx, `INSERT INTO cost_entries(request_id,reservation_id,model,usage,cost_micro_usd,charged_at)
		VALUES($1,$2::uuid,$3,$4::jsonb,$5,$6) ON CONFLICT(request_id) DO NOTHING RETURNING request_id`,
		request.RequestID, request.ReservationID, request.Model, string(request.Usage), request.CostMicroUSD, now).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		// A concurrent request with the same global ID may have committed while
		// this transaction waited on the unique index; compare its full meaning.
		err = repos.Queries().QueryRow(ctx, `SELECT reservation_id=$2::uuid AND model=$3 AND usage=$4::jsonb
			AND cost_micro_usd=$5 FROM cost_entries WHERE request_id=$1 FOR UPDATE`, request.RequestID,
			request.ReservationID, request.Model, string(request.Usage), request.CostMicroUSD).Scan(&exact)
		if err != nil {
			return ChargeResult{}, fmt.Errorf("reload provider request replay: %w", err)
		}
		if !exact {
			return ChargeResult{}, ErrRequestConflict
		}
		return ChargeResult{Duplicate: true}, nil
	}
	if err != nil {
		return ChargeResult{}, fmt.Errorf("persist provider charge: %w", err)
	}
	var priorReserved int64
	var priorReservationSaturated bool
	if err := repos.Queries().QueryRow(ctx, `SELECT LEAST(COALESCE(SUM(remaining_micro_usd),0),9223372036854775807)::bigint,
		COALESCE(SUM(remaining_micro_usd),0)>9223372036854775807
		FROM budget_reservations WHERE task_id=$1::uuid AND status IN ('RESERVED','SETTLING','UNKNOWN')`, task.ID).Scan(&priorReserved, &priorReservationSaturated); err != nil {
		return ChargeResult{}, fmt.Errorf("verify task reservation cache before charge: %w", err)
	}
	ledgerMismatch := !priorReservationSaturated && task.ReservedMicroUSD != priorReserved
	consumed := request.CostMicroUSD
	if consumed > reservation.RemainingMicroUSD {
		consumed = reservation.RemainingMicroUSD
	}
	remaining := reservation.RemainingMicroUSD - consumed
	var totalOverEnvelope bool
	err = repos.Queries().QueryRow(ctx, `SELECT COALESCE(SUM(cost_micro_usd),0)>$2::numeric FROM cost_entries WHERE reservation_id=$1::uuid`, request.ReservationID, reservation.EnvelopeMicroUSD).Scan(&totalOverEnvelope)
	if err != nil {
		return ChargeResult{}, fmt.Errorf("check reservation envelope total: %w", err)
	}
	reopened := int64(0)
	if wasSettled {
		if err := repos.Queries().QueryRow(ctx, `SELECT GREATEST(0::numeric,$2::numeric-COALESCE(SUM(cost_micro_usd),0))::bigint FROM cost_entries WHERE reservation_id=$1::uuid`, request.ReservationID, reservation.EnvelopeMicroUSD).Scan(&reopened); err != nil {
			return ChargeResult{}, fmt.Errorf("calculate reopened reservation coverage: %w", err)
		}
		remaining = reopened
		if _, err := repos.Queries().Exec(ctx, `UPDATE budget_reservations SET remaining_micro_usd=$2,status='UNKNOWN',settled_at=NULL WHERE id=$1::uuid`, request.ReservationID, remaining); err != nil {
			return ChargeResult{}, fmt.Errorf("reopen reservation for late provider charge: %w", err)
		}
	} else if _, err := repos.Queries().Exec(ctx, `UPDATE budget_reservations SET remaining_micro_usd=$2 WHERE id=$1::uuid AND status<>'SETTLED'`, request.ReservationID, remaining); err != nil {
		return ChargeResult{}, fmt.Errorf("apply charged reservation amount: %w", err)
	}
	var newReserved int64
	var reservationSaturated bool
	err = repos.Queries().QueryRow(ctx, `SELECT LEAST(COALESCE(SUM(remaining_micro_usd),0),9223372036854775807)::bigint,
		COALESCE(SUM(remaining_micro_usd),0)>9223372036854775807
		FROM budget_reservations WHERE task_id=$1::uuid AND status IN ('RESERVED','SETTLING','UNKNOWN')`, task.ID).Scan(&newReserved, &reservationSaturated)
	if err != nil {
		return ChargeResult{}, fmt.Errorf("recalculate authoritative task reservations: %w", err)
	}
	var newSpent int64
	var overTaskBudget, saturated bool
	err = repos.Queries().QueryRow(ctx, `SELECT LEAST(COALESCE(SUM(c.cost_micro_usd),0),9223372036854775807)::bigint,
		COALESCE(SUM(c.cost_micro_usd),0)>$2::numeric,
		COALESCE(SUM(c.cost_micro_usd),0)>9223372036854775807
		FROM cost_entries c JOIN budget_reservations r ON r.id=c.reservation_id WHERE r.task_id=$1::uuid`, task.ID, task.BudgetLimitMicroUSD).Scan(&newSpent, &overTaskBudget, &saturated)
	if err != nil {
		return ChargeResult{}, fmt.Errorf("recalculate authoritative task spend: %w", err)
	}
	if _, err := repos.Queries().Exec(ctx, `UPDATE tasks SET spent_micro_usd=$2,reserved_micro_usd=$3,updated_at=$4 WHERE id=$1::uuid`,
		task.ID, newSpent, newReserved, now); err != nil {
		return ChargeResult{}, fmt.Errorf("settle task budget charge: %w", err)
	}
	result := ChargeResult{
		OverEnvelope:             totalOverEnvelope,
		OverTaskBudget:           overTaskBudget || reservationSaturated,
		TaskSpendSaturated:       saturated,
		TaskReservationSaturated: reservationSaturated,
		TaskLedgerMismatch:       ledgerMismatch,
		SettlementReopened:       wasSettled,
	}
	var orgOver bool
	orgBudget, err := lockedOrgBudget(ctx, repos, task.OrgID)
	if err != nil {
		return ChargeResult{}, err
	}
	err = repos.Queries().QueryRow(ctx, `SELECT COALESCE((SELECT SUM(c.cost_micro_usd) FROM cost_entries c
		JOIN budget_reservations r ON r.id=c.reservation_id WHERE r.org_id=$1
		AND c.charged_at > $2 AND c.charged_at <= $3),0)
		+ COALESCE((SELECT SUM(remaining_micro_usd) FROM budget_reservations
		WHERE org_id=$1 AND status IN ('RESERVED','SETTLING','UNKNOWN')),0) > $4::numeric`,
		task.OrgID, now.Add(-rollingWindow), now, orgBudget.RollingLimitMicroUSD).Scan(&orgOver)
	if err != nil {
		return ChargeResult{}, fmt.Errorf("check charged organization budget: %w", err)
	}
	result.OverOrganizationLimit = orgOver
	if result.OverEnvelope || result.OverTaskBudget || result.TaskSpendSaturated || orgOver || wasSettled || reservationSaturated || ledgerMismatch {
		if _, err := repos.Queries().Exec(ctx, `UPDATE org_budgets SET emergency_mode=TRUE WHERE org_id=$1`, task.OrgID); err != nil {
			return ChargeResult{}, fmt.Errorf("fence organization after budget overrun: %w", err)
		}
	}
	return result, nil
}

// MarkUnknown retains the outstanding reservation after cancellation or a
// lost/ambiguous provider response. It has no worker mutation authority.
func MarkUnknown(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, reservationID string) error {
	if ctx == nil || pool == nil || c == nil {
		return fmt.Errorf("mark budget usage unknown: %w", ErrInvalid)
	}
	return storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		return MarkUnknownLocked(ctx, repos, c, reservationID)
	})
}

// MarkUnknownLocked retains outstanding coverage in a caller's transaction.
func MarkUnknownLocked(ctx context.Context, repos *storage.Repositories, c clock.Clock, reservationID string) error {
	if ctx == nil || repos == nil || c == nil || !budgetID(reservationID) {
		return fmt.Errorf("mark budget usage unknown: %w", ErrInvalid)
	}
	route, err := routeReservation(ctx, repos, reservationID)
	if err != nil {
		return err
	}
	_, task, err := lockOrgTask(ctx, repos, route.taskID)
	if err != nil {
		return err
	}
	if _, err := lockHistoricalRun(ctx, repos, task.ID, route.runID); err != nil {
		return err
	}
	reservation, err := lockReservation(ctx, repos, reservationID)
	if err != nil {
		return err
	}
	if reservation.TaskID != task.ID || reservation.OrgID != task.OrgID || reservation.RunID != route.runID {
		return ErrReservationConflict
	}
	switch reservation.Status {
	case "UNKNOWN":
		return nil
	case "RESERVED", "SETTLING":
		_, err := repos.Queries().Exec(ctx, `UPDATE budget_reservations SET status='UNKNOWN' WHERE id=$1::uuid`, reservationID)
		if err != nil {
			return fmt.Errorf("retain unknown usage reservation: %w", err)
		}
		return nil
	default:
		return ErrReservationClosed
	}
}

// SettleReservation releases only unused capacity once all provider requests
// have authoritative final outcomes. Run completion is historical evidence;
// this call deliberately does not require a still-live lease.
func SettleReservation(ctx context.Context, pool *pgxpool.Pool, c clock.Clock, request SettlementRequest) error {
	if ctx == nil || pool == nil || c == nil {
		return fmt.Errorf("settle budget reservation: %w", ErrInvalid)
	}
	return storage.WithUnitOfWork(ctx, pool, c, func(ctx context.Context, repos *storage.Repositories) error {
		return SettleReservationLocked(ctx, repos, c, request)
	})
}

// SettleReservationLocked finalizes a reconciled run inside the caller's UOW.
func SettleReservationLocked(ctx context.Context, repos *storage.Repositories, c clock.Clock, request SettlementRequest) error {
	if ctx == nil || repos == nil || c == nil || !budgetID(request.ReservationID) || !budgetID(request.RunID) {
		return fmt.Errorf("settle budget reservation: %w", ErrInvalid)
	}
	if !request.FinalUsageKnown {
		return ErrUsageIncomplete
	}
	route, err := routeReservation(ctx, repos, request.ReservationID)
	if err != nil {
		return err
	}
	if route.runID != request.RunID {
		return ErrReservationConflict
	}
	_, task, err := lockOrgTask(ctx, repos, route.taskID)
	if err != nil {
		return err
	}
	runStatus, err := lockHistoricalRun(ctx, repos, task.ID, request.RunID)
	if err != nil {
		return err
	}
	reservation, err := lockReservation(ctx, repos, request.ReservationID)
	if err != nil {
		return err
	}
	if reservation.TaskID != task.ID || reservation.OrgID != task.OrgID || reservation.RunID != request.RunID {
		return ErrReservationConflict
	}
	if reservation.Status == "SETTLED" {
		return nil
	}
	if runStatus == "RUNNING" {
		return fmt.Errorf("cannot release budget for running run: %w", ErrAdmission)
	}
	now := c.Now().UTC()
	if _, err := repos.Queries().Exec(ctx, `UPDATE budget_reservations SET remaining_micro_usd=0,status='SETTLED',settled_at=$2 WHERE id=$1::uuid`, request.ReservationID, now); err != nil {
		return fmt.Errorf("finalize budget reservation: %w", err)
	}
	var reserved int64
	var saturated bool
	if err := repos.Queries().QueryRow(ctx, `SELECT LEAST(COALESCE(SUM(remaining_micro_usd),0),9223372036854775807)::bigint,
		COALESCE(SUM(remaining_micro_usd),0)>9223372036854775807
		FROM budget_reservations WHERE task_id=$1::uuid AND status IN ('RESERVED','SETTLING','UNKNOWN')`, task.ID).Scan(&reserved, &saturated); err != nil {
		return fmt.Errorf("recalculate task reservation after settlement: %w", err)
	}
	if _, err := repos.Queries().Exec(ctx, `UPDATE tasks SET reserved_micro_usd=$2,updated_at=$3 WHERE id=$1::uuid`, task.ID, reserved, now); err != nil {
		return fmt.Errorf("apply released task budget: %w", err)
	}
	if saturated {
		if _, err := repos.Queries().Exec(ctx, `UPDATE org_budgets SET emergency_mode=TRUE WHERE org_id=$1`, task.OrgID); err != nil {
			return fmt.Errorf("fence organization after reservation cache overflow: %w", err)
		}
	}
	return nil
}

type reservationRoute struct {
	taskID string
	orgID  string
	runID  string
}

func lockOrgTask(ctx context.Context, repos *storage.Repositories, taskID string) (storage.LockedOrgBudget, storage.Task, error) {
	var orgID string
	err := repos.Queries().QueryRow(ctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, taskID).Scan(&orgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.LockedOrgBudget{}, storage.Task{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.LockedOrgBudget{}, storage.Task{}, fmt.Errorf("read task organization: %w", err)
	}
	org, lockedTask, err := repos.LockOrgBudgetAndTask(ctx, orgID, taskID)
	if err != nil {
		return storage.LockedOrgBudget{}, storage.Task{}, err
	}
	return org, lockedTask.Record(), nil
}

func lockedOrgBudget(ctx context.Context, repos *storage.Repositories, orgID string) (storage.OrgBudget, error) {
	var limit int64
	var emergency bool
	err := repos.Queries().QueryRow(ctx, `SELECT rolling_limit_micro_usd,emergency_mode FROM org_budgets WHERE org_id=$1 FOR UPDATE`, orgID).Scan(&limit, &emergency)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.OrgBudget{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.OrgBudget{}, fmt.Errorf("lock organization budget: %w", err)
	}
	return storage.OrgBudget{OrgID: orgID, RollingLimitMicroUSD: limit, EmergencyMode: emergency}, nil
}

func loadLeaseJob(ctx context.Context, repos *storage.Repositories, taskID, jobID string) (contracts.Job, error) {
	var payload []byte
	if err := repos.Queries().QueryRow(ctx, `SELECT payload FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`, jobID, taskID).Scan(&payload); err != nil {
		return contracts.Job{}, fmt.Errorf("load durable inference job: %w", err)
	}
	job, err := contracts.DecodeJob(payload)
	if err != nil {
		return contracts.Job{}, fmt.Errorf("decode durable inference job: %w", err)
	}
	return job, nil
}

func reservationForRun(ctx context.Context, repos *storage.Repositories, taskID, runID string) (Reservation, bool, error) {
	r, err := scanReservation(repos.Queries().QueryRow(ctx, `SELECT id::text,task_id::text,org_id,run_id::text,envelope_micro_usd,remaining_micro_usd,
		pricing_version,admission_envelope,status,created_at,settled_at FROM budget_reservations WHERE task_id=$1::uuid AND run_id=$2::uuid FOR UPDATE`, taskID, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, false, nil
	}
	if err != nil {
		return Reservation{}, false, fmt.Errorf("find existing run reservation: %w", err)
	}
	return r, true, nil
}

func routeReservation(ctx context.Context, repos *storage.Repositories, reservationID string) (reservationRoute, error) {
	var route reservationRoute
	err := repos.Queries().QueryRow(ctx, `SELECT task_id::text,org_id,run_id::text FROM budget_reservations WHERE id=$1::uuid`, reservationID).Scan(&route.taskID, &route.orgID, &route.runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return reservationRoute{}, storage.ErrNotFound
	}
	if err != nil {
		return reservationRoute{}, fmt.Errorf("load budget reservation route: %w", err)
	}
	return route, nil
}

func lockHistoricalRun(ctx context.Context, repos *storage.Repositories, taskID, runID string) (string, error) {
	var jobID string
	err := repos.Queries().QueryRow(ctx, `SELECT job_id::text FROM agent_runs WHERE id=$1::uuid AND task_id=$2::uuid`, runID, taskID).Scan(&jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", storage.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("find run job for billing: %w", err)
	}
	var operation string
	err = repos.Queries().QueryRow(ctx, `SELECT operation_type FROM jobs WHERE id=$1::uuid AND task_id=$2::uuid FOR UPDATE`, jobID, taskID).Scan(&operation)
	if err != nil {
		return "", fmt.Errorf("lock run job for billing: %w", err)
	}
	var role, status string
	err = repos.Queries().QueryRow(ctx, `SELECT agent_type,execution_status FROM agent_runs WHERE id=$1::uuid AND task_id=$2::uuid AND job_id=$3::uuid FOR UPDATE`, runID, taskID, jobID).Scan(&role, &status)
	if err != nil {
		return "", fmt.Errorf("lock agent run for billing: %w", err)
	}
	expectedRole, supported := leases.AgentTypeForOperation(operation)
	if !supported || !leases.RequiresInference(operation) || role != expectedRole {
		return "", ErrAdmission
	}
	return status, nil
}

func lockReservation(ctx context.Context, repos *storage.Repositories, reservationID string) (Reservation, error) {
	r, err := scanReservation(repos.Queries().QueryRow(ctx, `SELECT id::text,task_id::text,org_id,run_id::text,envelope_micro_usd,remaining_micro_usd,
		pricing_version,admission_envelope,status,created_at,settled_at FROM budget_reservations WHERE id=$1::uuid FOR UPDATE`, reservationID))
	if err != nil {
		return Reservation{}, fmt.Errorf("lock budget reservation: %w", err)
	}
	return r, nil
}

func scanReservation(row pgx.Row) (Reservation, error) {
	var r Reservation
	var envelope []byte
	err := row.Scan(&r.ID, &r.TaskID, &r.OrgID, &r.RunID, &r.EnvelopeMicroUSD, &r.RemainingMicroUSD,
		&r.PricingVersion, &envelope, &r.Status, &r.CreatedAt, &r.SettledAt)
	if err == nil {
		err = json.Unmarshal(envelope, &r.Envelope)
	}
	return r, err
}

func usageObject(raw json.RawMessage) bool {
	var object map[string]json.RawMessage
	return len(raw) != 0 && json.Unmarshal(raw, &object) == nil && object != nil
}

func budgetID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
