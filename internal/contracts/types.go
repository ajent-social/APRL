// Package contracts contains the versioned messages shared by APRL services.
// Decode functions are the runtime boundary: they reject unknown fields,
// missing required fields, malformed JSON, and invalid authority identities.
package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// VersionV1 is the first frozen APRL message version.
const VersionV1 = 1

// Token and call limits cap the integer bounds representable by a v1 envelope.
const (
	MaxInputTokens  int64 = 10_000_000
	MaxOutputTokens int64 = 1_000_000
	MaxCalls        int64 = 10_000
)

// ErrInvalidContract marks JSON that cannot be admitted as a v1 contract.
var (
	ErrInvalidContract = errors.New("invalid contract")
	idPattern          = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	shaPattern         = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

// Snapshot binds work to the Git commit pair APRL observed. An unattached task
// may have an empty snapshot; a partial pair is always invalid.
type Snapshot struct {
	HeadSHA        string `json:"head_sha,omitempty"`
	BaseSHA        string `json:"base_sha,omitempty"`
	IntegrationSHA string `json:"integration_sha,omitempty"`
}

// BudgetEnvelope is expressed in integer micro-USD. PricingVersion identifies
// the exact approved rate card used to calculate MaxCostMicroUSD.
type BudgetEnvelope struct {
	MaxCostMicroUSD int64  `json:"max_cost_micro_usd"`
	MaxInputTokens  int64  `json:"max_input_tokens"`
	MaxOutputTokens int64  `json:"max_output_tokens"`
	MaxCalls        int64  `json:"max_calls"`
	PricingVersion  string `json:"pricing_version"`
}

// Event is a routed, durable event. Raw GitHub deliveries are normalized into
// this form only after APRL has assigned the event to a task and logical job.
type Event struct {
	Version       int      `json:"version"`
	EventID       string   `json:"event_id"`
	EventType     string   `json:"event_type"`
	TaskID        string   `json:"task_id"`
	JobID         string   `json:"job_id"`
	Generation    int64    `json:"generation"`
	Snapshot      Snapshot `json:"snapshot"`
	OperationID   string   `json:"operation_id"`
	CorrelationID string   `json:"correlation_id"`
}

// Job is a leased logical operation ready for execution. Generation is
// presence-checked; explicit generation zero is allowed for the initial task
// generation, while an omitted field never silently becomes authority.
type Job struct {
	Version       int            `json:"version"`
	TaskID        string         `json:"task_id"`
	JobID         string         `json:"job_id"`
	Generation    int64          `json:"generation"`
	LeaseToken    string         `json:"lease_token,omitempty"`
	RunID         string         `json:"run_id,omitempty"`
	Snapshot      Snapshot       `json:"snapshot"`
	Attempt       int32          `json:"attempt"`
	OperationID   string         `json:"operation_id"`
	CorrelationID string         `json:"correlation_id"`
	Operation     string         `json:"operation"`
	Envelope      BudgetEnvelope `json:"budget_envelope,omitzero"`
}

// Result is an execution completion bound to the exact task/job/run lease and
// snapshot. OperationID is the idempotency identity; replays must retain the
// same operation and correlation identities and byte-equivalent meaning.
type Result struct {
	Version       int      `json:"version"`
	TaskID        string   `json:"task_id"`
	JobID         string   `json:"job_id"`
	RunID         string   `json:"run_id"`
	Generation    int64    `json:"generation"`
	LeaseToken    string   `json:"lease_token"`
	Snapshot      Snapshot `json:"snapshot"`
	Attempt       int32    `json:"attempt"`
	OperationID   string   `json:"operation_id"`
	CorrelationID string   `json:"correlation_id"`
	Status        string   `json:"status"`
	Summary       string   `json:"summary"`
}

// Verdict is the normalized review decision.
type Verdict string

// Verdict constants are the only decisions accepted by Review v1.
const (
	VerdictApprove        Verdict = "approve"
	VerdictRequestChanges Verdict = "request_changes"
)

// Severity is the normalized impact level of a persisted review finding.
type Severity string

// Severity constants are the only levels accepted by Review v1.
const (
	SeverityBlocker Severity = "blocker"
	SeverityWarning Severity = "warning"
	SeverityNit     Severity = "nit"
)

// Anchor is a complete location in a pinned diff.
type Anchor struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Side string `json:"side"`
}

// Finding permits an absent Anchor, including for blockers. If present, the
// anchor is a complete file/line/side tuple.
type Finding struct {
	ID         string   `json:"id"`
	Severity   Severity `json:"severity"`
	Comment    string   `json:"comment"`
	Suggestion string   `json:"suggestion,omitempty"`
	Anchor     *Anchor  `json:"anchor,omitempty"`
}

// Review is a lease-bound decision over an attached head/base snapshot.
type Review struct {
	Version         int       `json:"version"`
	TaskID          string    `json:"task_id"`
	JobID           string    `json:"job_id"`
	RunID           string    `json:"run_id"`
	Generation      int64     `json:"generation"`
	LeaseToken      string    `json:"lease_token"`
	Snapshot        Snapshot  `json:"snapshot"`
	Attempt         int32     `json:"attempt"`
	OperationID     string    `json:"operation_id"`
	CorrelationID   string    `json:"correlation_id"`
	Verdict         Verdict   `json:"verdict"`
	Summary         string    `json:"summary"`
	RequestedChange string    `json:"requested_change,omitempty"`
	Findings        []Finding `json:"findings"`
}

// Validate checks the SHA pair and requires one when anchored is true.
func (s Snapshot) Validate(anchored bool) error {
	if s.HeadSHA == "" && s.BaseSHA == "" {
		if s.IntegrationSHA != "" || anchored {
			return invalid("snapshot", "head_sha and base_sha are required together")
		}
		return nil
	}
	if !shaPattern.MatchString(s.HeadSHA) || !shaPattern.MatchString(s.BaseSHA) {
		return invalid("snapshot", "head_sha and base_sha must be 40 lowercase hexadecimal characters")
	}
	if s.IntegrationSHA != "" && !shaPattern.MatchString(s.IntegrationSHA) {
		return invalid("snapshot.integration_sha", "must be a 40-character lowercase hexadecimal SHA")
	}
	return nil
}

// Validate checks integer money/token limits and explicit pricing identity.
func (e BudgetEnvelope) Validate() error {
	if e.MaxCostMicroUSD <= 0 {
		return invalid("budget_envelope.max_cost_micro_usd", "must be a positive integer")
	}
	if e.MaxInputTokens <= 0 || e.MaxInputTokens > MaxInputTokens {
		return invalid("budget_envelope.max_input_tokens", "must be between 1 and %d", MaxInputTokens)
	}
	if e.MaxOutputTokens <= 0 || e.MaxOutputTokens > MaxOutputTokens {
		return invalid("budget_envelope.max_output_tokens", "must be between 1 and %d", MaxOutputTokens)
	}
	if e.MaxCalls <= 0 || e.MaxCalls > MaxCalls {
		return invalid("budget_envelope.max_calls", "must be between 1 and %d", MaxCalls)
	}
	if strings.TrimSpace(e.PricingVersion) == "" {
		return invalid("budget_envelope.pricing_version", "is required")
	}
	return nil
}

// Validate checks a normalized routed event and its task/job identities.
func (e Event) Validate() error {
	if err := validateVersion(e.Version); err != nil {
		return err
	}
	if err := validateGeneration(e.Generation); err != nil {
		return err
	}
	if err := validateID("event_id", e.EventID); err != nil {
		return err
	}
	if strings.TrimSpace(e.EventType) == "" {
		return invalid("event_type", "is required")
	}
	if err := validateID("task_id", e.TaskID); err != nil {
		return err
	}
	if err := validateID("job_id", e.JobID); err != nil {
		return err
	}
	if err := validateID("operation_id", e.OperationID); err != nil {
		return err
	}
	if err := validateID("correlation_id", e.CorrelationID); err != nil {
		return err
	}
	return e.Snapshot.Validate(false)
}

// Validate checks a queued or claimed logical job payload.
func (j Job) Validate() error {
	if err := validateVersion(j.Version); err != nil {
		return err
	}
	if err := validateGeneration(j.Generation); err != nil {
		return err
	}
	if err := validateID("task_id", j.TaskID); err != nil {
		return err
	}
	if err := validateID("job_id", j.JobID); err != nil {
		return err
	}
	if (j.LeaseToken == "") != (j.RunID == "") {
		return invalid("lease_token/run_id", "must both be absent for a queued job or both be present for a claimed job")
	}
	if j.LeaseToken != "" {
		if err := validateID("lease_token", j.LeaseToken); err != nil {
			return err
		}
		if err := validateID("run_id", j.RunID); err != nil {
			return err
		}
	}
	if err := validateID("operation_id", j.OperationID); err != nil {
		return err
	}
	if err := validateID("correlation_id", j.CorrelationID); err != nil {
		return err
	}
	if j.Attempt == 0 {
		return invalid("attempt", "must be positive")
	}
	if strings.TrimSpace(j.Operation) == "" {
		return invalid("operation", "is required")
	}
	if err := j.Snapshot.Validate(false); err != nil {
		return err
	}
	if !j.Envelope.empty() {
		return j.Envelope.Validate()
	}
	return nil
}

// ValidateForExecution is required after a worker claims a Postgres lease.
// A Redis queued delivery alone is never execution authority.
func (j Job) ValidateForExecution() error {
	if err := j.Validate(); err != nil {
		return err
	}
	if j.LeaseToken == "" || j.RunID == "" {
		return invalid("lease_token/run_id", "claimed execution requires both")
	}
	return nil
}

func (e BudgetEnvelope) empty() bool {
	return e.MaxCostMicroUSD == 0 && e.MaxInputTokens == 0 && e.MaxOutputTokens == 0 && e.MaxCalls == 0 && e.PricingVersion == ""
}

// Validate checks a completion result and permits the empty pre-PR snapshot.
func (r Result) Validate() error {
	if err := validateVersion(r.Version); err != nil {
		return err
	}
	if err := validateGeneration(r.Generation); err != nil {
		return err
	}
	if err := validateID("task_id", r.TaskID); err != nil {
		return err
	}
	if err := validateID("job_id", r.JobID); err != nil {
		return err
	}
	if err := validateID("run_id", r.RunID); err != nil {
		return err
	}
	if err := validateID("lease_token", r.LeaseToken); err != nil {
		return err
	}
	if err := validateID("operation_id", r.OperationID); err != nil {
		return err
	}
	if err := validateID("correlation_id", r.CorrelationID); err != nil {
		return err
	}
	if r.Attempt == 0 {
		return invalid("attempt", "must be positive")
	}
	if err := r.Snapshot.Validate(false); err != nil {
		return err
	}
	if r.Status != "succeeded" && r.Status != "failed" && r.Status != "cancelled" {
		return invalid("status", "must be succeeded, failed, or cancelled")
	}
	return nil
}

// Validate checks snapshot, finding, anchor and verdict consistency.
func (r Review) Validate() error {
	if err := validateVersion(r.Version); err != nil {
		return err
	}
	if err := validateGeneration(r.Generation); err != nil {
		return err
	}
	if err := validateID("task_id", r.TaskID); err != nil {
		return err
	}
	if err := validateID("job_id", r.JobID); err != nil {
		return err
	}
	if err := validateID("run_id", r.RunID); err != nil {
		return err
	}
	if err := validateID("lease_token", r.LeaseToken); err != nil {
		return err
	}
	if err := validateID("operation_id", r.OperationID); err != nil {
		return err
	}
	if err := validateID("correlation_id", r.CorrelationID); err != nil {
		return err
	}
	if r.Attempt == 0 {
		return invalid("attempt", "must be positive")
	}
	if err := r.Snapshot.Validate(true); err != nil {
		return err
	}
	if r.Verdict != VerdictApprove && r.Verdict != VerdictRequestChanges {
		return invalid("verdict", "must be approve or request_changes")
	}
	blockers, actionable := 0, strings.TrimSpace(r.RequestedChange) != ""
	seen := make(map[string]struct{}, len(r.Findings))
	for i, finding := range r.Findings {
		if err := finding.Validate(); err != nil {
			return fmt.Errorf("%w: findings[%d]: %v", ErrInvalidContract, i, err)
		}
		if _, exists := seen[finding.ID]; exists {
			return invalid("findings", "finding IDs must be unique")
		}
		seen[finding.ID] = struct{}{}
		if finding.Severity == SeverityBlocker {
			blockers++
		}
		if strings.TrimSpace(finding.Comment) != "" {
			actionable = true
		}
	}
	if r.Verdict == VerdictApprove && blockers > 0 {
		return invalid("verdict", "approve cannot include blocker findings")
	}
	if r.Verdict == VerdictRequestChanges && !actionable {
		return invalid("verdict", "request_changes requires at least one actionable finding")
	}
	return nil
}

// Validate checks a finding while allowing blockers without inline anchors.
func (f Finding) Validate() error {
	if err := validateID("findings.id", f.ID); err != nil {
		return err
	}
	switch f.Severity {
	case SeverityBlocker, SeverityWarning, SeverityNit:
	default:
		return invalid("findings.severity", "must be blocker, warning, or nit")
	}
	if strings.TrimSpace(f.Comment) == "" {
		return invalid("findings.comment", "is required")
	}
	if f.Anchor != nil {
		if strings.TrimSpace(f.Anchor.File) == "" {
			return invalid("findings.anchor.file", "is required")
		}
		if f.Anchor.Line < 1 {
			return invalid("findings.anchor.line", "must be positive")
		}
		if f.Anchor.Side != "LEFT" && f.Anchor.Side != "RIGHT" {
			return invalid("findings.anchor.side", "must be LEFT or RIGHT")
		}
	}
	return nil
}

// DecodeEvent strictly decodes and validates one routed Event JSON value.
func DecodeEvent(data []byte) (Event, error) {
	var value Event
	err := decode(data, &value, "version", "event_id", "event_type", "task_id", "job_id", "generation", "snapshot", "operation_id", "correlation_id")
	if err == nil {
		err = value.Validate()
	}
	return value, err
}

// DecodeJob strictly decodes and validates one queued or claimed Job value.
func DecodeJob(data []byte) (Job, error) {
	var value Job
	err := decode(data, &value, "version", "task_id", "job_id", "generation", "snapshot", "attempt", "operation_id", "correlation_id", "operation")
	if err == nil {
		err = rejectNull(data, "lease_token", "run_id", "budget_envelope")
	}
	if err == nil {
		err = validateJobPresence(data, value)
	}
	if err == nil {
		err = value.Validate()
	}
	return value, err
}

func validateJobPresence(data []byte, job Job) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("%w: malformed JSON: %v", ErrInvalidContract, err)
	}
	_, hasLease := fields["lease_token"]
	_, hasRun := fields["run_id"]
	if hasLease != hasRun {
		return invalid("lease_token/run_id", "must both be absent for a queued job or both be present for a claimed job")
	}
	if hasLease {
		if err := validateID("lease_token", job.LeaseToken); err != nil {
			return err
		}
		if err := validateID("run_id", job.RunID); err != nil {
			return err
		}
	}
	if _, hasEnvelope := fields["budget_envelope"]; hasEnvelope {
		return job.Envelope.Validate()
	}
	return nil
}

func rejectNull(data []byte, names ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("%w: malformed JSON: %v", ErrInvalidContract, err)
	}
	for _, name := range names {
		if raw, ok := fields[name]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return invalid(name, "must be omitted or contain a value")
		}
	}
	return nil
}

// DecodeResult strictly decodes and validates one execution Result value.
func DecodeResult(data []byte) (Result, error) {
	var value Result
	err := decode(data, &value, "version", "task_id", "job_id", "run_id", "generation", "lease_token", "snapshot", "attempt", "operation_id", "correlation_id", "status", "summary")
	if err == nil {
		err = value.Validate()
	}
	return value, err
}

// DecodeReview strictly decodes and validates one lease-bound Review value.
func DecodeReview(data []byte) (Review, error) {
	var value Review
	err := decode(data, &value, "version", "task_id", "job_id", "run_id", "generation", "lease_token", "snapshot", "attempt", "operation_id", "correlation_id", "verdict", "summary", "findings")
	if err == nil {
		err = rejectNullInFindings(data)
	}
	if err == nil {
		err = value.Validate()
	}
	return value, err
}

func rejectNullInFindings(data []byte) error {
	var payload struct {
		Findings []map[string]json.RawMessage `json:"findings"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("%w: malformed JSON: %v", ErrInvalidContract, err)
	}
	for i, finding := range payload.Findings {
		if raw, ok := finding["anchor"]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return invalid(fmt.Sprintf("findings[%d].anchor", i), "must be omitted or contain a complete anchor")
		}
	}
	return nil
}

func decode(data []byte, dst any, required ...string) error {
	if err := rejectDuplicateKeys(data); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("%w: malformed JSON: %v", ErrInvalidContract, err)
	}
	if fields == nil {
		return invalid("payload", "must be a JSON object")
	}
	for _, name := range required {
		if raw, ok := fields[name]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return invalid(name, "is required")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidContract, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return invalid("payload", "must contain exactly one JSON value")
	}
	return nil
}

func rejectDuplicateKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value func() error
	value = func() error {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%w: malformed JSON: %v", ErrInvalidContract, err)
		}
		delimiter, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("%w: malformed JSON object key: %v", ErrInvalidContract, err)
				}
				key, ok := keyToken.(string)
				if !ok {
					return invalid("payload", "object key must be a string")
				}
				if _, exists := seen[key]; exists {
					return invalid(key, "must not be repeated")
				}
				seen[key] = struct{}{}
				if err := value(); err != nil {
					return err
				}
			}
			closeToken, err := decoder.Token()
			if err != nil || closeToken != json.Delim('}') {
				return fmt.Errorf("%w: malformed JSON object: %v", ErrInvalidContract, err)
			}
		case '[':
			for decoder.More() {
				if err := value(); err != nil {
					return err
				}
			}
			closeToken, err := decoder.Token()
			if err != nil || closeToken != json.Delim(']') {
				return fmt.Errorf("%w: malformed JSON array: %v", ErrInvalidContract, err)
			}
		default:
			return invalid("payload", "contains an unexpected closing delimiter")
		}
		return nil
	}
	if err := value(); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return invalid("payload", "must contain exactly one JSON value")
	}
	return nil
}

func validateVersion(version int) error {
	if version != VersionV1 {
		return invalid("version", "must equal %d", VersionV1)
	}
	return nil
}

func validateGeneration(generation int64) error {
	if generation < 0 {
		return invalid("generation", "must be between 0 and %d", int64(^uint64(0)>>1))
	}
	return nil
}

func validateID(field, value string) error {
	if !idPattern.MatchString(value) || strings.EqualFold(value, "00000000-0000-0000-0000-000000000000") {
		return invalid(field, "must be a non-zero UUID")
	}
	return nil
}

func invalid(field, format string, args ...any) error {
	return fmt.Errorf("%w: %s %s", ErrInvalidContract, field, fmt.Sprintf(format, args...))
}
