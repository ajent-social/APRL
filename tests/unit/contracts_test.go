package unit

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ajent-social/APRL/internal/contracts"
)

const validJobV1 = `{"version":1,"task_id":"11111111-1111-4111-8111-111111111111","job_id":"22222222-2222-4222-8222-222222222222","generation":0,"snapshot":{},"attempt":1,"operation_id":"33333333-3333-4333-8333-333333333333","correlation_id":"44444444-4444-4444-8444-444444444444","operation":"author"}`

const validReviewV1 = `{"version":1,"task_id":"11111111-1111-4111-8111-111111111111","job_id":"22222222-2222-4222-8222-222222222222","run_id":"55555555-5555-4555-8555-555555555555","generation":0,"lease_token":"66666666-6666-4666-8666-666666666666","snapshot":{"head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","base_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"attempt":1,"operation_id":"33333333-3333-4333-8333-333333333333","correlation_id":"44444444-4444-4444-8444-444444444444","verdict":"request_changes","summary":"Review findings need changes.","findings":[{"id":"77777777-7777-4777-8777-777777777777","severity":"blocker","comment":"The unsafe path remains reachable."}]}`

const validEventV1 = `{"version":1,"event_id":"88888888-8888-4888-8888-888888888888","event_type":"check_suite.completed","task_id":"11111111-1111-4111-8111-111111111111","job_id":"22222222-2222-4222-8222-222222222222","generation":0,"snapshot":{},"operation_id":"33333333-3333-4333-8333-333333333333","correlation_id":"44444444-4444-4444-8444-444444444444"}`

func TestContracts(t *testing.T) {
	t.Run("queued_serialization", func(t *testing.T) {
		job, err := contracts.DecodeJob([]byte(validJobV1))
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(job)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "budget_envelope") {
			t.Fatal("zero optional envelope was serialized as an invalid financial bound")
		}
		if _, err := contracts.DecodeJob(body); err != nil {
			t.Fatalf("queued job failed strict JSON round trip: %v", err)
		}
		job.Envelope = contracts.BudgetEnvelope{MaxCostMicroUSD: 1000, MaxInputTokens: 100, MaxOutputTokens: 10, MaxCalls: 1, PricingVersion: "test-v1"}
		body, err = json.Marshal(job)
		if err != nil {
			t.Fatal(err)
		}
		roundTrip, err := contracts.DecodeJob(body)
		if err != nil {
			t.Fatal(err)
		}
		if roundTrip.Envelope != job.Envelope {
			t.Fatal("nonzero envelope lost during serialization")
		}
	})
	t.Run("valid_v1", func(t *testing.T) {
		job, err := contracts.DecodeJob([]byte(validJobV1))
		if err != nil {
			t.Fatalf("DecodeJob() error = %v", err)
		}
		if job.Generation != 0 {
			t.Fatalf("generation = %d, want explicit initial generation 0", job.Generation)
		}
		if err := job.ValidateForExecution(); err == nil {
			t.Fatal("queued job without its claimed lease unexpectedly had execution authority")
		}
		claimed := strings.TrimSuffix(validJobV1, "}") + `,"lease_token":"66666666-6666-4666-8666-666666666666","run_id":"55555555-5555-4555-8555-555555555555","budget_envelope":{"max_cost_micro_usd":1200000,"max_input_tokens":100000,"max_output_tokens":5000,"max_calls":3,"pricing_version":"test-rate-card-1"}}`
		claimedJob, err := contracts.DecodeJob([]byte(claimed))
		if err != nil {
			t.Fatalf("DecodeJob(claimed) error = %v", err)
		}
		if err := claimedJob.ValidateForExecution(); err != nil {
			t.Fatalf("ValidateForExecution() error = %v", err)
		}
		result := `{"version":1,"task_id":"11111111-1111-4111-8111-111111111111","job_id":"22222222-2222-4222-8222-222222222222","run_id":"55555555-5555-4555-8555-555555555555","generation":0,"lease_token":"66666666-6666-4666-8666-666666666666","snapshot":{},"attempt":1,"operation_id":"33333333-3333-4333-8333-333333333333","correlation_id":"44444444-4444-4444-8444-444444444444","status":"succeeded","summary":"authoring complete"}`
		if _, err := contracts.DecodeResult([]byte(result)); err != nil {
			t.Fatalf("DecodeResult(pre-PR) error = %v", err)
		}
		if _, err := contracts.DecodeEvent([]byte(validEventV1)); err != nil {
			t.Fatalf("DecodeEvent() error = %v", err)
		}
		if _, err := contracts.DecodeReview([]byte(validReviewV1)); err != nil {
			t.Fatalf("DecodeReview() error = %v", err)
		}
	})

	t.Run("missing_generation", func(t *testing.T) {
		payload := strings.Replace(validJobV1, `"generation":0,`, "", 1)
		if _, err := contracts.DecodeJob([]byte(payload)); err == nil {
			t.Fatal("DecodeJob() accepted a payload with generation removed")
		} else {
			t.Logf("expected red condition observed: removed required generation was rejected: %v", err)
		}
	})

	t.Run("malformed_payload", func(t *testing.T) {
		if _, err := contracts.DecodeJob([]byte(`{"version":1`)); err == nil {
			t.Fatal("DecodeJob() accepted malformed JSON")
		}
		payload := strings.TrimSuffix(validJobV1, "}") + `,"unexpected":true}`
		if _, err := contracts.DecodeJob([]byte(payload)); err == nil {
			t.Fatal("DecodeJob() accepted an unknown field")
		}
		duplicateGeneration := strings.Replace(validJobV1, `"generation":0`, `"generation":0,"generation":1`, 1)
		if _, err := contracts.DecodeJob([]byte(duplicateGeneration)); err == nil {
			t.Fatal("DecodeJob() accepted duplicate authority fields")
		}
		queuedWithEmptyLease := strings.TrimSuffix(validJobV1, "}") + `,"lease_token":"","run_id":""}`
		if _, err := contracts.DecodeJob([]byte(queuedWithEmptyLease)); err == nil {
			t.Fatal("DecodeJob() treated explicitly empty authority fields as absent")
		}
		emptyEnvelope := strings.TrimSuffix(validJobV1, "}") + `,"budget_envelope":{}}`
		if _, err := contracts.DecodeJob([]byte(emptyEnvelope)); err == nil {
			t.Fatal("DecodeJob() accepted an explicitly empty budget envelope")
		}
	})

	t.Run("inconsistent_verdict", func(t *testing.T) {
		payload := strings.Replace(validReviewV1, `"verdict":"request_changes"`, `"verdict":"approve"`, 1)
		if _, err := contracts.DecodeReview([]byte(payload)); err == nil {
			t.Fatal("DecodeReview() accepted approve with a blocker")
		}
		withoutAction := strings.Replace(validReviewV1, `,"findings":[{"id":"77777777-7777-4777-8777-777777777777","severity":"blocker","comment":"The unsafe path remains reachable."}]`, `,"findings":[]`, 1)
		if _, err := contracts.DecodeReview([]byte(withoutAction)); err == nil {
			t.Fatal("DecodeReview() accepted request_changes without an actionable finding")
		}
		explicitRequest := strings.Replace(withoutAction, `"summary":"Review findings need changes."`, `"summary":"Review findings need changes.","requested_change":"Provide the missing compatibility check."`, 1)
		if _, err := contracts.DecodeReview([]byte(explicitRequest)); err != nil {
			t.Fatalf("DecodeReview() rejected an explicit requested change: %v", err)
		}
	})

	t.Run("incomplete_anchor", func(t *testing.T) {
		payload := strings.Replace(validReviewV1, `"comment":"The unsafe path remains reachable."`, `"comment":"The unsafe path remains reachable.","anchor":{"file":"internal/a.go"}`, 1)
		if _, err := contracts.DecodeReview([]byte(payload)); err == nil {
			t.Fatal("DecodeReview() accepted an incomplete inline anchor")
		}
		nullAnchor := strings.Replace(validReviewV1, `"comment":"The unsafe path remains reachable."`, `"comment":"The unsafe path remains reachable.","anchor":null`, 1)
		if _, err := contracts.DecodeReview([]byte(nullAnchor)); err == nil {
			t.Fatal("DecodeReview() accepted null instead of an omitted or complete anchor")
		}
	})

	t.Run("unanchored_blocker", func(t *testing.T) {
		review, err := contracts.DecodeReview([]byte(validReviewV1))
		if err != nil {
			t.Fatalf("DecodeReview() rejected an unanchored blocker: %v", err)
		}
		if review.Findings[0].Anchor != nil || review.Findings[0].Severity != contracts.SeverityBlocker {
			t.Fatalf("finding changed during validation: %+v", review.Findings[0])
		}
	})

	t.Run("replay_identity", func(t *testing.T) {
		payload := `{"version":1,"task_id":"11111111-1111-4111-8111-111111111111","job_id":"22222222-2222-4222-8222-222222222222","run_id":"55555555-5555-4555-8555-555555555555","generation":0,"lease_token":"66666666-6666-4666-8666-666666666666","snapshot":{},"attempt":1,"correlation_id":"44444444-4444-4444-8444-444444444444","status":"succeeded","summary":"done"}`
		if _, err := contracts.DecodeResult([]byte(payload)); err == nil {
			t.Fatal("DecodeResult() accepted a result without its replay operation identity")
		}
	})
}
