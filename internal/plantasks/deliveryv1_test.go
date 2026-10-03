package plantasks

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeliveryV1RequestGoldenBytesAndDigest(t *testing.T) {
	fixture := deliveryV1RequestFixture(t)
	request, err := DecodeDeliveryV1Request(fixture)
	if err != nil {
		t.Fatalf("DecodeDeliveryV1Request(golden): %v", err)
	}

	encoded, err := EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("EncodeDeliveryV1Request(golden): %v", err)
	}
	if !bytes.Equal(encoded, fixture) {
		t.Fatalf("canonical request differs from golden fixture\n got: %s\nwant: %s", encoded, fixture)
	}
	if len(encoded) > 0 && encoded[len(encoded)-1] == '\n' {
		t.Fatal("canonical request has trailing newline")
	}

	wantDigestBytes, err := os.ReadFile(filepath.Join("..", "..", "docs", "contracts", "fixtures", "code-delivery-v1-request.sha256"))
	if err != nil {
		t.Fatalf("read golden digest: %v", err)
	}
	wantDigest := strings.TrimSpace(string(wantDigestBytes))
	gotDigest, err := request.Digest()
	if err != nil {
		t.Fatalf("Request.Digest(): %v", err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("request digest = %q, want %q", gotDigest, wantDigest)
	}
}

func TestDeliveryV1RequestAcceptsMinimumAttemptLimitWithoutGrantingAuthority(t *testing.T) {
	request := deliveryV1Request(t)
	request.Spec.Envelope.MaxAttempts = 1
	request.Spec.Envelope.MaxConcurrent = 1

	if err := request.Validate(); err != nil {
		t.Fatalf("minimum structurally valid envelope rejected: %v", err)
	}
	encoded, err := EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode minimum structurally valid request: %v", err)
	}
	decoded, err := DecodeDeliveryV1Request(encoded)
	if err != nil {
		t.Fatalf("decode minimum structurally valid request: %v", err)
	}
	if decoded.Spec.Envelope.MaxAttempts != 1 || decoded.Spec.Envelope.MaxConcurrent != 1 {
		t.Fatalf("minimum envelope changed in round trip: %+v", decoded.Spec.Envelope)
	}
	// The codec validates wire structure only. It has no host authorization or
	// admission method, so this success cannot be interpreted as execution grant.
}

func TestDeliveryV1RequestPreservesNanosecondUTCExpiry(t *testing.T) {
	request := deliveryV1Request(t)
	request.Spec.Envelope.ExpiresAt = time.Date(2030, 1, 2, 3, 4, 5, 123456789, time.UTC)

	encoded, err := EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode nanosecond expiry: %v", err)
	}
	if !bytes.Contains(encoded, []byte(`"expires_at":"2030-01-02T03:04:05.123456789Z"`)) {
		t.Fatalf("encoded request lost exact UTC nanoseconds: %s", encoded)
	}
	decoded, err := DecodeDeliveryV1Request(encoded)
	if err != nil {
		t.Fatalf("decode nanosecond expiry: %v", err)
	}
	if !decoded.Spec.Envelope.ExpiresAt.Equal(request.Spec.Envelope.ExpiresAt) {
		t.Fatalf("expiry changed: got %s want %s", decoded.Spec.Envelope.ExpiresAt, request.Spec.Envelope.ExpiresAt)
	}
}

func TestDeliveryV1DecodeRequestDocumentSizeBoundary(t *testing.T) {
	fixture := deliveryV1RequestFixture(t)
	if len(fixture) >= DeliveryV1MaxJSONBytes {
		t.Fatalf("golden fixture unexpectedly occupies %d bytes", len(fixture))
	}
	atLimit := append(append([]byte(nil), fixture...), bytes.Repeat([]byte(" "), DeliveryV1MaxJSONBytes-len(fixture))...)
	if len(atLimit) != DeliveryV1MaxJSONBytes {
		t.Fatalf("boundary fixture has %d bytes", len(atLimit))
	}
	if _, err := DecodeDeliveryV1Request(atLimit); err != nil {
		t.Fatalf("document exactly at size limit rejected: %v", err)
	}
	overLimit := append(append([]byte(nil), atLimit...), ' ')
	if _, err := DecodeDeliveryV1Request(overLimit); !errors.Is(err, ErrDeliveryV1Oversize) {
		t.Fatalf("oversized request error = %v, want ErrDeliveryV1Oversize", err)
	}
}

func TestDeliveryV1DecodeRequestMalformedDocuments(t *testing.T) {
	fixture := string(deliveryV1RequestFixture(t))
	withoutTask := strings.Replace(fixture, `"task_id":"task-example",`, "", 1)
	if withoutTask == fixture {
		t.Fatal("test setup could not remove task_id")
	}
	duplicateTopLevel := `{"version":"code-delivery/v1",` + strings.TrimPrefix(fixture, "{")
	duplicateNested := strings.Replace(fixture, `"max_attempts":3,`, `"max_attempts":3,"max_attempts":3,`, 1)
	duplicateEscapedName := strings.Replace(fixture, `"delegation_id":"delivery-example-1",`, `"delegation_id":"delivery-example-1","delegation_\u0069d":"other",`, 1)
	unknownNested := strings.Replace(fixture, `"envelope":{`, `"envelope":{"unexpected":true,`, 1)
	caseFoldAlias := strings.Replace(fixture, `"version":`, `"Version":`, 1)

	cases := []struct {
		name string
		data []byte
	}{
		{name: "invalid utf8", data: []byte{'"', 0xff, '"'}},
		{name: "malformed json", data: []byte(`{"version":`)},
		{name: "duplicate top-level member", data: []byte(duplicateTopLevel)},
		{name: "duplicate nested member", data: []byte(duplicateNested)},
		{name: "escaped duplicate member", data: []byte(duplicateEscapedName)},
		{name: "unknown nested member", data: []byte(unknownNested)},
		{name: "case-folded alias", data: []byte(caseFoldAlias)},
		{name: "missing required member", data: []byte(withoutTask)},
		{name: "trailing second value", data: []byte(fixture + ` {}`)},
		{name: "null root", data: []byte(`null`)},
		{name: "array root", data: []byte(`[]`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeDeliveryV1Request(tc.data); !errors.Is(err, ErrDeliveryV1Malformed) {
				t.Fatalf("error = %v, want ErrDeliveryV1Malformed", err)
			}
		})
	}
}

func TestDeliveryV1RequestSemanticValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*DeliveryV1Request)
	}{
		{"wrong version", func(r *DeliveryV1Request) { r.Version = "code-delivery/v2" }},
		{"empty opaque task id", func(r *DeliveryV1Request) { r.TaskID = "" }},
		{"oversized opaque task id", func(r *DeliveryV1Request) { r.TaskID = strings.Repeat("a", 129) }},
		{"invalid opaque id control", func(r *DeliveryV1Request) { r.CallerID = "caller\x7f" }},
		{"invalid plan revision", func(r *DeliveryV1Request) { r.PlanRevision = 0 }},
		{"invalid plan digest", func(r *DeliveryV1Request) { r.PlanDigest = strings.Repeat("g", 64) }},
		{"empty acceptance", func(r *DeliveryV1Request) { r.Spec.Acceptance = nil }},
		{"too many acceptance entries", func(r *DeliveryV1Request) {
			r.Spec.Acceptance = make([]string, 65)
			for i := range r.Spec.Acceptance {
				r.Spec.Acceptance[i] = "accept"
			}
		}},
		{"paid mode", func(r *DeliveryV1Request) { r.Spec.ExecutionMode = "paid_api" }},
		{"positive cost", func(r *DeliveryV1Request) { r.Spec.Envelope.MaxCostCents = 1 }},
		{"negative cost", func(r *DeliveryV1Request) { r.Spec.Envelope.MaxCostCents = -1 }},
		{"zero attempts", func(r *DeliveryV1Request) { r.Spec.Envelope.MaxAttempts = 0 }},
		{"zero concurrency", func(r *DeliveryV1Request) { r.Spec.Envelope.MaxConcurrent = 0 }},
		{"concurrency above six", func(r *DeliveryV1Request) { r.Spec.Envelope.MaxConcurrent = 7 }},
		{"invalid commit", func(r *DeliveryV1Request) { r.Spec.SourceCommit = strings.Repeat("x", 40) }},
		{"non-utc expiry", func(r *DeliveryV1Request) {
			r.Spec.Envelope.ExpiresAt = time.Date(2030, 1, 1, 0, 0, 0, 0, time.FixedZone("zero", 0))
		}},
		{"invalid branch", func(r *DeliveryV1Request) { r.Spec.TargetBranch = "../main" }},
		{"noncanonical repository", func(r *DeliveryV1Request) { r.Spec.Repository = "https://user@example.com/owner/repo.git?x=1" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := deliveryV1Request(t)
			tc.mutate(&request)
			if err := request.Validate(); !errors.Is(err, ErrDeliveryV1Invalid) {
				t.Fatalf("Validate error = %v, want ErrDeliveryV1Invalid", err)
			}
			if _, err := EncodeDeliveryV1Request(request); !errors.Is(err, ErrDeliveryV1Invalid) {
				t.Fatalf("Encode error = %v, want ErrDeliveryV1Invalid", err)
			}
		})
	}
}

func TestDeliveryV1RequestAcceptsSHA1AndSHA256ObjectIDs(t *testing.T) {
	for _, id := range []string{strings.Repeat("a", 40), strings.Repeat("b", 64)} {
		t.Run(fmt.Sprintf("length_%d", len(id)), func(t *testing.T) {
			request := deliveryV1Request(t)
			request.Spec.SourceCommit = id
			if err := request.Validate(); err != nil {
				t.Fatalf("valid object ID rejected: %v", err)
			}
		})
	}
}

func TestDeliveryV1RequestCanonicalEscapingAndOrdering(t *testing.T) {
	request := deliveryV1Request(t)
	request.Spec.Acceptance = []string{"keep <&> exact"}
	encoded, err := EncodeDeliveryV1Request(request)
	if err != nil {
		t.Fatalf("encode escaped text: %v", err)
	}
	wantOrder := []string{`"version":`, `"delegation_id":`, `"caller_id":`, `"project_id":`, `"job_id":`, `"task_id":`, `"plan_revision":`, `"plan_digest":`, `"spec":`, `"repository":`, `"target_branch":`, `"source_commit":`, `"acceptance":`, `"policy_revision":`, `"profile_revision":`, `"execution_mode":`, `"envelope":`, `"max_attempts":`, `"max_concurrent":`, `"max_cost_cents":`, `"expires_at":`}
	last := -1
	for _, key := range wantOrder {
		idx := bytes.Index(encoded, []byte(key))
		if idx <= last {
			t.Fatalf("key %s is missing or out of order in %s", key, encoded)
		}
		last = idx
	}
	if !bytes.Contains(encoded, []byte(`keep \u003c\u0026\u003e exact`)) {
		t.Fatalf("Go JSON escaping differs from frozen contract: %s", encoded)
	}
}

func TestDeliveryV1ObservationCanonicalizationAndRequestBinding(t *testing.T) {
	request := deliveryV1Request(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("request digest: %v", err)
	}
	observation := deliveryV1Observation(request, digest)
	observation.Children = []DeliveryV1ChildTask{{
		ID: "child-author", Kind: "author", State: "pending",
		DependsOn: []string{}, FindingIDs: []string{},
	}}
	encoded, err := EncodeDeliveryV1Observation(observation, request)
	if err != nil {
		t.Fatalf("encode observation: %v", err)
	}
	if bytes.HasSuffix(encoded, []byte("\n")) {
		t.Fatal("canonical observation has trailing newline")
	}
	wantOrder := []string{`"version":`, `"delegation_id":`, `"request_digest":`, `"lifecycle_id":`, `"sequence":`, `"state":`, `"children":`, `"accounting":`, `"spent_cents":`, `"reserved_cents":`, `"unknown_cents":`, `"settlement_id":`, `"reason":`}
	last := -1
	for _, key := range wantOrder {
		idx := bytes.Index(encoded, []byte(key))
		if idx <= last {
			t.Fatalf("observation key %s is missing or out of order in %s", key, encoded)
		}
		last = idx
	}
	if !bytes.Contains(encoded, []byte(`"depends_on":[]`)) || !bytes.Contains(encoded, []byte(`"finding_ids":[]`)) {
		t.Fatalf("explicit empty arrays did not remain arrays: %s", encoded)
	}
	if bytes.Contains(encoded, []byte(`"landed"`)) {
		t.Fatalf("nil landed receipt must be omitted: %s", encoded)
	}
	decoded, err := DecodeDeliveryV1Observation(encoded, request)
	if err != nil {
		t.Fatalf("decode canonical observation: %v", err)
	}
	if err := decoded.Validate(request); err != nil {
		t.Fatalf("validate decoded observation: %v", err)
	}

	wrongRequest := request
	wrongRequest.DelegationID = "different-delegation"
	if _, err := EncodeDeliveryV1Observation(observation, wrongRequest); !errors.Is(err, ErrDeliveryV1Invalid) {
		t.Fatalf("wrong request binding error = %v, want ErrDeliveryV1Invalid", err)
	}
	wrongDigest := observation
	wrongDigest.RequestDigest = strings.Repeat("0", 64)
	if err := wrongDigest.Validate(request); !errors.Is(err, ErrDeliveryV1Invalid) {
		t.Fatalf("wrong digest error = %v, want ErrDeliveryV1Invalid", err)
	}
}

func TestDeliveryV1ObservationPreservesCallerNilArrayEncoding(t *testing.T) {
	request := deliveryV1Request(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("request digest: %v", err)
	}
	observation := deliveryV1Observation(request, digest)
	observation.Children = nil
	encoded, err := EncodeDeliveryV1Observation(observation, request)
	if err != nil {
		t.Fatalf("encode nil children: %v", err)
	}
	if !bytes.Contains(encoded, []byte(`"children":null`)) {
		t.Fatalf("nil children did not preserve caller JSON representation: %s", encoded)
	}
	decoded, err := DecodeDeliveryV1Observation(encoded, request)
	if err != nil {
		t.Fatalf("decode nil children: %v", err)
	}
	if decoded.Children != nil {
		t.Fatalf("nil children changed during round trip: %#v", decoded.Children)
	}

	observation.Children = []DeliveryV1ChildTask{{ID: "author", Kind: "author", State: "pending"}}
	encoded, err = EncodeDeliveryV1Observation(observation, request)
	if err != nil {
		t.Fatalf("encode nil child arrays: %v", err)
	}
	if !bytes.Contains(encoded, []byte(`"depends_on":null`)) || !bytes.Contains(encoded, []byte(`"finding_ids":null`)) {
		t.Fatalf("nil child arrays did not preserve caller JSON representation: %s", encoded)
	}
	decoded, err = DecodeDeliveryV1Observation(encoded, request)
	if err != nil {
		t.Fatalf("decode nil child arrays: %v", err)
	}
	if decoded.Children[0].DependsOn != nil || decoded.Children[0].FindingIDs != nil {
		t.Fatalf("nil child arrays changed during round trip: %+v", decoded.Children[0])
	}
}

func TestDeliveryV1DecodeObservationMalformedAndOversized(t *testing.T) {
	request := deliveryV1Request(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("request digest: %v", err)
	}
	observation := deliveryV1Observation(request, digest)
	encoded, err := EncodeDeliveryV1Observation(observation, request)
	if err != nil {
		t.Fatalf("encode observation: %v", err)
	}
	duplicate := bytes.Replace(encoded, []byte(`"sequence":1,`), []byte(`"sequence":1,"sequence":1,`), 1)
	unknown := bytes.Replace(encoded, []byte(`"reason":""`), []byte(`"reason":"","unknown":true`), 1)
	caseAlias := bytes.Replace(encoded, []byte(`"version":`), []byte(`"Version":`), 1)
	missing := bytes.Replace(encoded, []byte(`,"reason":""`), nil, 1)
	trailing := append(append([]byte(nil), encoded...), []byte(` {}`)...)
	badUTF8 := []byte{'{', '"', 0xff, '"', ':', '1', '}'}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"invalid utf8", badUTF8},
		{"duplicate member", duplicate},
		{"unknown member", unknown},
		{"case alias", caseAlias},
		{"missing required member", missing},
		{"trailing value", trailing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeDeliveryV1Observation(tc.data, request); !errors.Is(err, ErrDeliveryV1Malformed) {
				t.Fatalf("error = %v, want ErrDeliveryV1Malformed", err)
			}
		})
	}
	overLimit := append(append([]byte(nil), encoded...), bytes.Repeat([]byte(" "), DeliveryV1MaxJSONBytes-len(encoded)+1)...)
	if _, err := DecodeDeliveryV1Observation(overLimit, request); !errors.Is(err, ErrDeliveryV1Oversize) {
		t.Fatalf("oversized observation error = %v, want ErrDeliveryV1Oversize", err)
	}
}

func TestDeliveryV1ObservationEnumsBindingsAndGraphs(t *testing.T) {
	request := deliveryV1Request(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("request digest: %v", err)
	}
	valid := deliveryV1Observation(request, digest)
	valid.Children = []DeliveryV1ChildTask{
		{ID: "author", Kind: "author", State: "completed", PRURL: "https://github.com/example/project/pull/12", HeadCommit: strings.Repeat("a", 40)},
		{ID: "review", Kind: "review", State: "ready", DependsOn: []string{"author"}, PRURL: "https://github.com/example/project/pull/12", HeadCommit: strings.Repeat("a", 40)},
		{ID: "fix", Kind: "fix", State: "pending", DependsOn: []string{"review"}, FindingIDs: []string{"finding-1"}, PRURL: "https://github.com/example/project/pull/12"},
		{ID: "rereview", Kind: "re_review", State: "pending", DependsOn: []string{"fix"}, PRURL: "https://github.com/example/project/pull/12"},
		{ID: "delivery", Kind: "delivery", State: "pending", DependsOn: []string{"rereview"}},
	}
	if err := valid.Validate(request); err != nil {
		t.Fatalf("supported child kinds/states rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*DeliveryV1Observation)
	}{
		{"unknown observation state", func(o *DeliveryV1Observation) { o.State = "succeeded" }},
		{"zero sequence", func(o *DeliveryV1Observation) { o.Sequence = 0 }},
		{"wrong delegation", func(o *DeliveryV1Observation) { o.DelegationID = "other" }},
		{"duplicate child ID", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "author", State: "pending"}, {ID: "x", Kind: "review", State: "pending"}}
		}},
		{"unsupported child kind", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "fixer", State: "pending"}}
		}},
		{"unsupported child state", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "author", State: "done"}}
		}},
		{"unresolved dependency", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "author", State: "pending", DependsOn: []string{"missing"}}}
		}},
		{"self dependency", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "author", State: "pending", DependsOn: []string{"x"}}}
		}},
		{"cycle", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "author", State: "pending", DependsOn: []string{"y"}}, {ID: "y", Kind: "fix", State: "pending", DependsOn: []string{"x"}, FindingIDs: []string{"f"}}}
		}},
		{"duplicate dependency", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "author", State: "pending"}, {ID: "y", Kind: "review", State: "pending", DependsOn: []string{"x", "x"}}}
		}},
		{"duplicate findings", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "fix", State: "pending", FindingIDs: []string{"f", "f"}}}
		}},
		{"invalid PR URL", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "review", State: "ready", PRURL: "https://evil.example/pull/1", HeadCommit: strings.Repeat("a", 40)}}
		}},
		{"review without exact head", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "review", State: "ready", PRURL: "https://github.com/example/project/pull/12"}}
		}},
		{"author completed without handoff", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "author", State: "completed"}}
		}},
		{"fix without finding", func(o *DeliveryV1Observation) {
			o.Children = []DeliveryV1ChildTask{{ID: "x", Kind: "fix", State: "pending"}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := valid
			candidate.Children = append([]DeliveryV1ChildTask(nil), valid.Children...)
			tc.mutate(&candidate)
			if err := candidate.Validate(request); !errors.Is(err, ErrDeliveryV1Invalid) {
				t.Fatalf("Validate error = %v, want ErrDeliveryV1Invalid", err)
			}
		})
	}
}

func TestDeliveryV1ObservationAcceptsEveryFrozenEnum(t *testing.T) {
	request := deliveryV1Request(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("request digest: %v", err)
	}
	for _, state := range []string{"admitted", "running", "changes_requested", "landed", "failed", "paused", "canceled", "unknown"} {
		t.Run("observation_"+state, func(t *testing.T) {
			observation := deliveryV1Observation(request, digest)
			if state == "landed" {
				observation.Landed = &DeliveryV1LandedReceipt{
					Repository: request.Spec.Repository, TargetBranch: request.Spec.TargetBranch,
					PRURL: "https://github.com/example/project/pull/12", ReviewedHead: strings.Repeat("a", 40),
					ReviewedBase: strings.Repeat("b", 40), PolicyRevision: request.Spec.PolicyRevision,
					LandedCommit: strings.Repeat("c", 40), SourceDigest: strings.Repeat("d", 64),
					Reviewer: "reviewer", Author: "author", Verifier: "verifier", VerifiedAt: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
				}
			}
			observation.State = state
			if err := observation.Validate(request); err != nil {
				t.Fatalf("frozen observation state rejected: %v", err)
			}
		})
	}
	for _, kind := range []string{"author", "review", "fix", "re_review", "delivery"} {
		t.Run("child_kind_"+kind, func(t *testing.T) {
			observation := deliveryV1Observation(request, digest)
			child := DeliveryV1ChildTask{ID: "child-1", Kind: kind, State: "pending"}
			if kind == "fix" {
				child.FindingIDs = []string{"finding-1"}
			}
			observation.Children = []DeliveryV1ChildTask{child}
			if err := observation.Validate(request); err != nil {
				t.Fatalf("frozen child kind rejected: %v", err)
			}
		})
	}
	for _, state := range []string{"pending", "ready", "running", "completed", "blocked", "canceled"} {
		t.Run("child_state_"+state, func(t *testing.T) {
			observation := deliveryV1Observation(request, digest)
			child := DeliveryV1ChildTask{ID: "author-1", Kind: "author", State: state}
			if state == "completed" {
				child.PRURL = "https://github.com/example/project/pull/12"
				child.HeadCommit = strings.Repeat("a", 40)
			}
			observation.Children = []DeliveryV1ChildTask{child}
			if err := observation.Validate(request); err != nil {
				t.Fatalf("frozen child state rejected: %v", err)
			}
		})
	}
}

func TestDeliveryV1ObservationRequiresCompleteIndependentLandedReceipt(t *testing.T) {
	request := deliveryV1Request(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("request digest: %v", err)
	}
	validReceipt := DeliveryV1LandedReceipt{
		Repository: request.Spec.Repository, TargetBranch: request.Spec.TargetBranch,
		PRURL:        "https://github.com/example/project/pull/12",
		ReviewedHead: strings.Repeat("a", 40), ReviewedBase: strings.Repeat("b", 40),
		PolicyRevision: request.Spec.PolicyRevision, LandedCommit: strings.Repeat("c", 64),
		SourceDigest: strings.Repeat("d", 64), Reviewer: "reviewer-1", Author: "author-1",
		Verifier: "host-verifier-1", VerifiedAt: time.Date(2030, 1, 2, 3, 4, 5, 123456789, time.UTC),
	}
	observation := deliveryV1Observation(request, digest)
	observation.State = "landed"
	observation.Landed = &validReceipt
	if err := observation.Validate(request); err != nil {
		t.Fatalf("complete landed receipt rejected: %v", err)
	}
	encoded, err := EncodeDeliveryV1Observation(observation, request)
	if err != nil {
		t.Fatalf("encode landed observation: %v", err)
	}
	if !bytes.Contains(encoded, []byte(`"verified_at":"2030-01-02T03:04:05.123456789Z"`)) {
		t.Fatalf("landed verification time lost UTC nanoseconds: %s", encoded)
	}
	decoded, err := DecodeDeliveryV1Observation(encoded, request)
	if err != nil {
		t.Fatalf("decode landed observation: %v", err)
	}
	if decoded.Landed == nil || !decoded.Landed.VerifiedAt.Equal(validReceipt.VerifiedAt) {
		t.Fatalf("landed evidence did not survive round trip: %+v", decoded.Landed)
	}

	cases := []struct {
		name   string
		mutate func(*DeliveryV1Observation)
	}{
		{"missing receipt", func(o *DeliveryV1Observation) { o.Landed = nil }},
		{"reviewer equals author", func(o *DeliveryV1Observation) { r := *o.Landed; r.Reviewer = r.Author; o.Landed = &r }},
		{"wrong repository", func(o *DeliveryV1Observation) {
			r := *o.Landed
			r.Repository = "https://github.com/other/repo"
			o.Landed = &r
		}},
		{"wrong target", func(o *DeliveryV1Observation) { r := *o.Landed; r.TargetBranch = "release"; o.Landed = &r }},
		{"wrong policy", func(o *DeliveryV1Observation) { r := *o.Landed; r.PolicyRevision = "other-policy"; o.Landed = &r }},
		{"bad source digest", func(o *DeliveryV1Observation) { r := *o.Landed; r.SourceDigest = "bad"; o.Landed = &r }},
		{"non-UTC verification time", func(o *DeliveryV1Observation) {
			r := *o.Landed
			r.VerifiedAt = time.Date(2030, 1, 2, 3, 4, 5, 0, time.FixedZone("zero", 0))
			o.Landed = &r
		}},
		{"receipt on non-landed state", func(o *DeliveryV1Observation) { o.State = "running" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := observation
			receipt := validReceipt
			candidate.Landed = &receipt
			tc.mutate(&candidate)
			if err := candidate.Validate(request); !errors.Is(err, ErrDeliveryV1Invalid) {
				t.Fatalf("Validate error = %v, want ErrDeliveryV1Invalid", err)
			}
		})
	}
}

func TestDeliveryV1ObservationRejectsAccountingOverflowAndNonzeroCost(t *testing.T) {
	request := deliveryV1Request(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("request digest: %v", err)
	}
	cases := []struct {
		name       string
		accounting DeliveryV1Accounting
	}{
		{"negative", DeliveryV1Accounting{SpentCents: -1}},
		{"spent and reserved overflow", DeliveryV1Accounting{SpentCents: int64(^uint64(0) >> 1), ReservedCents: 1}},
		{"unknown overflow", DeliveryV1Accounting{SpentCents: int64(^uint64(0) >> 1), UnknownCents: 1}},
		{"nonzero subscription spend", DeliveryV1Accounting{SpentCents: 1}},
		{"nonzero reserved", DeliveryV1Accounting{ReservedCents: 1}},
		{"nonzero unknown exposure", DeliveryV1Accounting{UnknownCents: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			observation := deliveryV1Observation(request, digest)
			observation.Accounting = tc.accounting
			if err := observation.Validate(request); !errors.Is(err, ErrDeliveryV1Invalid) {
				t.Fatalf("Validate error = %v, want ErrDeliveryV1Invalid", err)
			}
			if _, err := EncodeDeliveryV1Observation(observation, request); !errors.Is(err, ErrDeliveryV1Invalid) {
				t.Fatalf("Encode error = %v, want ErrDeliveryV1Invalid", err)
			}
		})
	}
}

func deliveryV1RequestFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "contracts", "fixtures", "code-delivery-v1-request.json"))
	if err != nil {
		t.Fatalf("read v1 request fixture: %v", err)
	}
	return data
}

func deliveryV1Request(t *testing.T) DeliveryV1Request {
	t.Helper()
	request, err := DecodeDeliveryV1Request(deliveryV1RequestFixture(t))
	if err != nil {
		t.Fatalf("decode v1 request fixture: %v", err)
	}
	return request
}

func deliveryV1Observation(request DeliveryV1Request, requestDigest string) DeliveryV1Observation {
	return DeliveryV1Observation{
		Version: DeliveryV1ProtocolVersion, DelegationID: request.DelegationID,
		RequestDigest: requestDigest, LifecycleID: "lifecycle-example-1", Sequence: 1,
		State: "admitted", Children: []DeliveryV1ChildTask{},
		Accounting: DeliveryV1Accounting{}, Reason: "",
	}
}
