//go:build aprl_host_task_shim

package system

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/plantasks"
)

func TestCodeDeliveryV1HTTPPutLostReplyConcurrentReplayAndConflict(t *testing.T) {
	f := newDeliveryV1Fixture(t, true)
	endpoint := "/v1/delegations/" + f.request.DelegationID

	wrongCaller := f.request
	wrongCaller.CallerID = "caller-unrelated"
	wrongBody, err := plantasks.EncodeDeliveryV1Request(wrongCaller)
	if err != nil {
		t.Fatalf("encode caller-mismatch request: %v", err)
	}
	status, body := f.httpRequest(t, http.MethodPut, endpoint, f.caller, wrongBody)
	if status != http.StatusUnauthorized {
		t.Fatalf("caller-mismatched PUT status=%d body=%s, want 401", status, body)
	}
	if count := codeDeliveryV1BindingCount(t, f); count != 0 {
		t.Fatalf("caller mismatch persisted %d delegation bindings", count)
	}

	// This test-only TLS hook drops the response after Submit and projection
	// commit, so GET must recover the durable binding.
	f.submitLostReply(t)
	recovered := f.observation(t)
	if recovered.LifecycleID == "" || recovered.Sequence == 0 || recovered.State != "admitted" {
		t.Fatalf("caller-scoped GET did not recover the committed PUT: %+v", recovered)
	}
	if f.lifecycleID != recovered.LifecycleID {
		t.Fatalf("recovered lifecycle %s differs from submitted lifecycle %s", recovered.LifecycleID, f.lifecycleID)
	}

	const concurrentPuts = 8
	type putResult struct {
		status int
		body   []byte
	}
	start := make(chan struct{})
	results := make(chan putResult, concurrentPuts)
	var group sync.WaitGroup
	for range concurrentPuts {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			status, body := f.httpRequest(t, http.MethodPut, endpoint, f.caller, f.raw)
			results <- putResult{status: status, body: body}
		}()
	}
	close(start)
	group.Wait()
	close(results)
	for result := range results {
		if result.status != http.StatusOK {
			t.Fatalf("identical concurrent PUT status=%d body=%s, want idempotent 200", result.status, result.body)
		}
		observation, err := plantasks.DecodeDeliveryV1Observation(result.body, f.request)
		if err != nil || observation.LifecycleID != recovered.LifecycleID {
			t.Fatalf("concurrent PUT returned binding %+v err=%v, want lifecycle %s", observation, err, recovered.LifecycleID)
		}
	}
	if count := codeDeliveryV1BindingCount(t, f); count != 1 {
		t.Fatalf("identical concurrent PUTs created %d bindings, want one", count)
	}
	beforeRestart := f.observation(t)
	afterRestart := codeDeliveryV1ReopenAndGet(t, f)
	if !reflect.DeepEqual(beforeRestart, afterRestart) {
		t.Fatalf("caller-scoped observation changed across service/store reconstruction:\nbefore=%+v\nafter=%+v", beforeRestart, afterRestart)
	}

	changed := f.request
	changed.Spec.Acceptance = append(append([]string(nil), changed.Spec.Acceptance...), "a distinct immutable acceptance item")
	changedRaw, err := plantasks.EncodeDeliveryV1Request(changed)
	if err != nil {
		t.Fatalf("encode changed-digest request: %v", err)
	}
	status, body = f.httpRequest(t, http.MethodPut, endpoint, f.caller, changedRaw)
	if status != http.StatusConflict || codeDeliveryV1HTTPError(body) != "conflict" {
		t.Fatalf("changed digest PUT status=%d body=%s, want 409 conflict", status, body)
	}
	foreignStatus, foreignBody := f.httpRequest(t, http.MethodGet, endpoint, "caller-foreign", nil)
	if foreignStatus != http.StatusNotFound || codeDeliveryV1HTTPError(foreignBody) != "not_found" {
		t.Fatalf("foreign caller GET status=%d body=%s, want caller-scoped 404", foreignStatus, foreignBody)
	}
}

func codeDeliveryV1ReopenAndGet(t *testing.T, f *deliveryV1Fixture) plantasks.DeliveryV1Observation {
	t.Helper()
	store, err := plantasks.NewStore(f.db.Pool, f.manual)
	if err != nil {
		t.Fatalf("reconstruct durable delegation store: %v", err)
	}
	delegations, err := plantasks.NewDelegations(store, plantasks.DeliveryV1HTTPCallerAuthenticator{}, deliveryV1AdmissionPolicy{}, f.factory)
	if err != nil {
		t.Fatalf("reconstruct caller-scoped delegation service: %v", err)
	}
	projector, err := plantasks.NewDeliveryV1Projector(store)
	if err != nil {
		t.Fatalf("reconstruct durable delegation projector: %v", err)
	}
	handler, err := plantasks.NewDeliveryV1HTTPHandler(deliveryV1Bearer{}, delegations, projector)
	if err != nil {
		t.Fatalf("reconstruct TLS delegation handler: %v", err)
	}
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 10 * time.Second
	request, err := http.NewRequestWithContext(f.ctx, http.MethodGet, server.URL+"/v1/delegations/"+f.request.DelegationID, nil)
	if err != nil {
		t.Fatalf("construct post-restart GET: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+f.caller)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET after service/store reconstruction: %v", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, plantasks.DeliveryV1MaxJSONBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read reconstructed GET response: read=%v close=%v", readErr, closeErr)
	}
	if response.StatusCode != http.StatusOK || len(body) > plantasks.DeliveryV1MaxJSONBytes {
		t.Fatalf("GET after reconstruction status=%d body=%s", response.StatusCode, body)
	}
	return codeDeliveryV1DecodeObservation(t, body, f.request)
}

func codeDeliveryV1DecodeObservation(t *testing.T, body []byte, request plantasks.DeliveryV1Request) plantasks.DeliveryV1Observation {
	t.Helper()
	observation, err := plantasks.DecodeDeliveryV1Observation(body, request)
	if err != nil {
		t.Fatalf("decode caller-scoped observation: %v", err)
	}
	return observation
}

func TestCodeDeliveryV1StandaloneAndDelegatedLifecycleThroughGenericShim(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		name := "standalone"
		if delegated {
			name = "delegated"
		}
		t.Run(name, func(t *testing.T) {
			f := newDeliveryV1Fixture(t, delegated)
			f.submit(t)
			shim := newDeliveryV1Shim(t, f)
			initial := shim.List(t)
			if initial.LifecycleID != f.lifecycleID || initial.DeliveryGateID == "" || initial.DeliveryGateClaimID == "" {
				t.Fatalf("generic provider omitted the durable lifecycle/gate binding: %+v", initial)
			}
			gateID, gateClaimID := initial.DeliveryGateID, initial.DeliveryGateClaimID
			if gateClaimID != "T-"+gateID {
				t.Fatalf("stable gate claim ID=%q, want T-%s", gateClaimID, gateID)
			}
			author := codeDeliveryV1VisibleTask(t, f, initial, plantasks.StageAuthor, 0)
			gate := codeDeliveryV1VisibleTask(t, f, initial, plantasks.StageReview, 0)
			if author.TaskID == gate.TaskID || author.ClaimID == gate.ClaimID || gate.Ready {
				t.Fatalf("initial author/review projection has invalid identity or readiness: author=%+v review=%+v", author, gate)
			}
			if author.Dependencies == nil || len(author.Dependencies) != 0 {
				t.Fatalf("generic provider author dependencies must be an empty list: %+v", author.Dependencies)
			}
			if len(initial.Tasks) != 2 {
				t.Fatalf("initial projection contains %d task rows, want actual author and stable review only", len(initial.Tasks))
			}
			followup := codeDeliveryV1Followup{ID: "T-followup", BlockedBy: []string{gateClaimID}}
			codeDeliveryV1RequireFollowup(t, followup, initial, false)

			authorPR := f.pr()
			authorClaim := codeDeliveryV1Admit(t, f, shim, author.TaskID, f.author, f.request.Spec.Envelope.ExpiresAt)
			authorReceipt := codeDeliveryV1Record(t, f, shim, author.TaskID, plantasks.OutcomeCodingHandoff, &authorPR, nil, authorClaim)
			shim.Release(t, author.TaskID, authorClaim)
			if authorReceipt.Actor.ActorID != f.author.ActorID || authorReceipt.Actor.SourceRevision != authorPR.HeadSHA {
				t.Fatalf("author handoff lost actor/revision provenance: %+v", authorReceipt)
			}

			afterHandoff := shim.List(t)
			codeDeliveryV1RequireStableGate(t, afterHandoff, gateID, gateClaimID)
			codeDeliveryV1RequireFollowup(t, followup, afterHandoff, false)
			gate = codeDeliveryV1VisibleTask(t, f, afterHandoff, plantasks.StageReview, 0)
			if !gate.Ready || gate.PRURL != authorPR.URL || gate.HeadSHA != authorPR.HeadSHA || gate.BaseSHA != authorPR.BaseSHA {
				t.Fatalf("handed-off review is not bound to the published PR snapshot: %+v", gate)
			}
			reviewClaim := codeDeliveryV1Admit(t, f, shim, gate.TaskID, f.reviewer, f.request.Spec.Envelope.ExpiresAt)
			findings := []string{deliveryV1NewID(t)}
			reviewReceipt := codeDeliveryV1Record(t, f, shim, gate.TaskID, plantasks.OutcomeChangesRequest, &authorPR, findings, reviewClaim)
			shim.Release(t, gate.TaskID, reviewClaim)
			if reviewReceipt.Actor.ActorID == authorReceipt.Actor.ActorID || reviewReceipt.Outcome != plantasks.OutcomeChangesRequest {
				t.Fatalf("review did not remain independent and negative: %+v", reviewReceipt)
			}

			afterNegative := shim.List(t)
			codeDeliveryV1RequireStableGate(t, afterNegative, gateID, gateClaimID)
			codeDeliveryV1RequireFollowup(t, followup, afterNegative, false)
			if len(afterNegative.Tasks) != 4 {
				t.Fatalf("bounded correction projection has %d task rows, want author, stable review, fix and re-review", len(afterNegative.Tasks))
			}
			if got := codeDeliveryV1PoolTask(t, afterNegative, gateClaimID)["review_outcome"]; got != string(plantasks.OutcomeChangesRequest) {
				t.Fatalf("negative review projected outcome=%v, want changes_requested", got)
			}
			if got := codeDeliveryV1PoolTask(t, afterNegative, gateClaimID)["status"]; got == "done" || got == "landed" {
				t.Fatalf("negative review prematurely completed the stable gate: status=%v", got)
			}
			fix := codeDeliveryV1VisibleTask(t, f, afterNegative, plantasks.StageFix, 1)
			rereview := codeDeliveryV1VisibleTask(t, f, afterNegative, plantasks.StageRereview, 1)
			if !fix.Ready || rereview.Ready || !reflect.DeepEqual(fix.FindingIDs, findings) {
				t.Fatalf("correction/re-review readiness or findings are wrong: fix=%+v rereview=%+v", fix, rereview)
			}

			fixedPR := authorPR
			fixedPR.HeadSHA = strings.Repeat("c", 40)
			fixer := f.fixer
			fixer.SourceRevision = fixedPR.HeadSHA
			f.fixer = fixer
			f.setActor(fixer)
			fixClaim := codeDeliveryV1Admit(t, f, shim, fix.TaskID, fixer, f.request.Spec.Envelope.ExpiresAt)
			fixReceipt := codeDeliveryV1Record(t, f, shim, fix.TaskID, plantasks.OutcomeCodingHandoff, &fixedPR, nil, fixClaim)
			shim.Release(t, fix.TaskID, fixClaim)
			if fixReceipt.Actor.ActorID == reviewReceipt.Actor.ActorID || fixReceipt.PR.HeadSHA == authorPR.HeadSHA {
				t.Fatalf("fix did not publish a distinct corrected revision: %+v", fixReceipt)
			}

			f.reviewer.SourceRevision = fixedPR.HeadSHA
			f.setActor(f.reviewer)
			afterFix := shim.List(t)
			codeDeliveryV1RequireStableGate(t, afterFix, gateID, gateClaimID)
			codeDeliveryV1RequireFollowup(t, followup, afterFix, false)
			rereview = codeDeliveryV1VisibleTask(t, f, afterFix, plantasks.StageRereview, 1)
			if !rereview.Ready || rereview.HeadSHA != fixedPR.HeadSHA || rereview.BaseSHA != fixedPR.BaseSHA {
				t.Fatalf("re-review is not bound to the corrected head/base: %+v", rereview)
			}

			rereviewClaim := codeDeliveryV1Admit(t, f, shim, rereview.TaskID, f.reviewer, f.request.Spec.Envelope.ExpiresAt)
			approvedReceipt := codeDeliveryV1Record(t, f, shim, rereview.TaskID, plantasks.OutcomeApproved, &fixedPR, nil, rereviewClaim)
			if approvedReceipt.Actor.ActorID != reviewReceipt.Actor.ActorID {
				t.Fatal("independent reviewer identity changed between review stages")
			}
			afterApproval := shim.List(t)
			codeDeliveryV1RequireStableGate(t, afterApproval, gateID, gateClaimID)
			codeDeliveryV1RequireFollowup(t, followup, afterApproval, false)
			mergedReceipt := codeDeliveryV1Record(t, f, shim, rereview.TaskID, plantasks.OutcomeMerged, &fixedPR, nil, rereviewClaim)
			if mergedReceipt.PR == nil || mergedReceipt.PR.HeadSHA != fixedPR.HeadSHA || mergedReceipt.PR.BaseSHA != fixedPR.BaseSHA {
				t.Fatalf("merge receipt did not preserve approved source binding: %+v", mergedReceipt)
			}
			beforeLanding := shim.List(t)
			codeDeliveryV1RequireStableGate(t, beforeLanding, gateID, gateClaimID)
			codeDeliveryV1RequireFollowup(t, followup, beforeLanding, false)
			if got := codeDeliveryV1PoolTask(t, beforeLanding, gateClaimID)["status"]; got == "done" || got == "landed" {
				t.Fatalf("approved or merged review prematurely released delivery gate: status=%v", got)
			}
			landedReceipt := f.receipt(t, rereview.TaskID, plantasks.OutcomeLanded, &fixedPR, nil)
			landedReceipt.MergeCommit = mergedReceipt.MergeCommit
			landedReceipt.LandedCommit = mergedReceipt.MergeCommit
			f.trustLanding(t, landedReceipt)
			if err := shim.Result(landedReceipt, rereviewClaim, f.load(t).Revision); err != nil {
				t.Fatalf("record trusted landed receipt through generic shim: %v", err)
			}
			shim.Release(t, rereview.TaskID, rereviewClaim)
			afterLanding := shim.List(t)
			codeDeliveryV1RequireStableGate(t, afterLanding, gateID, gateClaimID)
			codeDeliveryV1RequireFollowup(t, followup, afterLanding, true)
			if afterLanding.DeliveryReceiptID != landedReceipt.ID {
				t.Fatalf("landed gate receipt ID=%q, want %s", afterLanding.DeliveryReceiptID, landedReceipt.ID)
			}
			gateRow := codeDeliveryV1PoolTask(t, afterLanding, gateClaimID)
			if gateRow["delivery_status"] != "landed" || gateRow["status"] != "done" {
				t.Fatalf("exact verified landing did not complete the stable gate: %+v", gateRow)
			}
			persisted := f.load(t)
			if persisted.DeliveryReceiptID != landedReceipt.ID || persisted.Completed[rereview.TaskID] != plantasks.OutcomeLanded {
				t.Fatalf("durable lifecycle did not record exact landing: delivery_receipt=%s completed=%v", persisted.DeliveryReceiptID, persisted.Completed)
			}
		})
	}
}

func TestCodeDeliveryV1AdmissionDenialsPreserveDurableState(t *testing.T) {
	t.Run("stale revision", func(t *testing.T) {
		f := newDeliveryV1Fixture(t, true)
		f.submit(t)
		shim := newDeliveryV1Shim(t, f)
		initial := shim.List(t)
		author := codeDeliveryV1VisibleTask(t, f, initial, plantasks.StageAuthor, 0)
		claim := shim.Claim(t, author.TaskID, f.author.ActorID)
		f.setActor(f.author)
		fresh := shim.List(t)
		if err := shim.Admit(author.TaskID, claim, fresh.Revision, f.request.Spec.Envelope.ExpiresAt); err != nil {
			t.Fatalf("initial valid author admission: %v", err)
		}
		before := f.load(t)
		beforeBinding := f.binding(t)
		providerCalls := shim.ProviderCalls()
		response, callErr := shim.Call(map[string]any{"action": "admit", "lifecycle_id": f.lifecycleID, "task_id": author.TaskID, "expected_revision": fresh.Revision, "claim_sha": claim, "expires_at": f.request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano)})
		codeDeliveryV1RequireProviderDenial(t, shim, providerCalls, response, callErr)
		codeDeliveryV1AssertUnchanged(t, f, before, beforeBinding)
		shim.Release(t, author.TaskID, claim)
	})

	t.Run("exact expiry", func(t *testing.T) {
		f := newDeliveryV1Fixture(t, true)
		f.submit(t)
		shim := newDeliveryV1Shim(t, f)
		initial := shim.List(t)
		author := codeDeliveryV1VisibleTask(t, f, initial, plantasks.StageAuthor, 0)
		claim := shim.Claim(t, author.TaskID, f.author.ActorID)
		f.setActor(f.author)
		before := f.load(t)
		beforeBinding := f.binding(t)
		f.manual.Set(f.request.Spec.Envelope.ExpiresAt.Add(time.Nanosecond))
		providerCalls := shim.ProviderCalls()
		response, callErr := shim.Call(map[string]any{"action": "admit", "lifecycle_id": f.lifecycleID, "task_id": author.TaskID, "expected_revision": initial.Revision, "claim_sha": claim, "expires_at": f.request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano)})
		codeDeliveryV1RequireProviderDenial(t, shim, providerCalls, response, callErr)
		codeDeliveryV1AssertUnchanged(t, f, before, beforeBinding)
		shim.Release(t, author.TaskID, claim)
	})

	t.Run("cancellation fences claim and result", func(t *testing.T) {
		f := newDeliveryV1Fixture(t, true)
		f.submit(t)
		shim := newDeliveryV1Shim(t, f)
		initial := shim.List(t)
		author := codeDeliveryV1VisibleTask(t, f, initial, plantasks.StageAuthor, 0)
		claim := shim.Claim(t, author.TaskID, f.author.ActorID)
		f.setActor(f.author)
		status, body := f.httpRequest(t, http.MethodPost, "/v1/delegations/"+f.request.DelegationID+"/cancel", f.caller, []byte("{\n  }"))
		if status != http.StatusOK {
			t.Fatalf("cancel active scope status=%d body=%s", status, body)
		}
		canceled := f.load(t)
		binding := f.binding(t)
		before := canceled
		providerCalls := shim.ProviderCalls()
		response, callErr := shim.Call(map[string]any{"action": "admit", "lifecycle_id": f.lifecycleID, "task_id": author.TaskID, "expected_revision": canceled.Revision, "claim_sha": claim, "expires_at": f.request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano)})
		codeDeliveryV1RequireProviderDenial(t, shim, providerCalls, response, callErr)
		codeDeliveryV1AssertUnchanged(t, f, before, binding)
		shim.Release(t, author.TaskID, claim)
	})
}

func TestCodeDeliveryV1LateResultsAreDeniedAfterStaleExpiryOrCancellation(t *testing.T) {
	for _, mode := range []string{"stale_revision", "expired_scope", "cancelled_scope"} {
		t.Run(mode, func(t *testing.T) {
			f := newDeliveryV1Fixture(t, true)
			f.submit(t)
			shim := newDeliveryV1Shim(t, f)
			initial := shim.List(t)
			author := codeDeliveryV1VisibleTask(t, f, initial, plantasks.StageAuthor, 0)
			claim := codeDeliveryV1Admit(t, f, shim, author.TaskID, f.author, f.request.Spec.Envelope.ExpiresAt)

			// An UNKNOWN receipt preserves the live claim and the ability to report
			// further evidence, while granting no completion or delivery authority.
			beforeUnknown := f.load(t)
			unknown := codeDeliveryV1Record(t, f, shim, author.TaskID, plantasks.OutcomeUnknown, nil, nil, claim)
			beforeFence := f.load(t)
			if _, ok := beforeFence.Claims[author.TaskID]; !ok {
				t.Fatalf("UNKNOWN receipt discarded the live claim needed to bind later evidence: claims=%v", beforeFence.Claims)
			}
			if beforeFence.Receipts[unknown.ID].Outcome != plantasks.OutcomeUnknown {
				t.Fatalf("UNKNOWN audit receipt was not retained: id=%s receipts=%v", unknown.ID, beforeFence.Receipts)
			}
			if beforeFence.Completed[author.TaskID] != "" || beforeFence.DeliveryReceiptID != "" {
				t.Fatalf("UNKNOWN receipt granted completion/delivery authority: completed=%v delivery=%q", beforeFence.Completed, beforeFence.DeliveryReceiptID)
			}
			positive := f.receipt(t, author.TaskID, plantasks.OutcomeCodingHandoff, ptrPR(f.pr()), nil)
			if positive.ID == unknown.ID {
				t.Fatal("fixture reused receipt identity")
			}
			var expectedRevision int64
			switch mode {
			case "stale_revision":
				expectedRevision = beforeUnknown.Revision
				if expectedRevision == beforeFence.Revision {
					t.Fatal("UNKNOWN receipt did not advance the lifecycle revision")
				}
			case "expired_scope":
				f.manual.Set(f.request.Spec.Envelope.ExpiresAt.Add(time.Nanosecond))
				expectedRevision = beforeFence.Revision
			case "cancelled_scope":
				status, body := f.httpRequest(t, http.MethodPost, "/v1/delegations/"+f.request.DelegationID+"/cancel", f.caller, []byte("{}"))
				if status != http.StatusOK {
					t.Fatalf("cancel after UNKNOWN receipt status=%d body=%s", status, body)
				}
				beforeFence = f.load(t)
				expectedRevision = beforeFence.Revision
			}

			before := f.load(t)
			beforeBinding := f.binding(t)
			providerCalls := shim.ProviderCalls()
			response, callErr := shim.Call(map[string]any{"action": "result", "lifecycle_id": f.lifecycleID, "task_id": positive.TaskID, "expected_revision": expectedRevision, "claim_sha": claim, "receipt": positive})
			codeDeliveryV1RequireProviderDenial(t, shim, providerCalls, response, callErr)
			codeDeliveryV1AssertUnchanged(t, f, before, beforeBinding)
			shim.Release(t, author.TaskID, claim)
		})
	}
}

func ptrPR(pr plantasks.PRBinding) *plantasks.PRBinding { return &pr }

func TestCodeDeliveryV1GlobalAttemptCapIncludesMandatoryReview(t *testing.T) {
	f := newDeliveryV1Fixture(t, true)
	f.request.Spec.Envelope.MaxAttempts = 3
	var err error
	f.raw, err = plantasks.EncodeDeliveryV1Request(f.request)
	if err != nil {
		t.Fatalf("encode three-attempt request: %v", err)
	}
	f.submit(t)
	shim := newDeliveryV1Shim(t, f)
	initial := shim.List(t)
	gateID, gateClaimID := initial.DeliveryGateID, initial.DeliveryGateClaimID
	author := codeDeliveryV1VisibleTask(t, f, initial, plantasks.StageAuthor, 0)
	pr := f.pr()
	authorClaim := codeDeliveryV1Admit(t, f, shim, author.TaskID, f.author, f.request.Spec.Envelope.ExpiresAt)
	codeDeliveryV1Record(t, f, shim, author.TaskID, plantasks.OutcomeCodingHandoff, &pr, nil, authorClaim)
	afterHandoff := shim.List(t)
	gate := codeDeliveryV1VisibleTask(t, f, afterHandoff, plantasks.StageReview, 0)
	reviewClaim := codeDeliveryV1Admit(t, f, shim, gate.TaskID, f.reviewer, f.request.Spec.Envelope.ExpiresAt)
	finding := deliveryV1NewID(t)
	codeDeliveryV1Record(t, f, shim, gate.TaskID, plantasks.OutcomeChangesRequest, &pr, []string{finding}, reviewClaim)
	afterNegative := shim.List(t)
	fix := codeDeliveryV1VisibleTask(t, f, afterNegative, plantasks.StageFix, 1)
	fixedPR := pr
	fixedPR.HeadSHA = strings.Repeat("c", 40)
	f.fixer.SourceRevision = fixedPR.HeadSHA
	fixClaim := codeDeliveryV1Admit(t, f, shim, fix.TaskID, f.fixer, f.request.Spec.Envelope.ExpiresAt)
	codeDeliveryV1Record(t, f, shim, fix.TaskID, plantasks.OutcomeCodingHandoff, &fixedPR, nil, fixClaim)
	shim.Release(t, fix.TaskID, fixClaim)
	before := f.load(t)
	beforeBinding := f.binding(t)
	f.reviewer.SourceRevision = fixedPR.HeadSHA
	f.setActor(f.reviewer)
	afterFix := shim.List(t)
	rereview := codeDeliveryV1VisibleTask(t, f, afterFix, plantasks.StageRereview, 1)
	if rereview.Ready {
		t.Fatalf("mandatory re-review ignored the exhausted aggregate attempt cap: %+v", rereview)
	}
	reviewAttempt := shim.Claim(t, rereview.TaskID, f.reviewer.ActorID)
	f.setActor(f.reviewer)
	providerCalls := shim.ProviderCalls()
	response, callErr := shim.Call(map[string]any{"action": "admit", "lifecycle_id": f.lifecycleID, "task_id": rereview.TaskID, "expected_revision": afterFix.Revision, "claim_sha": reviewAttempt, "expires_at": f.request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano)})
	codeDeliveryV1RequireProviderDenial(t, shim, providerCalls, response, callErr)
	codeDeliveryV1AssertUnchanged(t, f, before, beforeBinding)
	codeDeliveryV1RequireStableGate(t, shim.List(t), gateID, gateClaimID)
	shim.Release(t, rereview.TaskID, reviewAttempt)
}

func TestCodeDeliveryV1ReadyProjectionDoesNotGrantAdmission(t *testing.T) {
	f := newDeliveryV1Fixture(t, true)
	f.submit(t)
	f.setActor(f.author)
	shim := newDeliveryV1Shim(t, f)
	snapshot := shim.List(t)
	author := codeDeliveryV1VisibleTask(t, f, snapshot, plantasks.StageAuthor, 0)
	if !author.Ready {
		t.Fatalf("fixture author should be advisory-ready before a claim: %+v", author)
	}
	before := f.load(t)
	beforeBinding := f.binding(t)
	providerCalls := shim.ProviderCalls()
	response, callErr := shim.Call(map[string]any{"action": "admit", "lifecycle_id": f.lifecycleID, "task_id": author.TaskID, "expected_revision": snapshot.Revision, "claim_sha": strings.Repeat("f", 40), "expires_at": f.request.Spec.Envelope.ExpiresAt.Format(time.RFC3339Nano)})
	codeDeliveryV1RequireProviderDenial(t, shim, providerCalls, response, callErr)
	codeDeliveryV1AssertUnchanged(t, f, before, beforeBinding)
}

func codeDeliveryV1Admit(t *testing.T, f *deliveryV1Fixture, shim *deliveryV1Shim, taskID string, actor plantasks.Provenance, expiresAt time.Time) string {
	t.Helper()
	f.setActor(actor)
	claim := shim.Claim(t, taskID, actor.ActorID)
	snapshot := shim.List(t)
	if snapshot.LifecycleID != f.lifecycleID {
		t.Fatalf("fresh snapshot lifecycle=%s, want %s", snapshot.LifecycleID, f.lifecycleID)
	}
	if err := shim.Admit(taskID, claim, snapshot.Revision, expiresAt); err != nil {
		t.Fatalf("admit actual WON claim for task %s: %v", taskID, err)
	}
	return claim
}

func codeDeliveryV1Record(t *testing.T, f *deliveryV1Fixture, shim *deliveryV1Shim, taskID string, outcome plantasks.Outcome, pr *plantasks.PRBinding, findings []string, claim string) plantasks.Receipt {
	t.Helper()
	state := f.load(t)
	task, ok := state.Tasks[taskID]
	if !ok {
		t.Fatalf("result task %s is not in lifecycle", taskID)
	}
	f.setActor(codeDeliveryV1ActorForStage(f, task))
	receipt := f.receipt(t, taskID, outcome, pr, findings)
	if outcome == plantasks.OutcomeLanded {
		f.trustLanding(t, receipt)
	}
	snapshot := shim.List(t)
	if snapshot.LifecycleID != f.lifecycleID {
		t.Fatalf("result snapshot lifecycle=%s, want %s", snapshot.LifecycleID, f.lifecycleID)
	}
	if err := shim.Result(receipt, claim, snapshot.Revision); err != nil {
		t.Fatalf("record %s through generic shim: %v", outcome, err)
	}
	return receipt
}

func codeDeliveryV1ActorForStage(f *deliveryV1Fixture, task plantasks.Task) plantasks.Provenance {
	switch task.Stage {
	case plantasks.StageAuthor:
		return f.author
	case plantasks.StageFix:
		return f.fixer
	case plantasks.StageReview, plantasks.StageRereview:
		return f.reviewer
	default:
		return plantasks.Provenance{}
	}
}

func codeDeliveryV1VisibleTask(t *testing.T, f *deliveryV1Fixture, snapshot deliveryV1ShimSnapshot, stage plantasks.Stage, correction int) plantasks.VisibleTask {
	t.Helper()
	state := f.load(t)
	var found []plantasks.VisibleTask
	for _, row := range snapshot.Tasks {
		if row.Stage != stage {
			continue
		}
		task, ok := state.Tasks[row.TaskID]
		if ok && task.Correction == correction {
			found = append(found, row)
		}
	}
	if len(found) != 1 {
		t.Fatalf("snapshot has %d visible %s correction-%d rows, want one: %+v", len(found), stage, correction, snapshot.Tasks)
	}
	return found[0]
}

type codeDeliveryV1Followup struct {
	ID        string
	BlockedBy []string
}

func codeDeliveryV1RequireFollowup(t *testing.T, followup codeDeliveryV1Followup, snapshot deliveryV1ShimSnapshot, wantReady bool) {
	t.Helper()
	if followup.ID != "T-followup" || len(followup.BlockedBy) != 1 || followup.BlockedBy[0] != snapshot.DeliveryGateClaimID {
		t.Fatalf("test-owned follow-up is not blocked by the stable delivery gate: followup=%+v gate=%s", followup, snapshot.DeliveryGateClaimID)
	}
	for _, row := range snapshot.PoolTasks {
		if row["task_id"] == followup.ID || row["claim_id"] == followup.ID {
			t.Fatalf("ordinary follow-up unexpectedly entered canonical lifecycle projection: %+v", row)
		}
	}
	byClaim := make(map[string]map[string]any, len(snapshot.PoolTasks))
	for _, row := range snapshot.PoolTasks {
		claimID, _ := row["claim_id"].(string)
		if claimID != "" {
			byClaim[claimID] = row
		}
	}
	ready := true
	for _, dependency := range followup.BlockedBy {
		row, ok := byClaim[dependency]
		if !ok || row["status"] != "done" {
			ready = false
		}
	}
	gate := codeDeliveryV1PoolTask(t, snapshot, snapshot.DeliveryGateClaimID)
	if ready != wantReady {
		t.Fatalf("ordinary follow-up readiness=%t from actual pool status=%v, want %t", ready, gate["status"], wantReady)
	}
}

func codeDeliveryV1RequireStableGate(t *testing.T, snapshot deliveryV1ShimSnapshot, gateID, gateClaimID string) {
	t.Helper()
	if snapshot.DeliveryGateID != gateID || snapshot.DeliveryGateClaimID != gateClaimID || gateClaimID != "T-"+gateID {
		t.Fatalf("delivery gate claim identity changed: gate=%s claim=%s want gate=%s claim=%s", snapshot.DeliveryGateID, snapshot.DeliveryGateClaimID, gateID, gateClaimID)
	}
}

func codeDeliveryV1PoolTask(t *testing.T, snapshot deliveryV1ShimSnapshot, claimID string) map[string]any {
	t.Helper()
	for _, row := range snapshot.PoolTasks {
		if row["claim_id"] == claimID {
			return row
		}
	}
	t.Fatalf("generic pool snapshot omits stable claim ID %s: %+v", claimID, snapshot.PoolTasks)
	return nil
}

func codeDeliveryV1RequireProviderDenial(t *testing.T, shim *deliveryV1Shim, previousCalls int64, response map[string]any, callErr error) {
	t.Helper()
	if callErr == nil || len(response) != 0 {
		t.Fatalf("published shim call did not fail closed on provider denial: response=%v err=%v", response, callErr)
	}
	if shim.ProviderCalls() != previousCalls+1 {
		t.Fatalf("denial did not reach the real provider: calls=%d before=%d", shim.ProviderCalls(), previousCalls)
	}
	if shim.LastProviderError() != "admission_denied" {
		t.Fatalf("provider returned error=%q, want admission_denied (shim error %v response %v)", shim.LastProviderError(), callErr, response)
	}
}

func codeDeliveryV1AssertUnchanged(t *testing.T, f *deliveryV1Fixture, before plantasks.State, beforeBinding plantasks.DelegationBinding) {
	t.Helper()
	after := f.load(t)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("denied generic operation changed lifecycle state\nbefore=%+v\nafter=%+v", before, after)
	}
	if f.delegated {
		afterBinding := f.binding(t)
		if beforeBinding.Sequence != afterBinding.Sequence || beforeBinding.LifecycleRevision != afterBinding.LifecycleRevision || beforeBinding.Cancelled != afterBinding.Cancelled {
			t.Fatalf("denied generic operation changed binding sequence/revision/cancellation: before=%+v after=%+v", beforeBinding, afterBinding)
		}
	}
}

func codeDeliveryV1BindingCount(t *testing.T, f *deliveryV1Fixture) int {
	t.Helper()
	var count int
	if err := f.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM plan_delegations WHERE caller_id=$1 AND delegation_id=$2`, f.caller, f.request.DelegationID).Scan(&count); err != nil {
		t.Fatalf("count durable delegations: %v", err)
	}
	return count
}

func codeDeliveryV1HTTPError(body []byte) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return ""
	}
	return envelope.Error.Code
}
