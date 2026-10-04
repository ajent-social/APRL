package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/hostauth"
	"github.com/ajent-social/APRL/internal/httpapi"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/resultclient"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

// Exercise the real signed HTTP boundary and durable result service together.
// The signing key is ephemeral fixture material, never a deployment credential.
func TestHostResultTransport(t *testing.T) {
	db := testutil.RequireDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 4, 10, 0, 0, 0, time.UTC)
	c := clock.NewManual(now)
	seed := time.Now().UnixNano()
	org := fmt.Sprintf("host-result-%d", seed)
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, org); err != nil {
		t.Fatal(err)
	}
	var taskID string
	if err := db.Pool.QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version) VALUES($1,'owner/host-result',$2,'host','AUTHORING','host-result-v1') RETURNING id::text`, org, org).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	jobID := fmt.Sprintf("%08x-2222-4222-8222-%012x", seed>>32, seed&0xffffffffffff)
	operationID := fmt.Sprintf("%08x-3333-4333-8333-%012x", seed>>32, seed&0xffffffffffff)
	correlationID := fmt.Sprintf("%08x-4444-4444-8444-%012x", seed>>32, seed&0xffffffffffff)
	job := contracts.Job{Version: 1, TaskID: taskID, JobID: jobID, Generation: 0, Attempt: 1, OperationID: operationID, CorrelationID: correlationID, Operation: "ci_reconcile"}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO jobs(id,task_id,logical_key,operation_type,generation,payload) VALUES($1::uuid,$2::uuid,$3,'ci_reconcile',0,$4::jsonb)`, jobID, taskID, org, payload); err != nil {
		t.Fatal(err)
	}
	lease, err := leases.Claim(ctx, db.Pool, c, leases.ClaimRequest{TaskID: taskID, JobID: jobID, TTL: time.Minute, AgentType: "A", PromptHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SupervisorIdentity: "host-result", CredentialID: "host-key-reference"})
	if err != nil {
		t.Fatal(err)
	}
	principal := results.Principal{TaskID: taskID, RunID: lease.RunID, Identity: "host-result", CredentialID: "host-key-reference"}
	result := contracts.Result{Version: 1, TaskID: taskID, JobID: jobID, RunID: lease.RunID, Generation: lease.Generation, LeaseToken: lease.Token, Snapshot: lease.Snapshot, Attempt: lease.Attempt, OperationID: operationID, CorrelationID: correlationID, Status: "succeeded", Summary: "host transport completed"}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	auth, err := hostauth.New(hostauth.Config{Issuer: "aprl-test-host", Audience: hostauth.Audience, ActiveKeyID: "test-key", Keys: map[string][]byte{"test-key": key}, TTL: time.Minute}, c)
	if err != nil {
		t.Fatal(err)
	}
	service, err := results.New(db.Pool, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewResultsHandler(service, auth)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	trustedClient := &http.Client{Transport: transport, Timeout: 8 * time.Second}
	client, err := resultclient.New(server.URL+"/internal/results", trustedClient, auth)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)

	post := func(t *testing.T, token string, value contracts.Result) int {
		t.Helper()
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/internal/results", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := trustedClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
		return response.StatusCode
	}
	t.Run("missing_authentication", func(t *testing.T) {
		if status := post(t, "", result); status != http.StatusUnauthorized {
			t.Fatalf("status=%d want 401", status)
		}
	})
	t.Run("signed_wrong_host_is_not_admission", func(t *testing.T) {
		wrong := principal
		wrong.Identity = "another-host"
		token, err := auth.Token(ctx, wrong)
		if err != nil {
			t.Fatal(err)
		}
		if status := post(t, token, result); status != http.StatusForbidden {
			t.Fatalf("status=%d want 403", status)
		}
	})
	t.Run("signed_wrong_run_cannot_use_body_identity", func(t *testing.T) {
		wrong := principal
		wrong.RunID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		token, err := auth.Token(ctx, wrong)
		if err != nil {
			t.Fatal(err)
		}
		if status := post(t, token, result); status != http.StatusForbidden {
			t.Fatalf("status=%d want 403", status)
		}
	})
	t.Run("stale_fence_rejected", func(t *testing.T) {
		stale := result
		stale.LeaseToken = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
		if _, err := client.Submit(ctx, principal, stale); !errors.Is(err, resultclient.ErrStale) {
			t.Fatalf("error=%v want stale", err)
		}
	})
	t.Run("valid_result_and_exact_replay", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			accepted, err := client.Submit(ctx, principal, result)
			if err != nil {
				t.Fatal(err)
			}
			if accepted.OperationID != operationID {
				t.Fatalf("operation ID=%s", accepted.OperationID)
			}
		}
		var count int
		if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM worker_result_receipts WHERE operation_id=$1::uuid`, operationID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("durable receipts=%d want 1", count)
		}
	})
	t.Run("conflicting_replay_stays_rejected", func(t *testing.T) {
		conflict := result
		conflict.Summary = "different semantic outcome"
		if _, err := client.Submit(ctx, principal, conflict); !errors.Is(err, resultclient.ErrStale) {
			t.Fatalf("error=%v want stale", err)
		}
	})
	t.Run("expired_token_cannot_replay_receipt", func(t *testing.T) {
		token, err := auth.Token(ctx, principal)
		if err != nil {
			t.Fatal(err)
		}
		c.Advance(time.Minute)
		if status := post(t, token, result); status != http.StatusUnauthorized {
			t.Fatalf("status=%d want 401", status)
		}
	})
}
