package spikes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ajent-social/APRL/tests/testutil"
	"github.com/redis/go-redis/v9"
)

func TestRuntimeProbe(t *testing.T) {
	fixture := testutil.RequireRedis(t)
	token := probeToken(t)
	stream := fixture.KeyPrefix + "runtime_probe_" + token
	group := "aprl_probe:" + token + ":group"
	consumerName := "aprl_probe:" + token + ":consumer_a"
	reclaimerName := "aprl_probe:" + token + ":consumer_b"

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := fixture.Client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		t.Fatalf("create owned Redis test group: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		if err := fixture.Client.XGroupDestroy(cleanupCtx, stream, group).Err(); err != nil && !errors.Is(err, redis.Nil) {
			t.Errorf("destroy owned Redis test group: %v", err)
		}
		if err := fixture.Client.Del(cleanupCtx, stream).Err(); err != nil {
			t.Errorf("delete owned Redis test stream: %v", err)
		}
	})

	jobUUID, err := probeUUID()
	if err != nil {
		t.Fatal(err)
	}
	entryID, err := fixture.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"logical_job_uuid": jobUUID},
	}).Result()
	if err != nil {
		t.Fatalf("append owned Redis test entry: %v", err)
	}
	consumer := redis.NewClient(fixture.Client.Options())
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
			t.Errorf("close Redis test consumer: %v", err)
		}
	})
	firstDelivery := redis.XMessage{}
	consumerClientClosed := false
	var reclaimed redis.XMessage
	var reclaimer *redis.Client

	t.Run("consumer_redelivery", func(t *testing.T) {
		streams, err := consumer.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: consumerName, Streams: []string{stream, ">"}, Count: 1, Block: -1,
		}).Result()
		if err != nil {
			t.Fatalf("read new consumer-group delivery: %v", err)
		}
		firstDelivery, err = findMessage(streams, stream)
		if err != nil {
			t.Fatal(err)
		}
		if firstDelivery.ID != entryID || firstDelivery.Values["logical_job_uuid"] != jobUUID {
			t.Fatalf("initial delivery = (%s, %v), want stream ID %s and logical UUID %s", firstDelivery.ID, firstDelivery.Values, entryID, jobUUID)
		}
		pendingStreams, err := consumer.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: consumerName, Streams: []string{stream, "0"}, Count: 1, Block: -1,
		}).Result()
		if err != nil {
			t.Fatalf("redeliver this consumer's pending entry: %v", err)
		}
		redelivery, err := findMessage(pendingStreams, stream)
		if err != nil {
			t.Fatal(err)
		}
		if redelivery.ID != firstDelivery.ID || redelivery.Values["logical_job_uuid"] != jobUUID {
			t.Fatalf("pending redelivery changed stream ID or logical UUID: %+v", redelivery)
		}
	})

	t.Run("pending_reclaim", func(t *testing.T) {
		if err := consumer.Close(); err != nil {
			t.Fatalf("close abandoned consumer: %v", err)
		}
		closedCtx, closedCancel := context.WithTimeout(context.Background(), time.Second)
		consumerClientClosed = errors.Is(consumer.Ping(closedCtx).Err(), redis.ErrClosed)
		closedCancel()
		if !consumerClientClosed {
			t.Fatal("abandoned consumer client remained usable after Close")
		}

		reclaimer = redis.NewClient(fixture.Client.Options())
		t.Cleanup(func() {
			if err := reclaimer.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
				t.Errorf("close Redis reclaimer: %v", err)
			}
		})
		claimed, _, err := reclaimer.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: stream, Group: group, Consumer: reclaimerName, MinIdle: 0, Start: "0-0", Count: 1,
		}).Result()
		if err != nil {
			t.Fatalf("reclaim abandoned pending entry: %v", err)
		}
		if len(claimed) != 1 {
			t.Fatalf("XAUTOCLAIM returned %d entries, want one", len(claimed))
		}
		reclaimed = claimed[0]
		if reclaimed.ID != firstDelivery.ID || reclaimed.ID != entryID {
			t.Fatalf("reclaimed stream entry ID = %s, want original ID %s", reclaimed.ID, entryID)
		}
		if reclaimed.Values["logical_job_uuid"] != jobUUID {
			t.Fatalf("reclaimed logical job UUID = %v, want original UUID %s", reclaimed.Values["logical_job_uuid"], jobUUID)
		}
		pending, err := fixture.Client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream, Group: group, Start: "-", End: "+", Count: 1, Consumer: reclaimerName,
		}).Result()
		if err != nil {
			t.Fatalf("read reclaimed pending ownership: %v", err)
		}
		if len(pending) != 1 || pending[0].ID != entryID || pending[0].Consumer != reclaimerName {
			t.Fatalf("pending ownership = %+v, want entry %s owned by %s", pending, entryID, reclaimerName)
		}
		if err := fixture.Client.XAck(ctx, stream, group, entryID).Err(); err != nil {
			t.Fatalf("acknowledge reclaimed owned test entry: %v", err)
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		client := redis.NewClient(fixture.Client.Options())
		if err := client.Ping(ctx).Err(); err != nil {
			t.Fatalf("connect shutdown probe client: %v", err)
		}
		if err := client.Close(); err != nil {
			t.Fatalf("close shutdown probe client: %v", err)
		}
		closedCtx, closedCancel := context.WithTimeout(context.Background(), time.Second)
		defer closedCancel()
		if err := client.Ping(closedCtx).Err(); !errors.Is(err, redis.ErrClosed) {
			t.Fatalf("ping after Close error = %v, want redis.ErrClosed", err)
		}
	})

	t.Run("unknown_billing", func(t *testing.T) {
		if !consumerClientClosed || reclaimed.ID == "" || reclaimed.Values["logical_job_uuid"] != jobUUID {
			t.Fatal("Redis delivery and shutdown evidence must pass before capability admission is reported")
		}
		root, err := repositoryRoot()
		if err != nil {
			t.Fatal(err)
		}
		cmdCtx, cmdCancel := context.WithTimeout(ctx, 12*time.Second)
		defer cmdCancel()
		command := exec.CommandContext(cmdCtx, "go", "run", "./cmd/aprl-probe", "--local-only")
		command.Dir = root
		command.Env = withEnv(os.Environ(), "APRL_BILLING_ENVELOPE", "unsupported-test-envelope-v1")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("local-only probe failed: %v\n%s", err, output)
		}
		var result struct {
			Codex struct {
				Version     string `json:"version"`
				JSONL       bool   `json:"jsonl_supported"`
				Schema      bool   `json:"output_schema_supported"`
				Ephemeral   bool   `json:"ephemeral_supported"`
				ProviderRun string `json:"provider_execution"`
			} `json:"codex"`
			Billing struct {
				EnvelopeStatus string `json:"envelope_status"`
				PaidEnabled    bool   `json:"paid_execution_enabled"`
			} `json:"billing"`
			Redis struct {
				PendingReclaimed bool   `json:"pending_reclaimed"`
				LogicalID        string `json:"logical_job_uuid"`
				ReclaimedID      string `json:"reclaimed_logical_job_uuid"`
			} `json:"redis_streams"`
		}
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("decode local-only probe report: %v\n%s", err, output)
		}
		if result.Billing.EnvelopeStatus != "unsupported" || result.Billing.PaidEnabled {
			t.Fatalf("unsupported billing envelope admitted paid execution: %+v", result.Billing)
		}
		if !result.Codex.JSONL || !result.Codex.Schema || !result.Codex.Ephemeral {
			t.Fatalf("installed Codex CLI help did not prove required options: %+v", result.Codex)
		}
		if !strings.Contains(result.Codex.Version, "codex-cli") || result.Codex.ProviderRun == "" {
			t.Fatalf("Codex version or no-provider-call evidence is missing: %+v", result.Codex)
		}
		if !result.Redis.PendingReclaimed || result.Redis.LogicalID == "" || result.Redis.LogicalID != result.Redis.ReclaimedID {
			t.Fatalf("local CLI probe did not preserve the reclaimed logical job UUID: %+v", result.Redis)
		}
	})
}

func findMessage(streams []redis.XStream, wanted string) (redis.XMessage, error) {
	for _, stream := range streams {
		if stream.Stream == wanted && len(stream.Messages) > 0 {
			return stream.Messages[0], nil
		}
	}
	return redis.XMessage{}, errors.New("consumer group did not return the owned stream entry")
}

func probeToken(t *testing.T) string {
	t.Helper()
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatalf("generate owned Redis test token: %v", err)
	}
	return hex.EncodeToString(token[:])
}

func probeUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func repositoryRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("locate runtime probe test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..")), nil
}

func withEnv(env []string, key, value string) []string {
	filtered := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, key+"=") {
			filtered = append(filtered, entry)
		}
	}
	return append(filtered, key+"="+value)
}
