// Command aprl-probe checks disposable Redis Streams recovery and local Codex CLI capabilities.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/config"
	"github.com/redis/go-redis/v9"
)

type report struct {
	Codex   codexCapabilities `json:"codex"`
	Redis   redisEvidence     `json:"redis_streams"`
	Billing billingStatus     `json:"billing"`
}

type codexCapabilities struct {
	Version                 string `json:"version"`
	JSONL                   bool   `json:"jsonl_supported"`
	OutputSchema            bool   `json:"output_schema_supported"`
	Ephemeral               bool   `json:"ephemeral_supported"`
	ConfigOverrides         bool   `json:"config_overrides_supported"`
	StrictConfig            bool   `json:"strict_config_supported"`
	ProviderExecution       string `json:"provider_execution"`
	ConfigurationInspection string `json:"configuration_values_inspected"`
}

type redisEvidence struct {
	StreamEntryID           string `json:"stream_entry_id"`
	ReclaimedEntryID        string `json:"reclaimed_entry_id"`
	LogicalJobUUID          string `json:"logical_job_uuid"`
	ReclaimedLogicalJobUUID string `json:"reclaimed_logical_job_uuid"`
	ReclaimedBy             string `json:"reclaimed_by"`
	PendingReclaimed        bool   `json:"pending_reclaimed"`
	ConsumerClosed          bool   `json:"consumer_client_closed"`
	ReclaimerClosed         bool   `json:"reclaimer_client_closed"`
}

type billingStatus struct {
	EnvelopeStatus       string `json:"envelope_status"`
	PaidExecutionEnabled bool   `json:"paid_execution_enabled"`
	Reason               string `json:"reason"`
}

func main() {
	localOnly := flag.Bool("local-only", false, "run bounded local Redis and Codex help probes; never execute a provider request")
	flag.Parse()
	if !*localOnly {
		fatalf("--local-only is required; this probe never runs provider requests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, err := run(ctx, os.Getenv("TEST_REDIS_URL"), os.Getenv("TEST_RESOURCE_OWNER"), os.Getenv("APRL_BILLING_ENVELOPE"))
	if err != nil {
		fatalf("local probe failed: %v", err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fatalf("write local probe report: %v", err)
	}
}

func run(ctx context.Context, redisURL, resourceOwner, envelope string) (report, error) {
	if strings.TrimSpace(resourceOwner) == "" {
		return report{}, errors.New("TEST_RESOURCE_OWNER must be nonempty before creating disposable Redis resources")
	}
	parsedURL, err := config.RedisURLFromEnv(func(name string) string {
		if name == config.TestRedisURLEnv {
			return redisURL
		}
		return ""
	})
	if err != nil {
		return report{}, err
	}
	options, err := redis.ParseURL(parsedURL)
	if err != nil {
		return report{}, fmt.Errorf("%s is not a valid Redis URL", config.TestRedisURLEnv)
	}
	options.DialTimeout = time.Second
	options.ReadTimeout = time.Second
	options.WriteTimeout = time.Second
	options.PoolTimeout = time.Second
	options.MaxRetries = -1

	redisResult, err := probeRedis(ctx, options)
	if err != nil {
		return report{}, err
	}
	codexResult, err := probeCodex(ctx)
	if err != nil {
		return report{}, err
	}
	return report{
		Codex:   codexResult,
		Redis:   redisResult,
		Billing: capabilityForEnvelope(envelope),
	}, nil
}

func probeRedis(ctx context.Context, options *redis.Options) (result redisEvidence, err error) {
	admin := redis.NewClient(options)
	if err = admin.Ping(ctx).Err(); err != nil {
		_ = admin.Close()
		return redisEvidence{}, fmt.Errorf("connect to owned %s fixture: %w", config.TestRedisURLEnv, err)
	}
	token, err := randomToken()
	if err != nil {
		_ = admin.Close()
		return redisEvidence{}, err
	}
	stream := "aprl_test:" + token + ":stream"
	group := "aprl_probe:" + token + ":group"
	consumerName := "aprl_probe:" + token + ":consumer_a"
	reclaimerName := "aprl_probe:" + token + ":consumer_b"
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if destroyErr := admin.XGroupDestroy(cleanupCtx, stream, group).Err(); destroyErr != nil && !errors.Is(destroyErr, redis.Nil) {
			err = errors.Join(err, fmt.Errorf("destroy owned probe consumer group: %w", destroyErr))
		}
		if deleteErr := admin.Del(cleanupCtx, stream).Err(); deleteErr != nil {
			err = errors.Join(err, fmt.Errorf("delete owned probe stream: %w", deleteErr))
		}
		if closeErr := admin.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close Redis cleanup client: %w", closeErr))
		}
	}()

	if err := admin.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		return redisEvidence{}, fmt.Errorf("create owned Redis consumer group: %w", err)
	}
	logicalID, err := randomUUID()
	if err != nil {
		return redisEvidence{}, err
	}
	entryID, err := admin.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"logical_job_uuid": logicalID},
	}).Result()
	if err != nil {
		return redisEvidence{}, fmt.Errorf("append owned Redis probe entry: %w", err)
	}
	consumer := redis.NewClient(options)
	messages, err := consumer.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: consumerName, Streams: []string{stream, ">"}, Count: 1, Block: -1,
	}).Result()
	if err != nil {
		_ = consumer.Close()
		return redisEvidence{}, fmt.Errorf("read owned Redis stream entry: %w", err)
	}
	firstMessage, err := firstMessage(messages, stream)
	if err != nil {
		_ = consumer.Close()
		return redisEvidence{}, err
	}
	if firstMessage.ID != entryID || firstMessage.Values["logical_job_uuid"] != logicalID {
		_ = consumer.Close()
		return redisEvidence{}, errors.New("initial Redis delivery did not preserve the logical job UUID")
	}
	if err := consumer.Close(); err != nil {
		return redisEvidence{}, fmt.Errorf("close abandoned Redis consumer: %w", err)
	}
	closedCtx, closedCancel := context.WithTimeout(context.Background(), time.Second)
	consumerClosed := errors.Is(consumer.Ping(closedCtx).Err(), redis.ErrClosed)
	closedCancel()
	if !consumerClosed {
		return redisEvidence{}, errors.New("redis consumer remained usable after Close")
	}

	reclaimer := redis.NewClient(options)
	claimed, _, err := reclaimer.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: stream, Group: group, Consumer: reclaimerName, MinIdle: 0, Start: "0-0", Count: 1,
	}).Result()
	if err != nil {
		_ = reclaimer.Close()
		return redisEvidence{}, fmt.Errorf("reclaim abandoned Redis pending entry: %w", err)
	}
	if len(claimed) != 1 {
		_ = reclaimer.Close()
		return redisEvidence{}, fmt.Errorf("XAUTOCLAIM returned %d entries, want 1", len(claimed))
	}
	claimedMessage := claimed[0]
	claimedID, ok := claimedMessage.Values["logical_job_uuid"].(string)
	if !ok || claimedMessage.ID != entryID || claimedID != logicalID {
		_ = reclaimer.Close()
		return redisEvidence{}, errors.New("reclaimed Redis entry changed its stream ID or logical job UUID")
	}
	if err := reclaimer.XAck(ctx, stream, group, entryID).Err(); err != nil {
		_ = reclaimer.Close()
		return redisEvidence{}, fmt.Errorf("acknowledge reclaimed probe entry: %w", err)
	}
	if err := reclaimer.Close(); err != nil {
		return redisEvidence{}, fmt.Errorf("close Redis reclaimer: %w", err)
	}
	closedCtx, closedCancel = context.WithTimeout(context.Background(), time.Second)
	reclaimerClosed := errors.Is(reclaimer.Ping(closedCtx).Err(), redis.ErrClosed)
	closedCancel()
	if !reclaimerClosed {
		return redisEvidence{}, errors.New("redis reclaimer remained usable after Close")
	}
	return redisEvidence{
		StreamEntryID: entryID, ReclaimedEntryID: claimedMessage.ID,
		LogicalJobUUID: logicalID, ReclaimedLogicalJobUUID: claimedID,
		ReclaimedBy: reclaimerName, PendingReclaimed: true,
		ConsumerClosed: consumerClosed, ReclaimerClosed: reclaimerClosed,
	}, nil
}

func firstMessage(streams []redis.XStream, wanted string) (redis.XMessage, error) {
	for _, stream := range streams {
		if stream.Stream == wanted && len(stream.Messages) > 0 {
			return stream.Messages[0], nil
		}
	}
	return redis.XMessage{}, errors.New("redis consumer group returned no probe entry")
}

func probeCodex(ctx context.Context) (codexCapabilities, error) {
	executable, err := exec.LookPath("codex")
	if err != nil {
		return codexCapabilities{}, errors.New("codex CLI is not available on PATH")
	}
	versionOutput, err := runHelp(ctx, executable, "--version")
	if err != nil {
		return codexCapabilities{}, fmt.Errorf("read codex version: %w", err)
	}
	rootHelp, err := runHelp(ctx, executable, "--help")
	if err != nil {
		return codexCapabilities{}, fmt.Errorf("read codex help: %w", err)
	}
	execHelp, err := runHelp(ctx, executable, "exec", "--help")
	if err != nil {
		return codexCapabilities{}, fmt.Errorf("read codex exec help: %w", err)
	}
	return codexCapabilities{
		Version:                 strings.TrimSpace(string(versionOutput)),
		JSONL:                   strings.Contains(string(execHelp), "--json"),
		OutputSchema:            strings.Contains(string(execHelp), "--output-schema"),
		Ephemeral:               strings.Contains(string(execHelp), "--ephemeral"),
		ConfigOverrides:         strings.Contains(string(rootHelp), "--config"),
		StrictConfig:            strings.Contains(string(rootHelp), "--strict-config"),
		ProviderExecution:       "not invoked; help and version only",
		ConfigurationInspection: "no values read; CLI config flags only",
	}, nil
}

func runHelp(parent context.Context, executable string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	output, err := command.Output()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%s timed out", strings.Join(args, " "))
		}
		return nil, err
	}
	return output, nil
}

func capabilityForEnvelope(envelope string) billingStatus {
	status := "unproven"
	if strings.TrimSpace(envelope) != "" {
		status = "unsupported"
	}
	return billingStatus{
		EnvelopeStatus:       status,
		PaidExecutionEnabled: false,
		Reason:               "no real hard-budget enforcement proof exists; process termination does not bound accepted provider charges",
	}
}

func randomToken() (string, error) {
	var token [12]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate disposable Redis resource token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func randomUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate logical job UUID: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func fatalf(format string, args ...any) {
	if _, err := fmt.Fprintf(os.Stderr, format+"\n", args...); err != nil {
		os.Exit(2)
	}
	os.Exit(1)
}
