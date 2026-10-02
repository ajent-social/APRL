// Package queue adapts Redis Streams to APRL's at-least-once transport contract.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DispatchKind identifies executable logical jobs in the queue.
	DispatchKind = "DISPATCH"
	jobField     = "job_id"
	kindField    = "kind"
	commandLimit = 3 * time.Second
	maxReadBlock = 30 * time.Second
	maxReadCount = 1000
)

var (
	// ErrInvalid reports malformed Redis stream API input.
	ErrInvalid = errors.New("invalid queue operation")
	// ErrClosed reports use of an unavailable stream client.
	ErrClosed = errors.New("queue client is closed")
)

// Delivery separates Redis's transport entry ID from the stable Postgres job UUID.
type Delivery struct {
	EntryID string
	Kind    string
	JobID   string
}

// Streams manages one Redis stream and consumer group. The caller owns client lifecycle.
type Streams struct {
	client *redis.Client
	stream string
	group  string
}

// NewStreams binds a Redis client to an explicit stream and consumer group.
func NewStreams(client *redis.Client, stream, group string) (*Streams, error) {
	if client == nil || !client.Options().ContextTimeoutEnabled || strings.TrimSpace(stream) == "" || strings.TrimSpace(group) == "" {
		return nil, ErrInvalid
	}
	return &Streams{client: client, stream: stream, group: group}, nil
}

// EnsureGroup creates the consumer group at the beginning of the stream if absent.
func (s *Streams) EnsureGroup(ctx context.Context) error {
	if err := s.validate(ctx); err != nil {
		return err
	}
	commandCtx, cancel := context.WithTimeout(ctx, commandLimit)
	defer cancel()
	err := s.client.XGroupCreateMkStream(commandCtx, s.stream, s.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis consumer group: %w", err)
	}
	return nil
}

// Publish appends a logical hint. Repeated calls intentionally create separate
// transport entries with the same durable job UUID.
func (s *Streams) Publish(ctx context.Context, kind, jobID string) (string, error) {
	if err := s.validate(ctx); err != nil {
		return "", err
	}
	if !validKind(kind) || !validUUID(jobID) {
		return "", ErrInvalid
	}
	commandCtx, cancel := context.WithTimeout(ctx, commandLimit)
	defer cancel()
	id, err := s.client.XAdd(commandCtx, &redis.XAddArgs{Stream: s.stream, Values: map[string]any{kindField: kind, jobField: jobID}}).Result()
	if err != nil {
		return "", fmt.Errorf("append Redis queue entry: %w", err)
	}
	return id, nil
}

// Read blocks for new entries in the consumer group. Malformed hints are
// returned as deliveries with empty fields so callers can leave them pending.
func (s *Streams) Read(ctx context.Context, consumer string, block time.Duration, count int64) ([]Delivery, error) {
	if err := s.validate(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(consumer) == "" || block < 0 || block > maxReadBlock || count < 1 || count > maxReadCount {
		return nil, ErrInvalid
	}
	commandCtx, cancel := context.WithTimeout(ctx, block+commandLimit)
	defer cancel()
	result, err := s.client.XReadGroup(commandCtx, &redis.XReadGroupArgs{Group: s.group, Consumer: consumer, Streams: []string{s.stream, ">"}, Count: count, Block: block}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Redis consumer group: %w", err)
	}
	return deliveries(result), nil
}

// ReadPending redelivers entries already pending for this same consumer.
func (s *Streams) ReadPending(ctx context.Context, consumer, start string, count int64) ([]Delivery, error) {
	if err := s.validate(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(consumer) == "" || start == "" || count < 1 || count > maxReadCount {
		return nil, ErrInvalid
	}
	commandCtx, cancel := context.WithTimeout(ctx, commandLimit)
	defer cancel()
	result, err := s.client.XReadGroup(commandCtx, &redis.XReadGroupArgs{Group: s.group, Consumer: consumer, Streams: []string{s.stream, start}, Count: count, Block: -1}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redeliver pending Redis entries: %w", err)
	}
	return deliveries(result), nil
}

// Reclaim moves idle pending entries to this consumer; it does not transfer any
// PostgreSQL lease or execution authority.
func (s *Streams) Reclaim(ctx context.Context, consumer string, minIdle time.Duration, start string, count int64) ([]Delivery, string, error) {
	if err := s.validate(ctx); err != nil {
		return nil, "", err
	}
	if strings.TrimSpace(consumer) == "" || minIdle < 0 || start == "" || count < 1 || count > maxReadCount {
		return nil, "", ErrInvalid
	}
	commandCtx, cancel := context.WithTimeout(ctx, commandLimit)
	defer cancel()
	messages, next, err := s.client.XAutoClaim(commandCtx, &redis.XAutoClaimArgs{Stream: s.stream, Group: s.group, Consumer: consumer, MinIdle: minIdle, Start: start, Count: count}).Result()
	if err != nil {
		return nil, "", fmt.Errorf("reclaim Redis pending entries: %w", err)
	}
	return messageDeliveries(messages), next, nil
}

// Ack acknowledges a transport entry only after the caller has checked durable disposition.
func (s *Streams) Ack(ctx context.Context, entryID string) error {
	if err := s.validate(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(entryID) == "" {
		return ErrInvalid
	}
	commandCtx, cancel := context.WithTimeout(ctx, commandLimit)
	defer cancel()
	if err := s.client.XAck(commandCtx, s.stream, s.group, entryID).Err(); err != nil {
		return fmt.Errorf("acknowledge Redis queue entry: %w", err)
	}
	return nil
}

// Close closes the owned Redis connection pool.
func (s *Streams) Close() error {
	if s == nil || s.client == nil {
		return ErrClosed
	}
	if err := s.client.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
		return fmt.Errorf("close Redis queue client: %w", err)
	}
	return nil
}

func (s *Streams) validate(ctx context.Context) error {
	if s == nil || s.client == nil {
		return ErrClosed
	}
	if ctx == nil || ctx.Err() != nil {
		return ErrInvalid
	}
	return nil
}

func deliveries(streams []redis.XStream) []Delivery {
	var result []Delivery
	for _, stream := range streams {
		result = append(result, messageDeliveries(stream.Messages)...)
	}
	return result
}

func messageDeliveries(messages []redis.XMessage) []Delivery {
	result := make([]Delivery, 0, len(messages))
	for _, message := range messages {
		delivery := Delivery{EntryID: message.ID}
		if kind, ok := message.Values[kindField].(string); ok {
			delivery.Kind = kind
		}
		if jobID, ok := message.Values[jobField].(string); ok {
			delivery.JobID = jobID
		}
		result = append(result, delivery)
	}
	return result
}

func validKind(kind string) bool {
	return kind == DispatchKind || kind == "CANCEL" || kind == "LABEL_SYNC" || kind == "NOTIFY"
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return false
	}
	return value != "00000000-0000-0000-0000-000000000000"
}
