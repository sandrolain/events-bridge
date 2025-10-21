package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

const defaultStreamDataKey = "data"

func NewStreamSource(cfg *SourceConfig) (connectors.Source, error) {
	// Validate configuration
	if err := validateStreamConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid stream configuration: %w", err)
	}

	useConsumerGrp := cfg.ConsumerGroup != "" && cfg.ConsumerName != ""
	return &RedisStreamSource{
		config:         cfg,
		slog:           slog.Default().With("context", "RedisStream Source"),
		useConsumerGrp: useConsumerGrp,
	}, nil
}

func validateStreamConfig(cfg *SourceConfig) error {
	// Validate that both ConsumerGroup and ConsumerName are set together or not at all
	if (cfg.ConsumerGroup != "") != (cfg.ConsumerName != "") {
		return fmt.Errorf("ConsumerGroup and ConsumerName must be both set or both empty")
	}

	// Validate LastID values
	validLastIDs := map[string]bool{
		"0": true, // Start from beginning
		"$": true, // Start from newest
		">": true, // For consumer groups only
		"+": true, // End of stream
		"-": true, // Start of stream
		"":  true, // Empty (will use default)
	}

	if !validLastIDs[cfg.LastID] {
		// Check if it's a valid stream ID format (timestamp-sequence like "1234567890123-0")
		if cfg.LastID != "" && strings.Contains(cfg.LastID, "-") {
			parts := strings.Split(cfg.LastID, "-")
			if len(parts) == 2 && len(parts[0]) >= 10 && len(parts[1]) >= 1 {
				// Simple validation: timestamp part should be at least 10 digits, sequence part at least 1
				// This is a simplified validation - Redis will validate the actual format
				return nil
			}
		}
		return fmt.Errorf("LastID must be one of: '0', '$', '>', '+', '-', or a specific stream ID (format: timestamp-sequence)")
	}

	return nil
}

type RedisStreamSource struct {
	config         *SourceConfig
	slog           *slog.Logger
	c              chan *message.RunnerMessage
	client         *redis.Client
	lastID         string
	useConsumerGrp bool
	ctx            context.Context
	cancel         context.CancelFunc
}

func (s *RedisStreamSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)
	s.ctx, s.cancel = context.WithCancel(context.Background())

	tlsEnabled := s.config.TLS != nil && s.config.TLS.Enabled
	hasAuth := s.config.Username != "" || s.config.Password != ""

	s.slog.Info("starting Redis stream source",
		"address", s.config.Address,
		"stream", s.config.Stream,
		"db", s.config.DB,
		"tls", tlsEnabled,
		"auth", hasAuth,
		"strictValidation", s.config.StrictValidation,
	)

	opts, err := buildRedisStreamOptions(s.config)
	if err != nil {
		return nil, fmt.Errorf("failed to build Redis options: %w", err)
	}

	s.client = redis.NewClient(opts)

	// Test connection
	if err := s.client.Ping(s.ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	if s.useConsumerGrp {
		if err := s.client.XGroupCreateMkStream(s.ctx, s.config.Stream, s.config.ConsumerGroup, "0").Err(); err != nil {
			s.slog.Debug("consumer group already exists or creation failed", "error", err)
		}
		s.lastID = ">"
	} else {
		// Use configured LastID or default to "$" for newest messages
		s.lastID = s.config.LastID
		if s.lastID == "" {
			s.lastID = "$" // Default to newest messages
		}
	}
	go s.consume()

	return s.c, nil
}

// buildRedisStreamOptions creates Redis client options for stream source
func buildRedisStreamOptions(cfg *SourceConfig) (*redis.Options, error) {
	opts := &redis.Options{
		Addr: cfg.Address,
		DB:   cfg.DB,
	}

	// Add authentication if provided
	if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	if cfg.Password != "" {
		// Resolve password secret
		resolvedPassword, err := secrets.Resolve(cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve password: %w", err)
		}
		opts.Password = resolvedPassword
	}

	// Add TLS if configured
	tlsConf, err := buildRedisTLSConfig(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}
	if tlsConf != nil {
		opts.TLSConfig = tlsConf
	}

	return opts, nil
}

func (s *RedisStreamSource) consume() {
	stream := s.config.Stream
	dataKey := s.config.StreamDataKey
	if dataKey == "" {
		dataKey = defaultStreamDataKey // Default data key
	}

	for {
		select {
		case <-s.ctx.Done():
			s.slog.Info("Redis stream consumer stopped due to context cancellation")
			close(s.c)
			return
		default:
		}

		if err := s.readAndProcessMessages(stream, dataKey); err != nil {
			// Check if context was cancelled during error
			if s.ctx.Err() != nil {
				s.slog.Info("Redis stream consumer stopped due to context cancellation")
				close(s.c)
				return
			}
			s.slog.Error("error reading from Redis stream", "err", err)
			continue
		}
	}
}

func (s *RedisStreamSource) readAndProcessMessages(stream, dataKey string) error {
	var res []redis.XStream
	var err error

	if s.useConsumerGrp {
		res, err = s.client.XReadGroup(s.ctx, &redis.XReadGroupArgs{
			Group:    s.config.ConsumerGroup,
			Consumer: s.config.ConsumerName,
			Streams:  []string{stream, s.lastID},
			Count:    1,
			Block:    100, // Short block time for better cancellation response
			NoAck:    false,
		}).Result()
	} else {
		res, err = s.client.XRead(s.ctx, &redis.XReadArgs{
			Streams: []string{stream, s.lastID},
			Count:   1,
			Block:   100, // Short block time for better cancellation response
		}).Result()
	}

	if err != nil {
		return err
	}

	for _, xstream := range res {
		for _, xmsg := range xstream.Messages {
			m := &RedisStreamMessage{msg: xmsg, dataKey: dataKey}

			select {
			case s.c <- message.NewRunnerMessage(m):
				if s.useConsumerGrp {
					if err := s.client.XAck(s.ctx, s.config.Stream, s.config.ConsumerGroup, xmsg.ID).Err(); err != nil {
						s.slog.Warn("failed to acknowledge message", "id", xmsg.ID, "error", err)
					}
				} else {
					s.lastID = xmsg.ID
				}
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		}
	}
	return nil
}

func (s *RedisStreamSource) Close() error {
	// Cancel the context to stop the consumer goroutine
	if s.cancel != nil {
		s.cancel()
	}

	// Close the Redis client
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}
