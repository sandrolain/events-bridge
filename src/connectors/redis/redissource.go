package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/utils"
)

type SourceConfig struct {
	Address       string `yaml:"address" json:"address"`
	Channel       string `yaml:"channel" json:"channel"`
	Stream        string `yaml:"stream" json:"stream"`
	ConsumerGroup string `yaml:"consumer_group,omitempty" json:"consumer_group,omitempty"`
	ConsumerName  string `yaml:"consumer_name,omitempty" json:"consumer_name,omitempty"`
	StreamDataKey string `yaml:"stream_data_key" json:"stream_data_key"`
}

// parseSourceOptions builds a Redis source config from options map.
// For PubSub: address, channel. For Stream: address, stream, consumer_group, consumer_name, stream_data_key.
func parseSourceOptions(opts map[string]any) (*SourceConfig, error) {
	cfg := &SourceConfig{}
	op := &utils.OptsParser{}
	cfg.Address = op.OptString(opts, "address", "", utils.StringNonEmpty())
	cfg.Channel = op.OptString(opts, "channel", "")
	cfg.Stream = op.OptString(opts, "stream", "")
	cfg.ConsumerGroup = op.OptString(opts, "consumer_group", "")
	cfg.ConsumerName = op.OptString(opts, "consumer_name", "")
	cfg.StreamDataKey = op.OptString(opts, "stream_data_key", "")
	if err := op.Error(); err != nil {
		return nil, fmt.Errorf("invalid Redis source options: %w", err)
	}
	return cfg, nil
}

type RedisSource struct {
	config  *SourceConfig
	slog    *slog.Logger
	c       chan *message.RunnerMessage
	client  *redis.Client
	pubsub  *redis.PubSub
	started bool
}

type RedisStreamSource struct {
	config         *SourceConfig
	slog           *slog.Logger
	c              chan *message.RunnerMessage
	client         *redis.Client
	started        bool
	lastID         string
	useConsumerGrp bool
}

// NewSource creates a Redis source from options map.
func NewSource(opts map[string]any) (sources.Source, error) {
	cfg, err := parseSourceOptions(opts)
	if err != nil {
		return nil, err
	}
	if cfg.Stream != "" {
		useConsumerGrp := cfg.ConsumerGroup != "" && cfg.ConsumerName != ""
		return &RedisStreamSource{
			config:         cfg,
			slog:           slog.Default().With("context", "RedisStream"),
			useConsumerGrp: useConsumerGrp,
		}, nil
	}
	if cfg.Channel != "" {
		return &RedisSource{
			config: cfg,
			slog:   slog.Default().With("context", "Redis"),
		}, nil
	}
	return nil, fmt.Errorf("invalid config for Redis source")
}

func (s *RedisSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting Redis source", "address", s.config.Address, "channel", s.config.Channel)

	s.client = redis.NewClient(&redis.Options{
		Addr: s.config.Address,
	})

	s.pubsub = s.client.Subscribe(context.Background(), s.config.Channel)
	go s.consume()

	s.started = true
	return s.c, nil
}

func (s *RedisSource) consume() {
	ch := s.pubsub.Channel()
	for msg := range ch {
		m := &RedisMessage{msg: msg}
		s.c <- message.NewRunnerMessage(m)
	}
}

func (s *RedisSource) Close() error {
	if s.pubsub != nil {
		if err := s.pubsub.Close(); err != nil {
			return fmt.Errorf("error closing Redis pubsub: %w", err)
		}
	}
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}

func (s *RedisStreamSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting Redis stream source", "address", s.config.Address, "stream", s.config.Stream)

	s.client = redis.NewClient(&redis.Options{
		Addr: s.config.Address,
	})

	if s.useConsumerGrp {
		_ = s.client.XGroupCreateMkStream(context.Background(), s.config.Stream, s.config.ConsumerGroup, "0").Err()
		s.lastID = ">"
	} else {
		s.lastID = "$"
	}
	go s.consume()

	s.started = true
	return s.c, nil
}

func (s *RedisStreamSource) consume() {
	stream := s.config.Stream
	dataKey := s.config.StreamDataKey
	if dataKey == "" {
		dataKey = "data"
	}
	for {
		var res []redis.XStream
		var err error
		if s.useConsumerGrp {
			res, err = s.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
				Group:    s.config.ConsumerGroup,
				Consumer: s.config.ConsumerName,
				Streams:  []string{stream, s.lastID},
				Count:    1,
				Block:    0,
				NoAck:    false,
			}).Result()
		} else {
			res, err = s.client.XRead(context.Background(), &redis.XReadArgs{
				Streams: []string{stream, s.lastID},
				Count:   1,
				Block:   0,
			}).Result()
		}
		if err != nil {
			s.slog.Error("error reading from Redis stream", "err", err)
			continue
		}
		for _, xstream := range res {
			for _, xmsg := range xstream.Messages {
				m := &RedisStreamMessage{msg: xmsg, dataKey: dataKey}
				s.c <- message.NewRunnerMessage(m)
				if s.useConsumerGrp {
					_ = s.client.XAck(context.Background(), s.config.Stream, s.config.ConsumerGroup, xmsg.ID).Err()
				} else {
					s.lastID = xmsg.ID
				}
			}
		}
	}
}

func (s *RedisStreamSource) Close() error {
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}
