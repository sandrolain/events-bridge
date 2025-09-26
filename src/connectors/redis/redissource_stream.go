package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

func NewStreamSource(cfg *SourceConfig) (connectors.Source, error) {
	useConsumerGrp := cfg.ConsumerGroup != "" && cfg.ConsumerName != ""
	return &RedisStreamSource{
		config:         cfg,
		slog:           slog.Default().With("context", "RedisStream Source"),
		useConsumerGrp: useConsumerGrp,
	}, nil
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
