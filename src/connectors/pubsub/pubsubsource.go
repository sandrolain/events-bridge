package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
)

type PubSubSource struct {
	config  *sources.SourcePubSubConfig
	slog    *slog.Logger
	c       chan message.Message
	client  *pubsub.Client
	sub     *pubsub.Subscription
	started bool
}

func NewSource(cfg *sources.SourcePubSubConfig) (sources.Source, error) {
	if cfg.ProjectID == "" || cfg.Subscription == "" {
		return nil, fmt.Errorf("projectID and subscription are required for PubSub source")
	}
	return &PubSubSource{
		config: cfg,
		slog:   slog.Default().With("context", "PubSub"),
	}, nil
}

func (s *PubSubSource) Produce(buffer int) (<-chan message.Message, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, s.config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("error creating PubSub client: %w", err)
	}
	s.client = client

	// Automatically create subscription if requested
	if s.config.CreateIfNotExists {
		if s.config.Topic == "" {
			return nil, fmt.Errorf("topic is required to automatically create the subscription")
		}
		topic := client.Topic(s.config.Topic)
		// Optional parameters
		ackDeadline := 10
		if s.config.AckDeadline > 0 {
			ackDeadline = s.config.AckDeadline
		}
		retention := 24 * 3600
		if s.config.RetentionDuration > 0 {
			retention = s.config.RetentionDuration
		}
		_, err := client.CreateSubscription(ctx, s.config.Subscription, pubsub.SubscriptionConfig{
			Topic:               topic,
			AckDeadline:         time.Duration(ackDeadline) * time.Second,
			RetainAckedMessages: s.config.RetainAcked,
			RetentionDuration:   time.Duration(retention) * time.Second,
		})
		if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
			return nil, fmt.Errorf("error creating subscription: %w", err)
		}
	}
	s.sub = client.Subscription(s.config.Subscription)

	s.c = make(chan message.Message, buffer)

	s.slog.Info("starting PubSub source", "projectID", s.config.ProjectID, "subscription", s.config.Subscription)

	go func() {
		err := s.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			msg := &PubSubMessage{msg: m}
			s.c <- msg
		})
		if err != nil {
			s.slog.Error("error receiving from PubSub", "err", err)
		}
	}()

	s.started = true
	return s.c, nil
}

func (s *PubSubSource) Close() error {
	if s.c != nil {
		close(s.c)
	}
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
