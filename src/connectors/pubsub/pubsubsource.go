package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"google.golang.org/protobuf/types/known/durationpb"
)

type PubSubSource struct {
	config  *sources.SourcePubSubConfig
	slog    *slog.Logger
	c       chan message.Message
	client  *pubsub.Client
	sub     *pubsub.Subscriber
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
		// Optional parameters
		ackDeadline := int32(10)
		if s.config.AckDeadline > 0 {
			ackDeadline = int32(s.config.AckDeadline)
		}
		retention := int64(24 * 3600)
		if s.config.RetentionDuration > 0 {
			retention = int64(s.config.RetentionDuration)
		}
		// Create subscription via admin client
		_, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:                     fmt.Sprintf("projects/%s/subscriptions/%s", s.config.ProjectID, s.config.Subscription),
			Topic:                    fmt.Sprintf("projects/%s/topics/%s", s.config.ProjectID, s.config.Topic),
			AckDeadlineSeconds:       ackDeadline,
			RetainAckedMessages:      s.config.RetainAcked,
			MessageRetentionDuration: durationpb.New(time.Duration(retention) * time.Second),
		})
		if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
			return nil, fmt.Errorf("error creating subscription: %w", err)
		}
	}
	s.sub = client.Subscriber(s.config.Subscription)

	s.c = make(chan message.Message, buffer)

	s.slog.Info("starting PubSub source", "projectID", s.config.ProjectID, "subscription", s.config.Subscription)

	go func() {
		err := s.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			s.c <- &PubSubMessage{msg: m}
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
