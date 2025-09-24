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

type SourceConfig struct {
	ProjectID         string `yaml:"project_id" json:"project_id"`
	Subscription      string `yaml:"subscription" json:"subscription"`
	CreateIfNotExists bool   `yaml:"create_if_not_exists" json:"create_if_not_exists"`
	Topic             string `yaml:"topic" json:"topic"`
	AckDeadline       int    `yaml:"ack_deadline" json:"ack_deadline"`
	RetainAcked       bool   `yaml:"retain_acked" json:"retain_acked"`
	RetentionDuration int    `yaml:"retention_duration" json:"retention_duration"`
}

// NewSourceOptions builds a PubSub source config from options map.
// Expected keys: project_id, subscription, create_if_not_exists, topic, ack_deadline, retain_acked, retention_duration.
func NewSourceOptions(opts map[string]any) (sources.Source, error) {
	cfg := &SourceConfig{}
	if v, ok := opts["project_id"].(string); ok {
		cfg.ProjectID = v
	}
	if v, ok := opts["subscription"].(string); ok {
		cfg.Subscription = v
	}
	if v, ok := opts["create_if_not_exists"].(bool); ok {
		cfg.CreateIfNotExists = v
	}
	if v, ok := opts["topic"].(string); ok {
		cfg.Topic = v
	}
	if v, ok := opts["ack_deadline"].(int); ok {
		cfg.AckDeadline = v
	}
	if v, ok := opts["retention_duration"].(int); ok {
		cfg.RetentionDuration = v
	}
	if v, ok := opts["retain_acked"].(bool); ok {
		cfg.RetainAcked = v
	}
	return NewSource(cfg)
}

type PubSubSource struct {
	config  *SourceConfig
	slog    *slog.Logger
	c       chan *message.RunnerMessage
	client  *pubsub.Client
	sub     *pubsub.Subscriber
	started bool
}

func NewSource(cfg *SourceConfig) (sources.Source, error) {
	if cfg.ProjectID == "" || cfg.Subscription == "" {
		return nil, fmt.Errorf("projectID and subscription are required for PubSub source")
	}
	return &PubSubSource{
		config: cfg,
		slog:   slog.Default().With("context", "PubSub"),
	}, nil
}

func (s *PubSubSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
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

	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting PubSub source", "projectID", s.config.ProjectID, "subscription", s.config.Subscription)

	go func() {
		err := s.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			s.c <- message.NewRunnerMessage(&PubSubMessage{msg: m})
		})
		if err != nil {
			s.slog.Error("error receiving from PubSub", "err", err)
		}
	}()

	s.started = true
	return s.c, nil
}

func (s *PubSubSource) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
