package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
	"google.golang.org/protobuf/types/known/durationpb"
)

type SourceConfig struct {
	ProjectID         string `mapstructure:"projectId" validate:"required"`
	Subscription      string `mapstructure:"subscription" validate:"required"`
	CreateIfNotExists bool   `mapstructure:"createIfNotExists"`
	Topic             string `mapstructure:"topic" validate:"required"`
	AckDeadline       int    `mapstructure:"ackDeadline" default:"10" validate:"required"`
	RetainAcked       bool   `mapstructure:"retainAcked"`
	RetentionDuration int    `mapstructure:"retentionDuration" default:"86400" validate:"required,gt=0"`
}

type PubSubSource struct {
	cfg    *SourceConfig
	slog   *slog.Logger
	c      chan *message.RunnerMessage
	client *pubsub.Client
	sub    *pubsub.Subscriber
}

// NewSource creates a PubSub source from options map.
func NewSource(opts map[string]any) (connectors.Source, error) {
	cfg, err := common.ParseConfig[SourceConfig](opts)
	if err != nil {
		return nil, err
	}
	return &PubSubSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "PubSub Source"),
	}, nil
}

func (s *PubSubSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, s.cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("error creating PubSub client: %w", err)
	}
	s.client = client

	// Automatically create subscription if requested
	if s.cfg.CreateIfNotExists {
		if s.cfg.Topic == "" {
			return nil, fmt.Errorf("topic is required to automatically create the subscription")
		}
		// Optional parameters
		ackDeadline := int32(10)
		if s.cfg.AckDeadline > 0 {
			ackDeadline = int32(s.cfg.AckDeadline)
		}
		retention := int64(24 * 3600)
		if s.cfg.RetentionDuration > 0 {
			retention = int64(s.cfg.RetentionDuration)
		}
		// Create subscription via admin client
		_, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:                     fmt.Sprintf("projects/%s/subscriptions/%s", s.cfg.ProjectID, s.cfg.Subscription),
			Topic:                    fmt.Sprintf("projects/%s/topics/%s", s.cfg.ProjectID, s.cfg.Topic),
			AckDeadlineSeconds:       ackDeadline,
			RetainAckedMessages:      s.cfg.RetainAcked,
			MessageRetentionDuration: durationpb.New(time.Duration(retention) * time.Second),
		})
		if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
			return nil, fmt.Errorf("error creating subscription: %w", err)
		}
	}
	s.sub = client.Subscriber(s.cfg.Subscription)

	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting PubSub source", "projectID", s.cfg.ProjectID, "subscription", s.cfg.Subscription)

	go func() {
		err := s.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			s.c <- message.NewRunnerMessage(&PubSubMessage{msg: m})
		})
		if err != nil {
			s.slog.Error("error receiving from PubSub", "err", err)
		}
	}()

	return s.c, nil
}

func (s *PubSubSource) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
