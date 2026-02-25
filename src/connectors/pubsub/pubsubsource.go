package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/durationpb"
)

type SourceConfig struct {
	ProjectID           string `mapstructure:"projectId" validate:"required"`
	Subscription        string `mapstructure:"subscription" validate:"required"`
	CreateIfNotExists   bool   `mapstructure:"createIfNotExists"`
	Topic               string `mapstructure:"topic" validate:"required"`
	AckDeadline         int    `mapstructure:"ackDeadline" default:"10" validate:"required"`
	RetainAcked         bool   `mapstructure:"retainAcked"`
	RetentionDuration   int    `mapstructure:"retentionDuration" default:"86400" validate:"required,gt=0"`
	CredentialsFile     string `mapstructure:"credentialsFile"`                                 // Path to service account JSON file
	UseWorkloadIdentity bool   `mapstructure:"useWorkloadIdentity"`                             // Use Workload Identity instead of credentials file
	MaxMessages         int32  `mapstructure:"maxMessages" default:"1000" validate:"max=10000"` // Max messages to receive at once
}

type PubSubSource struct {
	cfg    *SourceConfig
	slog   *slog.Logger
	c      chan *message.RunnerMessage
	client *pubsub.Client
	sub    *pubsub.Subscriber
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// NewSource creates a PubSub source from options map.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate project ID format
	if err := validateProjectID(cfg.ProjectID); err != nil {
		return nil, fmt.Errorf("project ID validation failed: %w", err)
	}

	// Validate subscription name format
	if err := validateSubscriptionName(cfg.Subscription); err != nil {
		return nil, fmt.Errorf("subscription name validation failed: %w", err)
	}

	// Validate topic name if provided
	if cfg.Topic != "" {
		if err := validateTopicName(cfg.Topic); err != nil {
			return nil, fmt.Errorf("topic name validation failed: %w", err)
		}
	}

	// Validate MaxMessages
	if cfg.MaxMessages == 0 {
		cfg.MaxMessages = 1000 // default
	}
	if cfg.MaxMessages > 10000 {
		return nil, fmt.Errorf("maxMessages too large: %d (max 10000)", cfg.MaxMessages)
	}

	return &PubSubSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "PubSub Source"),
	}, nil
}

func (s *PubSubSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	ctx := context.Background()

	// Create client with appropriate credentials
	client, err := s.createClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating PubSub client: %w", err)
	}
	s.client = client

	// Create subscription if needed
	if s.cfg.CreateIfNotExists {
		if err := s.createSubscriptionIfNeeded(ctx); err != nil {
			return nil, err
		}
	}

	s.sub = client.Subscriber(s.cfg.Subscription)
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting PubSub source",
		"projectID", s.cfg.ProjectID,
		"subscription", s.cfg.Subscription,
		"maxMessages", s.cfg.MaxMessages)

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

// createClient creates a PubSub client with appropriate authentication
func (s *PubSubSource) createClient(ctx context.Context) (*pubsub.Client, error) {
	var opts []option.ClientOption

	// Use explicit credentials file if provided
	if s.cfg.CredentialsFile != "" {
		s.slog.Info("using credentials file", "file", s.cfg.CredentialsFile)
		jsonData, readErr := os.ReadFile(s.cfg.CredentialsFile)
		if readErr != nil {
			return nil, fmt.Errorf("error reading credentials file: %w", readErr)
		}
		opts = append(opts, option.WithCredentialsJSON(jsonData)) //nolint:staticcheck // WithCredentialsJSON is the non-file alternative; accepted deprecation
	} else if s.cfg.UseWorkloadIdentity {
		s.slog.Info("using Workload Identity")
		// Workload Identity uses Application Default Credentials, no extra options needed
	} else {
		s.slog.Info("using Application Default Credentials")
		// Use Application Default Credentials (ADC)
	}

	client, err := pubsub.NewClient(ctx, s.cfg.ProjectID, opts...)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// createSubscriptionIfNeeded creates subscription if it doesn't exist
func (s *PubSubSource) createSubscriptionIfNeeded(ctx context.Context) error {
	if s.cfg.Topic == "" {
		return fmt.Errorf("topic is required to automatically create the subscription")
	}

	// Optional parameters
	ackDeadline := int32(10)
	if s.cfg.AckDeadline > 0 {
		if s.cfg.AckDeadline > 2147483647 { // max int32 value
			return fmt.Errorf("ackDeadline too large: %d", s.cfg.AckDeadline)
		}
		ackDeadline = int32(s.cfg.AckDeadline) // #nosec G115 - checked above
	}

	retention := int64(24 * 3600)
	if s.cfg.RetentionDuration > 0 {
		retention = int64(s.cfg.RetentionDuration)
	}

	// Create subscription via admin client
	_, err := s.client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                     fmt.Sprintf("projects/%s/subscriptions/%s", s.cfg.ProjectID, s.cfg.Subscription),
		Topic:                    fmt.Sprintf("projects/%s/topics/%s", s.cfg.ProjectID, s.cfg.Topic),
		AckDeadlineSeconds:       ackDeadline,
		RetainAckedMessages:      s.cfg.RetainAcked,
		MessageRetentionDuration: durationpb.New(time.Duration(retention) * time.Second),
	})

	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		return fmt.Errorf("error creating subscription: %w", err)
	}

	return nil
}

func (s *PubSubSource) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
