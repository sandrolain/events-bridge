package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"google.golang.org/api/option"
)

type TargetConfig struct {
	ProjectID           string        `mapstructure:"projectId" validate:"required"`
	Topic               string        `mapstructure:"topic" validate:"required"`
	CredentialsFile     string        `mapstructure:"credentialsFile"`                                           // Path to service account JSON file
	UseWorkloadIdentity bool          `mapstructure:"useWorkloadIdentity"`                                       // Use Workload Identity instead of credentials file
	PublishTimeout      time.Duration `mapstructure:"publishTimeout" default:"10s" validate:"gt=0"`              // Timeout for publish operations
	MaxMessageSize      int64         `mapstructure:"maxMessageSize" default:"10485760" validate:"max=10485760"` // Max message size (10MB default, GCP limit)
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

// NewTarget creates a PubSub target from options map.
func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate project ID format
	if err := validateProjectID(cfg.ProjectID); err != nil {
		return nil, fmt.Errorf("project ID validation failed: %w", err)
	}

	// Validate topic name format
	if err := validateTopicName(cfg.Topic); err != nil {
		return nil, fmt.Errorf("topic name validation failed: %w", err)
	}

	// Set defaults
	if cfg.PublishTimeout == 0 {
		cfg.PublishTimeout = 10 * time.Second
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 10485760 // 10MB default
	}
	if cfg.MaxMessageSize > 10485760 {
		return nil, fmt.Errorf("maxMessageSize exceeds GCP limit: %d (max 10485760)", cfg.MaxMessageSize)
	}

	ctx := context.Background()

	// Create client with appropriate credentials
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		slog.Info("PubSub target using credentials file", "file", cfg.CredentialsFile)
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	} else if cfg.UseWorkloadIdentity {
		slog.Info("PubSub target using Workload Identity")
		// Workload Identity uses Application Default Credentials
	} else {
		slog.Info("PubSub target using Application Default Credentials")
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("error creating PubSub client: %w", err)
	}
	publisher := client.Publisher(cfg.Topic)

	l := slog.Default().With("context", "PubSub Target")
	l.Info("PubSub target connected",
		"projectID", cfg.ProjectID,
		"topic", cfg.Topic,
		"maxMessageSize", cfg.MaxMessageSize)

	return &PubSubTarget{
		cfg:       cfg,
		slog:      l,
		client:    client,
		publisher: publisher,
	}, nil
}

type PubSubTarget struct {
	cfg       *TargetConfig
	slog      *slog.Logger
	client    *pubsub.Client
	publisher *pubsub.Publisher
	stopCh    chan struct{}
}

func (t *PubSubTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	// Validate message size
	if int64(len(data)) > t.cfg.MaxMessageSize {
		return fmt.Errorf("message size %d exceeds maximum %d", len(data), t.cfg.MaxMessageSize)
	}

	meta, err := msg.GetMetadata()
	if err != nil {
		return fmt.Errorf("error getting metadata: %w", err)
	}

	// Get metadata and convert to PubSub attributes
	attributes := common.CopyMap(meta, nil)

	ctx, cancel := context.WithTimeout(context.Background(), t.cfg.PublishTimeout)
	defer cancel()

	result := t.publisher.Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	})
	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("error publishing to PubSub: %w", err)
	}
	t.slog.Debug("PubSub message published", "topic", t.cfg.Topic, "size", len(data))
	return nil
}

func (t *PubSubTarget) Close() error {
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.publisher != nil {
		t.publisher.Stop()
	}
	if t.client != nil {
		return t.client.Close()
	}
	return nil
}
