package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	ProjectID string `mapstructure:"projectId" validate:"required"`
	Topic     string `mapstructure:"topic" validate:"required"`
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

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("error creating PubSub client: %w", err)
	}
	publisher := client.Publisher(cfg.Topic)

	l := slog.Default().With("context", "PubSub Target")
	l.Info("PubSub target connected", "projectID", cfg.ProjectID, "topic", cfg.Topic)

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
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	// Get metadata and convert to PubSub attributes
	attributes := make(map[string]string)
	if meta, err := msg.GetTargetMetadata(); err == nil {
		for k, v := range meta {
			if len(v) > 0 {
				attributes[k] = v
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := t.publisher.Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	})
	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("error publishing to PubSub: %w", err)
	}
	t.slog.Debug("PubSub message published", "topic", t.cfg.Topic)
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
