package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/utils"
)

type TargetConfig struct {
	ProjectID string `yaml:"project_id" json:"project_id"`
	Topic     string `yaml:"topic" json:"topic"`
}

// parseTargetOptions builds a PubSub target config from options map.
// Expected keys: project_id, topic.
func parseTargetOptions(opts map[string]any) (*TargetConfig, error) {
	cfg := &TargetConfig{}
	op := &utils.OptsParser{}
	cfg.ProjectID = op.OptString(opts, "project_id", "", utils.StringNonEmpty())
	cfg.Topic = op.OptString(opts, "topic", "", utils.StringNonEmpty())
	if op.Error() != nil {
		return nil, op.Error()
	}
	return cfg, nil
}

// NewTarget creates a PubSub target from options map.
func NewTarget(opts map[string]any) (targets.Target, error) {
	cfg, err := parseTargetOptions(opts)
	if err != nil {
		return nil, err
	}

	l := slog.Default().With("context", "PubSub")

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("error creating PubSub client: %w", err)
	}
	publisher := client.Publisher(cfg.Topic)

	l.Info("PubSub target connected", "projectID", cfg.ProjectID, "topic", cfg.Topic)

	return &PubSubTarget{
		config:    cfg,
		slog:      l,
		client:    client,
		publisher: publisher,
	}, nil
}

type PubSubTarget struct {
	slog      *slog.Logger
	config    *TargetConfig
	stopped   bool
	stopCh    chan struct{}
	client    *pubsub.Client
	publisher *pubsub.Publisher
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
	t.slog.Debug("PubSub message published", "topic", t.config.Topic)
	return nil
}

func (t *PubSubTarget) Close() error {
	t.stopped = true
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
