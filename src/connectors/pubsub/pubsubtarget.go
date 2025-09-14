package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
)

func NewTarget(cfg *targets.TargetPubSubConfig) (targets.Target, error) {
	if cfg.ProjectID == "" || cfg.Topic == "" {
		return nil, fmt.Errorf("projectID and topic are required for PubSub target")
	}
	return &PubSubTarget{
		config: cfg,
		slog:   slog.Default().With("context", "PubSub"),
		stopCh: make(chan struct{}),
	}, nil
}

type PubSubTarget struct {
	slog      *slog.Logger
	config    *targets.TargetPubSubConfig
	stopped   bool
	stopCh    chan struct{}
	client    *pubsub.Client
	publisher *pubsub.Publisher
}

func (t *PubSubTarget) Consume(c <-chan message.Message) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, t.config.ProjectID)
	if err != nil {
		return fmt.Errorf("error creating PubSub client: %w", err)
	}
	t.client = client
	t.publisher = client.Publisher(t.config.Topic)

	t.slog.Info("PubSub target connected", "projectID", t.config.ProjectID, "topic", t.config.Topic)

	go t.startConsumer(ctx, c)
	return nil
}

func (t *PubSubTarget) startConsumer(ctx context.Context, c <-chan message.Message) {
	for {
		select {
		case <-t.stopCh:
			return
		case msg, ok := <-c:
			if !ok {
				return
			}
			t.handleMessage(msg)
		}
	}
}

func (t *PubSubTarget) handleMessage(msg message.Message) {
	if err := t.publish(msg); err != nil {
		t.slog.Error("error publishing message", "err", err)
		if nackErr := msg.Nak(); nackErr != nil {
			t.slog.Error("error naking message", "err", nackErr)
		}
		return
	}
	if ackErr := msg.Ack(); ackErr != nil {
		t.slog.Error("error acking message", "err", ackErr)
	}
}

func (t *PubSubTarget) publish(msg message.Message) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	// Get metadata and convert to PubSub attributes
	attributes := make(map[string]string)
	if meta, err := msg.GetMetadata(); err == nil {
		for k, v := range meta {
			if len(v) > 0 {
				attributes[k] = strings.Join(v, ";")
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
