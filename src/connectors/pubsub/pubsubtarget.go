package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
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
	slog    *slog.Logger
	config  *targets.TargetPubSubConfig
	stopped bool
	stopCh  chan struct{}
	client  *pubsub.Client
	topic   *pubsub.Topic
}

func (t *PubSubTarget) Consume(c <-chan message.Message) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, t.config.ProjectID)
	if err != nil {
		return fmt.Errorf("error creating PubSub client: %w", err)
	}
	t.client = client
	t.topic = client.Topic(t.config.Topic)

	t.slog.Info("PubSub target connected", "projectID", t.config.ProjectID, "topic", t.config.Topic)

	go func() {
		for {
			select {
			case <-t.stopCh:
				return
			case msg, ok := <-c:
				if !ok {
					return
				}
				err := t.publish(msg)
				if err != nil {
					msg.Nak()
					t.slog.Error("error publishing PubSub message", "err", err)
				} else {
					msg.Ack()
				}
			}
		}
	}()
	return nil
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

	result := t.topic.Publish(ctx, &pubsub.Message{
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
	if t.client != nil {
		return t.client.Close()
	}
	return nil
}
