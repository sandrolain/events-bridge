package main

import (
	"fmt"
	"log/slog"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
)

func NewTarget(cfg *targets.TargetNATSConfig) (targets.Target, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = targets.DefaultTimeout
	}
	return &NATSTarget{
		config:  cfg,
		timeout: timeout,
		slog:    slog.Default().With("context", "NATS"),
		stopCh:  make(chan struct{}),
	}, nil
}

type NATSTarget struct {
	slog    *slog.Logger
	config  *targets.TargetNATSConfig
	timeout time.Duration
	stopped bool
	stopCh  chan struct{}
	conn    *nats.Conn
}

func (t *NATSTarget) Consume(c <-chan message.Message) error {
	var err error
	t.conn, err = nats.Connect(t.config.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS server: %w", err)
	}
	t.slog.Info("NATS target connected", "address", t.config.Address, "subject", t.config.Subject)

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
					t.slog.Error("error publishing NATS message", "err", err)
				} else {
					msg.Ack()
				}
			}
		}
	}()
	return nil
}

func (t *NATSTarget) publish(msg message.Message) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	subject := t.config.Subject
	if t.config.SubjectFromMetadataKey != "" {
		metadata, _ := msg.GetMetadata()
		if v, ok := metadata[t.config.SubjectFromMetadataKey]; ok && len(v) > 0 {
			subject = v[0]
		}
	}

	t.slog.Debug("publishing NATS message", "subject", subject, "bodysize", len(data))

	err = t.conn.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("error publishing to NATS: %w", err)
	}
	t.slog.Debug("NATS message published", "subject", subject)
	return nil
}

func (t *NATSTarget) Close() error {
	t.stopped = true
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.conn != nil && t.conn.IsConnected() {
		t.conn.Close()
	}
	return nil
}
