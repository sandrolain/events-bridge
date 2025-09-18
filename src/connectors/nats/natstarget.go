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

	l := slog.Default().With("context", "NATS")

	var err error
	conn, err := nats.Connect(cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}
	l.Info("NATS target connected", "address", cfg.Address, "subject", cfg.Subject)

	return &NATSTarget{
		config:  cfg,
		timeout: timeout,
		slog:    l,
		conn:    conn,
	}, nil
}

type NATSTarget struct {
	slog    *slog.Logger
	config  *targets.TargetNATSConfig
	timeout time.Duration
	conn    *nats.Conn
}

func (t *NATSTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	subject := t.config.Subject
	if t.config.SubjectFromMetadataKey != "" {
		metadata, _ := msg.GetTargetMetadata()
		if v, ok := metadata[t.config.SubjectFromMetadataKey]; ok && len(v) > 0 {
			subject = v
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
	if t.conn != nil && t.conn.IsConnected() {
		t.conn.Close()
	}
	return nil
}
