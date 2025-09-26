package main

import (
	"fmt"
	"log/slog"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	Address                string        `mapstructure:"address" validate:"required"`
	Subject                string        `mapstructure:"subject" validate:"required"`
	SubjectFromMetadataKey string        `mapstructure:"subjectFromMetadataKey"`
	Timeout                time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
}

// NewTarget creates the NATS target from options map.
func NewTarget(opts map[string]any) (connectors.Target, error) {
	cfg, err := common.ParseConfig[TargetConfig](opts)
	if err != nil {
		return nil, err
	}

	l := slog.Default().With("context", "NATS Target")

	conn, err := nats.Connect(cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}
	l.Info("NATS target connected", "address", cfg.Address, "subject", cfg.Subject)

	return &NATSTarget{
		cfg:  cfg,
		slog: l,
		conn: conn,
	}, nil
}

type NATSTarget struct {
	cfg  *TargetConfig
	slog *slog.Logger
	conn *nats.Conn
}

func (t *NATSTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	subject := common.ResolveFromMetadata(msg, t.cfg.SubjectFromMetadataKey, t.cfg.Subject)

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
