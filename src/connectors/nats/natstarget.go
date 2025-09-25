package main

import (
	"fmt"
	"log/slog"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/utils"
)

type TargetConfig struct {
	Address                string        `yaml:"address" json:"address"`
	Subject                string        `yaml:"subject" json:"subject"`
	SubjectFromMetadataKey string        `yaml:"subjectFromMetadataKey" json:"subjectFromMetadataKey"`
	Timeout                time.Duration `yaml:"timeout" json:"timeout"`
}

// parseTargetOptions builds a NATS target config from options map.
// Expected keys: address, subject, subjectFromMetadataKey, timeout.
func parseTargetOptions(opts map[string]any) (*TargetConfig, error) {
	cfg := &TargetConfig{}
	op := &utils.OptsParser{}
	cfg.Address = op.OptString(opts, "address", "", utils.StringNonEmpty())
	cfg.Subject = op.OptString(opts, "subject", "", utils.StringNonEmpty())
	cfg.SubjectFromMetadataKey = op.OptString(opts, "subjectFromMetadataKey", "")
	cfg.Timeout = op.OptDuration(opts, "timeout", targets.DefaultTimeout)
	if err := op.Error(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// NewTarget creates the NATS target from options map.
func NewTarget(opts map[string]any) (targets.Target, error) {
	cfg, err := parseTargetOptions(opts)
	if err != nil {
		return nil, err
	}

	l := slog.Default().With("context", "NATS")

	conn, err := nats.Connect(cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}
	l.Info("NATS target connected", "address", cfg.Address, "subject", cfg.Subject)

	return &NATSTarget{
		config: cfg,
		slog:   l,
		conn:   conn,
	}, nil
}

type NATSTarget struct {
	slog   *slog.Logger
	config *TargetConfig
	conn   *nats.Conn
}

func (t *NATSTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	subject := utils.ResolveFromMetadata(msg, t.config.SubjectFromMetadataKey, t.config.Subject)

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
