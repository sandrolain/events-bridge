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

// NewTargetOptions builds a NATS target config from options map.
// Expected keys: address, subject, subjectFromMetadataKey, timeout (ns).
func NewTargetOptions(opts map[string]any) (targets.Target, error) {
	cfg := &TargetConfig{}
	if v, ok := opts["address"].(string); ok {
		cfg.Address = v
	}
	if v, ok := opts["subject"].(string); ok {
		cfg.Subject = v
	}
	if v, ok := opts["subjectFromMetadataKey"].(string); ok {
		cfg.SubjectFromMetadataKey = v
	}
	if v, ok := opts["timeout"].(int); ok {
		cfg.Timeout = time.Duration(v)
	}
	if v, ok := opts["timeout"].(int64); ok {
		cfg.Timeout = time.Duration(v)
	}
	if v, ok := opts["timeout"].(float64); ok {
		cfg.Timeout = time.Duration(int64(v))
	}
	return NewTarget(cfg)
}

func NewTarget(cfg *TargetConfig) (targets.Target, error) {
	if cfg.Address == "" || cfg.Subject == "" {
		return nil, fmt.Errorf("address and subject are required for NATS target")
	}
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
	config  *TargetConfig
	timeout time.Duration
	conn    *nats.Conn
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
