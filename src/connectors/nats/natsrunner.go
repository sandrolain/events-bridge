package main

import (
	"fmt"
	"log/slog"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

const (
	// Mode constants for NATS runner
	modePublish   = "publish"
	modeJetStream = "jetstream"
	modeKVSet     = "kv-set"
)

// RunnerConfig defines the configuration for a NATS runner connector.
type RunnerConfig struct {
	// Address is the NATS server address.
	// Example: "nats://localhost:4222" or "tls://localhost:4222"
	Address string `mapstructure:"address" validate:"required"`

	// Subject is the default NATS subject to publish to.
	// Can be overridden by SubjectFromMetadataKey.
	Subject string `mapstructure:"subject" validate:"required"`

	// SubjectFromMetadataKey is the metadata key to read the subject from.
	// If the key exists in message metadata, its value will be used as the subject.
	SubjectFromMetadataKey string `mapstructure:"subjectFromMetadataKey"`

	// Mode specifies the runner mode: "publish" (default), "jetstream", or "kv-set".
	// - publish: Standard NATS pub/sub publish
	// - jetstream: Publish to JetStream with ack
	// - kv-set: Set value in NATS KV bucket
	Mode string `mapstructure:"mode" default:"publish" validate:"oneof=publish jetstream kv-set"`

	// Stream is the JetStream stream name (required for jetstream mode).
	Stream string `mapstructure:"stream" validate:"required_if=Mode jetstream"`

	// KVBucket is the name of the KV bucket (required for kv-set mode).
	KVBucket string `mapstructure:"kvBucket" validate:"required_if=Mode kv-set"`

	// KVKey is the default key for KV operations (can be overridden by KVKeyFromMetadataKey).
	KVKey string `mapstructure:"kvKey" validate:"required_if=Mode kv-set"`

	// KVKeyFromMetadataKey is the metadata key to read the KV key from.
	KVKeyFromMetadataKey string `mapstructure:"kvKeyFromMetadataKey"`

	// Timeout is the maximum duration for publish operations.
	// Default: 5 seconds
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`

	// Username for NATS authentication.
	// Leave empty if authentication is not required.
	Username string `mapstructure:"username"`

	// Password for NATS authentication.
	// Leave empty if authentication is not required.
	// WARNING: Consider using environment variables or secret managers for production.
	Password string `mapstructure:"password"`

	// Token for NATS token-based authentication.
	// Alternative to username/password authentication.
	Token string `mapstructure:"token"`

	// NKeyFile is the path to the NKey seed file for NKey authentication.
	// Most secure authentication method for NATS.
	NKeyFile string `mapstructure:"nkeyFile"`

	// CredentialsFile is the path to the NATS credentials file (.creds).
	// Used for JWT-based authentication with NATS account system.
	CredentialsFile string `mapstructure:"credentialsFile"`

	// TLS holds TLS/SSL configuration for secure connections.
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// MaxReconnects is the maximum number of reconnection attempts.
	// Default: 60 (approx. 10 minutes with default interval)
	// Set to -1 for unlimited reconnects.
	MaxReconnects int `mapstructure:"maxReconnects" default:"60" validate:"min=-1"`

	// ReconnectWait is the time to wait between reconnection attempts.
	// Default: 10 seconds
	ReconnectWait time.Duration `mapstructure:"reconnectWait" default:"10s"`

	// Name is a client name for identification in NATS server logs.
	Name string `mapstructure:"name"`
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// NewRunner creates the NATS runner from options map.
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	l := slog.Default().With("context", "NATS Runner")

	// Build NATS connection options
	opts, err := buildRunnerConnectionOptions(cfg, l)
	if err != nil {
		return nil, fmt.Errorf("failed to build connection options: %w", err)
	}

	conn, err := nats.Connect(cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	runner := &NATSRunner{
		cfg:  cfg,
		slog: l,
		conn: conn,
	}

	// Initialize JetStream if needed
	if cfg.Mode == modeJetStream {
		js, err := conn.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get JetStream context: %w", err)
		}
		runner.js = js
	}

	// Initialize KV if needed
	if cfg.Mode == modeKVSet {
		js, err := conn.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get JetStream context: %w", err)
		}
		kv, err := js.KeyValue(cfg.KVBucket)
		if err != nil {
			return nil, fmt.Errorf("failed to get KV bucket: %w", err)
		}
		runner.kv = kv
	}

	l.Info("NATS runner connected",
		"address", cfg.Address,
		"mode", cfg.Mode,
		"subject", cfg.Subject,
		"stream", cfg.Stream,
		"kvBucket", cfg.KVBucket,
		"tls", cfg.TLS != nil && cfg.TLS.Enabled,
		"auth", hasRunnerAuthentication(cfg))

	return runner, nil
}

// buildRunnerConnectionOptions creates NATS connection options with authentication and TLS.
func buildRunnerConnectionOptions(cfg *RunnerConfig, logger *slog.Logger) ([]nats.Option, error) {
	opts := []nats.Option{}

	// Set client name if provided
	if cfg.Name != "" {
		opts = append(opts, nats.Name(cfg.Name))
	}

	// Configure reconnection behavior
	opts = append(opts,
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait),
	)

	// Configure authentication
	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	} else if cfg.NKeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.NKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load NKey file: %w", err)
		}
		opts = append(opts, opt)
	} else if cfg.Token != "" {
		resolvedToken, err := secrets.Resolve(cfg.Token)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve token: %w", err)
		}
		opts = append(opts, nats.Token(resolvedToken))
	} else if cfg.Username != "" {
		resolvedPassword, err := secrets.Resolve(cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve password: %w", err)
		}
		opts = append(opts, nats.UserInfo(cfg.Username, resolvedPassword))
	}

	// Configure TLS
	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsConfig, err := tlsconfig.BuildClientConfigIfEnabled(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		if tlsConfig != nil {
			opts = append(opts, nats.Secure(tlsConfig))
		}
	}

	// Add connection event handlers
	opts = append(opts,
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				logger.Warn("NATS disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Info("NATS connection closed")
		}),
	)

	return opts, nil
}

// hasRunnerAuthentication checks if any authentication method is configured.
func hasRunnerAuthentication(cfg *RunnerConfig) string {
	if cfg.CredentialsFile != "" {
		return "credentials"
	}
	if cfg.NKeyFile != "" {
		return "nkey"
	}
	if cfg.Token != "" {
		return "token"
	}
	if cfg.Username != "" {
		return "userpass"
	}
	return "none"
}

type NATSRunner struct {
	cfg  *RunnerConfig
	slog *slog.Logger
	conn *nats.Conn
	js   nats.JetStreamContext
	kv   nats.KeyValue
}

func (r *NATSRunner) Process(msg *message.RunnerMessage) error {
	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	switch r.cfg.Mode {
	case modeKVSet:
		return r.processKVSet(msg, metadata, data)
	case modeJetStream:
		return r.processJetStream(msg, metadata, data)
	default: // "publish"
		return r.processPublish(msg, metadata, data)
	}
}

// processPublish handles standard NATS pub/sub publishing.
func (r *NATSRunner) processPublish(msg *message.RunnerMessage, metadata map[string]string, data []byte) error {
	subject := r.cfg.Subject
	subject = message.ResolveFromMetadata(msg, r.cfg.SubjectFromMetadataKey, subject)

	r.slog.Debug("publishing NATS message",
		"subject", subject,
		"bodysize", len(data),
		"metadata", metadata,
	)

	if err := r.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("error publishing to NATS: %w", err)
	}

	if err := r.conn.FlushTimeout(r.cfg.Timeout); err != nil {
		return fmt.Errorf("error flushing NATS publish: %w", err)
	}

	r.slog.Debug("NATS message published", "subject", subject)
	return nil
}

// processJetStream handles JetStream publishing with acknowledgment.
func (r *NATSRunner) processJetStream(msg *message.RunnerMessage, metadata map[string]string, data []byte) error {
	subject := r.cfg.Subject
	subject = message.ResolveFromMetadata(msg, r.cfg.SubjectFromMetadataKey, subject)

	r.slog.Debug("publishing to JetStream",
		"stream", r.cfg.Stream,
		"subject", subject,
		"bodysize", len(data),
	)

	pubAck, err := r.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("error publishing to JetStream: %w", err)
	}

	r.slog.Debug("JetStream message published",
		"subject", subject,
		"stream", pubAck.Stream,
		"sequence", pubAck.Sequence,
	)
	return nil
}

// processKVSet handles NATS KV bucket operations.
func (r *NATSRunner) processKVSet(msg *message.RunnerMessage, metadata map[string]string, data []byte) error {
	key := r.cfg.KVKey
	key = message.ResolveFromMetadata(msg, r.cfg.KVKeyFromMetadataKey, key)

	r.slog.Debug("setting KV value",
		"bucket", r.cfg.KVBucket,
		"key", key,
		"size", len(data),
	)

	revision, err := r.kv.Put(key, data)
	if err != nil {
		return fmt.Errorf("error setting KV value: %w", err)
	}

	r.slog.Debug("KV value set", "key", key, "revision", revision)
	return nil
}

func (r *NATSRunner) Close() error {
	if r.conn != nil && !r.conn.IsClosed() {
		r.conn.Close()
	}
	return nil
}
