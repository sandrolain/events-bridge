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

// TargetConfig defines the configuration for a NATS target connector.
type TargetConfig struct {
	// Address is the NATS server address.
	// Example: "nats://localhost:4222" or "tls://localhost:4222"
	Address string `mapstructure:"address" validate:"required"`

	// Subject is the default NATS subject to publish to.
	// Can be overridden by SubjectFromMetadataKey.
	Subject string `mapstructure:"subject" validate:"required"`

	// SubjectFromMetadataKey is the metadata key to read the subject from.
	// If the key exists in message metadata, its value will be used as the subject.
	SubjectFromMetadataKey string `mapstructure:"subjectFromMetadataKey"`

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

func NewTargetConfig() any {
	return new(TargetConfig)
}

// NewTarget creates the NATS target from options map.
func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	l := slog.Default().With("context", "NATS Target")

	// Build NATS connection options
	opts, err := buildTargetConnectionOptions(cfg, l)
	if err != nil {
		return nil, fmt.Errorf("failed to build connection options: %w", err)
	}

	conn, err := nats.Connect(cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	l.Info("NATS target connected",
		"address", cfg.Address,
		"subject", cfg.Subject,
		"tls", cfg.TLS != nil && cfg.TLS.Enabled,
		"auth", hasTargetAuthentication(cfg))

	return &NATSTarget{
		cfg:  cfg,
		slog: l,
		conn: conn,
	}, nil
}

// buildTargetConnectionOptions creates NATS connection options with authentication and TLS.
func buildTargetConnectionOptions(cfg *TargetConfig, logger *slog.Logger) ([]nats.Option, error) {
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

	// Configure authentication (in order of precedence)
	if cfg.CredentialsFile != "" {
		// JWT-based authentication (highest precedence)
		// Resolve credentials file path
		resolvedCredsFile, err := secrets.Resolve(cfg.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve credentials file path: %w", err)
		}
		opts = append(opts, nats.UserCredentials(resolvedCredsFile))
		logger.Debug("using credentials file authentication")
	} else if cfg.NKeyFile != "" {
		// NKey authentication
		// Resolve NKey file path
		resolvedNKeyFile, err := secrets.Resolve(cfg.NKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve NKey file path: %w", err)
		}
		opt, err := nats.NkeyOptionFromSeed(resolvedNKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load NKey: %w", err)
		}
		opts = append(opts, opt)
		logger.Debug("using NKey authentication")
	} else if cfg.Token != "" {
		// Token authentication
		// Resolve token secret
		resolvedToken, err := secrets.Resolve(cfg.Token)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve token: %w", err)
		}
		opts = append(opts, nats.Token(resolvedToken))
		logger.Debug("using token authentication")
	} else if cfg.Username != "" {
		// Username/password authentication
		// Resolve password secret
		resolvedPassword, err := secrets.Resolve(cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve password: %w", err)
		}
		opts = append(opts, nats.UserInfo(cfg.Username, resolvedPassword))
		logger.Debug("using username/password authentication")
	}

	// Configure TLS
	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsConfig, err := cfg.TLS.BuildClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		opts = append(opts, nats.Secure(tlsConfig))
		logger.Debug("TLS enabled", "minVersion", cfg.TLS.MinVersion)
	}

	return opts, nil
}

// hasTargetAuthentication checks if any authentication method is configured.
func hasTargetAuthentication(cfg *TargetConfig) bool {
	return cfg.Username != "" ||
		cfg.Token != "" ||
		cfg.NKeyFile != "" ||
		cfg.CredentialsFile != ""
}

type NATSTarget struct {
	cfg  *TargetConfig
	slog *slog.Logger
	conn *nats.Conn
}

func (t *NATSTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	subject := message.ResolveFromMetadata(msg, t.cfg.SubjectFromMetadataKey, t.cfg.Subject)

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
