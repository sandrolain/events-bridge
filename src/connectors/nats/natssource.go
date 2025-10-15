package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// SourceConfig defines the configuration for a NATS source connector.
type SourceConfig struct {
	// Address is the NATS server address.
	// Example: "nats://localhost:4222" or "tls://localhost:4222"
	Address string `mapstructure:"address" validate:"required"`

	// Subject is the NATS subject to subscribe to.
	// Supports wildcards: * (single token), > (multiple tokens).
	Subject string `mapstructure:"subject" validate:"required"`

	// Stream is the JetStream stream name (optional, for JetStream).
	Stream string `mapstructure:"stream"`

	// Consumer is the JetStream consumer name (optional, for JetStream).
	Consumer string `mapstructure:"consumer"`

	// QueueGroup enables load balancing across multiple consumers (optional).
	// Messages are distributed among queue group members.
	QueueGroup string `mapstructure:"queueGroup"`

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

type NATSSource struct {
	cfg  *SourceConfig
	slog *slog.Logger
	c    chan *message.RunnerMessage
	nc   *nats.Conn
	js   nats.JetStreamContext
	sub  *nats.Subscription
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}
	return &NATSSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "NATS Source"),
	}, nil
}

func (s *NATSSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting NATS source",
		"address", s.cfg.Address,
		"subject", s.cfg.Subject,
		"stream", s.cfg.Stream,
		"consumer", s.cfg.Consumer,
		"queueGroup", s.cfg.QueueGroup,
		"tls", s.cfg.TLS != nil && s.cfg.TLS.Enabled,
		"auth", s.hasAuthentication())

	// Build NATS connection options
	opts, err := s.buildConnectionOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to build connection options: %w", err)
	}

	nc, err := nats.Connect(s.cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	s.nc = nc

	// If both stream and consumer are specified, use JetStream
	if s.cfg.Stream != "" && s.cfg.Consumer != "" {
		js, err := nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get JetStream context: %w", err)
		}
		s.js = js
		s.slog.Info("using JetStream", "stream", s.cfg.Stream, "consumer", s.cfg.Consumer)

		if err := s.consumeJetStream(); err != nil {
			return nil, fmt.Errorf("failed to start JetStream consumer: %w", err)
		}
	} else {
		// NATS core (o JetStream senza consumer)
		queue := s.cfg.QueueGroup
		if err := s.consumeCore(queue); err != nil {
			return nil, fmt.Errorf("failed to start NATS core consumer: %w", err)
		}
	}

	return s.c, nil
}

// buildConnectionOptions creates NATS connection options with authentication and TLS.
func (s *NATSSource) buildConnectionOptions() ([]nats.Option, error) {
	opts := []nats.Option{}

	// Set client name if provided
	if s.cfg.Name != "" {
		opts = append(opts, nats.Name(s.cfg.Name))
	}

	// Configure reconnection behavior
	opts = append(opts,
		nats.MaxReconnects(s.cfg.MaxReconnects),
		nats.ReconnectWait(s.cfg.ReconnectWait),
	)

	// Configure authentication (in order of precedence)
	if s.cfg.CredentialsFile != "" {
		// JWT-based authentication (highest precedence)
		// Resolve credentials file path
		resolvedCredsFile, err := secrets.Resolve(s.cfg.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve credentials file path: %w", err)
		}
		opts = append(opts, nats.UserCredentials(resolvedCredsFile))
		s.slog.Debug("using credentials file authentication")
	} else if s.cfg.NKeyFile != "" {
		// NKey authentication
		// Resolve NKey file path
		resolvedNKeyFile, err := secrets.Resolve(s.cfg.NKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve NKey file path: %w", err)
		}
		opt, err := nats.NkeyOptionFromSeed(resolvedNKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load NKey: %w", err)
		}
		opts = append(opts, opt)
		s.slog.Debug("using NKey authentication")
	} else if s.cfg.Token != "" {
		// Token authentication
		// Resolve token secret
		resolvedToken, err := secrets.Resolve(s.cfg.Token)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve token: %w", err)
		}
		opts = append(opts, nats.Token(resolvedToken))
		s.slog.Debug("using token authentication")
	} else if s.cfg.Username != "" {
		// Username/password authentication
		// Resolve password secret
		resolvedPassword, err := secrets.Resolve(s.cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve password: %w", err)
		}
		opts = append(opts, nats.UserInfo(s.cfg.Username, resolvedPassword))
		s.slog.Debug("using username/password authentication")
	}

	// Configure TLS
	if s.cfg.TLS != nil && s.cfg.TLS.Enabled {
		tlsConfig, err := s.cfg.TLS.BuildClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		opts = append(opts, nats.Secure(tlsConfig))
		s.slog.Debug("TLS enabled", "minVersion", s.cfg.TLS.MinVersion)
	}

	return opts, nil
}

// hasAuthentication checks if any authentication method is configured.
func (s *NATSSource) hasAuthentication() bool {
	return s.cfg.Username != "" ||
		s.cfg.Token != "" ||
		s.cfg.NKeyFile != "" ||
		s.cfg.CredentialsFile != ""
}

func (s *NATSSource) consumeCore(queue string) (err error) {
	handler := func(msg *nats.Msg) {
		m := &NATSMessage{
			msg: msg,
		}
		s.c <- message.NewRunnerMessage(m)
	}
	var e error
	if queue != "" {
		s.sub, e = s.nc.QueueSubscribe(s.cfg.Subject, queue, handler)
	} else {
		s.sub, e = s.nc.Subscribe(s.cfg.Subject, handler)
	}
	if e != nil {
		err = fmt.Errorf("failed to subscribe to subject: %w", e)
	}
	return
}

func (s *NATSSource) consumeJetStream() (err error) {
	js := s.js
	stream := s.cfg.Stream
	consumer := s.cfg.Consumer
	sub, e := js.PullSubscribe(s.cfg.Subject, consumer, nats.Bind(stream, consumer))
	if e != nil {
		err = fmt.Errorf("failed to create JetStream pull subscription: %w", e)
		return
	}
	s.sub = sub
	go func() {
		for {
			msgs, err := s.sub.Fetch(1, nats.MaxWait(5*time.Second))
			if err != nil {
				if err == nats.ErrTimeout {
					s.slog.Warn("JetStream fetch timeout")
					continue
				}
				s.slog.Error("error fetching from JetStream", "err", err)
				break
			}
			for _, msg := range msgs {
				m := &NATSMessage{
					msg: msg,
				}
				s.c <- message.NewRunnerMessage(m)
			}
		}
	}()
	return
}

func (s *NATSSource) Close() error {
	// Unsubscribe/Drain subscription before closing channel to avoid send-on-closed-channel
	if s.sub != nil {
		if err := s.sub.Drain(); err != nil {
			_ = s.sub.Unsubscribe()
		}
		s.sub = nil
	}
	if s.nc != nil {
		s.nc.Close()
		s.nc = nil
	}
	if s.c != nil {
		close(s.c)
		s.c = nil
	}
	return nil
}
