package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/common/jwtauth"
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

	// Mode specifies the source mode: "subscribe" (default), "request", or "kv-watch".
	// - subscribe: Standard pub/sub or JetStream consumer
	// - request: Sends request and waits for reply (like HTTP request/response)
	// - kv-watch: Watches a NATS KV bucket for changes
	Mode string `mapstructure:"mode" default:"subscribe" validate:"oneof=subscribe request kv-watch"`

	// RequestTimeout is the timeout for request mode (default: 5s).
	RequestTimeout time.Duration `mapstructure:"requestTimeout" default:"5s"`

	// KVBucket is the name of the KV bucket to watch (required for kv-watch mode).
	KVBucket string `mapstructure:"kvBucket" validate:"required_if=Mode kv-watch"`

	// KVKeys are the specific keys to watch in the KV bucket (optional, watches all if empty).
	KVKeys []string `mapstructure:"kvKeys"`

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

	// JWT authentication configuration (optional)
	JWT *jwtauth.Config `mapstructure:"jwt"`
}

type NATSSource struct {
	cfg     *SourceConfig
	slog    *slog.Logger
	c       chan *message.RunnerMessage
	nc      *nats.Conn
	js      nats.JetStreamContext
	sub     *nats.Subscription
	kv      nats.KeyValue
	watcher nats.KeyWatcher
	jwtAuth *jwtauth.Authenticator
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	logger := slog.Default().With("context", "NATS Source")

	// Initialize JWT authenticator if enabled
	jwtAuth, err := jwtauth.NewAuthenticator(cfg.JWT, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT authenticator: %w", err)
	}

	return &NATSSource{
		cfg:     cfg,
		slog:    logger,
		jwtAuth: jwtAuth,
	}, nil
}

func (s *NATSSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting NATS source",
		"address", s.cfg.Address,
		"subject", s.cfg.Subject,
		"mode", s.cfg.Mode,
		"stream", s.cfg.Stream,
		"consumer", s.cfg.Consumer,
		"queueGroup", s.cfg.QueueGroup,
		"kvBucket", s.cfg.KVBucket,
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

	// Route to appropriate consumption mode
	switch s.cfg.Mode {
	case "kv-watch":
		if err := s.consumeKVWatch(); err != nil {
			return nil, fmt.Errorf("failed to start KV watcher: %w", err)
		}
	case "request":
		if err := s.consumeRequest(); err != nil {
			return nil, fmt.Errorf("failed to start request mode: %w", err)
		}
	default: // "subscribe"
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
			// NATS core
			queue := s.cfg.QueueGroup
			if err := s.consumeCore(queue); err != nil {
				return nil, fmt.Errorf("failed to start NATS core consumer: %w", err)
			}
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

// extractMetadata extracts NATS headers and subject as metadata.
func (s *NATSSource) extractMetadata(msg *nats.Msg) map[string]string {
	metadata := map[string]string{"subject": msg.Subject}

	// Extract NATS headers if present
	if msg.Header != nil {
		for key, values := range msg.Header {
			if len(values) > 0 {
				// Join multiple header values with comma
				metadata[key] = values[0]
				if len(values) > 1 {
					for i := 1; i < len(values); i++ {
						metadata[key] += "," + values[i]
					}
				}
			}
		}
	}

	return metadata
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
		// Extract metadata
		metadata := s.extractMetadata(msg)

		// Validate JWT if configured
		if s.jwtAuth != nil {
			authResult := s.jwtAuth.Authenticate(metadata)
			if !authResult.Verified {
				s.slog.Warn("JWT validation failed, rejecting message",
					"subject", msg.Subject,
					"error", authResult.Error)
				if err := msg.Nak(); err != nil {
					s.slog.Error("failed to NAK message", "error", err)
				}
				return
			}
			// Use enriched metadata with JWT claims
			for k, v := range authResult.Metadata {
				metadata[k] = v
			}
		}

		m := &NATSMessage{
			msg:      msg,
			conn:     s.nc,
			metadata: metadata,
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

// consumeRequest subscribes and sends requests, waiting for replies.
func (s *NATSSource) consumeRequest() error {
	handler := func(msg *nats.Msg) {
		// Extract metadata
		metadata := s.extractMetadata(msg)

		// Validate JWT if configured
		if s.jwtAuth != nil {
			authResult := s.jwtAuth.Authenticate(metadata)
			if !authResult.Verified {
				s.slog.Warn("JWT validation failed, rejecting message",
					"subject", msg.Subject,
					"error", authResult.Error)
				if err := msg.Nak(); err != nil {
					s.slog.Error("failed to NAK message", "error", err)
				}
				return
			}
			// Use enriched metadata with JWT claims
			for k, v := range authResult.Metadata {
				metadata[k] = v
			}
		}

		// Create a reply inbox
		replyInbox := nats.NewInbox()
		replyChan := make(chan *nats.Msg, 1)

		// Subscribe to reply
		sub, err := s.nc.ChanSubscribe(replyInbox, replyChan)
		if err != nil {
			s.slog.Error("failed to subscribe to reply inbox", "error", err)
			return
		}
		defer func() {
			if err := sub.Unsubscribe(); err != nil {
				s.slog.Warn("failed to unsubscribe from reply inbox", "error", err)
			}
		}()

		// Create NATS message with reply data capability
		m := &NATSMessage{
			msg:        msg,
			conn:       s.nc,
			replyInbox: replyInbox,
			replyChan:  replyChan,
			timeout:    s.cfg.RequestTimeout,
			metadata:   metadata,
		}
		s.c <- message.NewRunnerMessage(m)
	}

	var err error
	if s.cfg.QueueGroup != "" {
		s.sub, err = s.nc.QueueSubscribe(s.cfg.Subject, s.cfg.QueueGroup, handler)
	} else {
		s.sub, err = s.nc.Subscribe(s.cfg.Subject, handler)
	}
	if err != nil {
		return fmt.Errorf("failed to subscribe for requests: %w", err)
	}
	return nil
}

// consumeKVWatch watches a NATS KV bucket for changes.
func (s *NATSSource) consumeKVWatch() error {
	js, err := s.nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	s.js = js

	kv, err := js.KeyValue(s.cfg.KVBucket)
	if err != nil {
		return fmt.Errorf("failed to get KV bucket: %w", err)
	}
	s.kv = kv

	// Watch all keys or specific keys
	var watcher nats.KeyWatcher
	if len(s.cfg.KVKeys) > 0 {
		// Watch specific keys
		for _, key := range s.cfg.KVKeys {
			w, err := kv.Watch(key)
			if err != nil {
				return fmt.Errorf("failed to watch KV key %s: %w", key, err)
			}
			if watcher == nil {
				watcher = w
				s.watcher = w
			}
			// For multiple keys, we'll use the first watcher
			// In production, you might want to handle multiple watchers
		}
	} else {
		// Watch all keys
		w, err := kv.WatchAll()
		if err != nil {
			return fmt.Errorf("failed to watch all KV keys: %w", err)
		}
		watcher = w
		s.watcher = w
	}

	// Process KV updates
	go func() {
		for entry := range watcher.Updates() {
			if entry == nil {
				continue
			}
			m := &NATSKVMessage{
				entry: entry,
			}
			s.c <- message.NewRunnerMessage(m)
		}
	}()

	return nil
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
	// Start background fetch loop for JetStream messages
	go s.jetStreamFetchLoop()
	return
}

// processBase extracts metadata from a NATS message and enforces JWT validation
// if an authenticator is configured. Returns the enriched metadata and true when
// the message should be processed. If JWT validation fails, the message will be
// NAKed and the function returns false.
func (s *NATSSource) processBase(msg *nats.Msg) (map[string]string, bool) {
	metadata := s.extractMetadata(msg)
	if s.jwtAuth != nil {
		authResult := s.jwtAuth.Authenticate(metadata)
		if !authResult.Verified {
			s.slog.Warn("JWT validation failed, rejecting message",
				"subject", msg.Subject,
				"error", authResult.Error)
			if err := msg.Nak(); err != nil {
				s.slog.Error("failed to NAK message", "error", err)
			}
			return nil, false
		}
		for k, v := range authResult.Metadata {
			metadata[k] = v
		}
	}
	return metadata, true
}

// jetStreamFetchLoop runs indefinitely fetching messages from the JetStream
// subscription and dispatching them to the runner channel.
func (s *NATSSource) jetStreamFetchLoop() {
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
			metadata, ok := s.processBase(msg)
			if !ok {
				continue
			}
			m := &NATSMessage{
				msg:      msg,
				conn:     s.nc,
				metadata: metadata,
			}
			s.c <- message.NewRunnerMessage(m)
		}
	}
}

func (s *NATSSource) Close() error {
	// Stop KV watcher if active
	if s.watcher != nil {
		if err := s.watcher.Stop(); err != nil {
			s.slog.Warn("failed to stop KV watcher", "error", err)
		}
		s.watcher = nil
	}

	// Close JWT authenticator
	if s.jwtAuth != nil {
		if err := s.jwtAuth.Close(); err != nil {
			s.slog.Warn("failed to close JWT authenticator", "error", err)
		}
	}

	// Unsubscribe/Drain subscription before closing channel to avoid send-on-closed-channel
	if s.sub != nil {
		if err := s.sub.Drain(); err != nil {
			if err := s.sub.Unsubscribe(); err != nil {
				s.slog.Warn("failed to unsubscribe", "error", err)
			}
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
