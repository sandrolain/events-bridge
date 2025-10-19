package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// SourceConfig defines the configuration for an MQTT source connector.
type SourceConfig struct {
	// Address is the MQTT broker address (host:port).
	// Example: "localhost:1883" for plain TCP, "localhost:8883" for TLS.
	Address string `mapstructure:"address" validate:"required"`

	// Topic is the MQTT topic to subscribe to.
	// Supports MQTT wildcards: + (single level), # (multi-level).
	Topic string `mapstructure:"topic" validate:"required"`

	// ClientID is the MQTT client identifier.
	// If empty, a cryptographically secure random ID will be generated.
	ClientID string `mapstructure:"clientId"`

	// ConsumerGroup enables shared subscriptions for load balancing.
	// When set, the topic will be prefixed with $share/{consumerGroup}/.
	ConsumerGroup string `mapstructure:"consumerGroup"`

	// QoS is the Quality of Service level for subscription (0, 1, or 2).
	// 0 = At most once (fire and forget)
	// 1 = At least once (acknowledged delivery)
	// 2 = Exactly once (assured delivery)
	// Default: 0
	QoS int `mapstructure:"qos" default:"0" validate:"min=0,max=2"`

	// CleanSession determines whether to start a clean session.
	// true = Broker will discard any previous session for this client
	// false = Broker will resume previous session if available
	// Default: true
	CleanSession bool `mapstructure:"cleanSession" default:"true"`

	// KeepAlive is the keep alive interval in seconds.
	// The client will send PINGREQ messages to keep the connection alive.
	// Default: 60 seconds
	KeepAlive int `mapstructure:"keepAlive" default:"60" validate:"min=0"`

	// Username for MQTT broker authentication.
	// Leave empty if authentication is not required.
	Username string `mapstructure:"username"`

	// Password for MQTT broker authentication.
	// Leave empty if authentication is not required.
	// WARNING: Consider using environment variables or secret managers for production.
	Password string `mapstructure:"password"`

	// TLS holds TLS/SSL configuration for secure connections.
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// MessageTimeout is the maximum time to wait for message Ack/Nak.
	// Default: 10 seconds
	MessageTimeout time.Duration `mapstructure:"messageTimeout" default:"10s"`
}

type MQTTSource struct {
	cfg    *SourceConfig
	slog   *slog.Logger
	c      chan *message.RunnerMessage
	client mqtt.Client
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// NewSource creates the MQTT source from options map.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	return &MQTTSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "MQTT Source"),
	}, nil
}

// generateSecureClientID creates a cryptographically secure random client ID.
func generateSecureClientID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random client ID: %w", err)
	}
	return "events-bridge-" + hex.EncodeToString(bytes), nil
}

func (s *MQTTSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	useTLS := tlsconfig.IsEnabled(s.cfg.TLS)
	protocol := "tcp"
	if useTLS {
		protocol = "ssl"
	}

	s.slog.Info("starting MQTT source",
		"address", s.cfg.Address,
		"topic", s.cfg.Topic,
		"consumerGroup", s.cfg.ConsumerGroup,
		"qos", s.cfg.QoS,
		"tls", useTLS,
	)

	// Build broker URL
	brokerURL := fmt.Sprintf("%s://%s", protocol, s.cfg.Address)
	opts := mqtt.NewClientOptions().AddBroker(brokerURL)

	// Generate or use provided client ID
	clientID := s.cfg.ClientID
	if clientID == "" {
		var err error
		clientID, err = generateSecureClientID()
		if err != nil {
			return nil, err
		}
	}
	opts.SetClientID(clientID)

	// Configure authentication
	if s.cfg.Username != "" {
		opts.SetUsername(s.cfg.Username)
		// Resolve password secret
		resolvedPassword, err := secrets.Resolve(s.cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve password: %w", err)
		}
		opts.SetPassword(resolvedPassword)
	}

	// Configure TLS
	if useTLS {
		tlsConfig, err := tlsconfig.BuildClientConfigIfEnabled(s.cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts.SetTLSConfig(tlsConfig)
	}

	// Configure connection options
	opts.SetCleanSession(s.cfg.CleanSession)
	opts.SetKeepAlive(time.Duration(s.cfg.KeepAlive) * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)

	// Create and connect client
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}
	s.client = client

	// Build topic with consumer group if specified
	topic := s.cfg.Topic
	if s.cfg.ConsumerGroup != "" {
		topic = fmt.Sprintf("$share/%s/%s", s.cfg.ConsumerGroup, topic)
	}

	s.slog.Info("subscribing to topic", "topic", topic, "qos", s.cfg.QoS)

	qos := byte(s.cfg.QoS)

	// Message handler
	handler := func(client mqtt.Client, msg mqtt.Message) {
		done := make(chan message.ResponseStatus)
		s.c <- message.NewRunnerMessage(&MQTTMessage{
			orig: msg,
			done: done,
		})
		// Wait for Ack/Nak or timeout
		select {
		case <-done:
			// Ack/Nak received, no need to respond to MQTT
		case <-time.After(s.cfg.MessageTimeout):
			// Timeout, ignore
		}
	}

	if token := client.Subscribe(topic, qos, handler); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to subscribe to topic: %w", token.Error())
	}

	return s.c, nil
}

func (s *MQTTSource) Close() error {
	if s.client != nil && s.client.IsConnected() {
		s.client.Disconnect(250)
	}
	return nil
}
