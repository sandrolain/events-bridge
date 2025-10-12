package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"log/slog"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// TargetConfig defines the configuration for an MQTT target connector.
type TargetConfig struct {
	// Address is the MQTT broker address (host:port).
	// Example: "localhost:1883" for plain TCP, "localhost:8883" for TLS.
	Address string `mapstructure:"address" validate:"required"`

	// Topic is the default MQTT topic to publish to.
	// Can be overridden by TopicFromMetadataKey.
	Topic string `mapstructure:"topic" validate:"required"`

	// ClientID is the MQTT client identifier.
	// If empty, a cryptographically secure random ID will be generated.
	ClientID string `mapstructure:"clientId"`

	// QoS is the Quality of Service level for publishing (0, 1, or 2).
	// 0 = At most once (fire and forget)
	// 1 = At least once (acknowledged delivery)
	// 2 = Exactly once (assured delivery)
	// Default: 0
	QoS int `mapstructure:"qos" default:"0" validate:"required,min=0,max=2"`

	// TopicFromMetadataKey is the metadata key to read the topic from.
	// If the key exists in message metadata, its value will be used as the topic.
	// Otherwise, the default Topic will be used.
	TopicFromMetadataKey string `mapstructure:"topicFromMetadataKey" validate:"required"`

	// Retained determines whether messages should be retained by the broker.
	// Retained messages are delivered to new subscribers immediately.
	// Default: false
	Retained bool `mapstructure:"retained" default:"false"`

	// Username for MQTT broker authentication.
	// Leave empty if authentication is not required.
	Username string `mapstructure:"username"`

	// Password for MQTT broker authentication.
	// Leave empty if authentication is not required.
	// WARNING: Consider using environment variables or secret managers for production.
	Password string `mapstructure:"password"`

	// TLS holds TLS/SSL configuration for secure connections.
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// KeepAlive is the keep alive interval in seconds.
	// The client will send PINGREQ messages to keep the connection alive.
	// Default: 60 seconds
	KeepAlive int `mapstructure:"keepAlive" default:"60" validate:"min=0"`

	// CleanSession determines whether to start a clean session.
	// true = Broker will discard any previous session for this client
	// false = Broker will resume previous session if available
	// Default: true
	CleanSession bool `mapstructure:"cleanSession" default:"true"`
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

// NewTarget creates an MQTT target from options map.
func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	useTLS := cfg.TLS != nil && cfg.TLS.Enabled
	protocol := "tcp"
	if useTLS {
		protocol = "ssl"
	}

	// Build broker URL
	brokerURL := fmt.Sprintf("%s://%s", protocol, cfg.Address)
	copts := mqtt.NewClientOptions().AddBroker(brokerURL)

	// Generate or use provided client ID
	clientID := cfg.ClientID
	if clientID == "" {
		var err error
		clientID, err = generateTargetSecureClientID()
		if err != nil {
			return nil, err
		}
	}
	copts.SetClientID(clientID)

	// Configure authentication
	if cfg.Username != "" {
		copts.SetUsername(cfg.Username)
		copts.SetPassword(cfg.Password)
	}

	// Configure TLS
	if useTLS {
		tlsConfig, err := cfg.TLS.BuildClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		copts.SetTLSConfig(tlsConfig)
	}

	// Configure connection options
	copts.SetCleanSession(cfg.CleanSession)
	copts.SetKeepAlive(time.Duration(cfg.KeepAlive) * time.Second)
	copts.SetAutoReconnect(true)
	copts.SetConnectRetry(true)
	copts.SetConnectRetryInterval(2 * time.Second)

	client := mqtt.NewClient(copts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	l := slog.Default().With("context", "MQTT Target")

	l.Info("MQTT target connected",
		"address", cfg.Address,
		"topic", cfg.Topic,
		"qos", cfg.QoS,
		"retained", cfg.Retained,
		"tls", useTLS,
	)

	return &MQTTTarget{
		cfg:    cfg,
		slog:   l,
		client: client,
	}, nil
}

// generateTargetSecureClientID creates a cryptographically secure random client ID for targets.
func generateTargetSecureClientID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random client ID: %w", err)
	}
	return "events-bridge-target-" + hex.EncodeToString(bytes), nil
}

type MQTTTarget struct {
	cfg    *TargetConfig
	slog   *slog.Logger
	client mqtt.Client
	stopCh chan struct{}
}

func (t *MQTTTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	topic := t.cfg.Topic
	topic = message.ResolveFromMetadata(msg, t.cfg.TopicFromMetadataKey, topic)

	qos := byte(t.cfg.QoS)
	if qos > 2 {
		qos = 0
	}

	retained := t.cfg.Retained

	t.slog.Debug("publishing MQTT message",
		"topic", topic,
		"qos", qos,
		"retained", retained,
		"bodysize", len(data),
	)

	token := t.client.Publish(topic, qos, retained, data)
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("error publishing to MQTT: %w", token.Error())
	}

	t.slog.Debug("MQTT message published", "topic", topic)

	return nil
}

func (t *MQTTTarget) Close() error {
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.client != nil && t.client.IsConnected() {
		t.client.Disconnect(250)
	}
	return nil
}
