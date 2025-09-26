package main

import (
	"fmt"
	"time"

	"log/slog"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	Address              string `mapstructure:"address" validate:"required"`
	Topic                string `mapstructure:"topic" validate:"required"`
	ClientID             string `mapstructure:"clientId" validate:"required"`
	QoS                  int    `mapstructure:"qos" default:"0" validate:"required,min=0,max=2"`
	TopicFromMetadataKey string `mapstructure:"topicFromMetadataKey" validate:"required"`
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

	copts := mqtt.NewClientOptions().AddBroker("tcp://" + cfg.Address)
	copts.SetClientID(cfg.ClientID)
	copts.SetAutoReconnect(true)
	copts.SetConnectRetry(true)
	copts.SetConnectRetryInterval(2 * time.Second)

	client := mqtt.NewClient(copts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	l := slog.Default().With("context", "MQTT Target")

	l.Info("MQTT target connected", "address", cfg.Address, "topic", cfg.Topic)

	return &MQTTTarget{
		cfg:    cfg,
		slog:   l,
		client: client,
	}, nil
}

type MQTTTarget struct {
	cfg     *TargetConfig
	slog    *slog.Logger
	client  mqtt.Client
	stopped bool
	stopCh  chan struct{}
}

func (t *MQTTTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	topic := t.cfg.Topic
	topic = common.ResolveFromMetadata(msg, t.cfg.TopicFromMetadataKey, topic)

	qos := byte(t.cfg.QoS)
	if qos > 2 {
		qos = 0
	}

	t.slog.Debug("publishing MQTT message", "topic", topic, "qos", qos, "bodysize", len(data))

	token := t.client.Publish(topic, qos, false, data)
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("error publishing to MQTT: %w", token.Error())
	}

	t.slog.Debug("MQTT message published", "topic", topic)

	return nil
}

func (t *MQTTTarget) Close() error {
	t.stopped = true
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.client != nil && t.client.IsConnected() {
		t.client.Disconnect(250)
	}
	return nil
}
