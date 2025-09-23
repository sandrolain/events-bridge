package main

import (
	"fmt"
	"time"

	"log/slog"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
)

func NewTarget(cfg *targets.TargetMQTTConfig) (targets.Target, error) {
	if cfg.Address == "" || cfg.Topic == "" {
		return nil, fmt.Errorf("address and topic are required for MQTT target")
	}

	opts := mqtt.NewClientOptions().AddBroker("tcp://" + cfg.Address)
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = "events-bridge-target-" + fmt.Sprint(time.Now().UnixNano())
	}
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	l := slog.Default().With("context", "MQTT")

	l.Info("MQTT target connected", "address", cfg.Address, "topic", cfg.Topic)

	return &MQTTTarget{
		config: cfg,
		slog:   l,
		client: client,
	}, nil
}

type MQTTTarget struct {
	slog    *slog.Logger
	config  *targets.TargetMQTTConfig
	stopped bool
	stopCh  chan struct{}
	client  mqtt.Client
}

func (t *MQTTTarget) Consume(msg *message.RunnerMessage) error {
	data, err := msg.GetTargetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	topic := t.config.Topic
	if t.config.TopicFromMetadataKey != "" {
		metadata, _ := msg.GetTargetMetadata()
		if v, ok := metadata[t.config.TopicFromMetadataKey]; ok && len(v) > 0 {
			topic = v
		}
	}

	qos := byte(t.config.QoS)
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
