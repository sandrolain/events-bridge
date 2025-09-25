package main

import (
	"fmt"
	"time"

	"log/slog"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/targets"
	"github.com/sandrolain/events-bridge/src/utils"
)

type TargetConfig struct {
	Address              string `yaml:"address" json:"address"`
	Topic                string `yaml:"topic" json:"topic"`
	ClientID             string `yaml:"clientID" json:"clientID"`
	QoS                  int    `yaml:"qos" json:"qos"`
	TopicFromMetadataKey string `yaml:"topicFromMetadataKey" json:"topicFromMetadataKey"`
}

// parseTargetOptions builds a config from options map with validation.
// Expected keys: address, topic, clientID, qos, topicFromMetadataKey.
func parseTargetOptions(opts map[string]any) (cfg *TargetConfig, err error) {
	cfg = &TargetConfig{}
	op := &utils.OptsParser{}
	cfg.Address = op.OptString(opts, "address", "", utils.StringNonEmpty())
	cfg.Topic = op.OptString(opts, "topic", "", utils.StringNonEmpty())
	cfg.ClientID = op.OptString(opts, "clientID", "")
	cfg.QoS = op.OptInt(opts, "qos", 0, utils.IntMin(0), utils.IntMax(2))
	cfg.TopicFromMetadataKey = op.OptString(opts, "topicFromMetadataKey", "")
	err = op.Error()
	return
}

func newTargetFromConfig(cfg *TargetConfig) (targets.Target, error) {
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

// NewTarget creates an MQTT target from options map.
func NewTarget(opts map[string]any) (targets.Target, error) {
	cfg, err := parseTargetOptions(opts)
	if err != nil {
		return nil, err
	}
	return newTargetFromConfig(cfg)
}

type MQTTTarget struct {
	slog    *slog.Logger
	config  *TargetConfig
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
	topic = utils.ResolveFromMetadata(msg, t.config.TopicFromMetadataKey, topic)

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
