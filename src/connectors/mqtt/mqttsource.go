package main

import (
	"fmt"
	"log/slog"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/utils"
)

type SourceConfig struct {
	Address       string `yaml:"address" json:"address"`
	Topic         string `yaml:"topic" json:"topic"`
	ClientID      string `yaml:"client_id" json:"client_id"`
	ConsumerGroup string `yaml:"consumer_group" json:"consumer_group"`
}

// parseSourceOptions builds a config from options map with validation.
// Expected keys: address, topic, client_id, consumer_group.
func parseSourceOptions(opts map[string]any) (*SourceConfig, error) {
	cfg := &SourceConfig{}
	op := &utils.OptsParser{}
	cfg.Address = op.OptString(opts, "address", "", utils.StringNonEmpty())
	cfg.Topic = op.OptString(opts, "topic", "", utils.StringNonEmpty())
	cfg.ClientID = op.OptString(opts, "client_id", "")
	cfg.ConsumerGroup = op.OptString(opts, "consumer_group", "")
	if err := op.Error(); err != nil {
		return nil, err
	}
	return cfg, nil
}

type MQTTSource struct {
	config  *SourceConfig
	slog    *slog.Logger
	c       chan *message.RunnerMessage
	client  mqtt.Client
	started bool
}

// NewSource creates the MQTT source from options map.
func NewSource(opts map[string]any) (sources.Source, error) {
	cfg, err := parseSourceOptions(opts)
	if err != nil {
		return nil, err
	}
	return &MQTTSource{
		config: cfg,
		slog:   slog.Default().With("context", "MQTT"),
	}, nil
}

func (s *MQTTSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting MQTT source", "address", s.config.Address, "topic", s.config.Topic, "consumerGroup", s.config.ConsumerGroup)

	opts := mqtt.NewClientOptions().AddBroker("tcp://" + s.config.Address)
	clientID := s.config.ClientID
	if clientID == "" {
		clientID = "events-bridge-" + fmt.Sprint(time.Now().UnixNano())
	}
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}
	s.client = client

	topic := s.config.Topic

	if s.config.ConsumerGroup != "" {
		topic = fmt.Sprintf("$share/%s/%s", s.config.ConsumerGroup, topic)
	}

	s.slog.Info("subscribing to topic", "topic", topic)

	qos := byte(0)

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
		case <-time.After(10 * time.Second):
			// Timeout, ignore
		}
	}

	if token := client.Subscribe(topic, qos, handler); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to subscribe to topic: %w", token.Error())
	}

	s.started = true
	return s.c, nil
}

func (s *MQTTSource) Close() error {
	if s.client != nil && s.client.IsConnected() {
		s.client.Disconnect(250)
	}
	return nil
}
