package main

import (
	"fmt"
	"log/slog"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	Address       string `mapstructure:"address" validate:"required"`
	Topic         string `mapstructure:"topic" validate:"required"`
	ClientID      string `mapstructure:"clientId" validate:"required"`
	ConsumerGroup string `mapstructure:"consumerGroup" validate:"required"`
}

type MQTTSource struct {
	cfg    *SourceConfig
	slog   *slog.Logger
	c      chan *message.RunnerMessage
	client mqtt.Client
}

// NewSource creates the MQTT source from options map.
func NewSource(opts map[string]any) (connectors.Source, error) {
	cfg, err := common.ParseConfig[SourceConfig](opts)
	if err != nil {
		return nil, err
	}
	return &MQTTSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "MQTT Source"),
	}, nil
}

func (s *MQTTSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting MQTT source", "address", s.cfg.Address, "topic", s.cfg.Topic, "consumerGroup", s.cfg.ConsumerGroup)

	opts := mqtt.NewClientOptions().AddBroker("tcp://" + s.cfg.Address)
	clientID := s.cfg.ClientID
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

	topic := s.cfg.Topic

	if s.cfg.ConsumerGroup != "" {
		topic = fmt.Sprintf("$share/%s/%s", s.cfg.ConsumerGroup, topic)
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

	return s.c, nil
}

func (s *MQTTSource) Close() error {
	if s.client != nil && s.client.IsConnected() {
		s.client.Disconnect(250)
	}
	return nil
}
