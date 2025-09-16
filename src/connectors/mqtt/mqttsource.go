package main

import (
	"fmt"
	"log/slog"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
)

type MQTTSource struct {
	config  *sources.SourceMQTTConfig
	slog    *slog.Logger
	c       chan message.Message
	client  mqtt.Client
	started bool
}

func NewSource(cfg *sources.SourceMQTTConfig) (sources.Source, error) {
	if cfg.Address == "" || cfg.Topic == "" {
		return nil, fmt.Errorf("address and topic are required for MQTT source")
	}
	return &MQTTSource{
		config: cfg,
		slog:   slog.Default().With("context", "MQTT"),
	}, nil
}

func (s *MQTTSource) Produce(buffer int) (<-chan message.Message, error) {
	s.c = make(chan message.Message, buffer)

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
		m := &MQTTMessage{
			orig: msg,
			done: done,
		}
		s.c <- m
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
	if s.c != nil {
		close(s.c)
	}
	if s.client != nil && s.client.IsConnected() {
		s.client.Disconnect(250)
	}
	return nil
}
