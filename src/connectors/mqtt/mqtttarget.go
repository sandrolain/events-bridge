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
	return &MQTTTarget{
		config: cfg,
		slog:   slog.Default().With("context", "MQTT"),
		stopCh: make(chan struct{}),
	}, nil
}

type MQTTTarget struct {
	slog    *slog.Logger
	config  *targets.TargetMQTTConfig
	stopped bool
	stopCh  chan struct{}
	client  mqtt.Client
}

func (t *MQTTTarget) Consume(c <-chan message.Message) error {
	opts := mqtt.NewClientOptions().AddBroker("tcp://" + t.config.Address)
	clientID := t.config.ClientID
	if clientID == "" {
		clientID = "events-bridge-target-" + fmt.Sprint(time.Now().UnixNano())
	}
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)

	t.client = mqtt.NewClient(opts)
	if token := t.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	t.slog.Info("MQTT target connected", "address", t.config.Address, "topic", t.config.Topic)

	go func() {
		for {
			select {
			case <-t.stopCh:
				return
			case msg, ok := <-c:
				if !ok {
					return
				}
				err := t.publish(msg)
				if err != nil {
					msg.Nak()
					t.slog.Error("error publishing MQTT message", "err", err)
				} else {
					msg.Ack()
				}
			}
		}
	}()
	return nil
}

func (t *MQTTTarget) publish(msg message.Message) error {
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("error getting data: %w", err)
	}

	topic := t.config.Topic
	if t.config.TopicFromMetadataKey != "" {
		metadata, _ := msg.GetMetadata()
		if v, ok := metadata[t.config.TopicFromMetadataKey]; ok && len(v) > 0 {
			topic = v[0]
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
