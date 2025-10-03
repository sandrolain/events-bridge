package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &MQTTMessage{}

type MQTTMessage struct {
	orig mqtt.Message
	done chan message.ResponseStatus
}

func (m *MQTTMessage) GetID() []byte {
	id := m.orig.MessageID()
	return []byte{byte(id >> 8), byte(id & 0xff)}
}

func (m *MQTTMessage) GetMetadata() (map[string]string, error) {
	return map[string]string{"topic": m.orig.Topic()}, nil
}

func (m *MQTTMessage) GetData() ([]byte, error) {
	return m.orig.Payload(), nil
}

func (m *MQTTMessage) Ack() error {
	message.SendResponseStatus(m.done, message.ResponseStatusAck)
	return nil
}

func (m *MQTTMessage) Nak() error {
	message.SendResponseStatus(m.done, message.ResponseStatusNak)
	return nil
}

func (m *MQTTMessage) Reply(resp *message.ReplyData) error {
	return nil
}
