package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/connectors/common"
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

func (m *MQTTMessage) GetMetadata() (message.MessageMetadata, error) {
	return message.MessageMetadata{"topic": m.orig.Topic()}, nil
}

func (m *MQTTMessage) GetData() ([]byte, error) {
	return m.orig.Payload(), nil
}

func (m *MQTTMessage) Ack() error {
	common.SendResponseStatus(m.done, message.ResponseStatusAck)
	return nil
}

func (m *MQTTMessage) Nak() error {
	common.SendResponseStatus(m.done, message.ResponseStatusNak)
	return nil
}

func (m *MQTTMessage) Reply(resp *message.ReplyData) error {
	return nil
}
