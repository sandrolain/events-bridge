package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/message"
)

type MQTTMessage struct {
	orig mqtt.Message
	done chan responseStatus
}

var _ message.Message = &MQTTMessage{}

type responseStatus int

const (
	statusAck responseStatus = iota
	statusNak
)

func (m *MQTTMessage) GetID() []byte {
	id := m.orig.MessageID()
	return []byte{byte(id >> 8), byte(id & 0xff)}
}

func (m *MQTTMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{"topic": {m.orig.Topic()}}, nil
}

func (m *MQTTMessage) GetData() ([]byte, error) {
	return m.orig.Payload(), nil
}

func (m *MQTTMessage) Ack() error {
	m.done <- statusAck
	return nil
}

func (m *MQTTMessage) Nak() error {
	m.done <- statusNak
	return nil
}
