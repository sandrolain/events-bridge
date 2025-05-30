package mqttsource

import (
	"github.com/sandrolain/events-bridge/src/message"
)

type MQTTMessage struct {
	topic   string
	payload []byte
	done    chan responseStatus
}

var _ message.Message = &MQTTMessage{}

type responseStatus int

const (
	statusAck responseStatus = iota
	statusNak
)

func (m *MQTTMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{"topic": {m.topic}}, nil
}

func (m *MQTTMessage) GetData() ([]byte, error) {
	return m.payload, nil
}

func (m *MQTTMessage) Ack() error {
	m.done <- statusAck
	return nil
}

func (m *MQTTMessage) Nak() error {
	m.done <- statusNak
	return nil
}
