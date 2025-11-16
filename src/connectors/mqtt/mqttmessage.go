package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &MQTTMessage{}

type MQTTMessage struct {
	orig     mqtt.Message
	done     chan message.ResponseStatus
	metadata map[string]string
}

func (m *MQTTMessage) GetID() []byte {
	id := m.orig.MessageID()
	return []byte{byte(id >> 8), byte(id & 0xff)}
}

func (m *MQTTMessage) GetMetadata() (map[string]string, error) {
	// Return pre-validated metadata (already enriched with JWT claims if configured)
	return m.metadata, nil
}

func (m *MQTTMessage) GetData() ([]byte, error) {
	return m.orig.Payload(), nil
}

// GetFilesystem returns nil as this message type does not provide filesystem access.
func (m *MQTTMessage) GetFilesystem() (fsutil.Filesystem, error) {
	return nil, nil
}

func (m *MQTTMessage) Ack(data *message.ReplyData) error {
	// MQTT doesn't support reply in ack
	message.SendResponseStatus(m.done, message.ResponseStatusAck)
	return nil
}

func (m *MQTTMessage) Nak() error {
	message.SendResponseStatus(m.done, message.ResponseStatusNak)
	return nil
}
