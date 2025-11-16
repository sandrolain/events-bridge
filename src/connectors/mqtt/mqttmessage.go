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

// GetFilesystem returns a virtual filesystem with message data accessible at /data.
func (m *MQTTMessage) GetFilesystem() (fsutil.Filesystem, error) {
	data, err := m.GetData()
	if err != nil {
		return nil, err
	}
	return fsutil.NewVirtualFS("/data", data), nil
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
