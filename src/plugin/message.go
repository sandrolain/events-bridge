package plugin

import (
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

var _ message.SourceMessage = &PluginMessage{}

type PluginMessage struct {
	res *proto.PluginMessage
}

func (m *PluginMessage) GetID() []byte {
	if m.res == nil {
		return nil
	}
	return []byte(m.res.Uuid)
}

func (m *PluginMessage) GetMetadata() (message.MessageMetadata, error) {
	metadata := make(message.MessageMetadata)
	for _, md := range m.res.Metadata {
		metadata[md.Name] = md.Value
	}
	return metadata, nil
}

func (m *PluginMessage) GetData() ([]byte, error) {
	if m.res == nil {
		return nil, fmt.Errorf("message is nil")
	}
	return m.res.Data, nil
}

func (m *PluginMessage) Ack() error {
	return nil
}

func (m *PluginMessage) Nak() error {
	return nil
}

func (m *PluginMessage) Reply(d *message.ReplyData) error {
	return nil
}
