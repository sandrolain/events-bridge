package plugin

import (
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

var _ message.Message = &PluginMessage{}

type PluginMessage struct {
	original message.Message
	res      *proto.PluginMessage
}

func (m *PluginMessage) GetID() []byte {
	if m.res == nil {
		return nil
	}
	return []byte(m.res.Uuid)
}

func (m *PluginMessage) GetMetadata() (map[string][]string, error) {
	metadata := make(map[string][]string)
	for _, md := range m.res.Metadata {
		metadata[md.Name] = append(metadata[md.Name], md.Value)
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
	return m.original.Ack()
}

func (m *PluginMessage) Nak() error {
	return m.original.Nak()
}

func (m *PluginMessage) Reply(data []byte, metadata map[string][]string) error {
	return m.original.Reply(data, metadata)
}
