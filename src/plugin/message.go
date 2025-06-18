package plugin

import (
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

var _ message.Message = &PluginMessage{}

type PluginMessage struct {
	res *proto.PluginMessage
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
	// TODO: Implement Ack logic if needed
	return nil
}

func (m *PluginMessage) Nak() error {
	// TODO: Implement Nak logic if needed
	return nil
}
