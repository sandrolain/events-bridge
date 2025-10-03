package manager

import (
	"fmt"

	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/connectors/plugin/proto"
	"github.com/sandrolain/events-bridge/src/message"
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

func (m *PluginMessage) GetMetadata() (map[string]string, error) {
	return common.CopyMap(m.res.Metadata, nil), nil
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
