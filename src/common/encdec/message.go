package encdec

import (
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = (*EncDecMessage)(nil)

func NewEncDecMessage(metadata map[string]string, data []byte) *EncDecMessage {
	return &EncDecMessage{
		metadata: metadata,
		data:     data,
	}
}

type EncDecMessage struct {
	metadata map[string]string
	data     []byte
}

func (m *EncDecMessage) GetID() []byte {
	return nil
}

func (m *EncDecMessage) GetMetadata() (map[string]string, error) {
	return m.metadata, nil
}

func (m *EncDecMessage) GetData() ([]byte, error) {
	return m.data, nil
}

// GetFilesystem returns nil as this message type does not provide filesystem access.
func (m *EncDecMessage) GetFilesystem() (fsutil.Filesystem, error) {
	return nil, nil
}

func (m *EncDecMessage) Ack(data *message.ReplyData) error {
	return nil
}

func (m *EncDecMessage) Nak() error {
	return nil
}
