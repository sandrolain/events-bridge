package encdec

import "github.com/sandrolain/events-bridge/src/message"

var _ message.SourceMessage = (*EncDecMessage)(nil)

func NewEncDecMessage(metadata message.MessageMetadata, data []byte) *EncDecMessage {
	return &EncDecMessage{
		metadata: metadata,
		data:     data,
	}
}

type EncDecMessage struct {
	metadata message.MessageMetadata
	data     []byte
}

func (m *EncDecMessage) GetID() []byte {
	return nil
}

func (m *EncDecMessage) GetMetadata() (message.MessageMetadata, error) {
	return m.metadata, nil
}

func (m *EncDecMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *EncDecMessage) Ack() error {
	return nil
}

func (m *EncDecMessage) Nak() error {
	return nil
}

func (m *EncDecMessage) Reply(*message.ReplyData) error {
	return nil
}
