package message

import "sync"

type MessageMetadata = map[string]string

type SourceMessage interface {
	GetID() []byte
	GetMetadata() (MessageMetadata, error)
	GetData() ([]byte, error)
	Ack() error
	Nak() error
	Reply(data *ReplyData) error
}

func NewRunnerMessage(original SourceMessage) *RunnerMessage {
	return &RunnerMessage{
		original: original,
	}
}

type RunnerMessage struct {
	original SourceMessage
	data     []byte
	metadata MessageMetadata
	metaMx   sync.Mutex
}

func (m *RunnerMessage) GetID() []byte {
	return m.original.GetID()
}

func (m *RunnerMessage) MergeMetadata(meta MessageMetadata) {
	m.metaMx.Lock()
	defer m.metaMx.Unlock()
	if m.metadata == nil {
		m.metadata = make(MessageMetadata)
	}
	for k, v := range meta {
		m.metadata[k] = v
	}
}

func (m *RunnerMessage) AddMetadata(key string, value string) {
	m.metaMx.Lock()
	defer m.metaMx.Unlock()
	if m.metadata == nil {
		m.metadata = make(MessageMetadata)
	}
	m.metadata[key] = value
}

func (m *RunnerMessage) SetMetadata(meta MessageMetadata) {
	m.metadata = make(MessageMetadata)
	for k, v := range meta {
		m.metadata[k] = v
	}
}

func (m *RunnerMessage) SetData(data []byte) {
	m.data = data
}

func (m *RunnerMessage) GetSourceMetadata() (MessageMetadata, error) {
	return m.original.GetMetadata()
}

func (m *RunnerMessage) GetTargetMetadata() (MessageMetadata, error) {
	if m.metadata != nil {
		return m.metadata, nil
	}
	return m.original.GetMetadata()
}

func (m *RunnerMessage) GetSourceData() ([]byte, error) {
	return m.original.GetData()
}

func (m *RunnerMessage) GetTargetData() ([]byte, error) {
	if m.data != nil {
		return m.data, nil
	}
	return m.original.GetData()
}

func (m *RunnerMessage) GetMetadata() MessageMetadata {
	return m.metadata
}

func (m *RunnerMessage) GetData() []byte {
	return m.data
}

func (m *RunnerMessage) Reply() error {
	return m.original.Reply(&ReplyData{
		Data:     m.data,
		Metadata: m.metadata,
	})
}

func (m *RunnerMessage) Ack() error {
	return m.original.Ack()
}

func (m *RunnerMessage) Nak() error {
	return m.original.Nak()
}

type ResponseStatus int

const (
	ResponseStatusNak ResponseStatus = iota
	ResponseStatusAck
)

type ReplyData struct {
	Data     []byte
	Metadata MessageMetadata
}
