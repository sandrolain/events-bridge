package message

import (
	"fmt"
	"sync"

	"github.com/sandrolain/events-bridge/src/common"
)

type SourceMessage interface {
	GetID() []byte
	GetMetadata() (map[string]string, error)
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

var _ SourceMessage = (*RunnerMessage)(nil)

type RunnerMessage struct {
	original SourceMessage
	data     []byte
	metadata map[string]string
	metaMx   sync.Mutex
	dataMx   sync.Mutex
}

func (m *RunnerMessage) GetID() []byte {
	return m.original.GetID()
}

func (m *RunnerMessage) GetOriginal() SourceMessage {
	return m.original
}

func (m *RunnerMessage) SetFromSourceMessage(msg SourceMessage) error {
	meta, err := msg.GetMetadata()
	if err != nil {
		return fmt.Errorf("failed to get source message metadata: %w", err)
	}
	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("failed to get source message data: %w", err)
	}
	m.MergeMetadata(meta)
	m.SetData(data)
	return nil
}

func (m *RunnerMessage) GetMetadataAndData() (map[string]string, []byte, error) {
	meta, err := m.GetMetadata()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get message metadata: %w", err)
	}
	data, err := m.GetData()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get message data: %w", err)
	}
	return meta, data, nil
}

func (m *RunnerMessage) MergeMetadata(meta map[string]string) {
	m.metaMx.Lock()
	defer m.metaMx.Unlock()
	m.metadata = common.CopyMap(meta, m.metadata)
}

func (m *RunnerMessage) AddMetadata(key string, value string) {
	m.metaMx.Lock()
	defer m.metaMx.Unlock()
	if m.metadata == nil {
		m.metadata = make(map[string]string)
	}
	m.metadata[key] = value
}

func (m *RunnerMessage) SetMetadata(meta map[string]string) {
	m.metaMx.Lock()
	defer m.metaMx.Unlock()
	m.metadata = common.CopyMap(meta, nil)
}

func (m *RunnerMessage) SetData(data []byte) {
	m.dataMx.Lock()
	defer m.dataMx.Unlock()
	m.data = data
}

func (m *RunnerMessage) GetSourceMetadata() (map[string]string, error) {
	return m.original.GetMetadata()
}

func (m *RunnerMessage) GetMetadata() (map[string]string, error) {
	m.metaMx.Lock()
	defer m.metaMx.Unlock()
	if m.metadata != nil {
		return m.metadata, nil
	}
	return m.original.GetMetadata()
}

func (m *RunnerMessage) GetSourceData() ([]byte, error) {
	return m.original.GetData()
}

func (m *RunnerMessage) GetData() ([]byte, error) {
	m.dataMx.Lock()
	defer m.dataMx.Unlock()
	if m.data != nil {
		return m.data, nil
	}
	return m.original.GetData()
}

func (m *RunnerMessage) Reply(d *ReplyData) error {
	return m.original.Reply(d)
}

func (m *RunnerMessage) ReplySource() error {
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
	Metadata map[string]string
}
