package message

import (
	"fmt"
	"sync"

	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
)

type SourceMessage interface {
	GetID() []byte
	GetMetadata() (map[string]string, error)
	GetData() ([]byte, error)
	GetFilesystem() (fsutil.Filesystem, error)
	Ack(data *ReplyData) error
	Nak() error
}

func NewRunnerMessage(original SourceMessage) *RunnerMessage {
	return &RunnerMessage{
		original: original,
	}
}

var _ SourceMessage = (*RunnerMessage)(nil)

// Part represents a single part in a multipart message
type Part struct {
	Name        string            // Part name (e.g., "logs", "artifact1")
	Filename    string            // Optional filename for file parts
	ContentType string            // MIME type
	Headers     map[string]string // Additional headers
	Data        []byte            // Part data
}

type RunnerMessage struct {
	original   SourceMessage
	data       []byte
	metadata   map[string]string
	filesystem fsutil.Filesystem
	parts      []Part // Multipart data
	metaMx     sync.Mutex
	dataMx     sync.Mutex
	fsMx       sync.Mutex
	partsMx    sync.Mutex // Protect parts
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

// GetFilesystem returns the filesystem associated with the message.
// If a filesystem was set on this RunnerMessage, it returns that.
// Otherwise, it delegates to the original message.
func (m *RunnerMessage) GetFilesystem() (fsutil.Filesystem, error) {
	m.fsMx.Lock()
	defer m.fsMx.Unlock()
	if m.filesystem != nil {
		return m.filesystem, nil
	}
	return m.original.GetFilesystem()
}

// SetFilesystem sets a filesystem for this message.
// This allows runners to modify or wrap the filesystem before passing to the next runner.
func (m *RunnerMessage) SetFilesystem(fs fsutil.Filesystem) {
	m.fsMx.Lock()
	defer m.fsMx.Unlock()
	m.filesystem = fs
}

func (m *RunnerMessage) GetAllMetadata() (map[string]string, error) {
	origMeta, err := m.original.GetMetadata()
	if err != nil {
		return nil, err
	}
	meta, err := m.GetMetadata()
	if err != nil {
		return nil, err
	}
	res := common.CopyMap(origMeta, nil)
	common.CopyMap(meta, res)
	return res, nil
}

func (m *RunnerMessage) Ack(d *ReplyData) error {
	return m.original.Ack(d)
}

func (m *RunnerMessage) AckSource(reply bool) error {
	if reply {
		return m.original.Ack(&ReplyData{
			Data:     m.data,
			Metadata: m.metadata,
		})
	}
	return m.original.Ack(nil)
}

func (m *RunnerMessage) Nak() error {
	return m.original.Nak()
}

// AddPart adds a part to the multipart message
func (m *RunnerMessage) AddPart(part Part) {
	m.partsMx.Lock()
	defer m.partsMx.Unlock()
	m.parts = append(m.parts, part)
}

// GetParts returns all message parts
func (m *RunnerMessage) GetParts() []Part {
	m.partsMx.Lock()
	defer m.partsMx.Unlock()
	// Return a copy to prevent external modification
	return append([]Part(nil), m.parts...)
}

// HasParts returns true if message has multipart data
func (m *RunnerMessage) HasParts() bool {
	m.partsMx.Lock()
	defer m.partsMx.Unlock()
	return len(m.parts) > 0
}

// ClearParts removes all parts
func (m *RunnerMessage) ClearParts() {
	m.partsMx.Lock()
	defer m.partsMx.Unlock()
	m.parts = nil
}

// SetParts replaces all parts
func (m *RunnerMessage) SetParts(parts []Part) {
	m.partsMx.Lock()
	defer m.partsMx.Unlock()
	m.parts = append([]Part(nil), parts...)
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
