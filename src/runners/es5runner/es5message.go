package main

import "github.com/sandrolain/events-bridge/src/message"

// ES5Message implements message.Message for processed data
// Exposes JS-friendly methods for data and metadata

type ES5Message struct {
	original message.Message
	data     []byte
	metadata map[string][]string
}

func (m *ES5Message) GetID() []byte {
	return m.original.GetID()
}

func (m *ES5Message) GetMetadata() (map[string][]string, error) {
	if m.metadata != nil {
		return m.metadata, nil
	}
	return m.original.GetMetadata()
}

func (m *ES5Message) SetMetadata(key string, value string) {
	if m.metadata == nil {
		orig, _ := m.original.GetMetadata()
		m.metadata = make(map[string][]string, len(orig))
		for k, v := range orig {
			vv := make([]string, len(v))
			copy(vv, v)
			m.metadata[k] = vv
		}
	}
	m.metadata[key] = []string{value}
}

func (m *ES5Message) AddMetadata(key string, value string) {
	if m.metadata == nil {
		orig, _ := m.original.GetMetadata()
		m.metadata = make(map[string][]string, len(orig))
		for k, v := range orig {
			vv := make([]string, len(v))
			copy(vv, v)
			m.metadata[k] = vv
		}
	}
	m.metadata[key] = append(m.metadata[key], value)
}

func (m *ES5Message) GetData() ([]byte, error) {
	if m.data != nil {
		return m.data, nil
	}
	return m.original.GetData()
}

func (m *ES5Message) SetData(data []byte) {
	m.data = data
}

func (m *ES5Message) Ack() error {
	return m.original.Ack()
}

func (m *ES5Message) Nak() error {
	return m.original.Nak()
}
