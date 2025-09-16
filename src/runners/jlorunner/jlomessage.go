package main

import "github.com/sandrolain/events-bridge/src/message"

var _ message.Message = &jsonlogicMessage{}

type jsonlogicMessage struct {
	original message.Message
	data     []byte
}

func (m *jsonlogicMessage) GetID() []byte {
	return m.original.GetID()
}

func (m *jsonlogicMessage) GetMetadata() (map[string][]string, error) {
	return nil, nil
}

func (m *jsonlogicMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *jsonlogicMessage) Ack() error {
	return m.original.Ack()
}

func (m *jsonlogicMessage) Nak() error {
	return m.original.Nak()
}

func (m *jsonlogicMessage) Reply(data []byte, metadata map[string][]string) error {
	return m.original.Reply(data, metadata)
}
