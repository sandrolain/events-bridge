package main

import "github.com/sandrolain/events-bridge/src/message"

type jsonlogicMessage struct {
	original message.Message
	data     []byte
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
