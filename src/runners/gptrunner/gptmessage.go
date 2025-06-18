package main

import "github.com/sandrolain/events-bridge/src/message"

type gptMessage struct {
	original message.Message
	data     []byte
}

func (m *gptMessage) GetID() []byte {
	return m.original.GetID()
}

func (m *gptMessage) GetMetadata() (map[string][]string, error) {
	return m.original.GetMetadata()
}

func (m *gptMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *gptMessage) Ack() error {
	return m.original.Ack()
}

func (m *gptMessage) Nak() error {
	return m.original.Nak()
}
