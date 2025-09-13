package main

import (
	"github.com/sandrolain/events-bridge/src/message"
)

// WasmMessage implements message.Message for processed data

type WasmMessage struct {
	original message.Message
	data     []byte
	metadata map[string][]string
}

func (m *WasmMessage) GetID() []byte {
	return m.original.GetID()
}

func (m *WasmMessage) GetMetadata() (map[string][]string, error) {
	return m.metadata, nil
}

func (m *WasmMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *WasmMessage) Ack() error {
	return m.original.Ack()
}

func (m *WasmMessage) Nak() error {
	return m.original.Nak()
}
