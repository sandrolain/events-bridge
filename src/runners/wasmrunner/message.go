package wasmrunner

import "github.com/sandrolain/events-bridge/src/message"

// WasmMessage implementa message.Message per i dati processati

type WasmMessage struct {
	original message.Message
	data     []byte
}

func (m *WasmMessage) GetMetadata() (map[string][]string, error) {
	return m.original.GetMetadata()
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
