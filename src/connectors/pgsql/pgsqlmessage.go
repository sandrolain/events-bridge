package main

import (
	"crypto/sha256"

	"github.com/sandrolain/events-bridge/src/message"
)

// PGSQLMessage implements message.Message
var _ message.Message = &PGSQLMessage{}

type PGSQLMessage struct {
	channel string
	payload string
}

func (m *PGSQLMessage) GetID() []byte {
	hash := sha256.Sum256([]byte(m.channel + ":" + m.payload))
	return hash[:]
}

func (m *PGSQLMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{"channel": {m.channel}}, nil
}

func (m *PGSQLMessage) GetData() ([]byte, error) {
	return []byte(m.payload), nil
}

func (m *PGSQLMessage) Ack() error {
	// Nessuna azione necessaria per Ack su NOTIFY
	return nil
}

func (m *PGSQLMessage) Nak() error {
	// Nessuna azione necessaria per Nak su NOTIFY
	return nil
}
