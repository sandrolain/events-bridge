package main

import (
	"crypto/sha256"

	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &PGSQLMessage{}

type PGSQLMessage struct {
	channel string
	payload string
}

func (m *PGSQLMessage) GetID() []byte {
	hash := sha256.Sum256([]byte(m.channel + ":" + m.payload))
	return hash[:]
}

func (m *PGSQLMessage) GetMetadata() (message.MessageMetadata, error) {
	return message.MessageMetadata{"channel": m.channel}, nil
}

func (m *PGSQLMessage) GetData() ([]byte, error) {
	return []byte(m.payload), nil
}

func (m *PGSQLMessage) Ack() error {
	return nil
}

func (m *PGSQLMessage) Nak() error {
	return nil
}

func (m *PGSQLMessage) Reply(reply *message.ReplyData) error {
	return nil
}
