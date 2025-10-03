package main

import (
	"crypto/sha256"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &PGSQLMessage{}

type PGSQLMessage struct {
	notification *pgconn.Notification
}

func (m *PGSQLMessage) GetID() []byte {
	hash := sha256.Sum256([]byte(m.notification.Channel + m.notification.Payload))
	return hash[:]
}

func (m *PGSQLMessage) GetMetadata() (map[string]string, error) {
	return map[string]string{"channel": m.notification.Channel}, nil
}

func (m *PGSQLMessage) GetData() ([]byte, error) {
	return []byte(m.notification.Payload), nil
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
