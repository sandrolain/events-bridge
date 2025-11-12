package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/message"
)

const NatsMessageIdHeader = "Nats-Msg-Id"

var _ message.SourceMessage = &NATSMessage{}
var _ message.SourceMessage = &NATSKVMessage{}

type NATSMessage struct {
	msg        *nats.Msg
	conn       *nats.Conn
	replyInbox string
	replyChan  chan *nats.Msg
	timeout    time.Duration
	metadata   map[string]string
}

func (m *NATSMessage) GetID() []byte {
	return []byte(m.msg.Header.Get(NatsMessageIdHeader))
}

func (m *NATSMessage) GetMetadata() (map[string]string, error) {
	// Return pre-validated metadata (already enriched with JWT claims if configured)
	return m.metadata, nil
}

func (m *NATSMessage) GetData() ([]byte, error) {
	return m.msg.Data, nil
}

func (m *NATSMessage) Ack(data *message.ReplyData) error {
	// If this is a request mode message with reply inbox
	if m.replyInbox != "" && m.replyChan != nil && m.conn != nil {
		if data != nil {
			// Send reply with data
			if err := m.conn.Publish(m.replyInbox, data.Data); err != nil {
				return fmt.Errorf("failed to send reply: %w", err)
			}
		}
		return nil
	}

	// Standard NATS message handling
	if m.msg.Reply == "" {
		return nil
	}
	if data != nil {
		// Send reply data if provided
		if err := m.msg.Respond(data.Data); err != nil {
			return err
		}
	}
	return m.msg.Ack()
}

func (m *NATSMessage) Nak() error {
	if m.msg.Reply == "" {
		return nil
	}
	return m.msg.Nak()
}

// NATSKVMessage represents a NATS KV bucket entry change.
type NATSKVMessage struct {
	entry nats.KeyValueEntry
}

func (m *NATSKVMessage) GetID() []byte {
	return []byte(fmt.Sprintf("%s:%d", m.entry.Key(), m.entry.Revision()))
}

func (m *NATSKVMessage) GetMetadata() (map[string]string, error) {
	metadata := map[string]string{
		"key":       m.entry.Key(),
		"bucket":    m.entry.Bucket(),
		"revision":  fmt.Sprintf("%d", m.entry.Revision()),
		"operation": m.entry.Operation().String(),
	}
	if m.entry.Created().Unix() > 0 {
		metadata["created"] = m.entry.Created().Format(time.RFC3339)
	}
	return metadata, nil
}

func (m *NATSKVMessage) GetData() ([]byte, error) {
	return m.entry.Value(), nil
}

func (m *NATSKVMessage) Ack(*message.ReplyData) error {
	// KV entries don't support acknowledgment
	return nil
}

func (m *NATSKVMessage) Nak() error {
	// KV entries don't support negative acknowledgment
	return nil
}
