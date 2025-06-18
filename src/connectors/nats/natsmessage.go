package main

import (
	"github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/message"
)

const NatsMessageIdHeader = "Nats-Msg-Id"

type NATSMessage struct {
	msg *nats.Msg
}

var _ message.Message = &NATSMessage{}

func (m *NATSMessage) GetID() []byte {
	return []byte(m.msg.Header.Get(NatsMessageIdHeader))
}

func (m *NATSMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{"subject": {m.msg.Subject}}, nil
}

func (m *NATSMessage) GetData() ([]byte, error) {
	return m.msg.Data, nil
}

func (m *NATSMessage) Ack() error {
	return m.msg.Ack()
}

func (m *NATSMessage) Nak() error {
	return m.msg.Nak()
}
