package main

import (
	"github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/message"
)

type NATSMessage struct {
	subject string
	payload []byte
	msg     *nats.Msg
}

var _ message.Message = &NATSMessage{}

func (m *NATSMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{"subject": {m.subject}}, nil
}

func (m *NATSMessage) GetData() ([]byte, error) {
	return m.payload, nil
}

func (m *NATSMessage) Ack() error {
	return m.msg.Ack()
}

func (m *NATSMessage) Nak() error {
	return m.msg.Nak()
}
