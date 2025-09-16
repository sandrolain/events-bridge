package main

import (
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.Message = &PubSubMessage{}

type PubSubMessage struct {
	msg *pubsub.Message
}

func (m *PubSubMessage) GetID() []byte {
	return []byte(m.msg.ID)
}

func (m *PubSubMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{"attributes": {fmt.Sprint(m.msg.Attributes)}}, nil
}

func (m *PubSubMessage) GetData() ([]byte, error) {
	return m.msg.Data, nil
}

func (m *PubSubMessage) Ack() error {
	m.msg.Ack()
	return nil
}

func (m *PubSubMessage) Nak() error {
	m.msg.Nack()
	return nil
}

func (m *PubSubMessage) Reply(data []byte, metadata map[string][]string) error {
	return nil
}
