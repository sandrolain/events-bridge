package main

import (
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/sandrolain/events-bridge/src/message"
)

// PubSubMessage implementa message.Message per Google Pub/Sub

type PubSubMessage struct {
	msg *pubsub.Message
}

var _ message.Message = &PubSubMessage{}

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
