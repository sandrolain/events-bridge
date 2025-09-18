package main

import (
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &PubSubMessage{}

type PubSubMessage struct {
	msg *pubsub.Message
}

func (m *PubSubMessage) GetID() []byte {
	return []byte(m.msg.ID)
}

func (m *PubSubMessage) GetMetadata() (message.MessageMetadata, error) {
	return message.MessageMetadata{"attributes": fmt.Sprint(m.msg.Attributes)}, nil
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

func (m *PubSubMessage) Reply(reply *message.ReplyData) error {
	return nil
}
