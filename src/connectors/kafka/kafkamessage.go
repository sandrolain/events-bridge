package main

import (
	"context"
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/segmentio/kafka-go"
)

var _ message.Message = &KafkaMessage{}

type KafkaMessage struct {
	msg    *kafka.Message
	reader *kafka.Reader
}

func (m *KafkaMessage) GetID() []byte {
	return m.msg.Key
}

func (m *KafkaMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{
		"topic":     {m.msg.Topic},
		"partition": {fmt.Sprint(m.msg.Partition)},
		"offset":    {fmt.Sprint(m.msg.Offset)},
	}, nil
}

func (m *KafkaMessage) GetData() ([]byte, error) {
	return m.msg.Value, nil
}

func (m *KafkaMessage) Ack() error {
	err := m.reader.CommitMessages(context.Background(), *m.msg)
	if err != nil {
		return err
	}
	return nil
}

func (m *KafkaMessage) Nak() error {
	return nil
}

func (m *KafkaMessage) Reply(data []byte, metadata map[string][]string) error {
	return nil
}
