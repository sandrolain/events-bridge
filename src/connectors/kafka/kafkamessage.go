package main

import (
	"context"
	"fmt"

	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/segmentio/kafka-go"
)

var _ message.SourceMessage = &KafkaMessage{}

type KafkaMessage struct {
	msg    *kafka.Message
	reader *kafka.Reader
}

func (m *KafkaMessage) GetID() []byte {
	return m.msg.Key
}

func (m *KafkaMessage) GetMetadata() (map[string]string, error) {
	return map[string]string{
		"topic":     m.msg.Topic,
		"partition": fmt.Sprint(m.msg.Partition),
		"offset":    fmt.Sprint(m.msg.Offset),
	}, nil
}

func (m *KafkaMessage) GetData() ([]byte, error) {
	return m.msg.Value, nil
}

// GetFilesystem returns a virtual filesystem with message data accessible at /data.
func (m *KafkaMessage) GetFilesystem() (fsutil.Filesystem, error) {
	data, err := m.GetData()
	if err != nil {
		return nil, err
	}
	return fsutil.NewVirtualFS("/data", data), nil
}

func (m *KafkaMessage) Ack(data *message.ReplyData) error {
	// Kafka doesn't support reply in ack
	if m.reader == nil {
		return nil
	}
	err := m.reader.CommitMessages(context.Background(), *m.msg)
	if err != nil {
		return err
	}
	return nil
}

func (m *KafkaMessage) Nak() error {
	return nil
}
