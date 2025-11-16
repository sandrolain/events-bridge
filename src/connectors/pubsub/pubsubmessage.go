package main

import (
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &PubSubMessage{}

type PubSubMessage struct {
	msg *pubsub.Message
}

func (m *PubSubMessage) GetID() []byte {
	return []byte(m.msg.ID)
}

func (m *PubSubMessage) GetMetadata() (map[string]string, error) {
	return map[string]string{"attributes": fmt.Sprint(m.msg.Attributes)}, nil
}

func (m *PubSubMessage) GetData() ([]byte, error) {
	return m.msg.Data, nil
}

// GetFilesystem returns a virtual filesystem with message data accessible at /data.
func (m *PubSubMessage) GetFilesystem() (fsutil.Filesystem, error) {
	data, err := m.GetData()
	if err != nil {
		return nil, err
	}
	return fsutil.NewVirtualFS("/data", data), nil
}

func (m *PubSubMessage) Ack(data *message.ReplyData) error {
	// Google Pub/Sub doesn't support reply
	m.msg.Ack()
	return nil
}

func (m *PubSubMessage) Nak() error {
	m.msg.Nack()
	return nil
}
