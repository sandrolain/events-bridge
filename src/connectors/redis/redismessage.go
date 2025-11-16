package main

import (
	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &RedisMessage{}

type RedisMessage struct {
	msg *redis.Message
}

func (m *RedisMessage) GetID() []byte {
	return []byte(m.msg.Channel)
}

func (m *RedisMessage) GetMetadata() (map[string]string, error) {
	return map[string]string{"channel": m.msg.Channel}, nil
}

func (m *RedisMessage) GetData() ([]byte, error) {
	return []byte(m.msg.Payload), nil
}

// GetFilesystem returns a virtual filesystem with message data accessible at /data.
func (m *RedisMessage) GetFilesystem() (fsutil.Filesystem, error) {
	data, err := m.GetData()
	if err != nil {
		return nil, err
	}
	return fsutil.NewVirtualFS("/data", data), nil
}

func (m *RedisMessage) Ack(data *message.ReplyData) error {
	// Redis pub/sub doesn't support reply
	return nil
}

func (m *RedisMessage) Nak() error {
	return nil
}
