package main

import (
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &RedisStreamMessage{}

type RedisStreamMessage struct {
	msg     redis.XMessage
	dataKey string
}

func (m *RedisStreamMessage) GetID() []byte {
	return []byte(m.msg.ID)
}

func (m *RedisStreamMessage) GetMetadata() (map[string]string, error) {
	meta := map[string]string{"id": m.msg.ID}
	for k, v := range m.msg.Values {
		meta[k] = fmt.Sprintf("%v", v)
	}
	return meta, nil
}

func (m *RedisStreamMessage) GetData() ([]byte, error) {
	dataKey := m.dataKey
	if dataKey == "" {
		dataKey = "data"
	}
	if v, ok := m.msg.Values[dataKey]; ok {
		switch val := v.(type) {
		case string:
			return []byte(val), nil
		case []byte:
			return val, nil
		}
	}
	return nil, fmt.Errorf("no data field in stream message")
}

// GetFilesystem returns a virtual filesystem with message data accessible at /data.
func (m *RedisStreamMessage) GetFilesystem() (fsutil.Filesystem, error) {
	data, err := m.GetData()
	if err != nil {
		return nil, err
	}
	return fsutil.NewVirtualFS("/data", data), nil
}

func (m *RedisStreamMessage) Ack(data *message.ReplyData) error {
	// Redis streams don't support reply
	return nil
}

func (m *RedisStreamMessage) Nak() error {
	return nil
}
