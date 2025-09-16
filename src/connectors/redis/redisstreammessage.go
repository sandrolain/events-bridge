package main

import (
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.Message = &RedisStreamMessage{}

type RedisStreamMessage struct {
	msg     redis.XMessage
	dataKey string
}

func (m *RedisStreamMessage) GetID() []byte {
	return []byte(m.msg.ID)
}

func (m *RedisStreamMessage) GetMetadata() (map[string][]string, error) {
	meta := map[string][]string{"id": {m.msg.ID}}
	for k, v := range m.msg.Values {
		meta[k] = []string{fmt.Sprintf("%v", v)}
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

func (m *RedisStreamMessage) Ack() error {
	return nil
}

func (m *RedisStreamMessage) Nak() error {
	return nil
}

func (m *RedisStreamMessage) Reply(data []byte, metadata map[string][]string) error {
	return nil
}
