package main

import (
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
)

type RedisStreamMessage struct {
	msg redis.XMessage
}

var _ message.Message = &RedisStreamMessage{}

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
	if v, ok := m.msg.Values["data"]; ok {
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
