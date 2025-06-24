package main

import (
	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/message"
)

type RedisMessage struct {
	msg *redis.Message
}

var _ message.Message = &RedisMessage{}

func (m *RedisMessage) GetID() []byte {
	return []byte(m.msg.Channel)
}

func (m *RedisMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{"channel": {m.msg.Channel}}, nil
}

func (m *RedisMessage) GetData() ([]byte, error) {
	return []byte(m.msg.Payload), nil
}

func (m *RedisMessage) Ack() error {
	return nil // Redis Pub/Sub does not support explicit ack
}

func (m *RedisMessage) Nak() error {
	return nil // Redis Pub/Sub does not support explicit nak
}
