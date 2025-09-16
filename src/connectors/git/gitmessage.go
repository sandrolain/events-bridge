package main

import (
	"github.com/bytedance/sonic"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.Message = &GitMessage{}

type GitMessage struct {
	changes []map[string]interface{}
}

func (m *GitMessage) GetID() []byte {
	if len(m.changes) > 0 {
		return []byte(m.changes[0]["commit"].(string))
	}
	return nil
}

func (m *GitMessage) GetMetadata() (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (m *GitMessage) GetData() ([]byte, error) {
	b, err := sonic.Marshal(m.changes)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (m *GitMessage) Ack() error {
	return nil
}

func (m *GitMessage) Nak() error {
	return nil
}

func (m *GitMessage) Reply(data []byte, metadata map[string][]string) error {
	return nil
}
