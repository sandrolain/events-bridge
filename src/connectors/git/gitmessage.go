package main

import (
	"github.com/bytedance/sonic"
	"github.com/sandrolain/events-bridge/src/message"
)

type GitMessage struct {
	changes []map[string]interface{}
}

var _ message.Message = &GitMessage{}

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

func (m *GitMessage) Ack() error { return nil }
func (m *GitMessage) Nak() error { return nil }
