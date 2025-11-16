package main

import (
	"github.com/bytedance/sonic"
	"github.com/go-git/go-billy/v5"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ message.SourceMessage = &GitMessage{}

type GitMessage struct {
	changes    []map[string]interface{}
	filesystem billy.Filesystem
}

func (m *GitMessage) GetID() []byte {
	if len(m.changes) > 0 {
		if commitHash, ok := m.changes[0]["commit"].(string); ok {
			return []byte(commitHash)
		}
	}
	return nil
}

func (m *GitMessage) GetMetadata() (map[string]string, error) {
	return map[string]string{}, nil
}

func (m *GitMessage) GetData() ([]byte, error) {
	b, err := sonic.Marshal(m.changes)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// GetFilesystem returns the Billy filesystem for accessing repository files.
// Returns a read-only filesystem wrapper around the git worktree.
func (m *GitMessage) GetFilesystem() (fsutil.Filesystem, error) {
	if m.filesystem == nil {
		return nil, nil
	}
	// Wrap billy.Filesystem in a read-only adapter
	return newReadOnlyBillyFS(m.filesystem), nil
}

func (m *GitMessage) Ack(data *message.ReplyData) error {
	// Git source doesn't support reply
	return nil
}

func (m *GitMessage) Nak() error {
	return nil
}
