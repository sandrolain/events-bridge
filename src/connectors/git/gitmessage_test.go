package main

import (
	"io"
	"testing"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGitMessageGetID(t *testing.T) {
	msg := &GitMessage{
		changes: []map[string]interface{}{
			{"commit": "abc123"},
		},
	}
	id := msg.GetID()
	if string(id) != "abc123" {
		t.Errorf("expected 'abc123', got '%s'", string(id))
	}

	msg2 := &GitMessage{changes: nil}
	if msg2.GetID() != nil {
		t.Error("expected nil for empty changes")
	}
}

func TestGitMessageGetMetadata(t *testing.T) {
	msg := &GitMessage{}
	meta, err := msg.GetMetadata()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(meta) != 0 {
		t.Errorf("expected empty metadata, got %v", meta)
	}
}

func TestGitMessageGetData(t *testing.T) {
	// Normal marshal
	msg := &GitMessage{
		changes: []map[string]interface{}{
			{"commit": "abc123", "foo": "bar"},
		},
	}
	data, err := msg.GetData()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty data")
	}

	// Unmarshalable value (force error)
	msg2 := &GitMessage{
		changes: []map[string]interface{}{
			{"commit": make(chan int)}, // channels cannot be marshaled
		},
	}
	_, err = msg2.GetData()
	if err == nil {
		t.Error("expected error for unmarshalable value, got nil")
	}
}

func TestGitMessageAckNak(t *testing.T) {
	msg := &GitMessage{}
	if err := msg.Ack(nil); err != nil {
		t.Errorf("Ack should return nil, got %v", err)
	}
	if err := msg.Nak(); err != nil {
		t.Errorf("Nak should return nil, got %v", err)
	}
}

// Dummy implementation for message.Message interface check
var _ message.SourceMessage = &GitMessage{}

func TestGitMessage_GetFilesystem(t *testing.T) {
	t.Run("returns nil when filesystem is not set", func(t *testing.T) {
		msg := &GitMessage{
			changes: []map[string]interface{}{
				{"commit": "abc123"},
			},
		}

		fs, err := msg.GetFilesystem()
		require.NoError(t, err)
		assert.Nil(t, fs)
	})

	t.Run("returns read-only filesystem when set", func(t *testing.T) {
		// Create in-memory billy filesystem
		billyFS := memfs.New()
		file, err := billyFS.Create("test.txt")
		require.NoError(t, err)
		_, err = file.Write([]byte("test content"))
		require.NoError(t, err)
		file.Close()

		msg := &GitMessage{
			changes: []map[string]interface{}{
				{"commit": "abc123"},
			},
			filesystem: billyFS,
		}

		fs, err := msg.GetFilesystem()
		require.NoError(t, err)
		require.NotNil(t, fs)

		// Should be able to read files
		f, err := fs.Open("test.txt")
		require.NoError(t, err)
		defer f.Close()

		content, err := io.ReadAll(f)
		require.NoError(t, err)
		assert.Equal(t, "test content", string(content))

		// Should not be able to create files
		_, err = fs.Create("newfile.txt")
		assert.Error(t, err)
	})
}
