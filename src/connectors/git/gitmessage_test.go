package main

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
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
	if err := msg.Ack(); err != nil {
		t.Errorf("Ack should return nil, got %v", err)
	}
	if err := msg.Nak(); err != nil {
		t.Errorf("Nak should return nil, got %v", err)
	}
}

// Dummy implementation for message.Message interface check
var _ message.Message = &GitMessage{}
