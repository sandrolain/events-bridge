package common_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/message"
)

type mockSourceMessage struct {
	metadata message.MessageMetadata
}

func (m *mockSourceMessage) GetID() []byte {
	return nil
}

func (m *mockSourceMessage) GetMetadata() (message.MessageMetadata, error) {
	return m.metadata, nil
}

func (m *mockSourceMessage) GetData() ([]byte, error) {
	return nil, nil
}

func (m *mockSourceMessage) Ack() error {
	return nil
}

func (m *mockSourceMessage) Nak() error {
	return nil
}

func (m *mockSourceMessage) Reply(data *message.ReplyData) error {
	return nil
}

type mockSourceMessageError struct {
	metadataErr error
}

func (m *mockSourceMessageError) GetID() []byte {
	return nil
}

func (m *mockSourceMessageError) GetMetadata() (message.MessageMetadata, error) {
	return nil, m.metadataErr
}

func (m *mockSourceMessageError) GetData() ([]byte, error) {
	return nil, nil
}

func (m *mockSourceMessageError) Ack() error {
	return nil
}

func (m *mockSourceMessageError) Nak() error {
	return nil
}

func (m *mockSourceMessageError) Reply(data *message.ReplyData) error {
	return nil
}

func TestResolveFromMetadata(t *testing.T) {
	base := message.MessageMetadata{"color": "blue"}
	msg := message.NewRunnerMessage(&mockSourceMessage{metadata: base})

	if got := common.ResolveFromMetadata(msg, "color", "fallback"); got != "blue" {
		t.Fatalf("expected metadata value to be returned, got %q", got)
	}

	if got := common.ResolveFromMetadata(msg, "missing", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for missing key, got %q", got)
	}

	if got := common.ResolveFromMetadata(msg, "", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for empty key, got %q", got)
	}

	msg.AddMetadata("color", "green")
	if got := common.ResolveFromMetadata(msg, "color", "fallback"); got != "green" {
		t.Fatalf("expected overridden metadata value, got %q", got)
	}
}

func TestResolveFromMetadataFallbackOnError(t *testing.T) {
	msg := message.NewRunnerMessage(&mockSourceMessageError{metadataErr: errors.New("boom")})

	if got := common.ResolveFromMetadata(msg, "color", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback when metadata retrieval fails, got %q", got)
	}
}

func TestResolveFromMetadataEmptyValue(t *testing.T) {
	base := message.MessageMetadata{"color": ""}
	msg := message.NewRunnerMessage(&mockSourceMessage{metadata: base})

	if got := common.ResolveFromMetadata(msg, "color", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback when metadata value empty, got %q", got)
	}
}

func TestAwaitReplyOrStatusReply(t *testing.T) {
	done := make(chan message.ResponseStatus, 1)
	reply := make(chan *message.ReplyData, 1)
	reply <- &message.ReplyData{Data: []byte("hello")}

	r, status, timeout := common.AwaitReplyOrStatus(50*time.Millisecond, done, reply)
	if timeout {
		t.Fatal("unexpected timeout")
	}
	if status != nil {
		t.Fatal("expected nil status when reply received")
	}
	if r == nil || string(r.Data) != "hello" {
		t.Fatalf("unexpected reply data: %+v", r)
	}
}

func TestSendResponseStatus(t *testing.T) {
	common.SendResponseStatus(nil, message.ResponseStatusAck)

	ch := make(chan message.ResponseStatus, 1)
	common.SendResponseStatus(ch, message.ResponseStatusAck)

	select {
	case status := <-ch:
		if status != message.ResponseStatusAck {
			t.Fatalf("unexpected status sent: %v", status)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timeout waiting for status send")
	}
}

func TestSendReply(t *testing.T) {
	common.SendReply(nil, &message.ReplyData{Data: []byte("noop")})

	ch := make(chan *message.ReplyData, 1)
	expected := &message.ReplyData{Data: []byte("hello")}
	common.SendReply(ch, expected)

	select {
	case reply := <-ch:
		if string(reply.Data) != "hello" {
			t.Fatalf("unexpected reply data: %s", reply.Data)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timeout waiting for reply send")
	}
}

func TestAwaitReplyOrStatusStatus(t *testing.T) {
	done := make(chan message.ResponseStatus, 1)
	reply := make(chan *message.ReplyData, 1)
	done <- message.ResponseStatusAck

	r, status, timeout := common.AwaitReplyOrStatus(50*time.Millisecond, done, reply)
	if timeout {
		t.Fatal("unexpected timeout")
	}
	if r != nil {
		t.Fatal("expected nil reply when status received")
	}
	if status == nil || *status != message.ResponseStatusAck {
		t.Fatalf("unexpected status: %v", status)
	}
}

func TestAwaitReplyOrStatusTimeout(t *testing.T) {
	done := make(chan message.ResponseStatus)
	reply := make(chan *message.ReplyData)

	r, status, timeout := common.AwaitReplyOrStatus(10*time.Millisecond, done, reply)
	if !timeout {
		t.Fatal("expected timeout to be true")
	}
	if r != nil {
		t.Fatal("expected nil reply on timeout")
	}
	if status != nil {
		t.Fatal("expected nil status on timeout")
	}
}
