package common_test

import (
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/message"
)

const unexpectedParserError = "unexpected parser error: %v"

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

	msg.SetMetadata("color", "green")
	if got := common.ResolveFromMetadata(msg, "color", "fallback"); got != "green" {
		t.Fatalf("expected overridden metadata value, got %q", got)
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
