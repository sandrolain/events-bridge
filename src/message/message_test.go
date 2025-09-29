package message

import (
	"errors"
	"testing"
)

type stubSourceMessage struct {
	id          []byte
	metadata    MessageMetadata
	metadataErr error
	data        []byte
	dataErr     error
	ackErr      error
	ackCalls    int
	nakErr      error
	nakCalls    int
	replyErr    error
	replyCalls  int
	replyData   *ReplyData
}

func (s *stubSourceMessage) GetID() []byte {
	return s.id
}

func (s *stubSourceMessage) GetMetadata() (MessageMetadata, error) {
	if s.metadataErr != nil {
		return nil, s.metadataErr
	}
	return s.metadata, nil
}

func (s *stubSourceMessage) GetData() ([]byte, error) {
	if s.dataErr != nil {
		return nil, s.dataErr
	}
	return s.data, nil
}

func (s *stubSourceMessage) Ack() error {
	s.ackCalls++
	return s.ackErr
}

func (s *stubSourceMessage) Nak() error {
	s.nakCalls++
	return s.nakErr
}

func (s *stubSourceMessage) Reply(d *ReplyData) error {
	s.replyCalls++
	s.replyData = d
	return s.replyErr
}

func TestRunnerMessageTargetDataFallback(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{data: []byte("origin")}
	msg := NewRunnerMessage(original)

	data, err := msg.GetTargetData()
	if err != nil {
		t.Fatalf("unexpected error getting target data: %v", err)
	}
	if string(data) != "origin" {
		t.Fatalf("unexpected target data: %q", data)
	}
}

func TestRunnerMessageTargetDataOverride(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{data: []byte("origin")}
	msg := NewRunnerMessage(original)
	msg.SetData([]byte("override"))

	data, err := msg.GetTargetData()
	if err != nil {
		t.Fatalf("unexpected error getting target data: %v", err)
	}
	if string(data) != "override" {
		t.Fatalf("unexpected target data: %q", data)
	}
}

func TestRunnerMessageTargetMetadataPreferredLocal(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{metadata: MessageMetadata{"foo": "bar"}}
	msg := NewRunnerMessage(original)
	msg.SetMetadata("foo", "override")
	msg.AddMetadata("baz", "qux")

	metadata, err := msg.GetTargetMetadata()
	if err != nil {
		t.Fatalf("unexpected error getting target metadata: %v", err)
	}
	if len(metadata) != 2 {
		t.Fatalf("unexpected metadata size: %d", len(metadata))
	}
	if metadata["foo"] != "override" {
		t.Fatalf("expected override for foo, got %q", metadata["foo"])
	}
	if metadata["baz"] != "qux" {
		t.Fatalf("expected qux for baz, got %q", metadata["baz"])
	}
}

func TestRunnerMessageTargetMetadataFallback(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{metadata: MessageMetadata{"foo": "bar"}}
	msg := NewRunnerMessage(original)

	metadata, err := msg.GetTargetMetadata()
	if err != nil {
		t.Fatalf("unexpected error getting target metadata: %v", err)
	}
	if len(metadata) != 1 || metadata["foo"] != "bar" {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}
}

func TestRunnerMessageMergeMetadata(t *testing.T) {
	t.Parallel()

	msg := NewRunnerMessage(&stubSourceMessage{})
	msg.MergeMetadata(MessageMetadata{"a": "1"})
	msg.MergeMetadata(MessageMetadata{"b": "2", "a": "3"})

	metadata := msg.GetMetadata()
	if len(metadata) != 2 {
		t.Fatalf("unexpected metadata size: %d", len(metadata))
	}
	if metadata["a"] != "3" {
		t.Fatalf("expected merged value 3 for key a, got %q", metadata["a"])
	}
	if metadata["b"] != "2" {
		t.Fatalf("expected value 2 for key b, got %q", metadata["b"])
	}
}

func TestRunnerMessageReplyDelegates(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{}
	msg := NewRunnerMessage(original)
	msg.SetData([]byte("payload"))
	msg.SetMetadata("k", "v")

	if err := msg.Reply(); err != nil {
		t.Fatalf("unexpected error replying: %v", err)
	}
	if original.replyCalls != 1 {
		t.Fatalf("expected reply to be invoked once, got %d", original.replyCalls)
	}
	if original.replyData == nil {
		t.Fatalf("expected reply payload to be forwarded")
	}
	if string(original.replyData.Data) != "payload" {
		t.Fatalf("unexpected reply data: %q", original.replyData.Data)
	}
	if original.replyData.Metadata["k"] != "v" {
		t.Fatalf("unexpected reply metadata: %#v", original.replyData.Metadata)
	}
}

func TestRunnerMessageAckNakDelegate(t *testing.T) {
	t.Parallel()

	ackErr := errors.New("ack")
	nakErr := errors.New("nak")

	original := &stubSourceMessage{ackErr: ackErr, nakErr: nakErr}
	msg := NewRunnerMessage(original)

	if err := msg.Ack(); !errors.Is(err, ackErr) {
		t.Fatalf("expected ack error, got %v", err)
	}
	if original.ackCalls != 1 {
		t.Fatalf("unexpected ack call count: %d", original.ackCalls)
	}

	if err := msg.Nak(); !errors.Is(err, nakErr) {
		t.Fatalf("expected nak error, got %v", err)
	}
	if original.nakCalls != 1 {
		t.Fatalf("unexpected nak call count: %d", original.nakCalls)
	}
}

func TestRunnerMessageGetters(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{id: []byte("id"), metadata: MessageMetadata{"foo": "bar"}, data: []byte("src")}
	msg := NewRunnerMessage(original)
	msg.SetData([]byte("dst"))
	msg.SetMetadata("foo", "baz")

	if string(msg.GetID()) != "id" {
		t.Fatalf("unexpected id: %q", msg.GetID())
	}
	if string(msg.GetData()) != "dst" {
		t.Fatalf("unexpected direct data: %q", msg.GetData())
	}
	if msg.GetMetadata()["foo"] != "baz" {
		t.Fatalf("unexpected metadata value: %q", msg.GetMetadata()["foo"])
	}
}
