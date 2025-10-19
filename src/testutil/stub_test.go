package testutil

import (
	"errors"
	"testing"
)

func TestNewStubSourceMessage(t *testing.T) {
	data := []byte(`{"test": true}`)
	metadata := map[string]string{"key": "value"}

	stub := NewStubSourceMessage(data, metadata)

	if stub.ID == nil {
		t.Error("expected default ID to be set")
	}

	gotData, err := stub.GetData()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if string(gotData) != string(data) {
		t.Errorf("expected data %q, got %q", string(data), string(gotData))
	}

	gotMetadata, err := stub.GetMetadata()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if gotMetadata["key"] != "value" {
		t.Errorf("expected metadata key=value, got %v", gotMetadata)
	}
}

func TestStubSourceMessageWithError(t *testing.T) {
	dataErr := errors.New("data error")
	metaErr := errors.New("meta error")
	ackErr := errors.New("ack error")
	nakErr := errors.New("nak error")
	replyErr := errors.New("reply error")

	stub := NewStubSourceMessage(nil, nil).
		WithError(dataErr, metaErr, ackErr, nakErr, replyErr)

	_, err := stub.GetData()
	if err != dataErr {
		t.Errorf("expected data error, got %v", err)
	}

	_, err = stub.GetMetadata()
	if err != metaErr {
		t.Errorf("expected meta error, got %v", err)
	}

	err = stub.Ack()
	if err != ackErr {
		t.Errorf("expected ack error, got %v", err)
	}

	err = stub.Nak()
	if err != nakErr {
		t.Errorf("expected nak error, got %v", err)
	}

	testReply := struct{ data string }{data: "test"}
	err = stub.Reply(testReply)
	if err != replyErr {
		t.Errorf("expected reply error, got %v", err)
	}
}

func TestStubSourceMessageCallCounters(t *testing.T) {
	stub := NewStubSourceMessage([]byte("test"), nil)

	if stub.AckCalls != 0 {
		t.Errorf("expected 0 ack calls, got %d", stub.AckCalls)
	}

	_ = stub.Ack()
	_ = stub.Ack()
	if stub.AckCalls != 2 {
		t.Errorf("expected 2 ack calls, got %d", stub.AckCalls)
	}

	_ = stub.Nak()
	if stub.NakCalls != 1 {
		t.Errorf("expected 1 nak call, got %d", stub.NakCalls)
	}

	replyData := "test reply"
	_ = stub.Reply(replyData)
	if stub.ReplyCalls != 1 {
		t.Errorf("expected 1 reply call, got %d", stub.ReplyCalls)
	}
	if stub.ReplyData == nil {
		t.Error("expected reply data to be stored")
	}
}

func TestStubSourceMessageGetID(t *testing.T) {
	stub := NewStubSourceMessage(nil, nil)
	id := stub.GetID()
	if string(id) != "test-id" {
		t.Errorf("expected default ID 'test-id', got %q", string(id))
	}

	customID := []byte("custom-id")
	stub.ID = customID
	id = stub.GetID()
	if string(id) != string(customID) {
		t.Errorf("expected custom ID %q, got %q", string(customID), string(id))
	}
}
