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

	stub := NewStubSourceMessage(nil, nil).
		WithError(dataErr, metaErr, ackErr, nakErr)

	_, err := stub.GetData()
	if err != dataErr {
		t.Errorf("expected data error, got %v", err)
	}

	_, err = stub.GetMetadata()
	if err != metaErr {
		t.Errorf("expected meta error, got %v", err)
	}

	err = stub.Ack(nil)
	if err != ackErr {
		t.Errorf("expected ack error, got %v", err)
	}

	err = stub.Nak()
	if err != nakErr {
		t.Errorf("expected nak error, got %v", err)
	}
}

func TestStubSourceMessageCallCounters(t *testing.T) {
	stub := NewStubSourceMessage([]byte("test"), nil)

	if stub.AckCalls != 0 {
		t.Errorf("expected 0 ack calls, got %d", stub.AckCalls)
	}

	if err := stub.Ack(nil); err != nil {
		t.Logf("ack error (acceptable): %v", err)
	}
	if err := stub.Ack(nil); err != nil {
		t.Logf("ack error (acceptable): %v", err)
	}
	if stub.AckCalls != 2 {
		t.Errorf("expected 2 ack calls, got %d", stub.AckCalls)
	}

	if err := stub.Nak(); err != nil {
		t.Logf("nak error (acceptable): %v", err)
	}
	if stub.NakCalls != 1 {
		t.Errorf("expected 1 nak call, got %d", stub.NakCalls)
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
