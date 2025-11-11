package encdec

import (
	"testing"
)

func TestNewEncDecMessage(t *testing.T) {
	meta := map[string]string{"id": "123"}
	data := []byte("test data")
	msg := NewEncDecMessage(meta, data)
	if msg == nil {
		t.Fatal("Expected non-nil message")
	}
}

func TestEncDecMessage_GetID(t *testing.T) {
	msg := NewEncDecMessage(nil, nil)
	id := msg.GetID()
	if id != nil {
		t.Fatalf("Expected nil ID, got %v", id)
	}
}

func TestEncDecMessage_GetMetadata(t *testing.T) {
	meta := map[string]string{"id": "123"}
	msg := NewEncDecMessage(meta, nil)
	result, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result["id"] != "123" {
		t.Fatalf("Expected '123', got %v", result["id"])
	}
}

func TestEncDecMessage_GetData(t *testing.T) {
	data := []byte("test data")
	msg := NewEncDecMessage(nil, data)
	result, err := msg.GetData()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if string(result) != "test data" {
		t.Fatalf("Expected 'test data', got %v", string(result))
	}
}

func TestEncDecMessage_Ack(t *testing.T) {
	msg := NewEncDecMessage(nil, nil)
	err := msg.Ack(nil)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestEncDecMessage_Nak(t *testing.T) {
	msg := NewEncDecMessage(nil, nil)
	err := msg.Nak()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}
