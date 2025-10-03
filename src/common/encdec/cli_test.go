package encdec

import (
	"bytes"
	"testing"
)

func TestCLIDecoder_Encode(t *testing.T) {
	decoder := &CLIDecoder{}
	_, err := decoder.Encode(map[string]string{"key": "value"})
	if err == nil {
		t.Fatal("Expected error for generic encoding")
	}
}

func TestCLIDecoder_EncodeMessage(t *testing.T) {
	decoder := &CLIDecoder{}
	msg := NewEncDecMessage(map[string]string{"id": "123"}, []byte("test data"))
	encoded, err := decoder.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("Expected non-empty encoded data")
	}
}

func TestCLIDecoder_DecodeMessage(t *testing.T) {
	decoder := &CLIDecoder{}
	msg := NewEncDecMessage(map[string]string{"id": "123"}, []byte("test data"))
	encoded, _ := decoder.EncodeMessage(msg)
	decoded, err := decoder.DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	meta, _ := decoded.GetMetadata()
	if meta["id"] != "123" {
		t.Fatalf("Expected meta id '123', got %v", meta["id"])
	}
	d, _ := decoded.GetData()
	if string(d) != "test data" {
		t.Fatalf("Expected data 'test data', got %v", string(d))
	}
}

func TestCLIDecoder_DecodeStream(t *testing.T) {
	decoder := &CLIDecoder{}
	msg := NewEncDecMessage(map[string]string{"id": "123"}, []byte("test data"))
	encoded, _ := decoder.EncodeMessage(msg)
	reader := bytes.NewReader(encoded)
	stream := decoder.DecodeStream(reader)
	count := 0
	for result := range stream {
		if result.Error != nil {
			t.Fatalf("Expected no error, got %v", result.Error)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("Expected 1 message, got %d", count)
	}
}
