package encdec

import (
	"bytes"
	"testing"
)

func TestCBORDecoder_Encode(t *testing.T) {
	decoder := &CBORDecoder{}
	data := map[string]string{"key": "value"}
	encoded, err := decoder.Encode(data)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("Expected non-empty encoded data")
	}
}

func TestCBORDecoder_EncodeMessage(t *testing.T) {
	decoder := &CBORDecoder{metaKey: "meta", dataKey: "data"}
	msg := NewEncDecMessage(map[string]string{"id": "123"}, []byte("test data"))
	encoded, err := decoder.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("Expected non-empty encoded data")
	}
}

func TestCBORDecoder_DecodeMessage(t *testing.T) {
	decoder := &CBORDecoder{metaKey: "meta", dataKey: "data"}
	// For CBOR, we need to encode first to get valid CBOR data
	msg := NewEncDecMessage(map[string]string{"id": "123"}, []byte("test data"))
	encoded, err := decoder.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}
	decoded, err := decoder.DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	meta, err := decoded.GetMetadata()
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	if meta["id"] != "123" {
		t.Fatalf("Expected meta id '123', got %v", meta["id"])
	}
	d, err := decoded.GetData()
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}
	if string(d) != "test data" {
		t.Fatalf("Expected data 'test data', got %v", string(d))
	}
}

func TestCBORDecoder_DecodeStream(t *testing.T) {
	decoder := &CBORDecoder{metaKey: "meta", dataKey: "data"}
	msg := NewEncDecMessage(map[string]string{"id": "123"}, []byte("test data"))
	encoded, err := decoder.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}
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
