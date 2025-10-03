package encdec

import (
	"bytes"
	"testing"
)

func TestJSONDecoder_Encode(t *testing.T) {
	decoder := &JSONDecoder{}
	data := map[string]string{"key": "value"}
	encoded, err := decoder.Encode(data)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("Expected non-empty encoded data")
	}
}

func TestJSONDecoder_EncodeMessage(t *testing.T) {
	decoder := &JSONDecoder{metaKey: "meta", dataKey: "data"}
	msg := NewEncDecMessage(map[string]string{"id": "123"}, []byte("test data"))
	encoded, err := decoder.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("Expected non-empty encoded data")
	}
}

func TestJSONDecoder_DecodeMessage(t *testing.T) {
	decoder := &JSONDecoder{metaKey: "meta", dataKey: "data"}
	data := `{"meta":{"id":"123"},"data":"test data"}`
	decoded, err := decoder.DecodeMessage([]byte(data))
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

func TestJSONDecoder_DecodeStream(t *testing.T) {
	decoder := &JSONDecoder{metaKey: "meta", dataKey: "data"}
	data := `{"meta":{"id":"123"},"data":"test data"}` + "\n"
	reader := bytes.NewReader([]byte(data))
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
