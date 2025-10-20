package encdec

import (
	"testing"
)

const testValueString = "value"

func TestConvertToStringMap_MapStringString(t *testing.T) {
	input := map[string]string{"key": testValueString}
	result, err := convertToStringMap(input)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result["key"] != testValueString {
		t.Fatalf("Expected 'value', got %v", result["key"])
	}
}

func TestConvertToStringMap_MapStringAny(t *testing.T) {
	input := map[string]any{"key": testValueString}
	result, err := convertToStringMap(input)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result["key"] != testValueString {
		t.Fatalf("Expected 'value', got %v", result["key"])
	}
}

func TestConvertToStringMap_MapAnyAny(t *testing.T) {
	input := map[any]any{"key": testValueString}
	result, err := convertToStringMap(input)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result["key"] != testValueString {
		t.Fatalf("Expected 'value', got %v", result["key"])
	}
}

func TestConvertToStringMap_Invalid(t *testing.T) {
	input := "invalid"
	_, err := convertToStringMap(input)
	if err == nil {
		t.Fatal("Expected error for invalid type")
	}
}

func TestMapToMessage(t *testing.T) {
	decoder := &JSONDecoder{metaKey: "meta", dataKey: "data"}
	v := map[string]any{
		"meta": map[string]string{"id": "123"},
		"data": testDataString,
	}
	msg, err := mapToMessage(decoder, v, "meta", "data")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	meta, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	if meta["id"] != "123" {
		t.Fatalf("Expected '123', got %v", meta["id"])
	}
	d, err := msg.GetData()
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}
	if string(d) != testDataString {
		t.Fatalf("Expected 'test data', got %v", string(d))
	}
}

func TestMessageToMap(t *testing.T) {
	msg := NewEncDecMessage(map[string]string{"id": "123"}, []byte(testDataString))
	v, err := messageToMap(msg, "meta", "data")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if v["meta"].(map[string]string)["id"] != "123" {
		t.Fatalf("Expected '123', got %v", v["meta"])
	}
	if string(v["data"].([]byte)) != testDataString {
		t.Fatalf("Expected 'test data', got %v", v["data"])
	}
}
