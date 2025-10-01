package encdec

import (
	"testing"
)

func TestNewMessageDecoder_JSON(t *testing.T) {
	decoder, err := NewMessageDecoder("json", "meta", "data")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if _, ok := decoder.(*JSONDecoder); !ok {
		t.Fatalf("Expected JSONDecoder, got %T", decoder)
	}
}

func TestNewMessageDecoder_CBOR(t *testing.T) {
	decoder, err := NewMessageDecoder("cbor", "meta", "data")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if _, ok := decoder.(*CBORDecoder); !ok {
		t.Fatalf("Expected CBORDecoder, got %T", decoder)
	}
}

func TestNewMessageDecoder_CLI(t *testing.T) {
	decoder, err := NewMessageDecoder("cli", "", "")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if _, ok := decoder.(*CLIDecoder); !ok {
		t.Fatalf("Expected CLIDecoder, got %T", decoder)
	}
}

func TestNewMessageDecoder_Unknown(t *testing.T) {
	_, err := NewMessageDecoder("unknown", "", "")
	if err == nil {
		t.Fatal("Expected error for unknown type")
	}
}
