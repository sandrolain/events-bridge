package encdec

import (
	"bytes"
	"testing"
	"time"
)

type testStruct struct {
	ID   int    `json:"id" cbor:"id"`
	Name string `json:"name" cbor:"name"`
}

func TestJSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := testStruct{ID: 42, Name: "answer"}

	data, err := EncodeJSON(&original)
	if err != nil {
		t.Fatalf("EncodeJSON error: %v", err)
	}

	var decoded testStruct
	if err := DecodeJSON(data, &decoded); err != nil {
		t.Fatalf("DecodeJSON error: %v", err)
	}

	if decoded != original {
		t.Fatalf("unexpected decoded value: %#v", decoded)
	}
}

func TestCBORRoundTrip(t *testing.T) {
	t.Parallel()

	original := testStruct{ID: 7, Name: "lucky"}

	data, err := EncodeCBOR(&original)
	if err != nil {
		t.Fatalf("EncodeCBOR error: %v", err)
	}

	var decoded testStruct
	if err := DecodeCBOR(data, &decoded); err != nil {
		t.Fatalf("DecodeCBOR error: %v", err)
	}

	if decoded != original {
		t.Fatalf("unexpected decoded value: %#v", decoded)
	}
}

func TestDecodeJSONStream(t *testing.T) {
	t.Parallel()

	buf := bytes.NewBuffer(nil)
	for i := 0; i < 3; i++ {
		payload := map[string]int{"id": i}
		encoded, err := EncodeJSON(&payload)
		if err != nil {
			t.Fatalf("EncodeJSON error: %v", err)
		}
		buf.Write(encoded)
		buf.WriteByte('\n')
	}

	stream, err := DecodeJSONStream[map[string]int](buf)
	if err != nil {
		t.Fatalf("DecodeJSONStream error: %v", err)
	}

	for i := 0; i < 3; i++ {
		select {
		case payload, ok := <-stream:
			if !ok {
				t.Fatalf("stream closed before receiving %d values", 3)
			}
			if payload["id"] != i {
				t.Fatalf("unexpected id at position %d: %v", i, payload)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for stream item %d", i)
		}
	}

	select {
	case _, ok := <-stream:
		if ok {
			t.Fatalf("expected stream to be closed after reading all items")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for stream closure")
	}
}

func TestDecodeCBORStream(t *testing.T) {
	t.Parallel()

	buf := bytes.NewBuffer(nil)
	for i := 0; i < 3; i++ {
		payload := map[string]int{"id": i}
		encoded, err := EncodeCBOR(&payload)
		if err != nil {
			t.Fatalf("EncodeCBOR error: %v", err)
		}
		buf.Write(encoded)
	}

	stream, err := DecodeCBORStream[map[string]int](buf)
	if err != nil {
		t.Fatalf("DecodeCBORStream error: %v", err)
	}

	for i := 0; i < 3; i++ {
		select {
		case payload, ok := <-stream:
			if !ok {
				t.Fatalf("stream closed before receiving %d values", 3)
			}
			if payload["id"] != i {
				t.Fatalf("unexpected id at position %d: %v", i, payload)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for stream item %d", i)
		}
	}

	select {
	case _, ok := <-stream:
		if ok {
			t.Fatalf("expected stream to be closed after reading all items")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for stream closure")
	}
}

func TestDecodeJSONStreamInvalid(t *testing.T) {
	t.Parallel()

	buf := bytes.NewBufferString("{invalid")

	stream, err := DecodeJSONStream[map[string]int](buf)
	if err != nil {
		t.Fatalf("DecodeJSONStream error: %v", err)
	}

	select {
	case _, ok := <-stream:
		if ok {
			t.Fatalf("expected no values for invalid stream")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for invalid stream to close")
	}
}

func TestDecodeCBORStreamInvalid(t *testing.T) {
	t.Parallel()

	buf := bytes.NewBuffer([]byte{0xff, 0x00})

	stream, err := DecodeCBORStream[map[string]int](buf)
	if err != nil {
		t.Fatalf("DecodeCBORStream error: %v", err)
	}

	select {
	case _, ok := <-stream:
		if ok {
			t.Fatalf("expected no values for invalid stream")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for invalid stream to close")
	}
}
