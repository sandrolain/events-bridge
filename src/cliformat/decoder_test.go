package cliformat

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

const (
	errEncodeFrameFmt       = "failed to encode frame: %v"
	errUnexpectedPayloadFmt = "unexpected payload: got %q want %q"
)

func TestDecoderNextMultipleFrames(t *testing.T) {
	t.Parallel()

	frames := []struct {
		metadata message.MessageMetadata
		data     []byte
	}{
		{
			metadata: message.MessageMetadata{
				"id":   "1",
				"type": "greeting",
			},
			data: []byte("hello"),
		},
		{
			metadata: message.MessageMetadata{
				"id":      "2",
				"type":    "informative",
				"version": "v1",
			},
			data: []byte("hello world"),
		},
	}

	var buf bytes.Buffer
	for _, frame := range frames {
		payload, err := Encode(frame.metadata, frame.data)
		if err != nil {
			t.Fatalf(errEncodeFrameFmt, err)
		}
		buf.Write(payload)
	}

	decoder := NewDecoder(&buf)
	for _, expected := range frames {
		frame, err := decoder.Next()
		if err != nil {
			t.Fatalf("unexpected error decoding frame: %v", err)
		}
		assertMetadataEqual(t, frame.Metadata, expected.metadata)
		if string(frame.Data) != string(expected.data) {
			t.Fatalf(errUnexpectedPayloadFmt, frame.Data, expected.data)
		}
	}

	if _, err := decoder.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestDecodeInvalidMarker(t *testing.T) {
	t.Parallel()

	metadata := message.MessageMetadata{"id": "corrupt"}
	data := []byte("payload")

	payload, err := Encode(metadata, data)
	if err != nil {
		t.Fatalf(errEncodeFrameFmt, err)
	}

	corrupted := append([]byte(nil), payload...)
	corrupted[0] ^= 0xFF

	if _, _, err := Decode(corrupted); err == nil {
		t.Fatalf("expected error for invalid marker")
	}
}

func TestDecodeFromReader(t *testing.T) {
	t.Parallel()

	originalMeta := message.MessageMetadata{
		"id":   "reader",
		"type": "single",
	}
	originalData := []byte("hello")

	frame, err := Encode(originalMeta, originalData)
	if err != nil {
		t.Fatalf(errEncodeFrameFmt, err)
	}

	meta, data, err := DecodeFromReader(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("unexpected error decoding frame from reader: %v", err)
	}

	assertMetadataEqual(t, meta, originalMeta)
	if string(data) != string(originalData) {
		t.Fatalf(errUnexpectedPayloadFmt, data, originalData)
	}
}

func TestDecodeStream(t *testing.T) {
	t.Parallel()

	frames := []struct {
		metadata message.MessageMetadata
		data     []byte
	}{
		{
			metadata: message.MessageMetadata{
				"id":   "42",
				"type": "alpha",
			},
			data: []byte("foo"),
		},
		{
			metadata: message.MessageMetadata{
				"id":   "43",
				"type": "beta",
			},
			data: []byte("bar"),
		},
	}

	var buf bytes.Buffer
	for _, frame := range frames {
		payload, err := Encode(frame.metadata, frame.data)
		if err != nil {
			t.Fatalf(errEncodeFrameFmt, err)
		}
		buf.Write(payload)
	}

	ch, err := DecodeStream(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("unexpected error creating decoder: %v", err)
	}

	for _, expected := range frames {
		select {
		case frame, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed unexpectedly")
			}
			assertMetadataEqual(t, frame.Metadata, expected.metadata)
			if string(frame.Data) != string(expected.data) {
				t.Fatalf(errUnexpectedPayloadFmt, frame.Data, expected.data)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for frame")
		}
	}

	if _, ok := <-ch; ok {
		t.Fatalf("expected channel to be closed")
	}
}

func assertMetadataEqual(t *testing.T, got, want message.MessageMetadata) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unexpected metadata size: got %d want %d", len(got), len(want))
	}
	for k, expected := range want {
		if got[k] != expected {
			t.Fatalf("unexpected metadata value for %q: got %q want %q", k, got[k], expected)
		}
	}
}
