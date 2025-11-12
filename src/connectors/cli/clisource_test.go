package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/common/encdec"
	"github.com/sandrolain/events-bridge/src/message"
)

const (
	sourceErrCloseFmt              = "Close error: %v"
	sourceErrGetDataFmt            = "GetData error: %v"
	sourceErrGetMetadataFmt        = "GetMetadata error: %v"
	sourceErrNewSourceFmt          = "NewSource error: %v"
	sourceErrProduceFmt            = "Produce error: %v"
	sourceErrUnexpectedMetadataFmt = "unexpected metadata: %v"
	sourceErrUnexpectedPayloadFmt  = "unexpected payload text: %v"
	sourceErrWriteTempFileFmt      = "write temp file: %v"
)

func TestCLISourceJSON(t *testing.T) {
	records := []map[string]any{
		{
			"metadata": map[string]string{"foo": "bar"},
			"data":     map[string]any{"text": "hello"},
		},
		{
			"metadata": map[string]string{"foo": "baz"},
			"data":     map[string]any{"text": "world"},
		},
	}
	ch, src := setupCLISource(t, "json", records, "metadata", "data")
	defer closeSource(t, src)

	// Small delay to let the source fully start
	time.Sleep(100 * time.Millisecond)

	msg1 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg1), "foo", "bar")
	expectPayloadText(t, mustData(t, msg1), `{"text":"hello"}`)

	msg2 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg2), "foo", "baz")
	expectPayloadText(t, mustData(t, msg2), `{"text":"world"}`)
}

func TestCLISourceCBOR(t *testing.T) {
	records := []map[string]any{
		{
			"metadata": map[string]string{"foo": "bar"},
			"data":     map[string]any{"text": "hello"},
		},
		{
			"metadata": map[string]string{"foo": "baz"},
			"data":     map[string]any{"text": "world"},
		},
	}

	ch, src := setupCLISource(t, "cbor", records, "metadata", "data")
	defer closeSource(t, src)

	// Small delay to let the source fully start
	time.Sleep(50 * time.Millisecond)

	msg1 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg1), "foo", "bar")
	expectPayloadBytes(t, mustData(t, msg1), mustMarshalCBOR(t, map[string]any{"text": "hello"}))

	msg2 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg2), "foo", "baz")
	expectPayloadBytes(t, mustData(t, msg2), mustMarshalCBOR(t, map[string]any{"text": "world"}))
}

func TestCLISourceJSONWholeMap(t *testing.T) {
	record := map[string]any{
		"metadata": map[string]string{"foo": "bar"},
		"data":     42,
	}

	ch, src := setupCLISource(t, "json", []map[string]any{record}, "metadata", "data")
	defer closeSource(t, src)

	// Longer delay for race detector and CI environments
	time.Sleep(200 * time.Millisecond)

	msg := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg), "foo", "bar")

	encoder, err := encdec.NewMessageDecoder("json", "metadata", "data")
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}

	data, err := encoder.Encode(record)
	if err != nil {
		t.Fatalf("failed to encode record: %v", err)
	}

	decoded := decodePayload(t, "json", data)

	value, ok := decoded["data"].(float64)
	if !ok || value != 42 {
		t.Fatalf("unexpected value: %v", decoded["value"])
	}
}

func setupCLISource(t *testing.T, format string, records []map[string]any, metadataKey, dataKey string) (<-chan *message.RunnerMessage, *CLISource) {
	t.Helper()
	file := writeRecordsFile(t, format, records)

	cfg := &SourceConfig{
		Command:     "cat",
		Args:        []string{file},
		Timeout:     5 * time.Second, // Increased timeout for race detector
		Format:      string(format),
		MetadataKey: metadataKey,
		DataKey:     dataKey,
	}

	srcAny, err := NewSource(cfg)
	if err != nil {
		t.Fatalf(sourceErrNewSourceFmt, err)
	}
	src, ok := srcAny.(*CLISource)
	if !ok {
		t.Fatal("failed to cast source to CLISource")
	}

	ch, err := src.Produce(len(records))
	if err != nil {
		t.Fatalf(sourceErrProduceFmt, err)
	}

	return ch, src
}

func writeRecordsFile(t *testing.T, format string, records []map[string]any) string {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	for _, record := range records {
		encoder, err := encdec.NewMessageDecoder(string(format), "metadata", "data")
		if err != nil {
			t.Fatalf("failed to create encoder: %v", err)
		}

		data, err := encoder.Encode(record)
		if err != nil {
			t.Fatalf("failed to encode record: %v", err)
		}
		buf.Write(data)
		if format == "json" {
			buf.WriteByte('\n')
		}
	}

	dir := t.TempDir()
	filename := "events.json"
	if format == "cbor" {
		filename = "events.cbor"
	}
	path := filepath.Join(dir, filename)

	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf(sourceErrWriteTempFileFmt, err)
	}

	return path
}

func closeSource(t *testing.T, src *CLISource) {
	t.Helper()
	if err := src.Close(); err != nil {
		t.Fatalf(sourceErrCloseFmt, err)
	}
}

func mustMarshalCBOR(t *testing.T, v any) []byte {
	t.Helper()
	data, err := cbor.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal CBOR: %v", err)
	}
	return data
}

func mustMetadata(t *testing.T, msg *message.RunnerMessage) map[string]string {
	t.Helper()
	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(sourceErrGetMetadataFmt, err)
	}
	return metadata
}

func expectMetadataValue(t *testing.T, metadata map[string]string, key, expected string) {
	t.Helper()
	if metadata[key] != expected {
		t.Fatalf(sourceErrUnexpectedMetadataFmt, metadata)
	}
}

func mustData(t *testing.T, msg *message.RunnerMessage) []byte {
	t.Helper()
	data, err := msg.GetData()
	if err != nil {
		t.Fatalf(sourceErrGetDataFmt, err)
	}
	return data
}

func decodePayload(t *testing.T, format string, data []byte) map[string]any {
	t.Helper()

	encoder, err := encdec.NewMessageDecoder(string(format), "metadata", "data")
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}

	decodedData, err := encoder.DecodeMessage(data)
	if err != nil {
		t.Fatalf("failed to decode message: %v", err)
	}

	metadata, err := decodedData.GetMetadata()
	if err != nil {
		t.Fatalf(sourceErrGetMetadataFmt, err)
	}

	dataBytes, err := decodedData.GetData()
	if err != nil {
		t.Fatalf(sourceErrGetDataFmt, err)
	}

	var dataAny any
	err = json.Unmarshal(dataBytes, &dataAny)
	if err != nil {
		t.Fatalf("failed to unmarshal data to any: %v", err)
	}

	// For source tests, data is the encoded payload, so return as string
	return map[string]any{
		"metadata": metadata,
		"data":     dataAny,
	}
}

func expectPayloadText(t *testing.T, data []byte, expected string) {
	t.Helper()
	if string(data) != expected {
		t.Fatalf("data is not string: %v", string(data))
	}
}

func expectPayloadBytes(t *testing.T, data []byte, expected []byte) {
	t.Helper()
	if !bytes.Equal(data, expected) {
		t.Fatalf(sourceErrUnexpectedPayloadFmt, data)
	}
}

func receiveMessage(t *testing.T, ch <-chan *message.RunnerMessage) *message.RunnerMessage {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed unexpectedly")
		}
		return msg
	case <-time.After(10 * time.Second): // Increased timeout for CI and race detector
		t.Fatalf("timeout waiting for message")
	}
	return nil
}
