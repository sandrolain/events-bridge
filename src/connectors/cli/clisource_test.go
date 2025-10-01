package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	ch, src := setupCLISource(t, FormatJSON, records, "metadata", "data")
	defer closeSource(t, src)

	// Small delay to let the source fully start
	time.Sleep(50 * time.Millisecond)

	msg1 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg1), "foo", "bar")
	decoded1 := decodePayload(t, FormatJSON, mustData(t, msg1))
	expectPayloadText(t, decoded1, "data", `{"text":"hello"}`)

	msg2 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg2), "foo", "baz")
	decoded2 := decodePayload(t, FormatJSON, mustData(t, msg2))
	expectPayloadText(t, decoded2, "data", `{"text":"world"}`)
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

	ch, src := setupCLISource(t, FormatCBOR, records, "metadata", "data")
	defer closeSource(t, src)

	// Small delay to let the source fully start
	time.Sleep(50 * time.Millisecond)

	msg1 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg1), "foo", "bar")
	decoded1 := decodePayload(t, FormatCBOR, mustData(t, msg1))
	expectPayloadText(t, decoded1, "data", `{"text":"hello"}`)

	msg2 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg2), "foo", "baz")
	decoded2 := decodePayload(t, FormatCBOR, mustData(t, msg2))
	expectPayloadText(t, decoded2, "data", `{"text":"world"}`)
}

func TestCLISourceJSONWholeMap(t *testing.T) {
	record := map[string]any{
		"meta":  map[string]string{"foo": "bar"},
		"value": 42,
	}

	ch, src := setupCLISource(t, FormatJSON, []map[string]any{record}, "meta", "")
	defer closeSource(t, src)

	// Small delay to let the source fully start
	time.Sleep(50 * time.Millisecond)

	msg := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg), "foo", "bar")

	decoded := decodePayload(t, FormatJSON, mustData(t, msg))
	if _, ok := decoded["meta"]; ok {
		t.Fatalf("metadata key should not be present in data: %v", decoded)
	}
	value, ok := decoded["value"].(float64)
	if !ok || value != 42 {
		t.Fatalf("unexpected value: %v", decoded["value"])
	}
}

func setupCLISource(t *testing.T, format CLIFormat, records []map[string]any, metadataKey, dataKey string) (<-chan *message.RunnerMessage, *CLISource) {
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
	src := srcAny.(*CLISource)

	ch, err := src.Produce(len(records))
	if err != nil {
		t.Fatalf(sourceErrProduceFmt, err)
	}

	return ch, src
}

func writeRecordsFile(t *testing.T, format CLIFormat, records []map[string]any) string {
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
		if format == FormatJSON {
			buf.WriteByte('\n')
		}
	}

	dir := t.TempDir()
	filename := "events.json"
	if format == FormatCBOR {
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

func mustMetadata(t *testing.T, msg *message.RunnerMessage) message.MessageMetadata {
	t.Helper()
	metadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf(sourceErrGetMetadataFmt, err)
	}
	return metadata
}

func expectMetadataValue(t *testing.T, metadata message.MessageMetadata, key, expected string) {
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

func decodePayload(t *testing.T, format CLIFormat, data []byte) map[string]any {
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

	// For source tests, data is the encoded payload, so return as string
	return map[string]any{
		"metadata": metadata,
		"data":     string(dataBytes),
	}
}

func expectPayloadText(t *testing.T, decoded map[string]any, key, expected string) {
	t.Helper()
	dataStr, ok := decoded["data"].(string)
	if !ok {
		t.Fatalf("data is not string: %v", decoded["data"])
	}
	// For simplicity, check if expected is in dataStr
	if dataStr != expected {
		t.Fatalf(sourceErrUnexpectedPayloadFmt, dataStr)
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
	case <-time.After(5 * time.Second): // Increased timeout for race detector
		t.Fatalf("timeout waiting for message")
	}
	return nil
}
