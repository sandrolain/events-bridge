package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/encdec"
	"github.com/sandrolain/events-bridge/src/message"
)

const (
	errCloseFmt              = "Close error: %v"
	errConsumeFmt            = "Consume error: %v"
	errDecodeCBORFmt         = "DecodeCBOR error: %v"
	errDecodeJSONFmt         = "DecodeJSON error: %v"
	errGetTargetDataFmt      = "GetTargetData error: %v"
	errGetTargetMetadataFmt  = "GetTargetMetadata error: %v"
	errNewSourceFmt          = "NewSource error: %v"
	errNewTargetFmt          = "NewTarget error: %v"
	errProduceFmt            = "Produce error: %v"
	errReadFileFmt           = "ReadFile error: %v"
	errUnexpectedData        = "unexpected data: %q"
	errUnexpectedMetadataFmt = "unexpected metadata: %v"
	errUnexpectedPayloadFmt  = "unexpected payload text: %v"
	errWriteTempFileFmt      = "write temp file: %v"
)

func TestCLISourceJSON(t *testing.T) {
	records := []map[string]any{
		{
			"metadata": map[string]string{"foo": "bar"},
			"payload":  map[string]any{"text": "hello"},
		},
		{
			"metadata": map[string]string{"foo": "baz"},
			"payload":  map[string]any{"text": "world"},
		},
	}
	ch, src := setupCLISource(t, FormatJSON, records, "metadata", "payload")
	defer closeSource(t, src)

	// Small delay to let the source fully start
	time.Sleep(50 * time.Millisecond)

	msg1 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg1), "foo", "bar")
	decoded1 := decodePayload(t, FormatJSON, mustData(t, msg1))
	expectPayloadText(t, decoded1, "text", "hello")

	msg2 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg2), "foo", "baz")
	decoded2 := decodePayload(t, FormatJSON, mustData(t, msg2))
	expectPayloadText(t, decoded2, "text", "world")
}

func TestCLISourceCBOR(t *testing.T) {
	records := []map[string]any{
		{
			"metadata": map[string]string{"foo": "bar"},
			"payload":  map[string]any{"text": "hello"},
		},
		{
			"metadata": map[string]string{"foo": "baz"},
			"payload":  map[string]any{"text": "world"},
		},
	}

	ch, src := setupCLISource(t, FormatCBOR, records, "metadata", "payload")
	defer closeSource(t, src)

	// Small delay to let the source fully start
	time.Sleep(50 * time.Millisecond)

	msg1 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg1), "foo", "bar")
	decoded1 := decodePayload(t, FormatCBOR, mustData(t, msg1))
	expectPayloadText(t, decoded1, "text", "hello")

	msg2 := receiveMessage(t, ch)
	expectMetadataValue(t, mustMetadata(t, msg2), "foo", "baz")
	decoded2 := decodePayload(t, FormatCBOR, mustData(t, msg2))
	expectPayloadText(t, decoded2, "text", "world")
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

func TestCLITargetJSON(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "out.json")

	script := "sh"
	args := []string{"-c", fmt.Sprintf("cat > %q", tmpFile)}

	cfg := &TargetConfig{
		Command:     script,
		Args:        args,
		Timeout:     5 * time.Second, // Increased timeout for race detector
		Format:      "json",
		MetadataKey: "metadata",
		DataKey:     "payload",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errNewTargetFmt, err)
	}
	target := targetAny.(*CLITarget)

	msg := newRunnerMessage(map[string]string{"foo": "bar"}, []byte("hello"))

	if err := target.Consume(msg); err != nil {
		t.Fatalf(errConsumeFmt, err)
	}

	if err := target.Close(); err != nil {
		t.Fatalf(errCloseFmt, err)
	}

	data := readFileWithRetry(t, tmpFile)

	var payload targetPayload
	if err := encdec.DecodeJSON(data, &payload); err != nil {
		t.Fatalf(errDecodeJSONFmt, err)
	}

	if payload.Metadata["foo"] != "bar" {
		t.Fatalf(errUnexpectedMetadataFmt, payload.Metadata)
	}
	if string(payload.Payload) != "hello" {
		t.Fatalf(errUnexpectedData, payload.Payload)
	}
}

func TestCLITargetCBOR(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "out.cbor")

	script := "sh"
	args := []string{"-c", "cat > \"" + tmpFile + "\""}

	cfg := &TargetConfig{
		Command:     script,
		Args:        args,
		Timeout:     5 * time.Second, // Increased timeout for race detector
		Format:      "CBOR",
		MetadataKey: "metadata",
		DataKey:     "payload",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errNewTargetFmt, err)
	}

	msg := newRunnerMessage(map[string]string{"foo": "bar"}, []byte("hello"))

	if err := targetAny.Consume(msg); err != nil {
		t.Fatalf(errConsumeFmt, err)
	}

	if err := targetAny.Close(); err != nil {
		t.Fatalf(errCloseFmt, err)
	}

	data := readFileWithRetry(t, tmpFile)

	var payload targetPayload
	if err := encdec.DecodeCBOR(data, &payload); err != nil {
		t.Fatalf(errDecodeCBORFmt, err)
	}

	if payload.Metadata["foo"] != "bar" {
		t.Fatalf(errUnexpectedMetadataFmt, payload.Metadata)
	}
	if string(payload.Payload) != "hello" {
		t.Fatalf(errUnexpectedData, payload.Payload)
	}
}

func TestCLITargetJSONDataOnly(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "out.json")

	script := "sh"
	args := []string{"-c", fmt.Sprintf("cat > %q", tmpFile)}

	cfg := &TargetConfig{
		Command: script,
		Args:    args,
		Timeout: 5 * time.Second, // Increased timeout for race detector
		Format:  "json",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(errNewTargetFmt, err)
	}
	target := targetAny.(*CLITarget)

	msg := newRunnerMessage(map[string]string{"foo": "bar"}, []byte("hello"))

	if err := target.Consume(msg); err != nil {
		t.Fatalf(errConsumeFmt, err)
	}

	if err := target.Close(); err != nil {
		t.Fatalf(errCloseFmt, err)
	}

	data := readFileWithRetry(t, tmpFile)

	var decoded []byte
	if err := encdec.DecodeJSON(data, &decoded); err != nil {
		t.Fatalf(errDecodeJSONFmt, err)
	}

	if string(decoded) != "hello" {
		t.Fatalf(errUnexpectedData, decoded)
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
		t.Fatalf(errNewSourceFmt, err)
	}
	src := srcAny.(*CLISource)

	ch, err := src.Produce(len(records))
	if err != nil {
		t.Fatalf(errProduceFmt, err)
	}

	return ch, src
}

func writeRecordsFile(t *testing.T, format CLIFormat, records []map[string]any) string {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	for _, record := range records {
		entry := record
		var (
			data []byte
			err  error
		)
		switch format {
		case FormatJSON:
			data, err = encdec.EncodeJSON(&entry)
		case FormatCBOR:
			data, err = encdec.EncodeCBOR(&entry)
		default:
			t.Fatalf("unsupported format: %s", format)
		}
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
		t.Fatalf(errWriteTempFileFmt, err)
	}

	return path
}

func closeSource(t *testing.T, src *CLISource) {
	t.Helper()
	if err := src.Close(); err != nil {
		t.Fatalf(errCloseFmt, err)
	}
}

func mustMetadata(t *testing.T, msg *message.RunnerMessage) message.MessageMetadata {
	t.Helper()
	metadata, err := msg.GetTargetMetadata()
	if err != nil {
		t.Fatalf(errGetTargetMetadataFmt, err)
	}
	return metadata
}

func expectMetadataValue(t *testing.T, metadata message.MessageMetadata, key, expected string) {
	t.Helper()
	if metadata[key] != expected {
		t.Fatalf(errUnexpectedMetadataFmt, metadata)
	}
}

func mustData(t *testing.T, msg *message.RunnerMessage) []byte {
	t.Helper()
	data, err := msg.GetTargetData()
	if err != nil {
		t.Fatalf(errGetTargetDataFmt, err)
	}
	return data
}

func decodePayload(t *testing.T, format CLIFormat, data []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	switch format {
	case FormatJSON:
		if err := encdec.DecodeJSON(data, &decoded); err != nil {
			t.Fatalf(errDecodeJSONFmt, err)
		}
	case FormatCBOR:
		if err := encdec.DecodeCBOR(data, &decoded); err != nil {
			t.Fatalf(errDecodeCBORFmt, err)
		}
	default:
		t.Fatalf("unsupported format: %s", format)
	}
	return decoded
}

func expectPayloadText(t *testing.T, decoded map[string]any, key, expected string) {
	t.Helper()
	value, ok := decoded[key].(string)
	if !ok || value != expected {
		t.Fatalf(errUnexpectedPayloadFmt, decoded[key])
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

func readFileWithRetry(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			return data
		}
		if !os.IsNotExist(err) {
			t.Fatalf(errReadFileFmt, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf(errReadFileFmt, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type targetPayload struct {
	Metadata map[string]string `json:"metadata" cbor:"metadata"`
	Payload  []byte            `json:"payload" cbor:"payload"`
}

type stubSourceMessage struct {
	metadata message.MessageMetadata
	data     []byte
}

func newRunnerMessage(metadata message.MessageMetadata, data []byte) *message.RunnerMessage {
	stub := &stubSourceMessage{
		metadata: metadata,
		data:     data,
	}
	msg := message.NewRunnerMessage(stub)
	if metadata != nil {
		msg.MergeMetadata(metadata)
	}
	if data != nil {
		msg.SetData(data)
	}
	return msg
}

func (s *stubSourceMessage) GetID() []byte {
	return nil
}

func (s *stubSourceMessage) GetMetadata() (message.MessageMetadata, error) {
	if s.metadata == nil {
		return message.MessageMetadata{}, nil
	}
	return s.metadata, nil
}

func (s *stubSourceMessage) GetData() ([]byte, error) {
	return s.data, nil
}

func (s *stubSourceMessage) Ack() error { return nil }

func (s *stubSourceMessage) Nak() error { return nil }

func (s *stubSourceMessage) Reply(*message.ReplyData) error { return nil }

// Test for CLI Runner functions (0% coverage)
func TestCLIRunner(t *testing.T) {
	// Test NewRunnerConfig
	cfg := NewRunnerConfig()
	runnerCfg, ok := cfg.(*RunnerConfig)
	if !ok {
		t.Fatal("NewRunnerConfig should return *RunnerConfig")
	}
	if runnerCfg == nil {
		t.Fatal("NewRunnerConfig should not return nil")
	}

	// Test NewRunner with valid config
	runnerCfg.Command = "echo"
	runnerCfg.Args = []string{"test"}
	runnerCfg.Timeout = 5 * time.Second

	runner, err := NewRunner(runnerCfg)
	if err != nil {
		t.Fatalf("NewRunner error: %v", err)
	}

	// Test Close (skip Process test as it requires specific input format)
	err = runner.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}
}

// Test for config constructors (0% coverage)
func TestConfigConstructors(t *testing.T) {
	// Test NewSourceConfig
	srcCfg := NewSourceConfig()
	_, ok := srcCfg.(*SourceConfig)
	if !ok {
		t.Fatal("NewSourceConfig should return *SourceConfig")
	}

	// Test NewTargetConfig
	tgtCfg := NewTargetConfig()
	_, ok = tgtCfg.(*TargetConfig)
	if !ok {
		t.Fatal("NewTargetConfig should return *TargetConfig")
	}
}

// Test for CLISourceMessage methods (0% coverage)
func TestCLISourceMessageMethods(t *testing.T) {
	metadata := message.MessageMetadata{"test": "value"}
	data := []byte("test data")

	msg := newCLISourceMessage(metadata, data)

	// Test GetID
	id := msg.GetID()
	if id != nil {
		t.Fatal("GetID should return nil")
	}

	// Test GetMetadata
	gotMetadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("GetMetadata error: %v", err)
	}
	if gotMetadata["test"] != "value" {
		t.Fatalf("Expected metadata test=value, got %v", gotMetadata)
	}

	// Test GetData
	gotData, err := msg.GetData()
	if err != nil {
		t.Fatalf("GetData error: %v", err)
	}
	if string(gotData) != "test data" {
		t.Fatalf("Expected data 'test data', got %s", gotData)
	}

	// Test Ack
	err = msg.Ack()
	if err != nil {
		t.Fatalf("Ack error: %v", err)
	}

	// Test Nak
	err = msg.Nak()
	if err != nil {
		t.Fatalf("Nak error: %v", err)
	}

	// Test Reply
	replyData := &message.ReplyData{Data: []byte("reply")}
	err = msg.Reply(replyData)
	if err != nil {
		t.Fatalf("Reply error: %v", err)
	}
}

// Test for additional common.go functions
func TestCommonFunctions(t *testing.T) {
	// Test runnerToBaseConfig (0% coverage)
	runnerCfg := &RunnerConfig{
		Command: "test",
		Args:    []string{"arg1"},
		Timeout: 5 * time.Second,
		Envs:    map[string]string{"TEST": "value"},
	}

	baseCfg := runnerToBaseConfig(runnerCfg)
	if baseCfg.Command != "test" {
		t.Fatalf("Expected command 'test', got %s", baseCfg.Command)
	}
	if len(baseCfg.Args) != 1 || baseCfg.Args[0] != "arg1" {
		t.Fatalf("Expected args [arg1], got %v", baseCfg.Args)
	}
}

// Test error handling for better coverage
func TestErrorHandling(t *testing.T) {
	// Test invalid format
	_, err := parseFormat("invalid")
	if err == nil {
		t.Fatal("parseFormat should return error for invalid format")
	}

	// Test invalid command
	err = validateCommand("", []string{})
	if err == nil {
		t.Fatal("validateCommand should return error for empty command")
	}

	// Test dangerous command
	err = validateCommand("cmd;rm -rf /", []string{})
	if err == nil {
		t.Fatal("validateCommand should return error for dangerous command")
	}

	// Test copyStringStringMap (0% coverage)
	input := map[string]string{"key": "value"}
	result := copyStringStringMap(input)
	if result["key"] != "value" {
		t.Fatalf("Expected key=value, got %v", result)
	}
}

// Test CBOR encoding error paths for better coverage
func TestCBORErrorPaths(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "out.cbor")

	cfg := &TargetConfig{
		Command:     "sh",
		Args:        []string{"-c", fmt.Sprintf("cat > %q", tmpFile)},
		Timeout:     5 * time.Second,
		Format:      "cbor",
		MetadataKey: "metadata",
		DataKey:     "payload",
	}

	target, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf("NewTarget error: %v", err)
	}
	defer target.Close() //nolint:errcheck

	// Test with nil metadata to trigger different code paths
	msg := newRunnerMessage(nil, []byte("test data"))
	err = target.Consume(msg)
	if err != nil {
		t.Fatalf("Consume error: %v", err)
	}
}
