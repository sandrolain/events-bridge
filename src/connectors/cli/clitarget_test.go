package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common/encdec"
	"github.com/sandrolain/events-bridge/src/message"
)

const (
	targetErrCloseFmt              = "Close error: %v"
	targetErrConsumeFmt            = "Consume error: %v"
	targetErrDecodeCBORFmt         = "DecodeCBOR error: %v"
	targetErrDecodeJSONFmt         = "DecodeJSON error: %v"
	targetErrGetDataFmt            = "GetData error: %v"
	targetErrGetMetadataFmt        = "GetMetadata error: %v"
	targetErrNewTargetFmt          = "NewTarget error: %v"
	targetErrReadFileFmt           = "ReadFile error: %v"
	targetErrUnexpectedData        = "unexpected data: %q"
	targetErrUnexpectedMetadataFmt = "unexpected metadata: %v"
)

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
		DataKey:     "data",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(targetErrNewTargetFmt, err)
	}
	target := targetAny.(*CLITarget)

	msg := newRunnerMessage(map[string]string{"foo": "bar"}, []byte("hello"))

	if err := target.Consume(msg); err != nil {
		t.Fatalf(targetErrConsumeFmt, err)
	}

	if err := target.Close(); err != nil {
		t.Fatalf(targetErrCloseFmt, err)
	}

	data := readFileWithRetry(t, tmpFile)

	decoder, err := encdec.NewMessageDecoder("json", "metadata", "data")
	if err != nil {
		t.Fatalf(targetErrDecodeJSONFmt, err)
	}

	dmsg, err := decoder.DecodeMessage(data)

	metadata, err := dmsg.GetMetadata()
	if err != nil {
		t.Fatalf(targetErrGetMetadataFmt, err)
	}
	payload, err := dmsg.GetData()
	if err != nil {
		t.Fatalf(targetErrGetDataFmt, err)
	}

	if metadata["foo"] != "bar" {
		t.Fatalf(targetErrUnexpectedMetadataFmt, metadata)
	}
	if string(payload) != "hello" {
		t.Fatalf(targetErrUnexpectedData, payload)
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
		DataKey:     "data",
	}

	targetAny, err := NewTarget(cfg)
	if err != nil {
		t.Fatalf(targetErrNewTargetFmt, err)
	}

	msg := newRunnerMessage(map[string]string{"foo": "bar"}, []byte("hello"))

	if err := targetAny.Consume(msg); err != nil {
		t.Fatalf(targetErrConsumeFmt, err)
	}

	if err := targetAny.Close(); err != nil {
		t.Fatalf(targetErrCloseFmt, err)
	}

	data := readFileWithRetry(t, tmpFile)

	decoder, err := encdec.NewMessageDecoder("cbor", "metadata", "data")
	if err != nil {
		t.Fatalf(targetErrDecodeCBORFmt, err)
	}

	dmsg, err := decoder.DecodeMessage(data)
	if err != nil {
		t.Fatalf(targetErrDecodeCBORFmt, err)
	}

	metadata, err := dmsg.GetMetadata()
	if err != nil {
		t.Fatalf(targetErrGetMetadataFmt, err)
	}
	payload, err := dmsg.GetData()
	if err != nil {
		t.Fatalf(targetErrGetDataFmt, err)
	}

	if metadata["foo"] != "bar" {
		t.Fatalf(targetErrUnexpectedMetadataFmt, metadata)
	}
	if string(payload) != "hello" {
		t.Fatalf(targetErrUnexpectedData, payload)
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
		t.Fatalf(targetErrNewTargetFmt, err)
	}
	target := targetAny.(*CLITarget)

	msg := newRunnerMessage(map[string]string{"foo": "bar"}, []byte("hello"))

	if err := target.Consume(msg); err != nil {
		t.Fatalf(targetErrConsumeFmt, err)
	}

	if err := target.Close(); err != nil {
		t.Fatalf(targetErrCloseFmt, err)
	}
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
			t.Fatalf(targetErrReadFileFmt, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf(targetErrReadFileFmt, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type targetPayload struct {
	Metadata map[string]string `json:"metadata" cbor:"metadata"`
	Payload  []byte            `json:"data" cbor:"data"`
}

type targetStubSourceMessage struct {
	metadata message.MessageMetadata
	data     []byte
}

func newRunnerMessage(metadata message.MessageMetadata, data []byte) *message.RunnerMessage {
	stub := &targetStubSourceMessage{
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

func (s *targetStubSourceMessage) GetID() []byte {
	return nil
}

func (s *targetStubSourceMessage) GetMetadata() (message.MessageMetadata, error) {
	if s.metadata == nil {
		return message.MessageMetadata{}, nil
	}
	return s.metadata, nil
}

func (s *targetStubSourceMessage) GetData() ([]byte, error) {
	return s.data, nil
}

func (s *targetStubSourceMessage) Ack() error { return nil }

func (s *targetStubSourceMessage) Nak() error { return nil }

func (s *targetStubSourceMessage) Reply(*message.ReplyData) error { return nil }
