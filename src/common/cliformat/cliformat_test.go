package cliformat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

type staticErrorReader struct {
	err error
}

func (r staticErrorReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

const errUnexpectedFmt = "unexpected error: %v"

func TestEncodeMetadataEmpty(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeMetadata(map[string]string{})
	if err != nil {
		t.Fatalf(errUnexpectedFmt, err)
	}
	if len(encoded) != 0 {
		t.Fatalf("expected empty encoding, got %d bytes", len(encoded))
	}
}

func TestDecodeMetadataEmpty(t *testing.T) {
	t.Parallel()

	decoded, err := DecodeMetadata(nil)
	if err != nil {
		t.Fatalf(errUnexpectedFmt, err)
	}
	if len(decoded) != 0 {
		t.Fatalf("expected empty metadata, got %d entries", len(decoded))
	}
}

func TestDecodeMetadataInvalidFormat(t *testing.T) {
	t.Parallel()

	if _, err := DecodeMetadata([]byte("invalid")); err == nil {
		t.Fatalf("expected error for invalid metadata payload")
	}
}

func TestDecodeFrameTooSmall(t *testing.T) {
	t.Parallel()

	if _, _, err := Decode([]byte("short")); err == nil || !strings.Contains(err.Error(), "frame too small") {
		t.Fatalf("expected frame too small error, got %v", err)
	}
}

func TestDecodeInvalidFrameLength(t *testing.T) {
	t.Parallel()

	frame, err := Encode(map[string]string{"k": "v"}, []byte("data"))
	if err != nil {
		t.Fatalf(errUnexpectedFmt, err)
	}

	truncated := frame[:len(frame)-1]
	if _, _, err := Decode(truncated); err == nil || !strings.Contains(err.Error(), "invalid frame length") {
		t.Fatalf("expected invalid frame length error, got %v", err)
	}
}

func TestDecodeMetadataDecodeError(t *testing.T) {
	t.Parallel()

	meta := map[string]string{"key": "value"}
	payload := []byte("payload")
	frame, err := Encode(meta, payload)
	if err != nil {
		t.Fatalf(errUnexpectedFmt, err)
	}

	headerLen := frameHeaderSize
	metaLen := len(frame) - headerLen - len(payload)
	tampered := append([]byte(nil), frame...)
	replacement := bytes.Repeat([]byte("x"), metaLen)
	copy(tampered[headerLen:headerLen+metaLen], replacement)

	if _, _, err := Decode(tampered); err == nil || !strings.Contains(err.Error(), "failed to decode metadata") {
		t.Fatalf("expected metadata decode error, got %v", err)
	}
}

func TestReadFrameEOF(t *testing.T) {
	t.Parallel()

	_, _, err := readFrame(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReadFrameUnexpectedEOFWithoutBytes(t *testing.T) {
	t.Parallel()

	_, _, err := readFrame(staticErrorReader{err: io.ErrUnexpectedEOF})
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReadFrameTruncatedHeader(t *testing.T) {
	t.Parallel()

	_, _, err := readFrame(bytes.NewReader([]byte{0x01}))
	if err == nil || !strings.Contains(err.Error(), "failed to read frame header") {
		t.Fatalf("expected header read error, got %v", err)
	}
}

func TestReadFrameInvalidMarker(t *testing.T) {
	t.Parallel()

	buf := bytes.NewBuffer(nil)
	buf.Write([]byte{'B', 'A', 'D', '!'})
	header := make([]byte, 8)
	buf.Write(header)

	if _, _, err := readFrame(buf); err == nil || !strings.Contains(err.Error(), "invalid frame marker") {
		t.Fatalf("expected invalid marker error, got %v", err)
	}
}

func TestReadFrameMetadataReadError(t *testing.T) {
	t.Parallel()

	buf := bytes.NewBuffer(nil)
	buf.Write(frameMarker[:])
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], 4)
	binary.BigEndian.PutUint32(header[4:], 0)
	buf.Write(header)
	buf.Write([]byte{0x00, 0x01})

	if _, _, err := readFrame(buf); err == nil || !strings.Contains(err.Error(), "failed to read metadata") {
		t.Fatalf("expected metadata read error, got %v", err)
	}
}

func TestReadFramePayloadReadError(t *testing.T) {
	t.Parallel()

	buf := bytes.NewBuffer(nil)
	buf.Write(frameMarker[:])
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], 0)
	binary.BigEndian.PutUint32(header[4:], 4)
	buf.Write(header)
	if _, err := buf.Write([]byte{}); err != nil {
		t.Fatalf("unexpected error writing buffer: %v", err)
	}

	if _, _, err := readFrame(buf); err == nil || !strings.Contains(err.Error(), "failed to read payload") {
		t.Fatalf("expected payload read error, got %v", err)
	}
}
