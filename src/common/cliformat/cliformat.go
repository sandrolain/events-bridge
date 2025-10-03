package cliformat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Binary separators used for metadata encoding
// We use 0x1F (Unit Separator) between key and value, and 0x1E (Record Separator)
// between pairs. This assumes metadata keys/values do not contain these bytes
// nor newlines, which is generally true for textual headers.
const (
	kvSeparator     byte = 0x1F // key-value separator
	pairSeparator   byte = 0x1E // pair separator
	frameMarkerSize      = 4
	frameHeaderSize      = frameMarkerSize + 8
)

const maxFrameValue = ^uint32(0)

var (
	frameMarker     = [frameMarkerSize]byte{'E', 'B', 'F', '1'}
	maxFrameSegment = int(^uint(0) >> 1)
)

// EncodeMetadata encodes a map of metadata using binary separators.
// Format: key1<US>val1<RS>key2<US>val2...
func EncodeMetadata(metadata map[string]string) ([]byte, error) {
	if len(metadata) == 0 {
		return []byte{}, nil
	}

	// Deterministic ordering for stability
	keys := make([]string, 0, len(metadata))
	for k := range metadata {
		keys = append(keys, k)
	}
	// Local sort without importing extra packages
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	var buf bytes.Buffer
	for idx, k := range keys {
		v := metadata[k]
		buf.WriteString(k)
		buf.WriteByte(kvSeparator)
		buf.WriteString(v)
		if idx < len(keys)-1 {
			buf.WriteByte(pairSeparator)
		}
	}
	return buf.Bytes(), nil
}

// DecodeMetadata decodes metadata encoded with EncodeMetadata.
func DecodeMetadata(b []byte) (map[string]string, error) {
	m := make(map[string]string)
	if len(b) == 0 {
		return m, nil
	}

	parts := bytes.Split(b, []byte{pairSeparator})
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		i := bytes.IndexByte(part, kvSeparator)
		if i < 0 {
			return nil, fmt.Errorf("invalid metadata format: missing kv separator")
		}
		key := string(part[:i])
		val := string(part[i+1:])
		m[key] = val
	}
	return m, nil
}

// Encode serializes metadata and binary data into a self-describing binary frame.
// Frame layout:
//
//	marker(4B) | metadata length (uint32 BE) | data length (uint32 BE) | metadata | data
//
// The marker provides an easily recognizable frame start when decoding streams.
func Encode(metadata map[string]string, data []byte) ([]byte, error) {
	metaBytes, err := EncodeMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to encode metadata: %w", err)
	}
	if len(metaBytes) > maxFrameSegment {
		return nil, fmt.Errorf("metadata too large: %d bytes", len(metaBytes))
	}
	if len(data) > maxFrameSegment {
		return nil, fmt.Errorf("data too large: %d bytes", len(data))
	}

	frameLen := frameHeaderSize + len(metaBytes) + len(data)
	buf := bytes.NewBuffer(make([]byte, 0, frameLen))
	buf.Write(frameMarker[:])

	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], uint32(len(metaBytes))) // #nosec G115 - checked bounds above
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))      // #nosec G115 - checked bounds above
	buf.Write(header)
	buf.Write(metaBytes)
	buf.Write(data)
	return buf.Bytes(), nil
}

// Decode extracts metadata and payload from a CLI frame produced by Encode.
func Decode(input []byte) (map[string]string, []byte, error) {
	if len(input) < frameHeaderSize {
		return nil, nil, fmt.Errorf("frame too small: %d bytes", len(input))
	}
	if !bytes.Equal(input[:frameMarkerSize], frameMarker[:]) {
		return nil, nil, errors.New("invalid frame marker")
	}
	metaLen := binary.BigEndian.Uint32(input[frameMarkerSize : frameMarkerSize+4])
	dataLen := binary.BigEndian.Uint32(input[frameMarkerSize+4 : frameHeaderSize])
	if metaLen > maxFrameValue || dataLen > maxFrameValue {
		return nil, nil, errors.New("frame segment exceeds supported size")
	}
	totalExpected := frameHeaderSize + int(metaLen) + int(dataLen)
	if len(input) != totalExpected {
		return nil, nil, fmt.Errorf("invalid frame length: expected %d bytes got %d", totalExpected, len(input))
	}
	metaStart := frameHeaderSize
	metaEnd := metaStart + int(metaLen)
	metaBytes := input[metaStart:metaEnd]
	dataBytes := input[metaEnd:totalExpected]

	m, err := DecodeMetadata(metaBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	return m, dataBytes, nil
}

// DecodeFromReader reads a single CLI frame from the provided reader.
func DecodeFromReader(r io.Reader) (map[string]string, []byte, error) {
	return readFrame(r)
}

func readFrame(r io.Reader) (map[string]string, []byte, error) {
	header := make([]byte, frameHeaderSize)
	n, err := io.ReadFull(r, header)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return nil, nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) && n == 0 {
			return nil, nil, io.EOF
		}
		return nil, nil, fmt.Errorf("failed to read frame header: %w", err)
	}

	if !bytes.Equal(header[:frameMarkerSize], frameMarker[:]) {
		return nil, nil, errors.New("invalid frame marker")
	}
	metaLen := binary.BigEndian.Uint32(header[frameMarkerSize : frameMarkerSize+4])
	dataLen := binary.BigEndian.Uint32(header[frameMarkerSize+4:])

	if metaLen > maxFrameValue || dataLen > maxFrameValue {
		return nil, nil, errors.New("frame segment exceeds supported size")
	}
	metaBytes := make([]byte, int(metaLen))
	if _, err := io.ReadFull(r, metaBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	dataBytes := make([]byte, int(dataLen))
	if _, err := io.ReadFull(r, dataBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to read payload: %w", err)
	}

	m, err := DecodeMetadata(metaBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	return m, dataBytes, nil
}
