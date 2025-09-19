package cliformat

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/sandrolain/events-bridge/src/message"
)

// Binary separators used for metadata encoding
// We use 0x1F (Unit Separator) between key and value, and 0x1E (Record Separator)
// between pairs. This assumes metadata keys/values do not contain these bytes
// nor newlines, which is generally true for textual headers.
const (
	kvSeparator   byte = 0x1F // key-value separator
	pairSeparator byte = 0x1E // pair separator
)

// EncodeMetadata encodes a map of metadata using binary separators.
// Format: key1<US>val1<RS>key2<US>val2...
func EncodeMetadata(metadata message.MessageMetadata) ([]byte, error) {
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
func DecodeMetadata(b []byte) (message.MessageMetadata, error) {
	m := make(message.MessageMetadata)
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

// Encode serializes metadata and binary data in the required format:
// encoded-metadata + "\n" + binary data
func Encode(metadata message.MessageMetadata, data []byte) ([]byte, error) {
	q, err := EncodeMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to encode metadata: %w", err)
	}
	var buf bytes.Buffer
	buf.Write(q)
	buf.WriteByte('\n')
	buf.Write(data)
	return buf.Bytes(), nil
}

// Decode extracts metadata and binary data from the format:
// encoded-metadata + "\n" + binary data
func Decode(input []byte) (message.MessageMetadata, []byte, error) {
	i := bytes.IndexByte(input, '\n')
	if i < 0 {
		return nil, nil, errors.New("invalid format: missing newline separator")
	}
	meta := input[:i]
	data := input[i+1:]
	m, err := DecodeMetadata(meta)
	if err != nil {
		return nil, nil, err
	}
	return m, data, nil
}

// DecodeFromReader allows decoding from a stream (e.g., os.Stdin)
func DecodeFromReader(r io.Reader) (message.MessageMetadata, []byte, error) {
	var metaBuf bytes.Buffer
	for {
		b := make([]byte, 1)
		n, err := r.Read(b)
		if n == 1 {
			if b[0] == '\n' {
				break
			}
			metaBuf.WriteByte(b[0])
		} else if err != nil {
			return nil, nil, err
		}
	}
	metaStr := metaBuf.Bytes()
	m, err := DecodeMetadata(metaStr)
	if err != nil {
		return nil, nil, err
	}

	// Read the rest as binary data
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}

	return m, data, nil
}
