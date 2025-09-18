package cliformat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/sandrolain/events-bridge/src/message"
)

// Encode serializes metadata and binary data in the required format:
// querystring(metadata) + "\n" + binary data
func Encode(metadata message.MessageMetadata, data []byte) ([]byte, error) {
	q, err := json.Marshal(metadata) // to escape special characters
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
// querystring(metadata) + "\n" + binary data
func Decode(input []byte) (message.MessageMetadata, []byte, error) {
	i := bytes.IndexByte(input, '\n')
	if i < 0 {
		return nil, nil, errors.New("invalid format: missing newline separator")
	}
	meta := input[:i]
	data := input[i+1:]
	var m message.MessageMetadata
	err := json.Unmarshal(meta, &m) // to escape special characters
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

	var m message.MessageMetadata
	err := json.Unmarshal(metaStr, &m)
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
