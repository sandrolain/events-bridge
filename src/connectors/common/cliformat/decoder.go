package cliformat

import (
	"bufio"
	"io"

	"github.com/sandrolain/events-bridge/src/message"
)

// Frame represents a single CLI-formatted event composed of metadata and payload.
type Frame struct {
	Metadata message.MessageMetadata
	Data     []byte
}

// Decoder incrementally reads CLI-formatted frames from an input stream.
type Decoder struct {
	reader *bufio.Reader
}

// NewDecoder creates a new Decoder that consumes data from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{reader: bufio.NewReader(r)}
}

// Decode reads the next frame from the stream.
// It returns io.EOF when the stream is exhausted.
func (d *Decoder) Decode() (*Frame, error) {
	metadata, data, err := readFrame(d.reader)
	if err != nil {
		return nil, err
	}

	return &Frame{
		Metadata: metadata,
		Data:     data,
	}, nil
}
