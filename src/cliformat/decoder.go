package cliformat

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

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

// Next reads the next frame from the stream.
// It returns io.EOF when the stream is exhausted.
func (d *Decoder) Next() (*Frame, error) {
	metadata, data, err := readFrame(d.reader)
	if err != nil {
		return nil, err
	}

	return &Frame{
		Metadata: metadata,
		Data:     data,
	}, nil
}

// DecodeStream consumes CLI-formatted frames from the provided reader and exposes them through a channel.
// The returned channel is closed when the stream ends or an unrecoverable error occurs.
func DecodeStream(r io.Reader) (<-chan Frame, error) {
	out := make(chan Frame)
	decoder := NewDecoder(r)

	go func() {
		defer close(out)
		for {
			frame, err := decoder.Next()
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
					return
				}
				fmt.Fprintf(os.Stderr, "error decoding CLI stream: %v\n", err)
				return
			}
			out <- *frame
		}
	}()

	return out, nil
}
