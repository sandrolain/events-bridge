package encdec

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/destel/rill"
	"github.com/sandrolain/events-bridge/src/common/cliformat"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ MessageDecoder = (*CLIDecoder)(nil)

type CLIDecoder struct{}

func (e *CLIDecoder) Encode(d any) ([]byte, error) {
	return nil, fmt.Errorf("CLI encoder does not support generic encoding")
}

func (e *CLIDecoder) EncodeMessage(msg message.SourceMessage) ([]byte, error) {
	m, err := msg.GetMetadata()
	if err != nil {
		return nil, err
	}
	d, err := msg.GetData()
	if err != nil {
		return nil, err
	}
	return cliformat.Encode(m, d)
}

func (e *CLIDecoder) DecodeMessage(data []byte) (message.SourceMessage, error) {
	m, d, err := cliformat.Decode(data)
	if err != nil {
		return nil, err
	}
	return NewEncDecMessage(m, d), nil
}

func (e *CLIDecoder) DecodeStream(in io.Reader) <-chan rill.Try[message.SourceMessage] {
	dec := cliformat.NewDecoder(in)
	res := make(chan rill.Try[message.SourceMessage])

	go func() {
		defer close(res)
		for {
			f, err := dec.Decode()
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
					return
				}
				res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("error decoding stream: %w", err))
				return
			}
			var msg message.SourceMessage = NewEncDecMessage(f.Metadata, f.Data)
			res <- rill.Wrap(msg, nil)
		}
	}()

	return res
}
