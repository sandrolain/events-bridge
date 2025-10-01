package encdec

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/destel/rill"
	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ MessageDecoder = (*CBORDecoder)(nil)

type CBORDecoder struct {
	dataKey string
	metaKey string
}

func (e *CBORDecoder) Encode(d any) ([]byte, error) {
	return cbor.Marshal(d)
}

func (e *CBORDecoder) EncodeMessage(m message.SourceMessage) ([]byte, error) {
	v, err := messageToMap(m, e.metaKey, e.dataKey)
	if err != nil {
		return nil, err
	}
	return cbor.Marshal(v)
}

func (e *CBORDecoder) DecodeMessage(data []byte) (message.SourceMessage, error) {
	var v map[string]any
	if err := cbor.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return mapToMessage(e, v, e.metaKey, e.dataKey)
}

func (e *CBORDecoder) DecodeStream(in io.Reader) <-chan rill.Try[message.SourceMessage] {
	dec := cbor.NewDecoder(in)
	res := make(chan rill.Try[message.SourceMessage])

	go func() {
		defer close(res)
		for {
			var v map[string]any
			err := dec.Decode(&v)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
					return
				}
				res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("error decoding stream: %w", err))
				return
			}
			msg, err := mapToMessage(e, v, e.metaKey, e.dataKey)
			if err != nil {
				res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("error mapping to message: %w", err))
				return
			}
			res <- rill.Wrap(msg, nil)
		}
	}()

	return res
}
