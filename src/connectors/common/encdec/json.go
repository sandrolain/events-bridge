package encdec

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bytedance/sonic"
	"github.com/destel/rill"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ MessageDecoder = (*JSONDecoder)(nil)

type JSONDecoder struct {
	dataKey string
	metaKey string
}

func (e *JSONDecoder) Encode(msg message.SourceMessage) ([]byte, error) {
	v, err := messageToMap(msg, e.metaKey, e.dataKey)
	if err != nil {
		return nil, err
	}
	return sonic.Marshal(v)
}

func (e *JSONDecoder) Decode(in []byte) (message.SourceMessage, error) {
	var v map[string]any
	if err := sonic.Unmarshal(in, &v); err != nil {
		return nil, err
	}
	return mapToMessage(v, e.metaKey, e.dataKey)
}

func (e *JSONDecoder) DecodeStream(in io.Reader) <-chan rill.Try[message.SourceMessage] {
	dec := sonic.ConfigDefault.NewDecoder(in)
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
			msg, err := mapToMessage(v, e.metaKey, e.dataKey)
			if err != nil {
				res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("error mapping to message: %w", err))
				return
			}
			res <- rill.Wrap(msg, nil)
		}
	}()

	return res
}
