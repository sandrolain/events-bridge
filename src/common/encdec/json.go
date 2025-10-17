package encdec

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bytedance/sonic"
	"github.com/destel/rill"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/security/validation"
)

var _ MessageDecoder = (*JSONDecoder)(nil)

type JSONDecoder struct {
	dataKey string
	metaKey string
}

func (e *JSONDecoder) Encode(d any) ([]byte, error) {
	return sonic.Marshal(d)
}

func (e *JSONDecoder) EncodeMessage(msg message.SourceMessage) ([]byte, error) {
	v, err := messageToMap(msg, e.metaKey, e.dataKey)
	if err != nil {
		return nil, err
	}
	return sonic.Marshal(v)
}

func (e *JSONDecoder) DecodeMessage(in []byte) (message.SourceMessage, error) {
	// Validate JSON size
	if err := validation.ValidateJSONSize(len(in)); err != nil {
		return nil, fmt.Errorf("JSON validation failed: %w", err)
	}

	var v map[string]any
	if err := sonic.Unmarshal(in, &v); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
	}

	// Validate JSON structure (depth and array lengths)
	if err := validation.ValidateJSONStructure(v, 0); err != nil {
		return nil, fmt.Errorf("JSON structure validation failed: %w", err)
	}

	return mapToMessage(e, v, e.metaKey, e.dataKey)
}

func (e *JSONDecoder) DecodeStream(in io.Reader) <-chan rill.Try[message.SourceMessage] {
	dec := sonic.ConfigDefault.NewDecoder(in)
	res := make(chan rill.Try[message.SourceMessage], 100) // Buffered channel

	go func() {
		defer close(res)
		messageCount := 0
		const maxMessagesPerStream = 100000 // Limit messages per stream

		for {
			// Check message count limit
			if messageCount >= maxMessagesPerStream {
				res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("stream message limit exceeded: %d", maxMessagesPerStream))
				return
			}

			var v map[string]any
			err := dec.Decode(&v)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
					return
				}
				res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("error decoding stream: %w", err))
				return
			}

			// Validate JSON structure
			if err := validation.ValidateJSONStructure(v, 0); err != nil {
				res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("JSON structure validation failed: %w", err))
				return
			}

			msg, err := mapToMessage(e, v, e.metaKey, e.dataKey)
			if err != nil {
				res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("error mapping to message: %w", err))
				return
			}
			res <- rill.Wrap(msg, nil)
			messageCount++
		}
	}()

	return res
}
