package encdec

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/destel/rill"
	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/security/validation"
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
	// Validate CBOR size
	if err := validation.ValidateCBORSize(len(data)); err != nil {
		return nil, fmt.Errorf("CBOR validation failed: %w", err)
	}

	// Create safe decoder mode with limits
	dm, err := cbor.DecOptions{
		DupMapKey:        cbor.DupMapKeyEnforcedAPF,
		IndefLength:      cbor.IndefLengthForbidden,
		TagsMd:           cbor.TagsForbidden,
		MaxArrayElements: validation.MaxCBORArrayElems,
		MaxMapPairs:      validation.MaxCBORMapPairs,
		MaxNestedLevels:  validation.MaxCBORDepth,
	}.DecMode()
	if err != nil {
		return nil, fmt.Errorf("failed to create CBOR decoder: %w", err)
	}

	var v map[string]any
	if err := dm.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("failed to decode CBOR: %w", err)
	}

	return mapToMessage(e, v, e.metaKey, e.dataKey)
}

func (e *CBORDecoder) DecodeStream(in io.Reader) <-chan rill.Try[message.SourceMessage] {
	// Create safe decoder with limits
	dm, err := cbor.DecOptions{
		DupMapKey:        cbor.DupMapKeyEnforcedAPF,
		IndefLength:      cbor.IndefLengthForbidden,
		TagsMd:           cbor.TagsForbidden,
		MaxArrayElements: validation.MaxCBORArrayElems,
		MaxMapPairs:      validation.MaxCBORMapPairs,
		MaxNestedLevels:  validation.MaxCBORDepth,
	}.DecMode()

	res := make(chan rill.Try[message.SourceMessage], 100) // Buffered channel

	go func() {
		defer close(res)

		// If decoder creation failed, send error and return
		if err != nil {
			res <- rill.Wrap[message.SourceMessage](nil, fmt.Errorf("failed to create CBOR decoder: %w", err))
			return
		}

		dec := dm.NewDecoder(in)
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
