package encdec

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/destel/rill"
	"github.com/fxamacker/cbor/v2"
)

func DecodeCBOR[T any](data []byte, v *T) error {
	return cbor.Unmarshal(data, v)
}

func EncodeCBOR[T any](v *T) ([]byte, error) {
	return cbor.Marshal(v)
}

func DecodeCBORStream[T any](in io.Reader) <-chan rill.Try[T] {
	res := make(chan rill.Try[T])
	dec := cbor.NewDecoder(in)

	go func() {
		defer close(res)
		for {
			var e T
			err := dec.Decode(&e)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
					return
				}
				res <- rill.Wrap[T](e, fmt.Errorf("error decoding stream: %w", err))
				return
			}
			res <- rill.Wrap(e, nil)
		}
	}()

	return res
}
