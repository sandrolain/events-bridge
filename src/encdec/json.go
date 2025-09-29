package encdec

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bytedance/sonic"
	"github.com/destel/rill"
)

func DecodeJSON[T any](data []byte, v *T) error {
	return sonic.Unmarshal(data, v)
}

func EncodeJSON[T any](v *T) ([]byte, error) {
	return sonic.Marshal(v)
}

func DecodeJSONStream[T any](in io.Reader) <-chan rill.Try[T] {
	res := make(chan rill.Try[T])
	dec := sonic.ConfigDefault.NewDecoder(in)

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
