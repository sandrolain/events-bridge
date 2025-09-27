package encdec

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bytedance/sonic"
	"github.com/fxamacker/cbor/v2"
)

func DecodeCBORStream[T any](in io.Reader) (<-chan T, error) {
	res := make(chan T)
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
				fmt.Fprintf(os.Stderr, "error decoding stream: %v\n", err)
				return
			}
			res <- e
		}
	}()

	return res, nil
}

func DecodeJSONStream[T any](in io.Reader) (<-chan T, error) {
	res := make(chan T)
	var dec = sonic.ConfigDefault.NewDecoder(in)

	go func() {
		defer close(res)
		for {
			var e T
			err := dec.Decode(&e)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
					return
				}
				fmt.Fprintf(os.Stderr, "error decoding stream: %v\n", err)
				return
			}
			res <- e
		}
	}()

	return res, nil
}
