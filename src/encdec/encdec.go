package encdec

import (
	"github.com/bytedance/sonic"
	"github.com/fxamacker/cbor/v2"
)

func DecodeJSON[T any](data []byte, v *T) error {
	return sonic.Unmarshal(data, v)
}

func EncodeJSON[T any](v *T) ([]byte, error) {
	return sonic.Marshal(v)
}

func DecodeCBOR[T any](data []byte, v *T) error {
	return cbor.Unmarshal(data, v)
}

func EncodeCBOR[T any](v *T) ([]byte, error) {
	return cbor.Marshal(v)
}
