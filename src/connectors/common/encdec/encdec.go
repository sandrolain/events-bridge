package encdec

import (
	"io"

	"github.com/destel/rill"
	"github.com/sandrolain/events-bridge/src/message"
)

type MessageDecoder interface {
	Encode(message.SourceMessage) ([]byte, error)
	Decode(data []byte) (message.SourceMessage, error)
	DecodeStream(in io.Reader) <-chan rill.Try[message.SourceMessage]
}
