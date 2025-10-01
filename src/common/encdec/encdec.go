package encdec

import (
	"fmt"
	"io"
	"strings"

	"github.com/destel/rill"
	"github.com/sandrolain/events-bridge/src/message"
)

type MessageDecoder interface {
	Encode(any) ([]byte, error)
	EncodeMessage(message.SourceMessage) ([]byte, error)
	DecodeMessage(data []byte) (message.SourceMessage, error)
	DecodeStream(in io.Reader) <-chan rill.Try[message.SourceMessage]
}

func NewMessageDecoder(encType string, metaKey string, dataKey string) (MessageDecoder, error) {
	encType = strings.ToLower(encType)
	switch encType {
	case "json":
		return &JSONDecoder{
			metaKey: metaKey,
			dataKey: dataKey,
		}, nil
	case "cbor":
		return &CBORDecoder{
			metaKey: metaKey,
			dataKey: dataKey,
		}, nil
	case "cli", "cliformat":
		return &CLIDecoder{}, nil
	}
	return nil, fmt.Errorf("unknown encoder type: %s", encType)
}
