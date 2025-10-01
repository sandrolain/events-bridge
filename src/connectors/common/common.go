package main

import (
	"fmt"

	"github.com/sandrolain/events-bridge/src/connectors/common/encdec"
)

func NewMessageDecoder(encType string) (encdec.MessageDecoder, error) {
	switch encType {
	case "json":
		return &encdec.JSONDecoder{}, nil
	case "cbor":
		return &encdec.CBORDecoder{}, nil
	case "cli":
		return &encdec.CLIDecoder{}, nil
	}
	return nil, fmt.Errorf("unknown encoder type: %s", encType)
}
