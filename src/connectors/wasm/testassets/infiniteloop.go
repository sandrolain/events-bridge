//go:build ignore

package main

import (
	"log"
	"os"

	"github.com/sandrolain/events-bridge/src/common/cliformat"
)

func main() {
	// Read from stdin
	_, _, err := cliformat.DecodeFromReader(os.Stdin)
	if err != nil {
		log.Fatalf("decode error: %v", err)
	}

	// Infinite loop to trigger timeout
	for {
		// This will cause the WASM runtime to timeout
	}
}
