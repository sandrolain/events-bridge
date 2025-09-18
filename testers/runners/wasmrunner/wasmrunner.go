package main

import (
	"log"
	"os"

	"github.com/sandrolain/events-bridge/src/cliformat"
)

func main() {
	// Read from stdin
	meta, data, err := cliformat.DecodeFromReader(os.Stdin)
	if err != nil {
		log.Fatalf("decode error: %v", err)
	}

	// Example processing: add a key to metadata and modify data
	meta["wasm-processed"] = "true"
	newData := append([]byte("[WASM] "), data...)

	// Write to stdout
	out, err := cliformat.Encode(meta, newData)
	if err != nil {
		log.Fatalf("encode error: %v", err)
	}

	if _, err := os.Stdout.Write(out); err != nil {
		log.Fatalf("write error: %v", err)
	}
}
