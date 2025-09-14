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
	meta["wasm-processed"] = []string{"true"}
	newData := append([]byte("[WASM] "), data...)

	// Write to stdout
	out := cliformat.Encode(meta, newData)
	if _, err := os.Stdout.Write(out); err != nil {
		log.Fatalf("write error: %v", err)
	}
}
