package main

import (
	"encoding/json"
	"io/fs"
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

	newMeta := make(map[string]string)

	// Example processing: add a key to metadata and modify data
	newMeta["wasm-processed"] = "true"
	newMeta["eb-status"] = "400"

	fileName := os.Getenv("FILE_NAME")
	pfix, err := fs.ReadFile(os.DirFS("/"), fileName)
	if err != nil {
		log.Fatalf("failed to read prefix file: %v", err)
	}

	jsonData, err := json.Marshal(meta)
	if err != nil {
		log.Fatalf("failed to marshal metadata: %v", err)
	}

	newData := append(jsonData, pfix...)
	newData = append(newData, data...)

	// Write to stdout
	out, err := cliformat.Encode(newMeta, newData)
	if err != nil {
		log.Fatalf("encode error: %v", err)
	}

	if _, err := os.Stdout.Write(out); err != nil {
		log.Fatalf("write error: %v", err)
	}
}
