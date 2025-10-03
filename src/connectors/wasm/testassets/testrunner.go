//go:build ignore

package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/sandrolain/events-bridge/src/common/cliformat"
)

func main() {
	// Read from stdin
	meta, data, err := cliformat.DecodeFromReader(os.Stdin)
	if err != nil {
		log.Fatalf("decode error: %v", err)
	}

	newMeta := make(map[string]string)

	// Add metadata to indicate processing
	newMeta["wasm-processed"] = "true"
	newMeta["original-meta-count"] = string(rune(len(meta)))

	// Check for environment variables
	if envValue := os.Getenv("TEST_ENV"); envValue != "" {
		newMeta["test-env-value"] = envValue
	}

	// Check for command line args
	if len(os.Args) > 1 {
		argsJson, _ := json.Marshal(os.Args[1:])
		newMeta["args"] = string(argsJson)
	}

	// Modify data: prepend metadata as JSON and append data
	metaJson, err := json.Marshal(meta)
	if err != nil {
		log.Fatalf("failed to marshal metadata: %v", err)
	}

	newData := append([]byte("processed:"), metaJson...)
	newData = append(newData, ':')
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
