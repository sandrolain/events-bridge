//go:build ignore

package main

import (
	"log"
	"os"
	"time"

	"github.com/sandrolain/events-bridge/src/common/cliformat"
)

func main() {
	// Read from stdin
	_, _, err := cliformat.DecodeFromReader(os.Stdin)
	if err != nil {
		log.Fatalf("decode error: %v", err)
	}

	// Sleep longer than expected timeout (5+ seconds)
	time.Sleep(10 * time.Second)

	// This should never be reached in tests
	newMeta := make(map[string]string)
	newMeta["completed"] = "true"

	out, err := cliformat.Encode(newMeta, []byte("timeout test"))
	if err != nil {
		log.Fatalf("encode error: %v", err)
	}

	if _, err := os.Stdout.Write(out); err != nil {
		log.Fatalf("write error: %v", err)
	}
}
