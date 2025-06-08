package main

import (
	"log"
	"os"

	"github.com/sandrolain/events-bridge/src/cliformat"
)

func main() {
	// Leggi da stdin
	meta, data, err := cliformat.DecodeFromReader(os.Stdin)
	if err != nil {
		log.Fatalf("decode error: %v", err)
	}

	// Esempio di elaborazione: aggiungi una chiave ai metadati e modifica i dati
	meta["wasm-processed"] = []string{"true"}
	newData := append([]byte("[WASM] "), data...)

	// Scrivi su stdout
	out := cliformat.Encode(meta, newData)
	os.Stdout.Write(out)
}
