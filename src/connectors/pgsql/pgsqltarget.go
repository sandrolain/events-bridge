package main

import (
	"fmt"

	"github.com/sandrolain/events-bridge/src/connectors"
)

// Placeholder to satisfy plugin loader; real implementation not yet provided.
// Keeping NewTarget to avoid plugin load errors if referenced.
func NewTarget(opts map[string]any) (connectors.Target, error) {
	return nil, fmt.Errorf("PostgreSQL target not implemented")
}
