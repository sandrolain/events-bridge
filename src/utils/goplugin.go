package utils

import (
	"fmt"
	"os"
	"path/filepath"
	goplugin "plugin"
)

func LoadPlugin[T any, R any](relPath string, cfg T) (R, error) {
	exePath, err := os.Executable()
	if err != nil {
		var zero R
		return zero, fmt.Errorf("failed to get executable path: %w", err)
	}
	exeDir := filepath.Dir(exePath)
	absPath := relPath
	if !os.IsPathSeparator(relPath[0]) {
		absPath = fmt.Sprintf("%s/%s", exeDir, relPath)
	}

	p, err := goplugin.Open(absPath)
	if err != nil {
		var zero R
		return zero, fmt.Errorf("failed to open plugin: %w", err)
	}

	sym, err := p.Lookup("New")
	if err != nil {
		var zero R
		return zero, fmt.Errorf("failed to lookup New: %w", err)
	}

	constructor, ok := sym.(func(T) (R, error))
	if !ok {
		var zero R
		return zero, fmt.Errorf("invalid constructor signature in plugin")
	}

	return constructor(cfg)
}
