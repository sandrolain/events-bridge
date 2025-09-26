package utils

import (
	"fmt"
	"os"
	"path/filepath"
	goplugin "plugin"
)

func LoadPlugin[A any, R any](relPath string, method string, options A) (R, error) {
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

	if sym, err := p.Lookup(method); err == nil {
		if constructor, ok := sym.(func(A) (R, error)); ok {
			return constructor(options)
		}
	}
	var zero R
	return zero, fmt.Errorf("failed to find options-based constructor for %s", method)
}
