package utils

import (
	"fmt"
	"os"
	"path/filepath"
	goplugin "plugin"
)

func LoadPlugin[T any, R any](relPath string, method string, cfg T) (R, error) {
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

	sym, err := p.Lookup(method)
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

func LoadPluginDual[T any, R any](relPath string, method string, typedCfg T, options map[string]any) (R, error) {
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

	// 1) Try options-based constructor: method+"Options" (only if options provided)
	if options != nil && len(options) > 0 {
		if sym, err := p.Lookup(method + "Options"); err == nil {
			if constructor, ok := sym.(func(map[string]any) (R, error)); ok {
				return constructor(options)
			}
		}
	}

	// 2) Fallback to the typed constructor
	sym, err := p.Lookup(method)
	if err != nil {
		var zero R
		return zero, fmt.Errorf("failed to lookup constructor: %w", err)
	}
	if constructor, ok := sym.(func(T) (R, error)); ok {
		return constructor(typedCfg)
	}

	var zero R
	return zero, fmt.Errorf("invalid constructor signature in plugin")
}

// LoadPluginOptions loads a plugin and calls the options-based constructor only.
// Looks up symbol method+"Options" and calls it with the provided options.
func LoadPluginOptions[R any](relPath string, method string, options map[string]any) (R, error) {
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

	// Prefer exact method name taking options: func(map[string]any) (R, error)
	if sym, err := p.Lookup(method); err == nil {
		if constructor, ok := sym.(func(map[string]any) (R, error)); ok {
			return constructor(options)
		}
	}
	// Fallback to historical method+"Options"
	if sym, err := p.Lookup(method + "Options"); err == nil {
		if constructor, ok := sym.(func(map[string]any) (R, error)); ok {
			return constructor(options)
		}
	}
	var zero R
	return zero, fmt.Errorf("failed to find options-based constructor for %s", method)
}
