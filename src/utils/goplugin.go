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

// LoadPluginDual tries to resolve a constructor that accepts map[string]any (options-first),
// falling back to the typed constructor if not found. The 'method' parameter is the base
// symbol name (e.g., "NewSource" or "NewTarget"). For options-based constructors, the
// symbol is expected to be method+"Options" (e.g., "NewSourceOptions").
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

	sym, err := p.Lookup(method + "Options")
	if err != nil {
		var zero R
		return zero, fmt.Errorf("failed to lookup options constructor: %w", err)
	}
	constructor, ok := sym.(func(map[string]any) (R, error))
	if !ok {
		var zero R
		return zero, fmt.Errorf("invalid options constructor signature in plugin")
	}
	return constructor(options)
}
