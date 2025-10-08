package main

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
)

const errWriteNotPermitted = "write operation not permitted in read-only mode"

// SafeFS wraps a filesystem with security restrictions:
// - Read-only mode
// - Whitelist of allowed paths
// - Path traversal prevention
type SafeFS struct {
	base         fs.FS
	allowedPaths []string
	readOnly     bool
}

// NewSafeFS creates a new SafeFS with the given base filesystem and restrictions
func NewSafeFS(base fs.FS, allowedPaths []string, readOnly bool) *SafeFS {
	// Normalize allowed paths
	normalized := make([]string, len(allowedPaths))
	for i, p := range allowedPaths {
		normalized[i] = filepath.Clean(p)
	}
	return &SafeFS{
		base:         base,
		allowedPaths: normalized,
		readOnly:     readOnly,
	}
}

// Open implements fs.FS
func (s *SafeFS) Open(name string) (fs.File, error) {
	// Clean the path to prevent traversal
	cleanName := filepath.Clean(name)

	// Check for path traversal attempts
	if strings.Contains(cleanName, "..") {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}

	// If whitelist is specified, check if path is allowed
	if len(s.allowedPaths) > 0 {
		allowed := false
		for _, allowedPath := range s.allowedPaths {
			// Check if the requested path is within an allowed path
			if cleanName == allowedPath || strings.HasPrefix(cleanName, allowedPath+string(filepath.Separator)) {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
		}
	}

	// Open the file from the base filesystem
	file, err := s.base.Open(cleanName)
	if err != nil {
		return nil, err
	}

	// If read-only mode, wrap with read-only file
	if s.readOnly {
		return &readOnlyFile{file}, nil
	}

	return file, nil
}

// readOnlyFile wraps a file to make it read-only
type readOnlyFile struct {
	fs.File
}

// Write is disabled for read-only files
func (r *readOnlyFile) Write(p []byte) (n int, err error) {
	return 0, errors.New(errWriteNotPermitted)
}

// WriteAt is disabled for read-only files
func (r *readOnlyFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, errors.New(errWriteNotPermitted)
}

// ReadFrom prevents potential write operations
func (r *readOnlyFile) ReadFrom(reader io.Reader) (n int64, err error) {
	return 0, errors.New(errWriteNotPermitted)
}
