// Package main provides the WASM connector with secure filesystem access.
//
// This file implements SafeFS, a security-hardened filesystem wrapper that provides:
//   - Path traversal attack prevention
//   - Whitelist-based access control
//   - Read-only mode enforcement
//
// SafeFS is designed to sandbox WASM modules by restricting their filesystem
// access to specific paths and preventing write operations when configured.
//
// Security considerations:
//   - All paths are normalized using filepath.Clean before access
//   - Path traversal attempts (e.g., "../../../etc/passwd") are blocked
//   - Optional whitelist restricts access to specific directories
//   - Optional read-only mode prevents all write operations
//   - Defense-in-depth: multiple layers of security checks
//
// Example usage:
//
//	// Secure filesystem limited to public directories, read-only
//	baseFS := os.DirFS("/data")
//	safeFS := NewSafeFS(baseFS, []string{"public", "uploads"}, true)
//
//	// This will succeed
//	file, err := safeFS.Open("public/data.json")
//
//	// These will be blocked
//	file, err = safeFS.Open("../etc/passwd")     // Path traversal blocked
//	file, err = safeFS.Open("private/secret.db") // Not in whitelist
package main

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
)

// errWriteNotPermitted is the error message returned when write operations
// are attempted on read-only files.
const errWriteNotPermitted = "write operation not permitted in read-only mode"

// SafeFS is a secure filesystem wrapper that provides:
//   - Read-only mode enforcement
//   - Path traversal attack prevention
//   - Whitelist-based access control
//
// It wraps an existing fs.FS and applies security restrictions before
// delegating operations to the underlying filesystem.
//
// Security features:
//   - All paths are cleaned to prevent ".." traversal
//   - Optional whitelist restricts access to specific paths
//   - Optional read-only mode prevents write operations
//
// Example usage:
//
//	baseFS := os.DirFS("/data")
//	safeFS := NewSafeFS(baseFS, []string{"public", "uploads"}, true)
//	file, err := safeFS.Open("public/file.txt") // OK
//	file, err := safeFS.Open("../etc/passwd")   // Blocked
type SafeFS struct {
	base         fs.FS    // Underlying filesystem
	allowedPaths []string // Whitelist of allowed paths (normalized)
	readOnly     bool     // Read-only mode flag
}

// NewSafeFS creates a new SafeFS with the specified security restrictions.
//
// Parameters:
//   - base: The underlying filesystem to wrap
//   - allowedPaths: Whitelist of permitted paths. If empty/nil, all paths are allowed.
//     Paths are relative to the base filesystem and will be normalized.
//   - readOnly: If true, all write operations are blocked
//
// The allowedPaths are automatically cleaned using filepath.Clean to ensure
// consistent path matching.
//
// Example:
//
//	// Allow only specific directories, read-only
//	safeFS := NewSafeFS(os.DirFS("/data"), []string{"public", "uploads"}, true)
//
//	// Allow all paths, read-only
//	safeFS := NewSafeFS(os.DirFS("/data"), nil, true)
//
//	// Allow all paths, read-write
//	safeFS := NewSafeFS(os.DirFS("/data"), nil, false)
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

// Open opens the named file, applying security checks before accessing
// the underlying filesystem.
//
// Security checks performed:
//  1. Path normalization using filepath.Clean to prevent ".." traversal
//  2. Whitelist validation if allowedPaths is configured
//  3. Read-only wrapping if readOnly mode is enabled
//
// Parameters:
//   - name: The path to open, relative to the base filesystem
//
// Returns:
//   - fs.File: The opened file (potentially wrapped in read-only mode)
//   - error: ErrNotExist if path is blocked by whitelist, or underlying FS errors
//
// Path matching rules:
//   - Exact match: "public" matches "public"
//   - Prefix match: "public" allows "public/file.txt"
//   - Normalized: "../public" is cleaned to "public"
//
// Example:
//
//	safeFS := NewSafeFS(os.DirFS("/data"), []string{"public"}, true)
//	f, err := safeFS.Open("public/file.txt")    // OK
//	f, err := safeFS.Open("../etc/passwd")      // Blocked
//	f, err := safeFS.Open("private/secret.txt") // Blocked by whitelist
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

// readOnlyFile wraps an fs.File to enforce read-only access.
//
// It blocks all write operations by returning an error when
// Write, WriteAt, or ReadFrom methods are called.
//
// This wrapper is used when SafeFS is configured with readOnly=true,
// providing defense-in-depth by preventing write operations even if
// the underlying filesystem supports them.
//
// Blocked operations:
//   - Write: Direct write operations
//   - WriteAt: Positioned write operations
//   - ReadFrom: Copy operations that could write to the file
//
// All read operations (Read, ReadAt, Seek, Stat, etc.) are delegated
// to the underlying fs.File without modification.
type readOnlyFile struct {
	fs.File
}

// Write is disabled for read-only files.
//
// Returns:
//   - n: Always 0
//   - error: Always returns "write operation not permitted in read-only mode"
func (r *readOnlyFile) Write(p []byte) (n int, err error) {
	return 0, errors.New(errWriteNotPermitted)
}

// WriteAt is disabled for read-only files.
//
// Returns:
//   - n: Always 0
//   - error: Always returns "write operation not permitted in read-only mode"
func (r *readOnlyFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, errors.New(errWriteNotPermitted)
}

// ReadFrom prevents potential write operations via io.ReaderFrom interface.
//
// Returns:
//   - n: Always 0
//   - error: Always returns "write operation not permitted in read-only mode"
func (r *readOnlyFile) ReadFrom(reader io.Reader) (n int64, err error) {
	return 0, errors.New(errWriteNotPermitted)
}
