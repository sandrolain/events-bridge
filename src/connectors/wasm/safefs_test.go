package main

import (
	"testing"
	"testing/fstest"
)

const testFileName = "file.txt"

func TestSafeFSPathTraversal(t *testing.T) {
	// Create a mock filesystem
	mockFS := fstest.MapFS{
		"allowed/file.txt":     {Data: []byte("content")},
		"notallowed/file.txt":  {Data: []byte("secret")},
		"allowed/sub/file.txt": {Data: []byte("sub content")},
	}

	// Create SafeFS with whitelist
	safeFS := NewSafeFS(mockFS, []string{"allowed"}, true)

	tests := []struct {
		name      string
		path      string
		shouldErr bool
	}{
		{
			name:      "allowed path",
			path:      "allowed/file.txt",
			shouldErr: false,
		},
		{
			name:      "allowed subpath",
			path:      "allowed/sub/file.txt",
			shouldErr: false,
		},
		{
			name:      "path traversal attempt",
			path:      "allowed/../notallowed/file.txt",
			shouldErr: true,
		},
		{
			name:      "direct path traversal",
			path:      "../etc/passwd",
			shouldErr: true,
		},
		{
			name:      "not allowed path",
			path:      "notallowed/file.txt",
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := safeFS.Open(tt.path)
			if tt.shouldErr && err == nil {
				t.Errorf("expected error for path %s, got nil", tt.path)
			}
			if !tt.shouldErr && err != nil {
				t.Errorf("unexpected error for path %s: %v", tt.path, err)
			}
		})
	}
}

func TestSafeFSReadOnly(t *testing.T) {
	mockFS := fstest.MapFS{
		testFileName: {Data: []byte("content")},
	}

	safeFS := NewSafeFS(mockFS, nil, true)

	file, err := safeFS.Open(testFileName)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Logf("failed to close file: %v", err)
		}
	}()

	// Attempt to write (should fail)
	roFile, ok := file.(*readOnlyFile)
	if !ok {
		t.Fatal("expected readOnlyFile wrapper")
	}

	_, err = roFile.Write([]byte("test"))
	if err == nil {
		t.Error("expected write to fail on read-only file")
	}
	if err.Error() != errWriteNotPermitted {
		t.Errorf("expected error %q, got %q", errWriteNotPermitted, err.Error())
	}
}

func TestSafeFSNoWhitelist(t *testing.T) {
	mockFS := fstest.MapFS{
		"file1.txt": {Data: []byte("content1")},
		"file2.txt": {Data: []byte("content2")},
	}

	// SafeFS with no whitelist should allow all paths (except traversal)
	safeFS := NewSafeFS(mockFS, nil, false)

	tests := []struct {
		name      string
		path      string
		shouldErr bool
	}{
		{
			name:      "file1",
			path:      "file1.txt",
			shouldErr: false,
		},
		{
			name:      "file2",
			path:      "file2.txt",
			shouldErr: false,
		},
		{
			name:      "traversal still blocked",
			path:      "../etc/passwd",
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := safeFS.Open(tt.path)
			if tt.shouldErr && err == nil {
				t.Errorf("expected error for path %s, got nil", tt.path)
			}
			if !tt.shouldErr && err != nil {
				t.Errorf("unexpected error for path %s: %v", tt.path, err)
			}
		})
	}
}

func TestReadOnlyFileWriteOperations(t *testing.T) {
	mockFS := fstest.MapFS{
		testFileName: {Data: []byte("content")},
	}

	safeFS := NewSafeFS(mockFS, nil, true)
	file, err := safeFS.Open(testFileName)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Logf("failed to close file: %v", err)
		}
	}()

	roFile, ok := file.(*readOnlyFile)
	if !ok {
		t.Fatal("expected readOnlyFile wrapper")
	}

	// Test Write
	_, err = roFile.Write([]byte("test"))
	if err == nil {
		t.Error("Write should fail on read-only file")
	}

	// Test WriteAt
	_, err = roFile.WriteAt([]byte("test"), 0)
	if err == nil {
		t.Error("WriteAt should fail on read-only file")
	}

	// Test ReadFrom
	_, err = roFile.ReadFrom(nil)
	if err == nil {
		t.Error("ReadFrom should fail on read-only file")
	}
}

func TestSafeFSEmptyWhitelist(t *testing.T) {
	mockFS := fstest.MapFS{
		testFileName: {Data: []byte("content")},
	}

	// Empty whitelist should allow all paths (like nil whitelist)
	safeFS := NewSafeFS(mockFS, []string{}, true)

	_, err := safeFS.Open(testFileName)
	if err != nil {
		t.Errorf("expected file to be accessible with empty whitelist, got error: %v", err)
	}
}
