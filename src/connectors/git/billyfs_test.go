package main

import (
	"io"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyBillyFS_Open(t *testing.T) {
	// Create in-memory billy filesystem
	billyFS := memfs.New()
	err := billyFS.MkdirAll("testdir", 0755)
	require.NoError(t, err)

	file, err := billyFS.Create("testdir/test.txt")
	require.NoError(t, err)
	_, err = file.Write([]byte("test content"))
	require.NoError(t, err)
	file.Close()

	// Create read-only adapter
	roFS := newReadOnlyBillyFS(billyFS)

	// Test Open
	f, err := roFS.Open("testdir/test.txt")
	require.NoError(t, err)
	defer f.Close()

	// Should be able to read
	content, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, "test content", string(content))
}

func TestReadOnlyBillyFS_Stat(t *testing.T) {
	billyFS := memfs.New()
	file, err := billyFS.Create("test.txt")
	require.NoError(t, err)
	file.Close()

	roFS := newReadOnlyBillyFS(billyFS)

	// Test Stat
	info, err := roFS.Stat("test.txt")
	require.NoError(t, err)
	assert.Equal(t, "test.txt", info.Name())
}

func TestReadOnlyBillyFS_ReadDir(t *testing.T) {
	billyFS := memfs.New()

	// Create files
	file1, err := billyFS.Create("file1.txt")
	require.NoError(t, err)
	file1.Close()

	file2, err := billyFS.Create("file2.txt")
	require.NoError(t, err)
	file2.Close()

	roFS := newReadOnlyBillyFS(billyFS)

	// Test ReadDir
	entries, err := roFS.ReadDir("/")
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	names := []string{entries[0].Name(), entries[1].Name()}
	assert.Contains(t, names, "file1.txt")
	assert.Contains(t, names, "file2.txt")
}

func TestReadOnlyBillyFS_Create_Denied(t *testing.T) {
	billyFS := memfs.New()
	roFS := newReadOnlyBillyFS(billyFS)

	// Test Create (should be denied)
	_, err := roFS.Create("newfile.txt")
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_Mkdir_Denied(t *testing.T) {
	billyFS := memfs.New()
	roFS := newReadOnlyBillyFS(billyFS)

	// Test Mkdir (should be denied)
	err := roFS.Mkdir("newdir", 0755)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_MkdirAll_Denied(t *testing.T) {
	billyFS := memfs.New()
	roFS := newReadOnlyBillyFS(billyFS)

	// Test MkdirAll (should be denied)
	err := roFS.MkdirAll("path/to/dir", 0755)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_Remove_Denied(t *testing.T) {
	billyFS := memfs.New()
	file, err := billyFS.Create("test.txt")
	require.NoError(t, err)
	file.Close()

	roFS := newReadOnlyBillyFS(billyFS)

	// Test Remove (should be denied)
	err = roFS.Remove("test.txt")
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_RemoveAll_Denied(t *testing.T) {
	billyFS := memfs.New()
	roFS := newReadOnlyBillyFS(billyFS)

	// Test RemoveAll (should be denied)
	err := roFS.RemoveAll("somedir")
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_Rename_Denied(t *testing.T) {
	billyFS := memfs.New()
	file, err := billyFS.Create("old.txt")
	require.NoError(t, err)
	file.Close()

	roFS := newReadOnlyBillyFS(billyFS)

	// Test Rename (should be denied)
	err = roFS.Rename("old.txt", "new.txt")
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_Chmod_Denied(t *testing.T) {
	billyFS := memfs.New()
	file, err := billyFS.Create("test.txt")
	require.NoError(t, err)
	file.Close()

	roFS := newReadOnlyBillyFS(billyFS)

	// Test Chmod (should be denied)
	err = roFS.Chmod("test.txt", 0644)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_Chown_Denied(t *testing.T) {
	billyFS := memfs.New()
	file, err := billyFS.Create("test.txt")
	require.NoError(t, err)
	file.Close()

	roFS := newReadOnlyBillyFS(billyFS)

	// Test Chown (should be denied)
	err = roFS.Chown("test.txt", 1000, 1000)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_Chtimes_Denied(t *testing.T) {
	billyFS := memfs.New()
	file, err := billyFS.Create("test.txt")
	require.NoError(t, err)
	file.Close()

	roFS := newReadOnlyBillyFS(billyFS)

	// Test Chtimes (should be denied)
	now := time.Now()
	err = roFS.Chtimes("test.txt", now, now)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyBillyFS_OpenFile_ReadOnly(t *testing.T) {
	billyFS := memfs.New()
	file, err := billyFS.Create("test.txt")
	require.NoError(t, err)
	_, err = file.Write([]byte("content"))
	require.NoError(t, err)
	file.Close()

	roFS := newReadOnlyBillyFS(billyFS)

	// Test OpenFile with read-only flag (should work)
	f, err := roFS.OpenFile("test.txt", os.O_RDONLY, 0644)
	require.NoError(t, err)
	f.Close()
}

func TestReadOnlyBillyFS_OpenFile_Write_Denied(t *testing.T) {
	billyFS := memfs.New()
	roFS := newReadOnlyBillyFS(billyFS)

	// Test OpenFile with write flag (should be denied)
	_, err := roFS.OpenFile("test.txt", os.O_WRONLY, 0644)
	assert.ErrorIs(t, err, fs.ErrPermission)

	_, err = roFS.OpenFile("test.txt", os.O_RDWR, 0644)
	assert.ErrorIs(t, err, fs.ErrPermission)

	_, err = roFS.OpenFile("test.txt", os.O_CREATE, 0644)
	assert.ErrorIs(t, err, fs.ErrPermission)

	_, err = roFS.OpenFile("test.txt", os.O_APPEND, 0644)
	assert.ErrorIs(t, err, fs.ErrPermission)

	_, err = roFS.OpenFile("test.txt", os.O_TRUNC, 0644)
	assert.ErrorIs(t, err, fs.ErrPermission)
}
