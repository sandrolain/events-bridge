package fsutil

import (
	"errors"
	"io"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyFile_Write(t *testing.T) {
	mapFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data: []byte("test content"),
		},
	}

	file, err := mapFS.Open("test.txt")
	require.NoError(t, err)
	defer file.Close()

	roFile := NewReadOnlyFile(file)

	// Test Write
	n, err := roFile.Write([]byte("new content"))
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyFile_WriteAt(t *testing.T) {
	mapFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data: []byte("test content"),
		},
	}

	file, err := mapFS.Open("test.txt")
	require.NoError(t, err)
	defer file.Close()

	roFile := NewReadOnlyFile(file)

	// Test WriteAt
	n, err := roFile.WriteAt([]byte("new"), 0)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyFile_WriteString(t *testing.T) {
	mapFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data: []byte("test content"),
		},
	}

	file, err := mapFS.Open("test.txt")
	require.NoError(t, err)
	defer file.Close()

	roFile := NewReadOnlyFile(file)

	// Test WriteString
	n, err := roFile.WriteString("new content")
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyFile_Truncate(t *testing.T) {
	mapFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data: []byte("test content"),
		},
	}

	file, err := mapFS.Open("test.txt")
	require.NoError(t, err)
	defer file.Close()

	roFile := NewReadOnlyFile(file)

	// Test Truncate
	err = roFile.Truncate(5)
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func TestReadOnlyFile_Sync(t *testing.T) {
	mapFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data: []byte("test content"),
		},
	}

	file, err := mapFS.Open("test.txt")
	require.NoError(t, err)
	defer file.Close()

	roFile := NewReadOnlyFile(file)

	// Test Sync (should be no-op)
	err = roFile.Sync()
	assert.NoError(t, err)
}

func TestReadOnlyFile_Read(t *testing.T) {
	mapFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data: []byte("test content"),
		},
	}

	file, err := mapFS.Open("test.txt")
	require.NoError(t, err)
	defer file.Close()

	roFile := NewReadOnlyFile(file)

	// Test Read (should work)
	buf := make([]byte, 12)
	n, err := roFile.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 12, n)
	assert.Equal(t, "test content", string(buf))
}

func TestReadOnlyFile_Stat(t *testing.T) {
	mapFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data: []byte("test content"),
		},
	}

	file, err := mapFS.Open("test.txt")
	require.NoError(t, err)
	defer file.Close()

	roFile := NewReadOnlyFile(file)

	// Test Stat
	info, err := roFile.Stat()
	require.NoError(t, err)
	assert.Equal(t, "test.txt", info.Name())
	assert.Equal(t, int64(12), info.Size())
}

func TestReadOnlyFile_Seek_WithSeeker(t *testing.T) {
	// Create a mock file that implements io.Seeker
	mockFile := &mockSeekableFile{
		data: []byte("test content"),
		pos:  0,
	}

	roFile := NewReadOnlyFile(mockFile)

	// Test Seek
	pos, err := roFile.Seek(5, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), pos)
}

func TestReadOnlyFile_Seek_WithoutSeeker(t *testing.T) {
	// Create a mock file that doesn't implement io.Seeker
	mockFile := &mockNonSeekableFile{
		data: []byte("test content"),
	}

	roFile := NewReadOnlyFile(mockFile)

	// Test Seek (mockNonSeekableFile doesn't implement Seeker)
	pos, err := roFile.Seek(5, io.SeekStart)
	assert.ErrorIs(t, err, fs.ErrInvalid)
	assert.Equal(t, int64(0), pos)
}

// Mock seekable file for testing
type mockSeekableFile struct {
	data []byte
	pos  int64
}

func (f *mockSeekableFile) Read(p []byte) (n int, err error) {
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n = copy(p, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *mockSeekableFile) Close() error {
	return nil
}

func (f *mockSeekableFile) Stat() (fs.FileInfo, error) {
	return nil, errors.New("not implemented")
}

func (f *mockSeekableFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = int64(len(f.data)) + offset
	default:
		return 0, fs.ErrInvalid
	}
	return f.pos, nil
}

// Mock non-seekable file for testing
type mockNonSeekableFile struct {
	data []byte
	pos  int
}

func (f *mockNonSeekableFile) Read(p []byte) (n int, err error) {
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}
	n = copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *mockNonSeekableFile) Close() error {
	return nil
}

func (f *mockNonSeekableFile) Stat() (fs.FileInfo, error) {
	return nil, errors.New("not implemented")
}
