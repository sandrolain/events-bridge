package fsutil

import (
	"io"
	"io/fs"
	"os"
	"time"
)

// Filesystem provides an abstraction over various filesystem implementations.
// This interface is designed to be compatible with afero.Fs and other filesystem libraries
// while maintaining a stable API for the Events Bridge message system.
type Filesystem interface {
	// Create creates a file in the filesystem, returning the file and an error, if any happens.
	Create(name string) (File, error)

	// Mkdir creates a directory in the filesystem, return an error if any happens.
	Mkdir(name string, perm os.FileMode) error

	// MkdirAll creates a directory path and all parents that does not exist yet.
	MkdirAll(path string, perm os.FileMode) error

	// Open opens a file, returning it or an error, if any happens.
	Open(name string) (File, error)

	// OpenFile is the generalized open call; most users will use Open or Create instead.
	// It opens the named file with specified flag (O_RDONLY etc.) and perm.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)

	// Remove removes a file identified by name, returning an error, if any happens.
	Remove(name string) error

	// RemoveAll removes a directory path and any children it contains.
	RemoveAll(path string) error

	// Rename renames a file.
	Rename(oldname, newname string) error

	// Stat returns a FileInfo describing the named file, or an error, if any happens.
	Stat(name string) (os.FileInfo, error)

	// ReadDir reads the directory named by dirname and returns a list of directory entries.
	ReadDir(name string) ([]fs.DirEntry, error)

	// Chmod changes the mode of the named file to mode.
	Chmod(name string, mode os.FileMode) error

	// Chown changes the numeric uid and gid of the named file.
	Chown(name string, uid, gid int) error

	// Chtimes changes the access and modification times of the named file.
	Chtimes(name string, atime time.Time, mtime time.Time) error
}

// File represents a file in the filesystem.
// This interface extends fs.File with write operations.
type File interface {
	// Embed standard fs.File interface for read operations
	fs.File

	// Write writes len(p) bytes from p to the File.
	Write(p []byte) (n int, err error)

	// WriteAt writes len(p) bytes from p to the File starting at byte offset off.
	WriteAt(p []byte, off int64) (n int, err error)

	// WriteString writes the contents of string s to the File.
	WriteString(s string) (ret int, err error)

	// Sync commits the current contents of the file to stable storage.
	Sync() error

	// Truncate changes the size of the file.
	Truncate(size int64) error

	// Seek sets the offset for the next Read or Write on file to offset.
	Seek(offset int64, whence int) (int64, error)
}

// ReadOnlyFile wraps a fs.File to implement the File interface for read-only operations.
// Write operations will return an error.
type ReadOnlyFile struct {
	fs.File
}

// Write implements File.Write for read-only files (always returns error).
func (f *ReadOnlyFile) Write(p []byte) (n int, err error) {
	return 0, fs.ErrPermission
}

// WriteAt implements File.WriteAt for read-only files (always returns error).
func (f *ReadOnlyFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, fs.ErrPermission
}

// WriteString implements File.WriteString for read-only files (always returns error).
func (f *ReadOnlyFile) WriteString(s string) (ret int, err error) {
	return 0, fs.ErrPermission
}

// Sync implements File.Sync for read-only files (no-op).
func (f *ReadOnlyFile) Sync() error {
	return nil
}

// Truncate implements File.Truncate for read-only files (always returns error).
func (f *ReadOnlyFile) Truncate(size int64) error {
	return fs.ErrPermission
}

// Seek implements File.Seek by delegating to the underlying file if it supports seeking.
func (f *ReadOnlyFile) Seek(offset int64, whence int) (int64, error) {
	if seeker, ok := f.File.(io.Seeker); ok {
		return seeker.Seek(offset, whence)
	}
	return 0, fs.ErrInvalid
}

// NewReadOnlyFile wraps a fs.File to implement the File interface for read-only operations.
func NewReadOnlyFile(file fs.File) File {
	return &ReadOnlyFile{File: file}
}

// ReadFile reads the file named by filename from the filesystem and returns the contents.
// A successful call returns err == nil, not err == EOF.
func ReadFile(fsys Filesystem, filename string) ([]byte, error) {
	file, err := fsys.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close() //nolint:errcheck

	return io.ReadAll(file)
}
