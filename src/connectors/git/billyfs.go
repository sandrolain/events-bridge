package main

import (
	"io/fs"
	"os"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
)

// billyFileAdapter adapts billy.File to fs.File interface
type billyFileAdapter struct {
	billy.File
	fs   billy.Filesystem
	name string
}

// Stat implements fs.File.Stat by using the filesystem
func (f *billyFileAdapter) Stat() (fs.FileInfo, error) {
	return f.fs.Stat(f.name)
}

// Read implements io.Reader
func (f *billyFileAdapter) Read(p []byte) (n int, err error) {
	return f.File.Read(p)
}

// Close implements io.Closer
func (f *billyFileAdapter) Close() error {
	return f.File.Close()
}

// readOnlyBillyFS is a read-only adapter that wraps a billy.Filesystem
// to implement fsutil.Filesystem interface.
type readOnlyBillyFS struct {
	fs billy.Filesystem
}

// newReadOnlyBillyFS creates a new read-only adapter for billy.Filesystem
func newReadOnlyBillyFS(billyFS billy.Filesystem) fsutil.Filesystem {
	return &readOnlyBillyFS{fs: billyFS}
}

// Create returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) Create(name string) (fsutil.File, error) {
	return nil, fs.ErrPermission
}

// Mkdir returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) Mkdir(name string, perm os.FileMode) error {
	return fs.ErrPermission
}

// MkdirAll returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) MkdirAll(path string, perm os.FileMode) error {
	return fs.ErrPermission
}

// Open opens a file for reading
func (r *readOnlyBillyFS) Open(name string) (fsutil.File, error) {
	file, err := r.fs.Open(name)
	if err != nil {
		return nil, err
	}
	// Wrap billy.File in adapter, then in read-only wrapper
	adapted := &billyFileAdapter{
		File: file,
		fs:   r.fs,
		name: name,
	}
	return fsutil.NewReadOnlyFile(adapted), nil
}

// OpenFile opens a file with the specified flag and permissions.
// Only read-only operations are allowed.
func (r *readOnlyBillyFS) OpenFile(name string, flag int, perm os.FileMode) (fsutil.File, error) {
	// Only allow read operations
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fs.ErrPermission
	}
	file, err := r.fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	// Wrap billy.File in adapter, then in read-only wrapper
	adapted := &billyFileAdapter{
		File: file,
		fs:   r.fs,
		name: name,
	}
	return fsutil.NewReadOnlyFile(adapted), nil
}

// Remove returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) Remove(name string) error {
	return fs.ErrPermission
}

// RemoveAll returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) RemoveAll(path string) error {
	return fs.ErrPermission
}

// Rename returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) Rename(oldname, newname string) error {
	return fs.ErrPermission
}

// Stat returns file information
func (r *readOnlyBillyFS) Stat(name string) (os.FileInfo, error) {
	return r.fs.Stat(name)
}

// ReadDir reads the directory entries
func (r *readOnlyBillyFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := r.fs.ReadDir(name)
	if err != nil {
		return nil, err
	}

	// Convert []os.FileInfo to []fs.DirEntry
	dirEntries := make([]fs.DirEntry, len(entries))
	for i, info := range entries {
		dirEntries[i] = fs.FileInfoToDirEntry(info)
	}
	return dirEntries, nil
}

// Chmod returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) Chmod(name string, mode os.FileMode) error {
	return fs.ErrPermission
}

// Chown returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) Chown(name string, uid, gid int) error {
	return fs.ErrPermission
}

// Chtimes returns permission error (read-only filesystem)
func (r *readOnlyBillyFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fs.ErrPermission
}
