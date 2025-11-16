package fsutil

import (
	"bytes"
	"io/fs"
	"os"
	"path"
	"time"
)

// VirtualFS is a simple in-memory filesystem that exposes a single file at a given path.
// This is useful for exposing message data as a virtual file.
type VirtualFS struct {
	filePath string
	data     []byte
	modTime  time.Time
}

// NewVirtualFS creates a new virtual filesystem with a single file.
// The file will be accessible at the specified path with the given data.
func NewVirtualFS(filePath string, data []byte) *VirtualFS {
	return &VirtualFS{
		filePath: path.Clean(filePath),
		data:     data,
		modTime:  time.Now(),
	}
}

// Open opens the virtual file. Only the configured file path is accessible.
func (vfs *VirtualFS) Open(name string) (File, error) {
	cleanName := path.Clean(name)
	if cleanName != vfs.filePath {
		return nil, fs.ErrNotExist
	}
	return &virtualFile{
		reader:  bytes.NewReader(vfs.data),
		name:    vfs.filePath,
		size:    int64(len(vfs.data)),
		modTime: vfs.modTime,
	}, nil
}

// Stat returns file info for the virtual file.
func (vfs *VirtualFS) Stat(name string) (os.FileInfo, error) {
	cleanName := path.Clean(name)
	if cleanName != vfs.filePath {
		return nil, fs.ErrNotExist
	}
	return &virtualFileInfo{
		name:    path.Base(vfs.filePath),
		size:    int64(len(vfs.data)),
		modTime: vfs.modTime,
	}, nil
}

// ReadDir returns directory entries. Only root directory is supported.
func (vfs *VirtualFS) ReadDir(name string) ([]fs.DirEntry, error) {
	cleanName := path.Clean(name)
	if cleanName != "/" && cleanName != "." {
		return nil, fs.ErrNotExist
	}
	return []fs.DirEntry{
		&virtualDirEntry{
			name:    path.Base(vfs.filePath),
			size:    int64(len(vfs.data)),
			modTime: vfs.modTime,
		},
	}, nil
}

// Create is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) Create(name string) (File, error) {
	return nil, fs.ErrPermission
}

// Mkdir is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) Mkdir(name string, perm os.FileMode) error {
	return fs.ErrPermission
}

// MkdirAll is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) MkdirAll(path string, perm os.FileMode) error {
	return fs.ErrPermission
}

// OpenFile opens the virtual file (only read-only mode supported).
func (vfs *VirtualFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fs.ErrPermission
	}
	return vfs.Open(name)
}

// Remove is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) Remove(name string) error {
	return fs.ErrPermission
}

// RemoveAll is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) RemoveAll(path string) error {
	return fs.ErrPermission
}

// Rename is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) Rename(oldname, newname string) error {
	return fs.ErrPermission
}

// Chmod is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) Chmod(name string, mode os.FileMode) error {
	return fs.ErrPermission
}

// Chown is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) Chown(name string, uid, gid int) error {
	return fs.ErrPermission
}

// Chtimes is not supported in read-only virtual filesystem.
func (vfs *VirtualFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fs.ErrPermission
}

// virtualFile implements File interface for a virtual file.
type virtualFile struct {
	reader  *bytes.Reader
	name    string
	size    int64
	modTime time.Time
	closed  bool
}

func (f *virtualFile) Read(p []byte) (n int, err error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.reader.Read(p)
}

func (f *virtualFile) Close() error {
	f.closed = true
	return nil
}

func (f *virtualFile) Stat() (fs.FileInfo, error) {
	return &virtualFileInfo{
		name:    path.Base(f.name),
		size:    f.size,
		modTime: f.modTime,
	}, nil
}

func (f *virtualFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.reader.Seek(offset, whence)
}

func (f *virtualFile) Write(p []byte) (n int, err error) {
	return 0, fs.ErrPermission
}

func (f *virtualFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, fs.ErrPermission
}

func (f *virtualFile) WriteString(s string) (ret int, err error) {
	return 0, fs.ErrPermission
}

func (f *virtualFile) Sync() error {
	return nil
}

func (f *virtualFile) Truncate(size int64) error {
	return fs.ErrPermission
}

// virtualFileInfo implements fs.FileInfo for virtual files.
type virtualFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (fi *virtualFileInfo) Name() string       { return fi.name }
func (fi *virtualFileInfo) Size() int64        { return fi.size }
func (fi *virtualFileInfo) Mode() fs.FileMode  { return 0444 }
func (fi *virtualFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *virtualFileInfo) IsDir() bool        { return false }
func (fi *virtualFileInfo) Sys() any           { return nil }

// virtualDirEntry implements fs.DirEntry for virtual files.
type virtualDirEntry struct {
	name    string
	size    int64
	modTime time.Time
}

func (de *virtualDirEntry) Name() string      { return de.name }
func (de *virtualDirEntry) IsDir() bool       { return false }
func (de *virtualDirEntry) Type() fs.FileMode { return 0444 }
func (de *virtualDirEntry) Info() (fs.FileInfo, error) {
	return &virtualFileInfo{
		name:    de.name,
		size:    de.size,
		modTime: de.modTime,
	}, nil
}
