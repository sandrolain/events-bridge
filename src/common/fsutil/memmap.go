package fsutil

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// MemMapFS is a simple in-memory filesystem
type MemMapFS struct {
	mu    sync.RWMutex
	files map[string]*memFile
}

type memFile struct {
	data    []byte
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

// NewMemMapFS creates a new in-memory filesystem
func NewMemMapFS() *MemMapFS {
	return &MemMapFS{
		files: make(map[string]*memFile),
	}
}

func (m *MemMapFS) normPath(name string) string {
	return filepath.Clean(name)
}

func (m *MemMapFS) Create(name string) (File, error) {
	return m.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (m *MemMapFS) Mkdir(name string, perm os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = m.normPath(name)
	if _, exists := m.files[name]; exists {
		return fs.ErrExist
	}

	m.files[name] = &memFile{
		mode:    perm | os.ModeDir,
		modTime: time.Now(),
		isDir:   true,
	}
	return nil
}

func (m *MemMapFS) MkdirAll(path string, perm os.FileMode) error {
	path = m.normPath(path)

	current := ""
	for _, part := range filepath.SplitList(path) {
		if part == "" || part == string(filepath.Separator) {
			continue
		}
		current = filepath.Join(current, part)

		m.mu.RLock()
		f, exists := m.files[current]
		m.mu.RUnlock()

		if !exists {
			if err := m.Mkdir(current, perm); err != nil {
				return err
			}
		} else if !f.isDir {
			return fs.ErrInvalid
		}
	}

	return nil
}

func (m *MemMapFS) Open(name string) (File, error) {
	return m.OpenFile(name, os.O_RDONLY, 0)
}

func (m *MemMapFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	name = m.normPath(name)

	m.mu.Lock()
	defer m.mu.Unlock()

	f, exists := m.files[name]

	if !exists {
		if flag&os.O_CREATE == 0 {
			return nil, fs.ErrNotExist
		}

		// Create parent directories
		dir := filepath.Dir(name)
		if dir != "." && dir != "/" {
			m.mu.Unlock()
			if err := m.MkdirAll(dir, 0755); err != nil {
				m.mu.Lock()
				return nil, err
			}
			m.mu.Lock()
		}

		f = &memFile{
			data:    []byte{},
			mode:    perm,
			modTime: time.Now(),
			isDir:   false,
		}
		m.files[name] = f
	}

	if f.isDir {
		return nil, fs.ErrInvalid
	}

	data := f.data
	if flag&os.O_TRUNC != 0 {
		data = []byte{}
		f.data = data
		f.modTime = time.Now()
	}

	return &memMapFile{
		fs:       m,
		name:     name,
		data:     data,
		mode:     f.mode,
		modTime:  f.modTime,
		writable: flag&(os.O_WRONLY|os.O_RDWR) != 0,
	}, nil
}

func (m *MemMapFS) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = m.normPath(name)
	if _, exists := m.files[name]; !exists {
		return fs.ErrNotExist
	}

	delete(m.files, name)
	return nil
}

func (m *MemMapFS) RemoveAll(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path = m.normPath(path)

	// Remove all files with this prefix
	for name := range m.files {
		if name == path || strings.HasPrefix(name, path+string(filepath.Separator)) {
			delete(m.files, name)
		}
	}

	return nil
}

func (m *MemMapFS) Rename(oldname, newname string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldname = m.normPath(oldname)
	newname = m.normPath(newname)

	f, exists := m.files[oldname]
	if !exists {
		return fs.ErrNotExist
	}

	m.files[newname] = f
	delete(m.files, oldname)
	return nil
}

func (m *MemMapFS) Stat(name string) (os.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = m.normPath(name)
	f, exists := m.files[name]
	if !exists {
		return nil, fs.ErrNotExist
	}

	return &memFileInfo{
		name:    filepath.Base(name),
		size:    int64(len(f.data)),
		mode:    f.mode,
		modTime: f.modTime,
		isDir:   f.isDir,
	}, nil
}

func (m *MemMapFS) ReadDir(name string) ([]fs.DirEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = m.normPath(name)

	// Collect immediate children
	children := make(map[string]*memFile)

	for path, file := range m.files {
		if path == name {
			continue
		}

		// Check if this file is a direct child of name
		dir := filepath.Dir(path)
		if dir == name || (name == "/" && dir == "/") || (name == "." && !strings.HasPrefix(path, string(filepath.Separator))) {
			children[filepath.Base(path)] = file
		}
	}

	entries := make([]fs.DirEntry, 0, len(children))
	for childName, file := range children {
		entries = append(entries, &memDirEntry{
			name:    childName,
			size:    int64(len(file.data)),
			mode:    file.mode,
			modTime: file.modTime,
			isDir:   file.isDir,
		})
	}

	return entries, nil
}

func (m *MemMapFS) Chmod(name string, mode os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = m.normPath(name)
	f, exists := m.files[name]
	if !exists {
		return fs.ErrNotExist
	}

	f.mode = mode
	return nil
}

func (m *MemMapFS) Chown(name string, uid, gid int) error {
	// Not supported in memory fs
	return nil
}

func (m *MemMapFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = m.normPath(name)
	f, exists := m.files[name]
	if !exists {
		return fs.ErrNotExist
	}

	f.modTime = mtime
	return nil
}

// memMapFile implements File
type memMapFile struct {
	fs       *MemMapFS
	name     string
	data     []byte
	mode     os.FileMode
	modTime  time.Time
	offset   int64
	writable bool
	closed   bool
}

func (f *memMapFile) Read(p []byte) (n int, err error) {
	if f.closed {
		return 0, fs.ErrClosed
	}

	if f.offset >= int64(len(f.data)) {
		return 0, io.EOF
	}

	n = copy(p, f.data[f.offset:])
	f.offset += int64(n)
	return n, nil
}

func (f *memMapFile) Write(p []byte) (n int, err error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	if !f.writable {
		return 0, fs.ErrPermission
	}

	// Extend data if needed
	minLen := int(f.offset) + len(p)
	if minLen > len(f.data) {
		newData := make([]byte, minLen)
		copy(newData, f.data)
		f.data = newData
	}

	n = copy(f.data[f.offset:], p)
	f.offset += int64(n)
	f.modTime = time.Now()

	// Update in filesystem
	f.fs.mu.Lock()
	if file, exists := f.fs.files[f.name]; exists {
		file.data = f.data
		file.modTime = f.modTime
	}
	f.fs.mu.Unlock()

	return n, nil
}

func (f *memMapFile) WriteAt(p []byte, off int64) (n int, err error) {
	oldOffset := f.offset
	f.offset = off
	n, err = f.Write(p)
	f.offset = oldOffset
	return n, err
}

func (f *memMapFile) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

func (f *memMapFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}

	var newOffset int64
	switch whence {
	case 0: // io.SeekStart
		newOffset = offset
	case 1: // io.SeekCurrent
		newOffset = f.offset + offset
	case 2: // io.SeekEnd
		newOffset = int64(len(f.data)) + offset
	default:
		return 0, fs.ErrInvalid
	}

	if newOffset < 0 {
		return 0, fs.ErrInvalid
	}

	f.offset = newOffset
	return newOffset, nil
}

func (f *memMapFile) Close() error {
	f.closed = true
	return nil
}

func (f *memMapFile) Stat() (fs.FileInfo, error) {
	return &memFileInfo{
		name:    filepath.Base(f.name),
		size:    int64(len(f.data)),
		mode:    f.mode,
		modTime: f.modTime,
		isDir:   false,
	}, nil
}

func (f *memMapFile) Sync() error {
	return nil
}

func (f *memMapFile) Truncate(size int64) error {
	if f.closed {
		return fs.ErrClosed
	}
	if !f.writable {
		return fs.ErrPermission
	}

	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	} else {
		newData := make([]byte, size)
		copy(newData, f.data)
		f.data = newData
	}

	f.modTime = time.Now()

	// Update in filesystem
	f.fs.mu.Lock()
	if file, exists := f.fs.files[f.name]; exists {
		file.data = f.data
		file.modTime = f.modTime
	}
	f.fs.mu.Unlock()

	return nil
}

// memFileInfo implements fs.FileInfo
type memFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *memFileInfo) Name() string       { return fi.name }
func (fi *memFileInfo) Size() int64        { return fi.size }
func (fi *memFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *memFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *memFileInfo) IsDir() bool        { return fi.isDir }
func (fi *memFileInfo) Sys() any           { return nil }

// memDirEntry implements fs.DirEntry
type memDirEntry struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (de *memDirEntry) Name() string      { return de.name }
func (de *memDirEntry) IsDir() bool       { return de.isDir }
func (de *memDirEntry) Type() fs.FileMode { return de.mode }
func (de *memDirEntry) Info() (fs.FileInfo, error) {
	return &memFileInfo{
		name:    de.name,
		size:    de.size,
		mode:    de.mode,
		modTime: de.modTime,
		isDir:   de.isDir,
	}, nil
}
