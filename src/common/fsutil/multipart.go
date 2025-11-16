package fsutil

import (
	"bytes"
	"io"
	"io/fs"
	"mime/multipart"
	"os"
	"path"
	"time"
)

// MultipartFS is a filesystem that provides access to files uploaded via HTTP multipart/form-data.
type MultipartFS struct {
	files   map[string]*multipartFile
	modTime time.Time
}

// multipartFile holds the data for a single uploaded file.
type multipartFile struct {
	filename string
	data     []byte
	header   *multipart.FileHeader
}

// NewMultipartFS creates a new filesystem from multipart form files.
func NewMultipartFS(form *multipart.Form) (*MultipartFS, error) {
	if form == nil || form.File == nil {
		return &MultipartFS{
			files:   make(map[string]*multipartFile),
			modTime: time.Now(),
		}, nil
	}

	mfs := &MultipartFS{
		files:   make(map[string]*multipartFile),
		modTime: time.Now(),
	}

	for fieldName, headers := range form.File {
		for i, header := range headers {
			// Read file data
			file, err := header.Open()
			if err != nil {
				continue
			}
			data, err := io.ReadAll(file)
			if closeErr := file.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			if err != nil {
				continue
			} // Use filename from header, or generate one from field name
			filename := header.Filename
			if filename == "" {
				if i == 0 {
					filename = fieldName
				} else {
					filename = fieldName + "_" + string(rune('0'+i))
				}
			}

			// Store with clean path
			cleanPath := path.Clean("/" + filename)
			mfs.files[cleanPath] = &multipartFile{
				filename: filename,
				data:     data,
				header:   header,
			}
		}
	}

	return mfs, nil
}

// Open opens a file from the multipart upload.
func (mfs *MultipartFS) Open(name string) (File, error) {
	cleanName := path.Clean("/" + name)
	file, ok := mfs.files[cleanName]
	if !ok {
		return nil, fs.ErrNotExist
	}

	return &multipartFileReader{
		reader:   bytes.NewReader(file.data),
		name:     file.filename,
		size:     int64(len(file.data)),
		modTime:  mfs.modTime,
		header:   file.header,
		fullPath: cleanName,
	}, nil
}

// Stat returns file info for an uploaded file.
func (mfs *MultipartFS) Stat(name string) (os.FileInfo, error) {
	cleanName := path.Clean("/" + name)
	file, ok := mfs.files[cleanName]
	if !ok {
		return nil, fs.ErrNotExist
	}

	return &multipartFileInfo{
		name:    file.filename,
		size:    int64(len(file.data)),
		modTime: mfs.modTime,
	}, nil
}

// ReadDir returns all uploaded files.
func (mfs *MultipartFS) ReadDir(name string) ([]fs.DirEntry, error) {
	cleanName := path.Clean(name)
	if cleanName != "/" && cleanName != "." {
		return nil, fs.ErrNotExist
	}

	entries := make([]fs.DirEntry, 0, len(mfs.files))
	for _, file := range mfs.files {
		entries = append(entries, &multipartDirEntry{
			name:    file.filename,
			size:    int64(len(file.data)),
			modTime: mfs.modTime,
		})
	}
	return entries, nil
}

// Create is not supported (read-only filesystem).
func (mfs *MultipartFS) Create(name string) (File, error) {
	return nil, fs.ErrPermission
}

// Mkdir is not supported (read-only filesystem).
func (mfs *MultipartFS) Mkdir(name string, perm os.FileMode) error {
	return fs.ErrPermission
}

// MkdirAll is not supported (read-only filesystem).
func (mfs *MultipartFS) MkdirAll(path string, perm os.FileMode) error {
	return fs.ErrPermission
}

// OpenFile opens a file (only read-only mode supported).
func (mfs *MultipartFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fs.ErrPermission
	}
	return mfs.Open(name)
}

// Remove is not supported (read-only filesystem).
func (mfs *MultipartFS) Remove(name string) error {
	return fs.ErrPermission
}

// RemoveAll is not supported (read-only filesystem).
func (mfs *MultipartFS) RemoveAll(path string) error {
	return fs.ErrPermission
}

// Rename is not supported (read-only filesystem).
func (mfs *MultipartFS) Rename(oldname, newname string) error {
	return fs.ErrPermission
}

// Chmod is not supported (read-only filesystem).
func (mfs *MultipartFS) Chmod(name string, mode os.FileMode) error {
	return fs.ErrPermission
}

// Chown is not supported (read-only filesystem).
func (mfs *MultipartFS) Chown(name string, uid, gid int) error {
	return fs.ErrPermission
}

// Chtimes is not supported (read-only filesystem).
func (mfs *MultipartFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fs.ErrPermission
}

// multipartFileReader implements File interface for uploaded files.
type multipartFileReader struct {
	reader   *bytes.Reader
	name     string
	fullPath string
	size     int64
	modTime  time.Time
	header   *multipart.FileHeader
	closed   bool
}

func (f *multipartFileReader) Read(p []byte) (n int, err error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.reader.Read(p)
}

func (f *multipartFileReader) Close() error {
	f.closed = true
	return nil
}

func (f *multipartFileReader) Stat() (fs.FileInfo, error) {
	return &multipartFileInfo{
		name:    f.name,
		size:    f.size,
		modTime: f.modTime,
	}, nil
}

func (f *multipartFileReader) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.reader.Seek(offset, whence)
}

func (f *multipartFileReader) Write(p []byte) (n int, err error) {
	return 0, fs.ErrPermission
}

func (f *multipartFileReader) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, fs.ErrPermission
}

func (f *multipartFileReader) WriteString(s string) (ret int, err error) {
	return 0, fs.ErrPermission
}

func (f *multipartFileReader) Sync() error {
	return nil
}

func (f *multipartFileReader) Truncate(size int64) error {
	return fs.ErrPermission
}

// multipartFileInfo implements fs.FileInfo for uploaded files.
type multipartFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (fi *multipartFileInfo) Name() string       { return fi.name }
func (fi *multipartFileInfo) Size() int64        { return fi.size }
func (fi *multipartFileInfo) Mode() fs.FileMode  { return 0444 }
func (fi *multipartFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *multipartFileInfo) IsDir() bool        { return false }
func (fi *multipartFileInfo) Sys() any           { return nil }

// multipartDirEntry implements fs.DirEntry for uploaded files.
type multipartDirEntry struct {
	name    string
	size    int64
	modTime time.Time
}

func (de *multipartDirEntry) Name() string      { return de.name }
func (de *multipartDirEntry) IsDir() bool       { return false }
func (de *multipartDirEntry) Type() fs.FileMode { return 0444 }
func (de *multipartDirEntry) Info() (fs.FileInfo, error) {
	return &multipartFileInfo{
		name:    de.name,
		size:    de.size,
		modTime: de.modTime,
	}, nil
}
