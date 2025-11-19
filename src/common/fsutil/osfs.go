package fsutil

import (
	"io/fs"
	"os"
	"time"
)

// OSFS implements Filesystem using the OS filesystem
type OSFS struct{}

func NewOSFS() *OSFS {
	return &OSFS{}
}

func (fs *OSFS) Create(name string) (File, error) {
	//nolint:gosec // G304: file path comes from controlled source (message filesystem)
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	return &osFile{File: f}, nil
}

func (fs *OSFS) Mkdir(name string, perm os.FileMode) error {
	return os.Mkdir(name, perm)
}

func (fs *OSFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *OSFS) Open(name string) (File, error) {
	//nolint:gosec // G304: file path comes from controlled source (message filesystem)
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &osFile{File: f}, nil
}

func (fs *OSFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	//nolint:gosec // G304: file path comes from controlled source (message filesystem)
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &osFile{File: f}, nil
}

func (fs *OSFS) Remove(name string) error {
	return os.Remove(name)
}

func (fs *OSFS) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (fs *OSFS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (fs *OSFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *OSFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

func (fs *OSFS) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(name, mode)
}

func (fs *OSFS) Chown(name string, uid, gid int) error {
	return os.Chown(name, uid, gid)
}

func (fs *OSFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(name, atime, mtime)
}

type osFile struct {
	*os.File
}

func (f *osFile) Write(p []byte) (n int, err error) {
	return f.File.Write(p)
}

func (f *osFile) WriteAt(p []byte, off int64) (n int, err error) {
	return f.File.WriteAt(p, off)
}

func (f *osFile) WriteString(s string) (ret int, err error) {
	return f.File.WriteString(s)
}

func (f *osFile) Sync() error {
	return f.File.Sync()
}

func (f *osFile) Truncate(size int64) error {
	return f.File.Truncate(size)
}

func (f *osFile) Seek(offset int64, whence int) (int64, error) {
	return f.File.Seek(offset, whence)
}
