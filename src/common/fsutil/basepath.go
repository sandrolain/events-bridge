package fsutil

import (
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// BasePathFS restricts all operations to a given base path
type BasePathFS struct {
	source Filesystem
	base   string
}

func NewBasePathFS(source Filesystem, base string) *BasePathFS {
	return &BasePathFS{
		source: source,
		base:   filepath.Clean(base),
	}
}

func (b *BasePathFS) realPath(name string) (string, error) {
	if filepath.IsAbs(name) {
		return filepath.Join(b.base, name), nil
	}
	return filepath.Join(b.base, name), nil
}

func (b *BasePathFS) Create(name string) (File, error) {
	path, err := b.realPath(name)
	if err != nil {
		return nil, err
	}
	return b.source.Create(path)
}

func (b *BasePathFS) Mkdir(name string, perm os.FileMode) error {
	path, err := b.realPath(name)
	if err != nil {
		return err
	}
	return b.source.Mkdir(path, perm)
}

func (b *BasePathFS) MkdirAll(name string, perm os.FileMode) error {
	path, err := b.realPath(name)
	if err != nil {
		return err
	}
	return b.source.MkdirAll(path, perm)
}

func (b *BasePathFS) Open(name string) (File, error) {
	path, err := b.realPath(name)
	if err != nil {
		return nil, err
	}
	return b.source.Open(path)
}

func (b *BasePathFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	path, err := b.realPath(name)
	if err != nil {
		return nil, err
	}
	return b.source.OpenFile(path, flag, perm)
}

func (b *BasePathFS) Remove(name string) error {
	path, err := b.realPath(name)
	if err != nil {
		return err
	}
	return b.source.Remove(path)
}

func (b *BasePathFS) RemoveAll(name string) error {
	path, err := b.realPath(name)
	if err != nil {
		return err
	}
	return b.source.RemoveAll(path)
}

func (b *BasePathFS) Rename(oldname, newname string) error {
	oldPath, err := b.realPath(oldname)
	if err != nil {
		return err
	}
	newPath, err := b.realPath(newname)
	if err != nil {
		return err
	}
	return b.source.Rename(oldPath, newPath)
}

func (b *BasePathFS) Stat(name string) (os.FileInfo, error) {
	path, err := b.realPath(name)
	if err != nil {
		return nil, err
	}
	return b.source.Stat(path)
}

func (b *BasePathFS) ReadDir(name string) ([]fs.DirEntry, error) {
	path, err := b.realPath(name)
	if err != nil {
		return nil, err
	}
	return b.source.ReadDir(path)
}

func (b *BasePathFS) Chmod(name string, mode os.FileMode) error {
	path, err := b.realPath(name)
	if err != nil {
		return err
	}
	return b.source.Chmod(path, mode)
}

func (b *BasePathFS) Chown(name string, uid, gid int) error {
	path, err := b.realPath(name)
	if err != nil {
		return err
	}
	return b.source.Chown(path, uid, gid)
}

func (b *BasePathFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	path, err := b.realPath(name)
	if err != nil {
		return err
	}
	return b.source.Chtimes(path, atime, mtime)
}
