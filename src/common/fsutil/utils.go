package fsutil

import (
	"io"
	"os"
	"path/filepath"
)

// CopyToOS recursively copies files from a Filesystem to the OS filesystem
func CopyToOS(src Filesystem, srcPath, dstPath string) error {
	entries, err := src.ReadDir(srcPath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcFile := filepath.Join(srcPath, entry.Name())
		dstFile := filepath.Join(dstPath, entry.Name())

		if entry.IsDir() {
			if err := os.MkdirAll(dstFile, 0750); err != nil {
				return err
			}
			if err := CopyToOS(src, srcFile, dstFile); err != nil {
				return err
			}
		} else {
			// Read from src
			f, err := src.Open(srcFile)
			if err != nil {
				return err
			}
			defer func() {
				if cerr := f.Close(); cerr != nil && err == nil {
					err = cerr
				}
			}()

			// Get file info for permissions
			info, err := entry.Info()
			perm := os.FileMode(0644)
			if err == nil {
				perm = info.Mode().Perm()
			}

			// Create in dst
			//nolint:gosec // G304: destination path is controlled by Docker runner
			out, err := os.OpenFile(dstFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
			if err != nil {
				return err
			}
			defer func() {
				if cerr := out.Close(); cerr != nil && err == nil {
					err = cerr
				}
			}()

			_, err = io.Copy(out, f)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// WriteFile writes data to a file in the filesystem
func WriteFile(fs Filesystem, filename string, data []byte, perm os.FileMode) error {
	f, err := fs.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}
