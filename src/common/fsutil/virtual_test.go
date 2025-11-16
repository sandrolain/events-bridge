package fsutil

import (
	"bytes"
	"io"
	"io/fs"
	"mime/multipart"
	"os"
	"testing"
)

func TestVirtualFS_Open(t *testing.T) {
	data := []byte("test data")
	vfs := NewVirtualFS("/data", data)

	// Test opening existing file
	f, err := vfs.Open("/data")
	if err != nil {
		t.Fatalf("unexpected error opening /data: %v", err)
	}
	defer f.Close()

	// Read and verify content
	content, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("unexpected error reading: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("expected %q, got %q", data, content)
	}

	// Test opening non-existent file
	_, err = vfs.Open("/nonexistent")
	if err != fs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestVirtualFS_Stat(t *testing.T) {
	data := []byte("test data")
	vfs := NewVirtualFS("/data", data)

	info, err := vfs.Stat("/data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Name() != "data" {
		t.Errorf("expected name 'data', got %q", info.Name())
	}
	if info.Size() != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), info.Size())
	}
	if info.IsDir() {
		t.Error("expected file, not directory")
	}

	// Test non-existent file
	_, err = vfs.Stat("/nonexistent")
	if err != fs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestVirtualFS_ReadDir(t *testing.T) {
	data := []byte("test data")
	vfs := NewVirtualFS("/data", data)

	entries, err := vfs.ReadDir("/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Name() != "data" {
		t.Errorf("expected name 'data', got %q", entries[0].Name())
	}

	// Test non-root directory
	_, err = vfs.ReadDir("/subdir")
	if err != fs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestVirtualFS_WriteOperations(t *testing.T) {
	vfs := NewVirtualFS("/data", []byte("test"))

	// All write operations should return ErrPermission
	if _, err := vfs.Create("/new"); err != fs.ErrPermission {
		t.Errorf("Create: expected ErrPermission, got %v", err)
	}
	if err := vfs.Mkdir("/dir", 0755); err != fs.ErrPermission {
		t.Errorf("Mkdir: expected ErrPermission, got %v", err)
	}
	if err := vfs.Remove("/data"); err != fs.ErrPermission {
		t.Errorf("Remove: expected ErrPermission, got %v", err)
	}
	if err := vfs.Rename("/data", "/new"); err != fs.ErrPermission {
		t.Errorf("Rename: expected ErrPermission, got %v", err)
	}
}

func TestVirtualFile_Seek(t *testing.T) {
	data := []byte("0123456789")
	vfs := NewVirtualFS("/data", data)

	f, err := vfs.Open("/data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer f.Close()

	// Seek to position 5
	pos, err := f.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatalf("seek error: %v", err)
	}
	if pos != 5 {
		t.Errorf("expected position 5, got %d", pos)
	}

	// Read from position 5
	buf := make([]byte, 3)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if n != 3 || string(buf) != "567" {
		t.Errorf("expected '567', got %q", buf[:n])
	}
}

func TestMultipartFS_Empty(t *testing.T) {
	mfs, err := NewMultipartFS(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = mfs.Open("/any")
	if err != fs.ErrNotExist {
		t.Errorf("expected ErrNotExist for empty fs, got %v", err)
	}
}

func TestMultipartFS_WithFiles(t *testing.T) {
	// Create a multipart form with test files
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add first file
	part1, err := writer.CreateFormFile("file1", "test.txt")
	if err != nil {
		t.Fatalf("create form file error: %v", err)
	}
	part1.Write([]byte("content1"))

	// Add second file
	part2, err := writer.CreateFormFile("file2", "doc.pdf")
	if err != nil {
		t.Fatalf("create form file error: %v", err)
	}
	part2.Write([]byte("content2"))

	writer.Close()

	// Parse the form
	reader := multipart.NewReader(body, writer.Boundary())
	form, err := reader.ReadForm(10 << 20) // 10MB max
	if err != nil {
		t.Fatalf("read form error: %v", err)
	}
	defer form.RemoveAll()

	// Create filesystem
	mfs, err := NewMultipartFS(form)
	if err != nil {
		t.Fatalf("NewMultipartFS error: %v", err)
	}

	// Test opening first file
	f1, err := mfs.Open("/test.txt")
	if err != nil {
		t.Fatalf("open test.txt error: %v", err)
	}
	defer f1.Close()

	content1, err := io.ReadAll(f1)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(content1) != "content1" {
		t.Errorf("expected 'content1', got %q", content1)
	}

	// Test opening second file
	f2, err := mfs.Open("/doc.pdf")
	if err != nil {
		t.Fatalf("open doc.pdf error: %v", err)
	}
	defer f2.Close()

	content2, err := io.ReadAll(f2)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(content2) != "content2" {
		t.Errorf("expected 'content2', got %q", content2)
	}

	// Test ReadDir
	entries, err := mfs.ReadDir("/")
	if err != nil {
		t.Fatalf("readdir error: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

func TestMultipartFS_Stat(t *testing.T) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile("upload", "file.txt")
	if err != nil {
		t.Fatalf("create form file error: %v", err)
	}
	testData := []byte("test content")
	part.Write(testData)
	writer.Close()

	reader := multipart.NewReader(body, writer.Boundary())
	form, err := reader.ReadForm(10 << 20)
	if err != nil {
		t.Fatalf("read form error: %v", err)
	}
	defer form.RemoveAll()

	mfs, err := NewMultipartFS(form)
	if err != nil {
		t.Fatalf("NewMultipartFS error: %v", err)
	}

	info, err := mfs.Stat("/file.txt")
	if err != nil {
		t.Fatalf("stat error: %v", err)
	}

	if info.Name() != "file.txt" {
		t.Errorf("expected name 'file.txt', got %q", info.Name())
	}
	if info.Size() != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), info.Size())
	}
	if info.Mode()&os.ModePerm != 0444 {
		t.Errorf("expected mode 0444, got %o", info.Mode()&os.ModePerm)
	}
}

func TestMultipartFS_WriteOperations(t *testing.T) {
	mfs, _ := NewMultipartFS(nil)

	// All write operations should return ErrPermission
	if _, err := mfs.Create("/new"); err != fs.ErrPermission {
		t.Errorf("Create: expected ErrPermission, got %v", err)
	}
	if err := mfs.Mkdir("/dir", 0755); err != fs.ErrPermission {
		t.Errorf("Mkdir: expected ErrPermission, got %v", err)
	}
	if err := mfs.Remove("/file"); err != fs.ErrPermission {
		t.Errorf("Remove: expected ErrPermission, got %v", err)
	}
}
