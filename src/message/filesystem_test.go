package message

import (
	"io/fs"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFilesystem is a minimal mock that implements fsutil.Filesystem for testing
type mockFilesystem struct {
	name string
}

func (m *mockFilesystem) Create(name string) (fsutil.File, error)      { return nil, nil }
func (m *mockFilesystem) Mkdir(name string, perm fs.FileMode) error    { return nil }
func (m *mockFilesystem) MkdirAll(path string, perm fs.FileMode) error { return nil }
func (m *mockFilesystem) Open(name string) (fsutil.File, error)        { return nil, nil }
func (m *mockFilesystem) OpenFile(name string, flag int, perm fs.FileMode) (fsutil.File, error) {
	return nil, nil
}
func (m *mockFilesystem) Remove(name string) error                          { return nil }
func (m *mockFilesystem) RemoveAll(path string) error                       { return nil }
func (m *mockFilesystem) Rename(oldname, newname string) error              { return nil }
func (m *mockFilesystem) Stat(name string) (fs.FileInfo, error)             { return nil, nil }
func (m *mockFilesystem) ReadDir(name string) ([]fs.DirEntry, error)        { return nil, nil }
func (m *mockFilesystem) Chmod(name string, mode fs.FileMode) error         { return nil }
func (m *mockFilesystem) Chown(name string, uid, gid int) error             { return nil }
func (m *mockFilesystem) Chtimes(name string, atime, mtime time.Time) error { return nil }

func TestRunnerMessage_GetFilesystem_Fallback(t *testing.T) {
	t.Parallel()

	// Create a test filesystem
	testFS := &mockFilesystem{name: "test"}

	original := &stubSourceMessage{
		filesystem: testFS,
	}
	msg := NewRunnerMessage(original)

	// Should return the original filesystem
	filesystem, err := msg.GetFilesystem()
	require.NoError(t, err)
	assert.NotNil(t, filesystem)
	assert.Equal(t, "test", filesystem.(*mockFilesystem).name)
}

func TestRunnerMessage_GetFilesystem_Override(t *testing.T) {
	t.Parallel()

	// Create filesystems
	originalFS := &mockFilesystem{name: "original"}
	overrideFS := &mockFilesystem{name: "override"}

	original := &stubSourceMessage{
		filesystem: originalFS,
	}
	msg := NewRunnerMessage(original)

	// Override the filesystem
	msg.SetFilesystem(overrideFS)

	// Should return the override filesystem
	filesystem, err := msg.GetFilesystem()
	require.NoError(t, err)
	assert.Equal(t, overrideFS, filesystem)
	assert.Equal(t, "override", filesystem.(*mockFilesystem).name)
}

func TestRunnerMessage_GetFilesystem_Error(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{
		filesystemErr: fs.ErrNotExist,
	}
	msg := NewRunnerMessage(original)

	// Should return error from original
	filesystem, err := msg.GetFilesystem()
	assert.ErrorIs(t, err, fs.ErrNotExist)
	assert.Nil(t, filesystem)
}

func TestRunnerMessage_GetFilesystem_Nil(t *testing.T) {
	t.Parallel()

	original := &stubSourceMessage{}
	msg := NewRunnerMessage(original)

	// Should return nil filesystem and no error
	filesystem, err := msg.GetFilesystem()
	assert.NoError(t, err)
	assert.Nil(t, filesystem)
}

func TestRunnerMessage_SetFilesystem(t *testing.T) {
	t.Parallel()

	testFS := &mockFilesystem{name: "test"}

	original := &stubSourceMessage{}
	msg := NewRunnerMessage(original)

	// Set filesystem
	msg.SetFilesystem(testFS)

	// Should return the set filesystem
	filesystem, err := msg.GetFilesystem()
	require.NoError(t, err)
	assert.NotNil(t, filesystem)
	assert.Equal(t, "test", filesystem.(*mockFilesystem).name)
}

func TestRunnerMessage_GetFilesystem_ThreadSafety(t *testing.T) {
	t.Parallel()

	testFS := &mockFilesystem{name: "test"}

	original := &stubSourceMessage{}
	msg := NewRunnerMessage(original)

	// Test concurrent access
	done := make(chan bool, 2)

	go func() {
		msg.SetFilesystem(testFS)
		done <- true
	}()

	go func() {
		_, _ = msg.GetFilesystem()
		done <- true
	}()

	<-done
	<-done

	// Should not panic and filesystem should be set
	filesystem, err := msg.GetFilesystem()
	assert.NoError(t, err)
	assert.NotNil(t, filesystem)
}
