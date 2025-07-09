package main

import (
	"io/ioutil"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
)

func TestGitSourceCheckForChangesCloneAndFetch(t *testing.T) {
	remoteDir, err := ioutil.TempDir("", "gitsource-remote-")
	if err != nil {
		t.Fatalf("failed to create remote dir: %v", err)
	}
	defer os.RemoveAll(remoteDir)
	cmd := exec.Command("git", "init", "--bare")
	cmd.Dir = remoteDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to init bare repo: %v", err)
	}

	localDir, err := ioutil.TempDir("", "gitsource-local-")
	if err != nil {
		t.Fatalf("failed to create local dir: %v", err)
	}
	defer os.RemoveAll(localDir)
	cmd = exec.Command("git", "clone", remoteDir, localDir)
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to clone: %v", err)
	}
	filePath := filepath.Join(localDir, "foo.txt")
	if err := os.WriteFile(filePath, []byte("bar"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	cmd = exec.Command("git", "add", ".")
	cmd.Dir = localDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git add: %v", err)
	}
	cmd = exec.Command("git", "commit", "-m", "add foo", "--author=Test <test@example.com>")
	cmd.Dir = localDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git commit: %v", err)
	}
	cmd = exec.Command("git", "push", "origin", "master")
	cmd.Dir = localDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git push: %v", err)
	}

	cfg := &sources.SourceGitConfig{
		RemoteURL:    remoteDir,
		Branch:       "master",
		Path:         "",
		Remote:       "origin",
		PollInterval: 0,
	}
	src := &GitSource{config: cfg, slog: slog.Default()}
	src.c = make(chan message.Message, 1)
	src.lastHash = plumbing.Hash{}

	done := make(chan struct{})
	go func() {
		src.checkForChanges()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("checkForChanges did not return in time")
	}
}

func TestGitSourceCheckForChangesSubDirNoMatch(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "gitsource-test-repo-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	cmd := exec.Command("git", "init")
	cmd.Dir = tmpDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to init git repo: %v", err)
	}
	filePath := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(filePath, []byte("hello world"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	cmd = exec.Command("git", "add", ".")
	cmd.Dir = tmpDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git add: %v", err)
	}
	cmd = exec.Command("git", "commit", "-m", "initial commit", "--author=Test <test@example.com>")
	cmd.Dir = tmpDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git commit: %v", err)
	}

	cfg := &sources.SourceGitConfig{
		RemoteURL:    tmpDir,
		Branch:       "master",
		Path:         tmpDir,
		Remote:       "origin",
		SubDir:       "notfound/",
		PollInterval: 0,
	}
	src := &GitSource{config: cfg, slog: slog.Default()}
	src.lastHash = plumbing.Hash{}
	src.c = make(chan message.Message, 1)
	done := make(chan struct{})
	go func() {
		src.checkForChanges()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("checkForChanges did not return in time")
	}
}

func TestGitSourceCheckForChangesRealRepo(t *testing.T) {
	// Create a temporary directory for the git repo
	tmpDir, err := ioutil.TempDir("", "gitsource-test-repo-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize a new git repository
	cmd := exec.Command("git", "init")
	cmd.Dir = tmpDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to init git repo: %v", err)
	}

	// Create a file and commit it
	filePath := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(filePath, []byte("hello world"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	cmd = exec.Command("git", "add", ".")
	cmd.Dir = tmpDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git add: %v", err)
	}
	cmd = exec.Command("git", "commit", "-m", "initial commit", "--author=Test <test@example.com>")
	cmd.Dir = tmpDir
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git commit: %v", err)
	}

	// Setup config for local repo
	cfg := &sources.SourceGitConfig{
		RemoteURL:    tmpDir,
		Branch:       "master",
		Path:         tmpDir,
		Remote:       "origin",
		PollInterval: 0,
	}
	src := &GitSource{config: cfg, slog: slog.Default()}
	src.lastHash = plumbing.Hash{} // force detection
	src.c = make(chan message.Message, 1)

	// Should not panic and should process the commit
	done := make(chan struct{})
	go func() {
		src.checkForChanges()
		close(done)
	}()
	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Error("checkForChanges did not return in time")
	}
}

func TestNewSourceInvalidConfig(t *testing.T) {
	_, err := NewSource(&sources.SourceGitConfig{})
	if err == nil {
		t.Error("expected error for missing remote_url and branch")
	}
}

func TestGitSourceProduceAndClose(t *testing.T) {
	cfg := &sources.SourceGitConfig{
		RemoteURL: "https://github.com/sandrolain/events-bridge.git",
		Branch:    "main",
	}
	src, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gs, ok := src.(*GitSource)
	if !ok {
		t.Fatal("expected *GitSource type")
	}
	ch, err := gs.Produce(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ch == nil {
		t.Error("expected non-nil channel")
	}
	err = gs.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}
}

func TestGitSourceCheckForChangesNoRepo(t *testing.T) {
	cfg := &sources.SourceGitConfig{
		RemoteURL: "",
		Branch:    "main",
	}
	src := &GitSource{config: cfg, slog: slog.Default()}
	src.checkForChanges() // should not panic, just log error
}

func TestGitSourceCheckForChangesSameHash(t *testing.T) {
	cfg := &sources.SourceGitConfig{
		RemoteURL: "https://github.com/sandrolain/events-bridge.git",
		Branch:    "main",
	}
	src := &GitSource{config: cfg, slog: slog.Default()}
	src.lastHash = plumbing.NewHash("abc123")
	src.mu.Lock()
	src.lastHash = plumbing.NewHash("abc123")
	src.mu.Unlock()
	// This test is a placeholder: in real tests, use a mock repo
}

func TestGitSourceCheckForChangesTempDirError(t *testing.T) {
	// Simulate error creating temp dir by setting an invalid TMPDIR
	oldTmp := os.Getenv("TMPDIR")
	os.Setenv("TMPDIR", "/dev/null/doesnotexist")
	defer os.Setenv("TMPDIR", oldTmp)

	cfg := &sources.SourceGitConfig{
		RemoteURL: "dummy",
		Branch:    "main",
		Path:      "", // triggers temp dir creation
	}
	src := &GitSource{config: cfg, slog: slog.Default()}
	src.checkForChanges() // Should log error, not panic
}

func TestGitSourceCheckForChangesOpenRepoError(t *testing.T) {
	cfg := &sources.SourceGitConfig{
		RemoteURL: "dummy",
		Branch:    "main",
		Path:      "/dev/null/doesnotexist",
	}
	src := &GitSource{config: cfg, slog: slog.Default()}
	src.checkForChanges() // Should log error, not panic
}
