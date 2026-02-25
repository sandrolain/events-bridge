package main

import (
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

const (
	errWriteFile    = "failed to write file: %v"
	errGitAdd       = "failed to git add: %v"
	errGitCommit    = "failed to git commit: %v"
	errCheckTimeout = "checkForChanges did not return in time"

	gitAuthorArg = "--author=Test <test@example.com>"
	gitBranch    = "main"
)

func mustRunGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...) //nolint:gosec // test helper always invokes the "git" binary with controlled arguments
	cmd.Dir = dir
	env := os.Environ()
	env = append(env,
		"GIT_AUTHOR_NAME=Test",
		"GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Test",
		"GIT_COMMITTER_EMAIL=test@example.com",
	)
	cmd.Env = env
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to run git %v: %v", args, err)
	}
}

func initBareRepo(t *testing.T, dir string) {
	t.Helper()
	cmd := exec.Command("git", "init", "--bare", "--initial-branch", gitBranch)
	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		mustRunGit(t, dir, "init", "--bare")
		mustRunGit(t, dir, "symbolic-ref", "HEAD", "refs/heads/"+gitBranch)
	}
}

func initWorkRepo(t *testing.T, dir string) {
	t.Helper()
	cmd := exec.Command("git", "init", "-b", gitBranch)
	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		mustRunGit(t, dir, "init")
		mustRunGit(t, dir, "checkout", "-B", gitBranch)
	}
}

func TestGitSourceCheckForChangesCloneAndFetch(t *testing.T) {
	remoteDir := t.TempDir()
	initBareRepo(t, remoteDir)

	localParent := t.TempDir()
	localDir := filepath.Join(localParent, "gitsource-local")
	cmd := exec.Command("git", "clone", remoteDir, localDir) // #nosec G204 - test with controlled paths
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to clone: %v", err)
	}
	filePath := filepath.Join(localDir, "foo.txt")
	// #nosec G306 - test file, not sensitive
	if err := os.WriteFile(filePath, []byte("bar"), 0644); err != nil {
		t.Fatalf(errWriteFile, err)
	}
	mustRunGit(t, localDir, "add", ".")
	mustRunGit(t, localDir, "commit", "-m", "add foo", gitAuthorArg)
	mustRunGit(t, localDir, "push", "origin", gitBranch)

	cfg := &SourceConfig{
		RemoteURL:    remoteDir,
		Branch:       gitBranch,
		Path:         "",
		Remote:       "origin",
		PollInterval: 0,
	}
	src := &GitSource{cfg: cfg, slog: slog.Default()}
	src.c = make(chan *message.RunnerMessage, 1)
	src.lastHash = plumbing.Hash{}

	done := make(chan struct{})
	go func() {
		src.checkForChanges()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error(errCheckTimeout)
	}
}

func TestGitSourceCheckForChangesSubDirNoMatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gitsource-test-repo-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Errorf("failed to remove temp dir: %v", err)
		}
	}()
	initWorkRepo(t, tmpDir)
	filePath := filepath.Join(tmpDir, "test.txt")
	// #nosec G306 - test file, not sensitive
	if err := os.WriteFile(filePath, []byte("hello world"), 0644); err != nil {
		t.Fatalf(errWriteFile, err)
	}
	mustRunGit(t, tmpDir, "add", ".")
	mustRunGit(t, tmpDir, "commit", "-m", "initial commit", gitAuthorArg)

	cfg := &SourceConfig{
		RemoteURL:    tmpDir,
		Branch:       gitBranch,
		Path:         tmpDir,
		Remote:       "origin",
		SubDir:       "notfound/",
		PollInterval: 0,
	}
	src := &GitSource{cfg: cfg, slog: slog.Default()}
	src.lastHash = plumbing.Hash{}
	src.c = make(chan *message.RunnerMessage, 1)
	done := make(chan struct{})
	go func() {
		src.checkForChanges()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error(errCheckTimeout)
	}
}

func TestGitSourceCheckForChangesRealRepo(t *testing.T) {
	// Create a temporary directory for the git repo
	tmpDir, err := os.MkdirTemp("", "gitsource-test-repo-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Errorf("failed to remove temp dir: %v", err)
		}
	}()

	// Initialize a new git repository
	initWorkRepo(t, tmpDir)

	// Create a file and commit it
	filePath := filepath.Join(tmpDir, "test.txt")
	// #nosec G306 - test file, not sensitive
	if err := os.WriteFile(filePath, []byte("hello world"), 0644); err != nil {
		t.Fatalf(errWriteFile, err)
	}
	mustRunGit(t, tmpDir, "add", ".")
	mustRunGit(t, tmpDir, "commit", "-m", "initial commit", gitAuthorArg)

	// Setup config for local repo
	cfg := &SourceConfig{
		RemoteURL:    tmpDir,
		Branch:       gitBranch,
		Path:         tmpDir,
		Remote:       "origin",
		PollInterval: 0,
	}
	src := &GitSource{cfg: cfg, slog: slog.Default()}
	src.lastHash = plumbing.Hash{} // force detection
	src.c = make(chan *message.RunnerMessage, 1)

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
		t.Error(errCheckTimeout)
	}
}

func TestNewSourceInvalidConfig(t *testing.T) {
	cfg := new(SourceConfig)
	err := utils.ParseConfig(map[string]any{
		"remoteUrl": 123,
		"branch":    456,
	}, cfg)
	if err == nil {
		t.Error("expected error when options have invalid types")
	}
}

func TestGitSourceProduceAndClose(t *testing.T) {
	// Create temp directories manually to ensure proper cleanup
	remoteDir, err := os.MkdirTemp("", "git-test-remote-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(remoteDir)

	initBareRepo(t, remoteDir)

	localParent, err := os.MkdirTemp("", "git-test-local-parent-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(localParent)

	localDir := filepath.Join(localParent, "gitsource-local")
	cmd := exec.Command("git", "clone", remoteDir, localDir) // #nosec G204 - test with controlled paths
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to clone: %v", err)
	}
	filePath := filepath.Join(localDir, "foo.txt")
	// #nosec G306 - test file, not sensitive
	if err := os.WriteFile(filePath, []byte("data"), 0644); err != nil {
		t.Fatalf(errWriteFile, err)
	}
	mustRunGit(t, localDir, "add", ".")
	mustRunGit(t, localDir, "commit", "-m", "seed", gitAuthorArg)
	mustRunGit(t, localDir, "push", "origin", gitBranch)

	cloneParent, err := os.MkdirTemp("", "git-test-clone-parent-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(cloneParent)

	clonePath := filepath.Join(cloneParent, "gitsource-clone")
	cfg := new(SourceConfig)
	if err := utils.ParseConfig(map[string]any{
		"remoteUrl": remoteDir,
		"branch":    gitBranch,
		"path":      clonePath,
		"remote":    "origin",
	}, cfg); err != nil {
		t.Fatalf("failed to parse config: %v", err)
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
	cfg := &SourceConfig{
		RemoteURL: "",
		Branch:    gitBranch,
		Remote:    "origin",
	}
	src := &GitSource{cfg: cfg, slog: slog.Default()}
	src.checkForChanges() // should not panic, just log error
}

func TestGitSourceCheckForChangesSameHash(t *testing.T) {
	cfg := &SourceConfig{
		RemoteURL: "https://github.com/sandrolain/events-bridge.git",
		Branch:    gitBranch,
		Remote:    "origin",
	}
	src := &GitSource{cfg: cfg, slog: slog.Default()}
	src.lastHash = plumbing.NewHash("abc123")
	src.mu.Lock()
	src.lastHash = plumbing.NewHash("abc123")
	src.mu.Unlock()
	// This test is a placeholder: in real tests, use a mock repo
}

func TestGitSourceCheckForChangesTempDirError(t *testing.T) {
	// Simulate error creating temp dir by setting an invalid TMPDIR
	oldTmp := os.Getenv("TMPDIR")
	err := os.Setenv("TMPDIR", "/dev/null/doesnotexist")
	if err != nil {
		t.Fatalf("failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTmp); err != nil {
			t.Errorf("failed to restore TMPDIR: %v", err)
		}
	}()

	cfg := &SourceConfig{
		RemoteURL: "dummy",
		Branch:    gitBranch,
		Remote:    "origin",
		Path:      "", // triggers temp dir creation
	}
	src := &GitSource{cfg: cfg, slog: slog.Default()}
	src.checkForChanges() // Should log error, not panic
}

func TestGitSourceCheckForChangesOpenRepoError(t *testing.T) {
	cfg := &SourceConfig{
		RemoteURL: "dummy",
		Branch:    gitBranch,
		Remote:    "origin",
		Path:      "/dev/null/doesnotexist",
	}
	src := &GitSource{cfg: cfg, slog: slog.Default()}
	src.checkForChanges() // Should log error, not panic
}
