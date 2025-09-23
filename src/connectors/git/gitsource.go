package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
)

type GitSource struct {
	config   *sources.SourceGitConfig
	slog     *slog.Logger
	c        chan *message.RunnerMessage
	started  bool
	mu       sync.Mutex
	lastHash plumbing.Hash
}

func NewSource(cfg *sources.SourceGitConfig) (sources.Source, error) {
	if cfg.RemoteURL == "" || cfg.Branch == "" {
		return nil, fmt.Errorf("remote_url and branch are required for the git source")
	}
	return &GitSource{
		config: cfg,
		slog:   slog.Default().With("context", "GIT"),
	}, nil
}

func (s *GitSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)
	s.slog.Info("starting GIT source", "repo", s.config.Path, "remote", s.config.Remote, "branch", s.config.Branch, "subdir", s.config.SubDir)

	go s.pollLoop()
	s.started = true
	return s.c, nil
}

func (s *GitSource) pollLoop() {
	pollInterval := s.config.PollInterval
	if pollInterval == 0 {
		pollInterval = 10 // default 10s
	}
	for {
		s.checkForChanges()
		time.Sleep(time.Duration(pollInterval) * time.Second)
	}
}

func (s *GitSource) checkForChanges() {
	// Prepare temp dir if Path is empty
	repoPath := s.config.Path
	if repoPath == "" {
		tmpDir, err := os.MkdirTemp("", "gitsource-*")
		if err != nil {
			s.slog.Error("failed to create temp dir", "err", err)
			return
		}
		repoPath = tmpDir
	}

	var repo *git.Repository
	var err error
	if _, err = os.Stat(filepath.Join(repoPath, ".git")); os.IsNotExist(err) {
		// Clone if not present
		cloneOpts := &git.CloneOptions{
			URL:           s.config.RemoteURL,
			Progress:      nil,
			SingleBranch:  true,
			ReferenceName: plumbing.NewBranchReferenceName(s.config.Branch),
		}
		if s.config.Username != "" && s.config.Password != "" {
			cloneOpts.Auth = &http.BasicAuth{
				Username: s.config.Username,
				Password: s.config.Password,
			}
		}
		repo, err = git.PlainClone(repoPath, false, cloneOpts)
		if err != nil {
			s.slog.Error("git clone error", "err", err)
			return
		}
	} else {
		repo, err = git.PlainOpen(repoPath)
		if err != nil {
			s.slog.Error("cannot open repo", "err", err)
			return
		}
		// fetch
		fetchOpts := &git.FetchOptions{
			RemoteName: s.config.Remote,
			Progress:   nil,
			Force:      true,
			Tags:       git.AllTags,
		}
		if s.config.Username != "" && s.config.Password != "" {
			fetchOpts.Auth = &http.BasicAuth{
				Username: s.config.Username,
				Password: s.config.Password,
			}
		}
		_ = repo.Fetch(fetchOpts)
	}

	remoteName := s.config.Remote
	if remoteName == "" {
		remoteName = "origin"
	}
	refName := fmt.Sprintf("refs/remotes/%s/%s", remoteName, s.config.Branch)
	newRef, err := repo.Reference(plumbing.ReferenceName(refName), true)
	if err != nil {
		s.slog.Error("cannot get ref", "err", err)
		return
	}
	newHash := newRef.Hash()

	s.mu.Lock()
	oldHash := s.lastHash
	if oldHash == newHash {
		s.mu.Unlock()
		return // no changes
	}
	s.lastHash = newHash
	s.mu.Unlock()

	// diff between oldHash and newHash
	cIter, err := repo.Log(&git.LogOptions{From: newHash})
	if err != nil {
		s.slog.Error("cannot get log", "err", err)
		return
	}
	var changes []map[string]interface{}
	found := false
	_ = cIter.ForEach(func(c *object.Commit) error {
		if !oldHash.IsZero() && c.Hash == oldHash {
			return storer.ErrStop
		}
		files, _ := c.Files()
		_ = files.ForEach(func(f *object.File) error {
			if s.config.SubDir == "" || strings.HasPrefix(f.Name, s.config.SubDir) {
				found = true
				changes = append(changes, map[string]interface{}{
					"commit":  c.Hash.String(),
					"message": c.Message,
					"author":  c.Author.Name,
					"email":   c.Author.Email,
					"when":    c.Author.When,
					"file":    f.Name,
				})
			}
			return nil
		})
		return nil
	})
	if found {
		msg := &GitMessage{
			changes: changes,
		}
		s.c <- message.NewRunnerMessage(msg)
	}
}

func (s *GitSource) Close() error {
	return nil
}
