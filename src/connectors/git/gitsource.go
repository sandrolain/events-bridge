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
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	Path         string        `mapstructure:"path" validate:"required"`
	RemoteURL    string        `mapstructure:"remoteUrl" validate:"required"`
	Remote       string        `mapstructure:"remote" default:"origin" validate:"required"`
	Branch       string        `mapstructure:"branch" validate:"required"`
	Username     string        `mapstructure:"username"`
	Password     string        `mapstructure:"password"`
	SubDir       string        `mapstructure:"subdir"`
	PollInterval time.Duration `mapstructure:"pollInterval" default:"10s" validate:"gte=0"` // in seconds, 0 means no polling
}

type GitSource struct {
	cfg      *SourceConfig
	slog     *slog.Logger
	c        chan *message.RunnerMessage
	mu       sync.Mutex
	lastHash plumbing.Hash
}

func NewSource(opts map[string]any) (connectors.Source, error) {
	cfg, err := common.ParseConfig[SourceConfig](opts)
	if err != nil {
		return nil, err
	}
	return &GitSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "Git Source"),
	}, nil
}

func (s *GitSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)
	s.slog.Info("starting GIT source", "repo", s.cfg.Path, "remote", s.cfg.Remote, "branch", s.cfg.Branch, "subdir", s.cfg.SubDir)
	go s.pollLoop()
	return s.c, nil
}

func (s *GitSource) pollLoop() {
	pollInterval := s.cfg.PollInterval
	for {
		s.checkForChanges()
		time.Sleep(pollInterval)
	}
}

func (s *GitSource) checkForChanges() {
	// Prepare temp dir if Path is empty
	repoPath := s.cfg.Path
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
			URL:           s.cfg.RemoteURL,
			Progress:      nil,
			SingleBranch:  true,
			ReferenceName: plumbing.NewBranchReferenceName(s.cfg.Branch),
		}
		if s.cfg.Username != "" && s.cfg.Password != "" {
			cloneOpts.Auth = &http.BasicAuth{
				Username: s.cfg.Username,
				Password: s.cfg.Password,
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
			RemoteName: s.cfg.Remote,
			Progress:   nil,
			Force:      true,
			Tags:       git.AllTags,
		}
		if s.cfg.Username != "" && s.cfg.Password != "" {
			fetchOpts.Auth = &http.BasicAuth{
				Username: s.cfg.Username,
				Password: s.cfg.Password,
			}
		}
		_ = repo.Fetch(fetchOpts)
	}

	remoteName := s.cfg.Remote
	if remoteName == "" {
		remoteName = "origin"
	}
	refName := fmt.Sprintf("refs/remotes/%s/%s", remoteName, s.cfg.Branch)
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
			if s.cfg.SubDir == "" || strings.HasPrefix(f.Name, s.cfg.SubDir) {
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
