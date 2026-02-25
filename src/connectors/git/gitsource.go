package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	// Path is the local directory path for the git repository
	Path string `mapstructure:"path" validate:"required"`

	// RemoteURL is the git remote URL (https:// or ssh://)
	RemoteURL string `mapstructure:"remoteUrl" validate:"required"`

	// Remote is the name of the git remote (default: "origin")
	Remote string `mapstructure:"remote" default:"origin" validate:"required"`

	// Branch is the git branch to monitor
	Branch string `mapstructure:"branch" validate:"required"`

	// Username for HTTPS authentication
	Username string `mapstructure:"username"`

	// Password for HTTPS authentication (supports secrets: plain, env:VAR, file:/path)
	Password string `mapstructure:"password"` //nolint:gosec // user-configured credential field

	// SubDir filters changes to a specific subdirectory
	SubDir string `mapstructure:"subdir"`

	// PollInterval is the duration between checks for new commits (0 means no polling)
	PollInterval time.Duration `mapstructure:"pollInterval" default:"10s" validate:"gte=0"`

	// SSH authentication fields

	// SSHKeyFile is the path to the SSH private key for authentication
	SSHKeyFile string `mapstructure:"sshKeyFile"`

	// SSHKeyPassphrase is the passphrase for the SSH private key (supports secrets)
	SSHKeyPassphrase string `mapstructure:"sshKeyPassphrase"`

	// SSHKnownHostsFile is the path to known_hosts file (optional, for host verification)
	SSHKnownHostsFile string `mapstructure:"sshKnownHostsFile"`

	// Security settings

	// VerifySSL controls SSL certificate verification for HTTPS (default: true)
	VerifySSL bool `mapstructure:"verifySSL" default:"true"`

	// StrictBranchValidation enables strict branch name validation (default: true)
	StrictBranchValidation bool `mapstructure:"strictBranchValidation" default:"true"`
}

type GitSource struct {
	cfg      *SourceConfig
	slog     *slog.Logger
	c        chan *message.RunnerMessage
	mu       sync.Mutex
	lastHash plumbing.Hash
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// validateBranchName validates a git branch name to prevent command injection
// and path traversal attacks.
// Allows alphanumeric characters, underscores, hyphens, and forward slashes.
func validateBranchName(branch string, strict bool) error {
	if branch == "" {
		return fmt.Errorf("branch name cannot be empty")
	}

	// Check for path traversal attempts
	if strings.Contains(branch, "..") {
		return fmt.Errorf("branch name contains path traversal sequence: %s", branch)
	}

	// Check for dangerous characters that could be used for command injection
	dangerousChars := []string{";", "&", "|", "$", "`", "(", ")", "<", ">", "\n", "\r", "\\", "\"", "'"}
	for _, char := range dangerousChars {
		if strings.Contains(branch, char) {
			return fmt.Errorf("branch name contains dangerous character: %s", char)
		}
	}

	if strict {
		// Strict mode: only allow safe characters
		// Pattern: alphanumeric, underscore, hyphen, forward slash
		matched, err := regexp.MatchString(`^[a-zA-Z0-9/_-]+$`, branch)
		if err != nil {
			return fmt.Errorf("branch name validation regex error: %w", err)
		}
		if !matched {
			return fmt.Errorf("branch name contains invalid characters (strict mode): %s", branch)
		}
	}

	// Additional checks for common injection patterns
	if strings.HasPrefix(branch, "-") {
		return fmt.Errorf("branch name cannot start with hyphen: %s", branch)
	}

	return nil
}

// buildAuthMethod creates the appropriate authentication method based on configuration.
// Priority: SSH key > Username/Password > None
// buildSSHAuth creates SSH authentication from key file
func (s *GitSource) buildSSHAuth() (*ssh.PublicKeys, error) {
	passphrase := ""
	if s.cfg.SSHKeyPassphrase != "" {
		resolvedPassphrase, err := secrets.Resolve(s.cfg.SSHKeyPassphrase)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve SSH passphrase: %w", err)
		}
		passphrase = resolvedPassphrase
	}

	var auth *ssh.PublicKeys
	var err error
	if passphrase != "" {
		auth, err = ssh.NewPublicKeysFromFile("git", s.cfg.SSHKeyFile, passphrase)
	} else {
		auth, err = ssh.NewPublicKeysFromFile("git", s.cfg.SSHKeyFile, "")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load SSH key: %w", err)
	}

	// Configure host key callback if known_hosts file is provided
	if s.cfg.SSHKnownHostsFile != "" {
		callback, err := ssh.NewKnownHostsCallback(s.cfg.SSHKnownHostsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load known_hosts: %w", err)
		}
		auth.HostKeyCallback = callback
	}

	return auth, nil
}

func (s *GitSource) buildAuthMethod() (transport.AuthMethod, error) {
	// SSH authentication has priority
	if s.cfg.SSHKeyFile != "" {
		return s.buildSSHAuth()
	}

	// HTTP Basic authentication
	if s.cfg.Username != "" && s.cfg.Password != "" {
		resolvedPassword, err := secrets.Resolve(s.cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve password: %w", err)
		}
		return &http.BasicAuth{
			Username: s.cfg.Username,
			Password: resolvedPassword,
		}, nil
	}

	// No authentication
	return nil, nil
}

func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate branch name to prevent command injection
	if err := validateBranchName(cfg.Branch, cfg.StrictBranchValidation); err != nil {
		return nil, fmt.Errorf("invalid branch name: %w", err)
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

// prepareRepoPath ensures a valid repository path exists, creating temp dir if needed
func (s *GitSource) prepareRepoPath() (string, error) {
	repoPath := s.cfg.Path
	if repoPath == "" {
		tmpDir, err := os.MkdirTemp("", "gitsource-*")
		if err != nil {
			return "", fmt.Errorf("failed to create temp dir: %w", err)
		}
		repoPath = tmpDir
	}
	return repoPath, nil
}

// setAuthOnOptions applies authentication method to git options
func (s *GitSource) setAuthOnOptions(authMethod transport.AuthMethod, opts interface{}) {
	if authMethod == nil {
		return
	}

	if sshAuth, ok := authMethod.(*ssh.PublicKeys); ok {
		switch o := opts.(type) {
		case *git.CloneOptions:
			o.Auth = sshAuth
		case *git.FetchOptions:
			o.Auth = sshAuth
		}
	} else if httpAuth, ok := authMethod.(*http.BasicAuth); ok {
		switch o := opts.(type) {
		case *git.CloneOptions:
			o.Auth = httpAuth
		case *git.FetchOptions:
			o.Auth = httpAuth
		}
	}
}

// openOrCloneRepo opens existing repository or clones a new one
func (s *GitSource) openOrCloneRepo(repoPath string, authMethod transport.AuthMethod) (*git.Repository, error) {
	if _, err := os.Stat(filepath.Join(repoPath, ".git")); os.IsNotExist(err) {
		// Clone if not present
		cloneOpts := &git.CloneOptions{
			URL:           s.cfg.RemoteURL,
			Progress:      nil,
			SingleBranch:  true,
			ReferenceName: plumbing.NewBranchReferenceName(s.cfg.Branch),
		}
		s.setAuthOnOptions(authMethod, cloneOpts)

		repo, err := git.PlainClone(repoPath, false, cloneOpts)
		if err != nil {
			return nil, fmt.Errorf("git clone error: %w", err)
		}
		return repo, nil
	}

	// Open existing and fetch
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open repo: %w", err)
	}

	fetchOpts := &git.FetchOptions{
		RemoteName: s.cfg.Remote,
		Progress:   nil,
		Force:      true,
		Tags:       git.AllTags,
	}
	s.setAuthOnOptions(authMethod, fetchOpts)

	if err := repo.Fetch(fetchOpts); err != nil && err != git.NoErrAlreadyUpToDate {
		s.slog.Warn("failed to fetch repository", "error", err)
	}
	return repo, nil
}

// getNewReference retrieves the remote branch reference
func (s *GitSource) getNewReference(repo *git.Repository) (plumbing.Hash, error) {
	remoteName := s.cfg.Remote
	if remoteName == "" {
		remoteName = "origin"
	}
	refName := fmt.Sprintf("refs/remotes/%s/%s", remoteName, s.cfg.Branch)
	newRef, err := repo.Reference(plumbing.ReferenceName(refName), true)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("cannot get ref: %w", err)
	}
	return newRef.Hash(), nil
}

// processCommitFiles extracts file changes from a commit matching the configured subdir
func (s *GitSource) processCommitFiles(c *object.Commit, changes *[]map[string]interface{}, found *bool) {
	files, err := c.Files()
	if err != nil {
		s.slog.Warn("failed to get commit files", "error", err)
		return
	}

	if err := files.ForEach(func(f *object.File) error {
		if s.cfg.SubDir == "" || strings.HasPrefix(f.Name, s.cfg.SubDir) {
			*found = true
			*changes = append(*changes, map[string]interface{}{
				"commit":  c.Hash.String(),
				"message": c.Message,
				"author":  c.Author.Name,
				"email":   c.Author.Email,
				"when":    c.Author.When,
				"file":    f.Name,
			})
		}
		return nil
	}); err != nil {
		s.slog.Warn("failed to iterate files", "error", err)
	}
}

// collectCommitChanges iterates commits and collects file changes
func (s *GitSource) collectCommitChanges(repo *git.Repository, oldHash, newHash plumbing.Hash) ([]map[string]interface{}, bool) {
	cIter, err := repo.Log(&git.LogOptions{From: newHash})
	if err != nil {
		s.slog.Error("cannot get log", "err", err)
		return nil, false
	}

	var changes []map[string]interface{}
	found := false

	if err := cIter.ForEach(func(c *object.Commit) error {
		if !oldHash.IsZero() && c.Hash == oldHash {
			return storer.ErrStop
		}
		s.processCommitFiles(c, &changes, &found)
		return nil
	}); err != nil {
		s.slog.Warn("failed to iterate commits", "error", err)
	}

	return changes, found
}

func (s *GitSource) checkForChanges() {
	// Prepare repository path
	repoPath, err := s.prepareRepoPath()
	if err != nil {
		s.slog.Error("failed to prepare repo path", "err", err)
		return
	}

	// Build authentication method
	authMethod, err := s.buildAuthMethod()
	if err != nil {
		s.slog.Error("failed to build authentication", "err", err)
		return
	}

	// Open or clone repository
	repo, err := s.openOrCloneRepo(repoPath, authMethod)
	if err != nil {
		s.slog.Error("failed to access repository", "err", err)
		return
	}

	// Get new reference hash
	newHash, err := s.getNewReference(repo)
	if err != nil {
		s.slog.Error("failed to get reference", "err", err)
		return
	}

	// Check if hash changed
	s.mu.Lock()
	oldHash := s.lastHash
	if oldHash == newHash {
		s.mu.Unlock()
		return // no changes
	}
	s.lastHash = newHash
	s.mu.Unlock()

	// Collect changes from commits
	changes, found := s.collectCommitChanges(repo, oldHash, newHash)
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
