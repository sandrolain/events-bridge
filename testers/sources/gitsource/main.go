package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
)

func main() {
	opts := parseFlags()
	tmpDir := createTempDir()
	defer os.RemoveAll(tmpDir)

	repo := prepareRepo(tmpDir, opts)

	for {
		if err := doCommit(repo, tmpDir, opts.branch, opts.filename, opts.commitMessage, opts.username, opts.password, opts.remote); err != nil {
			fmt.Fprintf(os.Stderr, "Commit error: %v\n", err)
		} else {
			fmt.Printf("Committed to %s/%s at %s\n", opts.remote, opts.branch, time.Now().Format(time.RFC3339))
		}
		time.Sleep(opts.dur)
	}
}

type options struct {
	remote        string
	branch        string
	interval      string
	filename      string
	commitMessage string
	username      string
	password      string
	dur           time.Duration
}

func parseFlags() *options {
	remote := flag.String("remote", "", "Remote git repository URL (required)")
	branch := flag.String("branch", "main", "Branch to commit to")
	interval := flag.String("interval", "10s", "Interval between commits (e.g. 10s, 1m)")
	filename := flag.String("filename", "data.txt", "File to update in the repo")
	commitMessage := flag.String("message", "Automated commit", "Commit message")
	username := flag.String("username", "", "Username for remote repository (optional)")
	password := flag.String("password", "", "Password or token for remote repository (optional)")
	flag.Parse()

	if *remote == "" {
		fmt.Fprintf(os.Stderr, "--remote is required\n")
		os.Exit(1)
	}

	dur, err := time.ParseDuration(*interval)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid interval: %v\n", err)
		os.Exit(1)
	}

	return &options{
		remote:        *remote,
		branch:        *branch,
		interval:      *interval,
		filename:      *filename,
		commitMessage: *commitMessage,
		username:      *username,
		password:      *password,
		dur:           dur,
	}
}

func createTempDir() string {
	tmpDir, err := os.MkdirTemp("", "gitsource-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	return tmpDir
}

func prepareRepo(tmpDir string, opts *options) *git.Repository {
	fmt.Printf("Cloning %s (branch: %s) into %s...\n", opts.remote, opts.branch, tmpDir)
	cloneOpts := &git.CloneOptions{
		URL:           opts.remote,
		Progress:      os.Stdout,
		SingleBranch:  true,
		ReferenceName: plumbing.NewBranchReferenceName(opts.branch),
	}
	if opts.username != "" && opts.password != "" {
		cloneOpts.Auth = &http.BasicAuth{
			Username: opts.username,
			Password: opts.password,
		}
	}
	repo, err := git.PlainClone(tmpDir, false, cloneOpts)
	if err == nil {
		return repo
	}
	// Handle empty repository case
	if err == git.ErrRepositoryNotExists || (err.Error() == "remote repository is empty" || err.Error() == "repository is empty") {
		fmt.Println("Remote repository is empty, initializing new repository...")
		repo, initErr := git.PlainInit(tmpDir, false)
		if initErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to init repo: %v\n", initErr)
			os.Exit(1)
		}
		// Add remote origin
		_, remoteErr := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{opts.remote}})
		if remoteErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to add remote: %v\n", remoteErr)
			os.Exit(1)
		}
		checkoutOrCreateBranch(repo, opts.branch)
		return repo
	}
	if err.Error() == "couldn't find remote ref \"refs/heads/"+opts.branch+"\"" {
		// Branch does not exist remotely, try clone default branch and checkout/create requested branch
		fmt.Printf("Remote branch '%s' not found, cloning default branch and creating it locally...\n", opts.branch)
		cloneOpts2 := &git.CloneOptions{
			URL:      opts.remote,
			Progress: os.Stdout,
		}
		if opts.username != "" && opts.password != "" {
			cloneOpts2.Auth = &http.BasicAuth{
				Username: opts.username,
				Password: opts.password,
			}
		}
		repo, err = git.PlainClone(tmpDir, false, cloneOpts2)
		if err != nil {
			fmt.Fprintf(os.Stderr, "git clone error (default branch): %v\n", err)
			os.Exit(1)
		}
		checkoutOrCreateBranch(repo, opts.branch)
		return repo
	}
	fmt.Fprintf(os.Stderr, "git clone error: %v\n", err)
	os.Exit(1)
	return nil
}

func checkoutOrCreateBranch(repo *git.Repository, branch string) {
	wt, err := repo.Worktree()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get worktree: %v\n", err)
		os.Exit(1)
	}
	branchRef := plumbing.NewBranchReferenceName(branch)
	err = wt.Checkout(&git.CheckoutOptions{
		Branch: branchRef,
		Create: true,
		Force:  true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to checkout branch '%s': %v\n", branch, err)
		os.Exit(1)
	}
}

func doCommit(repo *git.Repository, repoPath, branch, filename, message, username, password, remote string) error {
	filePath := filepath.Join(repoPath, filename)
	content := fmt.Sprintf("Automated update at %s\n", time.Now().Format(time.RFC3339))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("get worktree: %w", err)
	}
	// git add
	if _, err := wt.Add(filename); err != nil {
		return fmt.Errorf("git add: %w", err)
	}
	// git commit
	_, err = wt.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "gitsource-bot",
			Email: "gitsource@example.com",
			When:  time.Now(),
		},
	})
	if err != nil && err.Error() != "nothing to commit, working tree clean" {
		return fmt.Errorf("git commit: %w", err)
	}
	// git push
	pushOpts := &git.PushOptions{
		RemoteName: "origin",
	}
	if username != "" && password != "" {
		pushOpts.Auth = &http.BasicAuth{
			Username: username,
			Password: password,
		}
	}
	if err := repo.Push(pushOpts); err != nil && err != git.NoErrAlreadyUpToDate {
		return fmt.Errorf("git push: %w", err)
	}
	return nil
}
