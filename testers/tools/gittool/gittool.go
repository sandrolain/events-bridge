package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "gitcli",
		Short: "Git source tester",
		Long:  "A simple Git CLI with only a send command that commits and pushes periodically.",
	}

	// flags
	var (
		remote        string
		branch        string
		interval      string
		filename      string
		commitMessage string
		username      string
		password      string
	)

	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Periodically commit and push to a git repo",
		RunE: func(cmd *cobra.Command, args []string) error {
			if remote == "" {
				return fmt.Errorf("--remote is required")
			}
			if _, err := time.ParseDuration(interval); err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}
			return runGitSend(remote, branch, interval, filename, commitMessage, username, password)
		},
	}

	sendCmd.Flags().StringVar(&remote, "remote", "", "Remote git repository URL (required)")
	sendCmd.Flags().StringVar(&branch, "branch", "main", "Branch to commit to")
	sendCmd.Flags().StringVar(&interval, "interval", "10s", "Interval between commits (e.g. 10s, 1m)")
	sendCmd.Flags().StringVar(&filename, "filename", "data.txt", "File to update in the repo")
	sendCmd.Flags().StringVar(&commitMessage, "message", "Automated commit", "Commit message")
	sendCmd.Flags().StringVar(&username, "username", "", "Username for remote repository (optional)")
	sendCmd.Flags().StringVar(&password, "password", "", "Password or token for remote repository (optional)")

	root.AddCommand(sendCmd)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runGitSend(remote, branch, interval, filename, message, username, password string) error {
	ctx := context.Background()
	_ = ctx // reserved for future use

	dur, _ := time.ParseDuration(interval)
	// working dir
	tmpDir, err := os.MkdirTemp("", "gittool-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to remove temp dir: %v\n", err)
		}
	}()

	repo, err := cloneOrInitRepo(tmpDir, remote, branch, username, password)
	if err != nil {
		return err
	}

	fmt.Printf("Ready. Remote: %s, branch: %s, file: %s. Interval: %s\n", remote, branch, filename, dur)
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for range ticker.C {
		if err := doCommit(repo, tmpDir, branch, filename, message, username, password, remote); err != nil {
			fmt.Fprintf(os.Stderr, "Commit error: %v\n", err)
		} else {
			fmt.Printf("Committed to %s/%s at %s\n", remote, branch, time.Now().Format(time.RFC3339))
		}
	}
	return nil
}

func cloneOrInitRepo(tmpDir, remote, branch, username, password string) (*git.Repository, error) {
	fmt.Printf("Cloning %s (branch: %s) into %s...\n", remote, branch, tmpDir)
	cloneOpts := &git.CloneOptions{
		URL:           remote,
		Progress:      os.Stdout,
		SingleBranch:  true,
		ReferenceName: plumbing.NewBranchReferenceName(branch),
	}
	if username != "" && password != "" {
		cloneOpts.Auth = &http.BasicAuth{Username: username, Password: password}
	}
	repo, err := git.PlainClone(tmpDir, false, cloneOpts)
	if err == nil {
		return repo, nil
	}
	if err == git.ErrRepositoryNotExists || (err.Error() == "remote repository is empty" || err.Error() == "repository is empty") {
		fmt.Println("Remote repository is empty, initializing new repository...")
		repo, initErr := git.PlainInit(tmpDir, false)
		if initErr != nil {
			return nil, fmt.Errorf("init repo: %w", initErr)
		}
		_, remoteErr := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{remote}})
		if remoteErr != nil {
			return nil, fmt.Errorf("add remote: %w", remoteErr)
		}
		if err := checkoutOrCreateBranch(repo, branch); err != nil {
			return nil, err
		}
		return repo, nil
	}
	if err.Error() == "couldn't find remote ref \"refs/heads/"+branch+"\"" {
		fmt.Printf("Remote branch '%s' not found, cloning default branch and creating it locally...\n", branch)
		cloneOpts2 := &git.CloneOptions{URL: remote, Progress: os.Stdout}
		if username != "" && password != "" {
			cloneOpts2.Auth = &http.BasicAuth{Username: username, Password: password}
		}
		repo, err = git.PlainClone(tmpDir, false, cloneOpts2)
		if err != nil {
			return nil, fmt.Errorf("git clone (default): %w", err)
		}
		if err := checkoutOrCreateBranch(repo, branch); err != nil {
			return nil, err
		}
		return repo, nil
	}
	return nil, fmt.Errorf("git clone error: %w", err)
}

func checkoutOrCreateBranch(repo *git.Repository, branch string) error {
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("get worktree: %w", err)
	}
	branchRef := plumbing.NewBranchReferenceName(branch)
	err = wt.Checkout(&git.CheckoutOptions{Branch: branchRef, Create: true, Force: true})
	if err != nil {
		return fmt.Errorf("checkout branch '%s': %w", branch, err)
	}
	return nil
}

func doCommit(repo *git.Repository, repoPath, branch, filename, message, username, password, remote string) error {
	filePath := filepath.Join(repoPath, filename)
	content := fmt.Sprintf("Automated update at %s\n", time.Now().Format(time.RFC3339))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600) // #nosec G304 -- test tool with controlled path
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to close file: %v\n", err)
		}
	}()
	if _, err := f.WriteString(content); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("get worktree: %w", err)
	}
	if _, err := wt.Add(filename); err != nil {
		return fmt.Errorf("git add: %w", err)
	}
	_, err = wt.Commit(message, &git.CommitOptions{Author: &object.Signature{Name: "gittool-bot", Email: "gittool@example.com", When: time.Now()}})
	if err != nil && err.Error() != "nothing to commit, working tree clean" {
		return fmt.Errorf("git commit: %w", err)
	}
	pushOpts := &git.PushOptions{RemoteName: "origin"}
	if username != "" && password != "" {
		pushOpts.Auth = &http.BasicAuth{Username: username, Password: password}
	}
	if err := repo.Push(pushOpts); err != nil && err != git.NoErrAlreadyUpToDate {
		return fmt.Errorf("git push: %w", err)
	}
	return nil
}
