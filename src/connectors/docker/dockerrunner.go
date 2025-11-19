package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/go-viper/mapstructure/v2"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// EnvFromMetadata maps metadata keys to environment variables
type EnvFromMetadata struct {
	MetadataKey string `mapstructure:"metadataKey" validate:"required"`
	EnvVar      string `mapstructure:"envVar" validate:"required"`
}

// ArtifactConfig defines files/directories to collect after execution
type ArtifactConfig struct {
	ContainerPath string `mapstructure:"containerPath" validate:"required"`
	MetadataKey   string `mapstructure:"metadataKey"`
	Compress      bool   `mapstructure:"compress"`
}

// VolumeMount defines additional volume mounts
type VolumeMount struct {
	HostPath      string `mapstructure:"hostPath" validate:"required"`
	ContainerPath string `mapstructure:"containerPath" validate:"required"`
	ReadOnly      bool   `mapstructure:"readOnly"`
}

// DockerRunnerConfig holds configuration for Docker runner
type DockerRunnerConfig struct {
	// Container config
	Image      string   `mapstructure:"image" validate:"required"`
	PullPolicy string   `mapstructure:"pullPolicy"`
	Commands   []string `mapstructure:"commands"`
	WorkingDir string   `mapstructure:"workingDir"`
	Entrypoint []string `mapstructure:"entrypoint"`
	User       string   `mapstructure:"user"`

	// Filesystem mounting
	MountPath     string `mapstructure:"mountPath"`
	MountReadOnly bool   `mapstructure:"mountReadOnly"`

	// Environment
	Env         []string          `mapstructure:"env"`
	EnvFromMeta []EnvFromMetadata `mapstructure:"envFromMetadata"`

	// Resources
	MemoryLimit string `mapstructure:"memoryLimit"`
	CPULimit    string `mapstructure:"cpuLimit"`
	Timeout     string `mapstructure:"timeout"`

	// Network
	NetworkMode string `mapstructure:"networkMode"`

	// Output
	CaptureOutput     bool   `mapstructure:"captureOutput"`
	OutputToMetadata  bool   `mapstructure:"outputToMetadata"`
	MetadataKeyPrefix string `mapstructure:"metadataKeyPrefix"`

	// Artifacts
	Artifacts []ArtifactConfig `mapstructure:"artifacts"`

	// Behavior
	FailOnNonZero bool `mapstructure:"failOnNonZeroExit"`

	// Security
	ReadOnlyRootfs     bool          `mapstructure:"readOnlyRootfs"`
	CapDrop            []string      `mapstructure:"capDrop"`
	CapAdd             []string      `mapstructure:"capAdd"`
	Privileged         bool          `mapstructure:"privileged"`
	EnableDockerSocket bool          `mapstructure:"enableDockerSocket"`
	Volumes            []VolumeMount `mapstructure:"volumes"`
}

// DockerRunner executes commands in Docker containers
type DockerRunner struct {
	cfg    *DockerRunnerConfig
	client *client.Client
	slog   *slog.Logger
}

// NewRunner creates a new Docker runner instance
func NewRunner(cfg map[string]any, logger *slog.Logger) (connectors.Runner, error) {
	runnerCfg := &DockerRunnerConfig{
		PullPolicy:        "IfNotPresent",
		WorkingDir:        "/workspace",
		MountPath:         "/workspace",
		MountReadOnly:     false,
		NetworkMode:       "none",
		CaptureOutput:     true,
		OutputToMetadata:  true,
		MetadataKeyPrefix: "docker_",
		FailOnNonZero:     true,
		Timeout:           "300s",
		MemoryLimit:       "512m",
		CPULimit:          "1.0",
		ReadOnlyRootfs:    false,
		CapDrop:           []string{"ALL"},
	}

	if err := mapstructure.Decode(cfg, runnerCfg); err != nil {
		return nil, fmt.Errorf("failed to decode docker runner config: %w", err)
	}

	if runnerCfg.Image == "" {
		return nil, fmt.Errorf("docker image is required")
	}

	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	return &DockerRunner{
		cfg:    runnerCfg,
		client: dockerClient,
		slog:   logger,
	}, nil
}

// Process executes the Docker container with the message filesystem
func (r *DockerRunner) Process(msg *message.RunnerMessage) error {
	timeout, err := time.ParseDuration(r.cfg.Timeout)
	if err != nil {
		timeout = 5 * time.Minute
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	r.slog.Info("starting docker execution",
		"image", r.cfg.Image,
		"commands", r.cfg.Commands,
		"workingDir", r.cfg.WorkingDir,
	)

	// Ensure image is available
	if err := r.ensureImage(ctx); err != nil {
		return fmt.Errorf("failed to ensure image: %w", err)
	}

	// Prepare filesystem
	hostDir, cleanup, err := r.prepareHostFilesystem(msg)
	if err != nil {
		return fmt.Errorf("failed to prepare filesystem: %w", err)
	}
	defer cleanup()

	// Build environment variables
	env, err := r.buildEnv(msg)
	if err != nil {
		return fmt.Errorf("failed to build environment: %w", err)
	}

	// Create and start container
	containerID, err := r.createContainer(ctx, hostDir, env)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}
	defer r.cleanupContainer(ctx, containerID)

	// Start container
	if err := r.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	r.slog.Info("container started", "id", containerID)

	// Wait for container to finish
	statusCh, errCh := r.client.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("error waiting for container: %w", err)
		}
	case status := <-statusCh:
		r.slog.Info("container finished", "id", containerID, "exitCode", status.StatusCode)

		if r.cfg.OutputToMetadata {
			msg.AddMetadata(r.cfg.MetadataKeyPrefix+"exit_code", strconv.FormatInt(status.StatusCode, 10))
		}

		if r.cfg.FailOnNonZero && status.StatusCode != 0 {
			return fmt.Errorf("container exited with non-zero status: %d", status.StatusCode)
		}
	}

	// Capture output
	if r.cfg.CaptureOutput {
		stdout, stderr, err := r.captureOutput(ctx, containerID)
		if err != nil {
			r.slog.Warn("failed to capture output", "error", err)
		} else if r.cfg.OutputToMetadata {
			msg.AddMetadata(r.cfg.MetadataKeyPrefix+"stdout", stdout)
			msg.AddMetadata(r.cfg.MetadataKeyPrefix+"stderr", stderr)
		}
	}

	// Collect artifacts
	if len(r.cfg.Artifacts) > 0 {
		if err := r.collectArtifacts(ctx, containerID, msg); err != nil {
			r.slog.Warn("failed to collect artifacts", "error", err)
		}
	}

	return nil
}

// ensureImage ensures the Docker image is available
func (r *DockerRunner) ensureImage(ctx context.Context) error {
	switch strings.ToLower(r.cfg.PullPolicy) {
	case "always":
		return r.pullImage(ctx)
	case "never":
		return nil
	case "ifnotpresent", "":
		// Check if image exists
		_, err := r.client.ImageInspect(ctx, r.cfg.Image)
		if err != nil {
			// Image doesn't exist, pull it
			return r.pullImage(ctx)
		}
		return nil
	default:
		return fmt.Errorf("invalid pull policy: %s", r.cfg.PullPolicy)
	}
}

// pullImage pulls the Docker image
func (r *DockerRunner) pullImage(ctx context.Context) error {
	r.slog.Info("pulling docker image", "image", r.cfg.Image)

	reader, err := r.client.ImagePull(ctx, r.cfg.Image, image.PullOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			r.slog.Warn("failed to close image pull reader", "error", err)
		}
	}()

	// Discard output to avoid blocking
	_, err = io.Copy(io.Discard, reader)
	return err
}

// prepareHostFilesystem copies message filesystem to host temp directory
func (r *DockerRunner) prepareHostFilesystem(msg *message.RunnerMessage) (string, func(), error) {
	fs, err := msg.GetFilesystem()
	if err != nil {
		return "", nil, fmt.Errorf("failed to get filesystem: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "events-bridge-docker-*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	cleanup := func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			r.slog.Warn("failed to cleanup temp directory", "error", err, "path", tmpDir)
		}
	}

	// Copy filesystem contents to tmpDir if filesystem is not nil
	if fs != nil {
		if err := fsutil.CopyToOS(fs, "/", tmpDir); err != nil {
			cleanup()
			return "", nil, fmt.Errorf("failed to copy filesystem: %w", err)
		}
	}

	return tmpDir, cleanup, nil
}

// buildEnv builds environment variables from config and metadata
func (r *DockerRunner) buildEnv(msg *message.RunnerMessage) ([]string, error) {
	env := make([]string, 0, len(r.cfg.Env)+len(r.cfg.EnvFromMeta))

	// Add configured env vars
	env = append(env, r.cfg.Env...)

	// Add env vars from metadata
	meta, err := msg.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	for _, envMap := range r.cfg.EnvFromMeta {
		value, exists := meta[envMap.MetadataKey]
		if !exists {
			continue
		}

		env = append(env, fmt.Sprintf("%s=%s", envMap.EnvVar, value))
	}

	return env, nil
}

// createContainer creates a Docker container
func (r *DockerRunner) createContainer(ctx context.Context, hostDir string, env []string) (string, error) {
	// Parse resource limits only if configured
	var memoryLimit int64
	if r.cfg.MemoryLimit != "" {
		limit, err := parseMemory(r.cfg.MemoryLimit)
		if err != nil {
			r.slog.Warn("invalid memory limit, ignoring", "error", err)
		} else {
			memoryLimit = limit
		}
	}

	var cpuQuota int64
	if r.cfg.CPULimit != "" {
		quota, err := parseCPU(r.cfg.CPULimit)
		if err != nil {
			r.slog.Warn("invalid cpu limit, ignoring", "error", err)
		} else {
			cpuQuota = quota
		}
	}

	// Build container config
	containerConfig := &container.Config{
		Image:      r.cfg.Image,
		Env:        env,
		WorkingDir: r.cfg.WorkingDir,
		Tty:        false,
		User:       r.cfg.User,
	}

	// Set command
	if len(r.cfg.Commands) > 0 {
		containerConfig.Cmd = r.cfg.Commands
	}

	// Set entrypoint if specified
	if len(r.cfg.Entrypoint) > 0 {
		containerConfig.Entrypoint = r.cfg.Entrypoint
	}

	// Build mounts
	mounts := []mount.Mount{
		{
			Type:     mount.TypeBind,
			Source:   hostDir,
			Target:   r.cfg.MountPath,
			ReadOnly: r.cfg.MountReadOnly,
		},
	}

	// Add additional volumes
	for _, vol := range r.cfg.Volumes {
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   vol.HostPath,
			Target:   vol.ContainerPath,
			ReadOnly: vol.ReadOnly,
		})
	}

	// Add Docker socket if enabled
	if r.cfg.EnableDockerSocket {
		mounts = append(mounts, mount.Mount{
			Type:   mount.TypeBind,
			Source: "/var/run/docker.sock",
			Target: "/var/run/docker.sock",
		})
	}

	// Build host config
	hostConfig := &container.HostConfig{
		Resources: container.Resources{
			Memory:   memoryLimit,
			NanoCPUs: cpuQuota,
		},
		NetworkMode:    container.NetworkMode(r.cfg.NetworkMode),
		Mounts:         mounts,
		CapDrop:        r.cfg.CapDrop,
		CapAdd:         r.cfg.CapAdd,
		Privileged:     r.cfg.Privileged,
		ReadonlyRootfs: r.cfg.ReadOnlyRootfs,
	}

	resp, err := r.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		return "", err
	}

	return resp.ID, nil
}

// captureOutput captures stdout and stderr from container
func (r *DockerRunner) captureOutput(ctx context.Context, containerID string) (string, string, error) {
	logs, err := r.client.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return "", "", err
	}
	defer func() {
		if err := logs.Close(); err != nil {
			r.slog.Warn("failed to close container logs", "error", err)
		}
	}()

	var stdoutBuf, stderrBuf bytes.Buffer

	// Docker multiplexes stdout/stderr, we need to demux
	// For simplicity, we'll capture both to stdout buffer
	if _, err := io.Copy(&stdoutBuf, logs); err != nil {
		return "", "", err
	}

	return stdoutBuf.String(), stderrBuf.String(), nil
}

// collectArtifacts copies artifacts from container to message filesystem
func (r *DockerRunner) collectArtifacts(ctx context.Context, containerID string, msg *message.RunnerMessage) error {
	fs, err := msg.GetFilesystem()
	if err != nil {
		return fmt.Errorf("failed to get filesystem: %w", err)
	}

	// Create filesystem if it doesn't exist
	if fs == nil {
		fs = fsutil.NewMemMapFS()
		msg.SetFilesystem(fs)
	}

	for _, artifact := range r.cfg.Artifacts {
		r.slog.Info("collecting artifact", "path", artifact.ContainerPath)

		// Copy from container
		reader, _, err := r.client.CopyFromContainer(ctx, containerID, artifact.ContainerPath)
		if err != nil {
			r.slog.Warn("failed to copy artifact", "path", artifact.ContainerPath, "error", err)
			continue
		}

		// Read all data
		data, err := io.ReadAll(reader)
		if err := reader.Close(); err != nil {
			r.slog.Warn("failed to close artifact reader", "error", err)
		}
		if err != nil {
			r.slog.Warn("failed to read artifact", "path", artifact.ContainerPath, "error", err)
			continue
		}

		// Optionally compress
		if artifact.Compress {
			compressed, err := compressGzip(data)
			if err != nil {
				r.slog.Warn("failed to compress artifact", "error", err)
			} else {
				data = compressed
			}
		}

		// Store in metadata if key is specified
		if artifact.MetadataKey != "" {
			encoded := base64.StdEncoding.EncodeToString(data)
			msg.AddMetadata(artifact.MetadataKey, encoded)
		}

		// Also store in filesystem
		artifactName := filepath.Base(artifact.ContainerPath)
		if artifact.Compress {
			artifactName += ".gz"
		}
		artifactPath := filepath.Join("/artifacts", artifactName)

		if err := fsutil.WriteFile(fs, artifactPath, data, 0644); err != nil {
			r.slog.Warn("failed to write artifact to filesystem", "error", err)
		}
	}

	return nil
}

// cleanupContainer removes the container
func (r *DockerRunner) cleanupContainer(ctx context.Context, containerID string) {
	if err := r.client.ContainerRemove(ctx, containerID, container.RemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	}); err != nil {
		r.slog.Warn("failed to remove container", "id", containerID, "error", err)
	}
}

// Close closes the Docker client
func (r *DockerRunner) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

// parseMemory parses memory string (e.g., "512m", "1g")
func parseMemory(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty memory string")
	}

	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid memory format: %s", s)
	}

	unit := s[len(s)-1]
	valueStr := s[:len(s)-1]

	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory value: %s", s)
	}

	switch unit {
	case 'k':
		return value * 1024, nil
	case 'm':
		return value * 1024 * 1024, nil
	case 'g':
		return value * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("invalid memory unit: %c", unit)
	}
}

// parseCPU parses CPU string (e.g., "1.0", "0.5")
func parseCPU(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty cpu string")
	}

	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid cpu value: %s", s)
	}

	return int64(value * 100000), nil
}

// compressGzip compresses data using gzip
func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
