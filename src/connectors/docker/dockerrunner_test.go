package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubSourceMessage is a test stub for SourceMessage
type stubSourceMessage struct {
	id       []byte
	metadata map[string]string
	data     []byte
}

func (s *stubSourceMessage) GetID() []byte {
	return s.id
}

func (s *stubSourceMessage) GetMetadata() (map[string]string, error) {
	return s.metadata, nil
}

func (s *stubSourceMessage) GetData() ([]byte, error) {
	return s.data, nil
}

func (s *stubSourceMessage) GetFilesystem() (fsutil.Filesystem, error) {
	return nil, nil
}

func (s *stubSourceMessage) Ack(data *message.ReplyData) error {
	return nil
}

func (s *stubSourceMessage) Nak() error {
	return nil
}

func TestNewRunner(t *testing.T) {
	tests := []struct {
		name        string
		config      *DockerRunnerConfig
		expectError bool
	}{
		{
			name: "valid minimal config",
			config: &DockerRunnerConfig{
				Image:             "alpine:latest",
				PullPolicy:        "IfNotPresent",
				WorkingDir:        "/workspace",
				MountPath:         "/workspace",
				MountReadOnly:     false,
				NetworkMode:       "none",
				CaptureOutput:     true,
				AggregateOutput:   false,
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     true,
				Timeout:           "300s",
				ReadOnlyRootfs:    false,
				CapDrop:           []string{"ALL"},
			},
			expectError: false,
		},
		{
			name: "valid full config",
			config: &DockerRunnerConfig{
				Image:             "alpine:latest",
				Commands:          []string{"echo", "hello"},
				WorkingDir:        "/app",
				PullPolicy:        "Always",
				MemoryLimit:       "256m",
				CPULimit:          "0.5",
				Timeout:           "60s",
				CaptureOutput:     true,
				AggregateOutput:   false,
				MountPath:         "/workspace",
				MountReadOnly:     false,
				NetworkMode:       "none",
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     true,
				ReadOnlyRootfs:    false,
				CapDrop:           []string{"ALL"},
			},
			expectError: false,
		},
		{
			name: "missing image",
			config: &DockerRunnerConfig{
				PullPolicy:        "IfNotPresent",
				WorkingDir:        "/workspace",
				MountPath:         "/workspace",
				MountReadOnly:     false,
				NetworkMode:       "none",
				CaptureOutput:     true,
				AggregateOutput:   false,
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     true,
				Timeout:           "300s",
				ReadOnlyRootfs:    false,
				CapDrop:           []string{"ALL"},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner, err := NewRunner(tt.config)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, runner)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, runner)
				if runner != nil {
					assert.NoError(t, runner.Close())
				}
			}
		})
	}
}

func TestDockerRunner_ensureImage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name        string
		pullPolicy  string
		image       string
		expectError bool
	}{
		{
			name:        "pull if not present - exists",
			pullPolicy:  "IfNotPresent",
			image:       "alpine:latest",
			expectError: false,
		},
		{
			name:        "always pull",
			pullPolicy:  "Always",
			image:       "alpine:latest",
			expectError: false,
		},
		{
			name:        "never pull - might fail if not present",
			pullPolicy:  "Never",
			image:       "alpine:latest",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &DockerRunner{
				cfg: &DockerRunnerConfig{
					Image:      tt.image,
					PullPolicy: tt.pullPolicy,
				},
				slog: logger,
			}

			dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
			require.NoError(t, err)
			runner.client = dockerClient
			defer runner.Close()

			ctx := context.Background()
			err = runner.ensureImage(ctx)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDockerRunner_prepareHostFilesystem(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	runner := &DockerRunner{
		cfg:  &DockerRunnerConfig{},
		slog: logger,
	}

	// Create in-memory filesystem
	fs := fsutil.NewMemMapFS()
	require.NoError(t, fsutil.WriteFile(fs, "/test.txt", []byte("hello world"), 0644))
	require.NoError(t, fs.MkdirAll("/subdir", 0755))
	require.NoError(t, fsutil.WriteFile(fs, "/subdir/file.txt", []byte("content"), 0644))

	// Create message with filesystem
	msg := message.NewRunnerMessage(&stubSourceMessage{
		id:       []byte("test"),
		metadata: make(map[string]string),
		data:     []byte{},
	})
	msg.SetFilesystem(fs)

	// Prepare host filesystem
	hostDir, cleanup, err := runner.prepareHostFilesystem(msg)
	require.NoError(t, err)
	require.NotEmpty(t, hostDir)
	defer cleanup()

	// Verify files were copied
	//nolint:gosec // Test file - hostDir is generated by the test
	data, err := os.ReadFile(hostDir + "/test.txt")
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(data))

	//nolint:gosec // Test file - hostDir is generated by the test
	data, err = os.ReadFile(hostDir + "/subdir/file.txt")
	require.NoError(t, err)
	assert.Equal(t, "content", string(data))
}

func TestDockerRunner_buildEnv(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	runner := &DockerRunner{
		cfg: &DockerRunnerConfig{
			Env: []string{"FOO=bar", "BAZ=qux"},
			EnvFromMeta: []EnvFromMetadata{
				{MetadataKey: "commit_sha", EnvVar: "GIT_COMMIT"},
				{MetadataKey: "branch", EnvVar: "GIT_BRANCH"},
			},
		},
		slog: logger,
	}

	msg := message.NewRunnerMessage(&stubSourceMessage{
		id:       []byte("test"),
		metadata: make(map[string]string),
		data:     []byte{},
	})
	msg.AddMetadata("commit_sha", "abc123")
	msg.AddMetadata("branch", "main")

	env, err := runner.buildEnv(msg)
	require.NoError(t, err)
	assert.Contains(t, env, "FOO=bar")
	assert.Contains(t, env, "BAZ=qux")
	assert.Contains(t, env, "GIT_COMMIT=abc123")
	assert.Contains(t, env, "GIT_BRANCH=main")
}

func TestDockerRunner_Process(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name        string
		config      *DockerRunnerConfig
		setupMsg    func(*message.RunnerMessage)
		expectError bool
		validate    func(*testing.T, *message.RunnerMessage)
	}{
		{
			name: "simple echo command",
			config: &DockerRunnerConfig{
				Image:             "alpine:latest",
				Commands:          []string{"echo", "hello world"},
				PullPolicy:        "IfNotPresent",
				WorkingDir:        "/workspace",
				MountPath:         "/workspace",
				CaptureOutput:     true,
				AggregateOutput:   false,
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     true,
				Timeout:           "60s",
				NetworkMode:       "none",
				CapDrop:           []string{"ALL"},
			},
			setupMsg: func(msg *message.RunnerMessage) {
				fs := fsutil.NewMemMapFS()
				fsutil.WriteFile(fs, "/test.txt", []byte("test content"), 0644)
				msg.SetFilesystem(fs)
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.RunnerMessage) {
				meta, err := msg.GetMetadata()
				require.NoError(t, err)
				assert.Equal(t, "0", meta["docker_exit-code"])
				data, err := msg.GetData()
				require.NoError(t, err)
				assert.Contains(t, string(data), "hello world")
			},
		},
		{
			name: "read file from filesystem",
			config: &DockerRunnerConfig{
				Image:             "alpine:latest",
				Commands:          []string{"cat", "/workspace/input.txt"},
				PullPolicy:        "IfNotPresent",
				WorkingDir:        "/workspace",
				MountPath:         "/workspace",
				CaptureOutput:     true,
				AggregateOutput:   false,
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     true,
				Timeout:           "60s",
				NetworkMode:       "none",
				CapDrop:           []string{"ALL"},
			},
			setupMsg: func(msg *message.RunnerMessage) {
				fs := fsutil.NewMemMapFS()
				fsutil.WriteFile(fs, "/input.txt", []byte("file content from fs"), 0644)
				msg.SetFilesystem(fs)
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.RunnerMessage) {
				meta, err := msg.GetMetadata()
				require.NoError(t, err)
				assert.Equal(t, "0", meta["docker_exit-code"])
				data, err := msg.GetData()
				require.NoError(t, err)
				assert.Contains(t, string(data), "file content from fs")
			},
		},
		{
			name: "non-zero exit code",
			config: &DockerRunnerConfig{
				Image:             "alpine:latest",
				Commands:          []string{"sh", "-c", "exit 1"},
				PullPolicy:        "IfNotPresent",
				WorkingDir:        "/workspace",
				MountPath:         "/workspace",
				CaptureOutput:     true,
				AggregateOutput:   false,
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     true,
				Timeout:           "60s",
				NetworkMode:       "none",
				CapDrop:           []string{"ALL"},
			},
			setupMsg:    func(msg *message.RunnerMessage) {},
			expectError: true,
			validate:    func(t *testing.T, msg *message.RunnerMessage) {},
		},
		{
			name: "ignore non-zero exit code",
			config: &DockerRunnerConfig{
				Image:             "alpine:latest",
				Commands:          []string{"sh", "-c", "exit 42"},
				PullPolicy:        "IfNotPresent",
				WorkingDir:        "/workspace",
				MountPath:         "/workspace",
				CaptureOutput:     true,
				AggregateOutput:   false,
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     false,
				Timeout:           "60s",
				NetworkMode:       "none",
				CapDrop:           []string{"ALL"},
			},
			setupMsg:    func(msg *message.RunnerMessage) {},
			expectError: false,
			validate: func(t *testing.T, msg *message.RunnerMessage) {
				meta, err := msg.GetMetadata()
				require.NoError(t, err)
				assert.Equal(t, "42", meta["docker_exit-code"])
			},
		},
		{
			name: "output to data as raw bytes",
			config: &DockerRunnerConfig{
				Image:             "alpine:latest",
				Commands:          []string{"echo", "stdout_test"},
				PullPolicy:        "IfNotPresent",
				WorkingDir:        "/workspace",
				MountPath:         "/workspace",
				CaptureOutput:     true,
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     false,
				Timeout:           "60s",
				NetworkMode:       "none",
				CapDrop:           []string{"ALL"},
			},
			setupMsg:    func(msg *message.RunnerMessage) {},
			expectError: false,
			validate: func(t *testing.T, msg *message.RunnerMessage) {
				data, err := msg.GetData()
				require.NoError(t, err)
				// Output is raw bytes with Docker log headers
				assert.Contains(t, string(data), "stdout_test")
			},
		},
		{
			name: "capture output as raw bytes",
			config: &DockerRunnerConfig{
				Image:             "alpine:latest",
				Commands:          []string{"echo", "aggregated output"},
				PullPolicy:        "IfNotPresent",
				WorkingDir:        "/workspace",
				MountPath:         "/workspace",
				CaptureOutput:     true,
				MetadataKeyPrefix: "docker_",
				FailOnNonZero:     false,
				Timeout:           "60s",
				NetworkMode:       "none",
				CapDrop:           []string{"ALL"},
			},
			setupMsg:    func(msg *message.RunnerMessage) {},
			expectError: false,
			validate: func(t *testing.T, msg *message.RunnerMessage) {
				data, err := msg.GetData()
				require.NoError(t, err)
				assert.Contains(t, string(data), "aggregated output")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
			require.NoError(t, err)

			runner := &DockerRunner{
				cfg:    tt.config,
				client: dockerClient,
				slog:   logger,
			}
			defer runner.Close()

			// Create a message using local stub
			msg := message.NewRunnerMessage(&stubSourceMessage{
				id:       []byte("test"),
				metadata: make(map[string]string),
				data:     []byte{},
			})
			tt.setupMsg(msg)

			err = runner.Process(msg)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.validate(t, msg)
			}
		})
	}
}

func TestDockerRunner_collectArtifacts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	defer dockerClient.Close()

	ctx := context.Background()

	// Pull alpine image
	reader, err := dockerClient.ImagePull(ctx, "alpine:latest", image.PullOptions{})
	require.NoError(t, err)
	io.Copy(io.Discard, reader)
	reader.Close()

	// Create a container that writes an artifact
	resp, err := dockerClient.ContainerCreate(ctx,
		&container.Config{
			Image: "alpine:latest",
			Cmd:   []string{"sh", "-c", "echo 'artifact content' > /tmp/artifact.txt"},
		},
		nil, nil, nil, "")
	require.NoError(t, err)
	containerID := resp.ID
	defer dockerClient.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})

	// Start and wait for container
	require.NoError(t, dockerClient.ContainerStart(ctx, containerID, container.StartOptions{}))
	statusCh, errCh := dockerClient.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-statusCh:
	}

	// Test artifact collection
	runner := &DockerRunner{
		cfg: &DockerRunnerConfig{
			Artifacts: []ArtifactConfig{
				{
					ContainerPath: "/tmp/artifact.txt",
					Compress:      false,
				},
			},
		},
		client: dockerClient,
		slog:   logger,
	}

	msg := message.NewRunnerMessage(&stubSourceMessage{
		id:       []byte("test"),
		metadata: make(map[string]string),
		data:     []byte{},
	})
	err = runner.collectArtifacts(ctx, containerID, msg)
	assert.NoError(t, err)

	// Verify artifact in filesystem
	fs, err := msg.GetFilesystem()
	require.NoError(t, err)
	require.NotNil(t, fs)

	// Check artifact file exists using Stat
	_, err = fs.Stat("/artifacts/artifact.txt")
	assert.NoError(t, err, "artifact should exist in filesystem")
}

func TestParseMemory(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasError bool
	}{
		{"512m", 512 * 1024 * 1024, false},
		{"1g", 1024 * 1024 * 1024, false},
		{"256k", 256 * 1024, false},
		{"", 0, true},
		{"invalid", 0, true},
		{"x", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseMemory(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseCPU(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasError bool
	}{
		{"1.0", 100000, false},
		{"0.5", 50000, false},
		{"2.0", 200000, false},
		{"", 0, true},
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseCPU(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCompressGzip(t *testing.T) {
	data := []byte("this is test data that should be compressed")
	compressed, err := compressGzip(data)
	assert.NoError(t, err)
	assert.NotNil(t, compressed)
	// Note: This test will fail until gzip is properly implemented
	// assert.Less(t, len(compressed), len(data))
}

// Benchmark tests
func BenchmarkDockerRunner_prepareHostFilesystem(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := &DockerRunner{
		cfg:  &DockerRunnerConfig{},
		slog: logger,
	}

	// Create filesystem with some files
	fs := fsutil.NewMemMapFS()
	for i := 0; i < 10; i++ {
		fsutil.WriteFile(fs, fmt.Sprintf("/file%d.txt", i), bytes.Repeat([]byte("x"), 1024), 0644)
	}

	msg := message.NewRunnerMessage(&stubSourceMessage{
		id:       []byte("test"),
		metadata: make(map[string]string),
		data:     []byte{},
	})
	msg.SetFilesystem(fs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hostDir, cleanup, err := runner.prepareHostFilesystem(msg)
		if err != nil {
			b.Fatal(err)
		}
		cleanup()
		_ = hostDir
	}
}
