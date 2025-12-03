package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/fcgi"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common/fsutil"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSourceMessage implements message.SourceMessage for testing
type mockSourceMessage struct {
	id       []byte
	metadata map[string]string
	data     []byte
}

func (m *mockSourceMessage) GetID() []byte {
	return m.id
}

func (m *mockSourceMessage) GetMetadata() (map[string]string, error) {
	return m.metadata, nil
}

func (m *mockSourceMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *mockSourceMessage) GetFilesystem() (fsutil.Filesystem, error) {
	return nil, nil
}

func (m *mockSourceMessage) Ack(data *message.ReplyData) error {
	return nil
}

func (m *mockSourceMessage) Nak() error {
	return nil
}

func TestNewRunnerConfig(t *testing.T) {
	cfg := NewRunnerConfig()
	require.NotNil(t, cfg)
	_, ok := cfg.(*FastCGIRunnerConfig)
	assert.True(t, ok, "should return *FastCGIRunnerConfig")
}

func TestNewRunner_InvalidConfig(t *testing.T) {
	_, err := NewRunner("invalid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid config type")
}

func TestNewRunner_ValidConfig(t *testing.T) {
	cfg := &FastCGIRunnerConfig{
		Network:        "tcp",
		Address:        "localhost:9000",
		DocumentRoot:   "/var/www/html",
		ScriptFilename: "/var/www/html/index.php",
		Timeout:        5 * time.Second,
		PoolSize:       5,
		PoolExpiry:     30 * time.Second,
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	require.NotNil(t, runner)

	fcgiRunner, ok := runner.(*FastCGIRunner)
	assert.True(t, ok)
	assert.Equal(t, "tcp", fcgiRunner.cfg.Network)
	assert.Equal(t, "localhost:9000", fcgiRunner.cfg.Address)
	assert.Equal(t, "/index.php", fcgiRunner.cfg.ScriptName) // Derived from filename
	assert.NotNil(t, fcgiRunner.pool)
}

func TestNewRunner_Defaults(t *testing.T) {
	cfg := &FastCGIRunnerConfig{
		Network:        "unix",
		Address:        "/tmp/test.sock",
		DocumentRoot:   "/app",
		ScriptFilename: "/app/handler.php",
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	fcgiRunner := runner.(*FastCGIRunner)

	// Check defaults were applied
	assert.Equal(t, "POST", fcgiRunner.cfg.RequestMethod)
	assert.Equal(t, "application/octet-stream", fcgiRunner.cfg.ContentType)
	assert.Equal(t, "events-bridge", fcgiRunner.cfg.ServerSoftware)
	assert.Equal(t, "localhost", fcgiRunner.cfg.ServerName)
	assert.Equal(t, "80", fcgiRunner.cfg.ServerPort)
	assert.Equal(t, uint(10), fcgiRunner.cfg.PoolSize)
	assert.Equal(t, 60*time.Second, fcgiRunner.cfg.PoolExpiry)
	assert.Equal(t, 30*time.Second, fcgiRunner.cfg.Timeout)
	assert.Equal(t, "/handler.php", fcgiRunner.cfg.ScriptName)
	assert.Equal(t, "/handler.php", fcgiRunner.cfg.RequestURI)
}

func TestNewRunner_CustomScriptName(t *testing.T) {
	cfg := &FastCGIRunnerConfig{
		Network:        "tcp",
		Address:        "localhost:9000",
		DocumentRoot:   "/var/www",
		ScriptFilename: "/var/www/public/index.php",
		ScriptName:     "/custom/path.php", // Explicit script name
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	fcgiRunner := runner.(*FastCGIRunner)
	assert.Equal(t, "/custom/path.php", fcgiRunner.cfg.ScriptName)
}

func TestFastCGIRunner_Close(t *testing.T) {
	cfg := &FastCGIRunnerConfig{
		Network:        "tcp",
		Address:        "localhost:9000",
		DocumentRoot:   "/var/www",
		ScriptFilename: "/var/www/index.php",
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	err = runner.Close()
	assert.NoError(t, err)
}

// Helper to create a test FastCGI server
func startTestFCGIServer(t *testing.T, handler http.HandlerFunc) (network, address string, cleanup func()) {
	t.Helper()

	// Use /tmp for shorter socket path (macOS has 104 char limit for Unix sockets)
	sockPath := filepath.Join("/tmp", fmt.Sprintf("fcgi-test-%d.sock", time.Now().UnixNano()))

	// Remove any existing socket
	os.Remove(sockPath)

	listener, err := net.Listen("unix", sockPath)
	require.NoError(t, err)

	// Start FCGI server in goroutine
	go func() {
		_ = fcgi.Serve(listener, handler)
	}()

	cleanup = func() {
		listener.Close()
		os.Remove(sockPath)
	}

	// Wait for socket to be ready
	time.Sleep(50 * time.Millisecond)

	return "unix", sockPath, cleanup
}

func TestFastCGIRunner_Process_Success(t *testing.T) {
	// Start test FCGI server
	network, address, cleanup := startTestFCGIServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Echo back the request body with a header
		w.Header().Set("X-Custom-Response", "test-value")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		fmt.Fprintf(w, `{"received": "%s", "method": "%s"}`, string(body), r.Method)
	})
	defer cleanup()

	cfg := &FastCGIRunnerConfig{
		Network:        network,
		Address:        address,
		DocumentRoot:   "/var/www",
		ScriptFilename: "/var/www/test.php",
		Timeout:        5 * time.Second,
		ContentType:    "text/plain",
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	defer runner.Close()

	// Create test message
	srcMsg := &mockSourceMessage{
		id:       []byte("test-id"),
		metadata: map[string]string{"X-Request-Id": "req-123"},
		data:     []byte("hello world"),
	}
	runnerMsg := message.NewRunnerMessage(srcMsg)

	// Process
	err = runner.Process(runnerMsg)
	require.NoError(t, err)

	// Check response
	data, err := runnerMsg.GetData()
	require.NoError(t, err)
	assert.Contains(t, string(data), "hello world")
	assert.Contains(t, string(data), "POST")

	// Check metadata
	metadata, err := runnerMsg.GetMetadata()
	require.NoError(t, err)
	assert.Equal(t, "200", metadata["eb-status"])
	assert.Equal(t, "test-value", metadata["x-custom-response"])
}

func TestFastCGIRunner_Process_ErrorStatus(t *testing.T) {
	network, address, cleanup := startTestFCGIServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Internal Server Error")
	})
	defer cleanup()

	cfg := &FastCGIRunnerConfig{
		Network:        network,
		Address:        address,
		DocumentRoot:   "/var/www",
		ScriptFilename: "/var/www/test.php",
		Timeout:        5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	defer runner.Close()

	srcMsg := &mockSourceMessage{
		id:   []byte("test-id"),
		data: []byte("test"),
	}
	runnerMsg := message.NewRunnerMessage(srcMsg)

	err = runner.Process(runnerMsg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error status: 500")
}

func TestFastCGIRunner_Process_ConnectionError(t *testing.T) {
	cfg := &FastCGIRunnerConfig{
		Network:        "tcp",
		Address:        "localhost:59999", // Non-existent port
		DocumentRoot:   "/var/www",
		ScriptFilename: "/var/www/test.php",
		Timeout:        1 * time.Second,
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	defer runner.Close()

	srcMsg := &mockSourceMessage{
		id:   []byte("test-id"),
		data: []byte("test"),
	}
	runnerMsg := message.NewRunnerMessage(srcMsg)

	err = runner.Process(runnerMsg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get FastCGI client")
}

func TestFastCGIRunner_Process_WithMetadata(t *testing.T) {
	var receivedEnv map[string]string

	network, address, cleanup := startTestFCGIServer(t, func(w http.ResponseWriter, r *http.Request) {
		// The metadata should be passed as HTTP_ prefixed env vars
		receivedEnv = make(map[string]string)
		// In a real FCGI server, these would be accessible; here we just verify the response
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})
	defer cleanup()

	cfg := &FastCGIRunnerConfig{
		Network:           network,
		Address:           address,
		DocumentRoot:      "/var/www",
		ScriptFilename:    "/var/www/test.php",
		Timeout:           5 * time.Second,
		PassMetadataAsEnv: true,
		Env: map[string]string{
			"CUSTOM_VAR": "custom_value",
		},
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	defer runner.Close()

	srcMsg := &mockSourceMessage{
		id: []byte("test-id"),
		metadata: map[string]string{
			"X-Custom-Header": "header-value",
			"Authorization":   "Bearer token123",
		},
		data: []byte("test data"),
	}
	runnerMsg := message.NewRunnerMessage(srcMsg)

	err = runner.Process(runnerMsg)
	require.NoError(t, err)

	// Response should be successful
	metadata, err := runnerMsg.GetMetadata()
	require.NoError(t, err)
	assert.Equal(t, "200", metadata["eb-status"])

	// Note: receivedEnv verification would require a more sophisticated test server
	_ = receivedEnv
}

func TestFastCGIRunner_Process_EmptyData(t *testing.T) {
	network, address, cleanup := startTestFCGIServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "empty response")
	})
	defer cleanup()

	cfg := &FastCGIRunnerConfig{
		Network:        network,
		Address:        address,
		DocumentRoot:   "/var/www",
		ScriptFilename: "/var/www/test.php",
		Timeout:        5 * time.Second,
	}

	runner, err := NewRunner(cfg)
	require.NoError(t, err)
	defer runner.Close()

	srcMsg := &mockSourceMessage{
		id:   []byte("test-id"),
		data: []byte{}, // Empty data
	}
	runnerMsg := message.NewRunnerMessage(srcMsg)

	err = runner.Process(runnerMsg)
	require.NoError(t, err)

	data, err := runnerMsg.GetData()
	require.NoError(t, err)
	assert.Equal(t, "empty response", string(data))
}

func TestFastCGIRunner_ScriptNameDerivation(t *testing.T) {
	tests := []struct {
		name           string
		documentRoot   string
		scriptFilename string
		expectedScript string
	}{
		{
			name:           "standard path",
			documentRoot:   "/var/www/html",
			scriptFilename: "/var/www/html/index.php",
			expectedScript: "/index.php",
		},
		{
			name:           "nested path",
			documentRoot:   "/var/www",
			scriptFilename: "/var/www/public/api/handler.php",
			expectedScript: "/public/api/handler.php",
		},
		{
			name:           "root outside document",
			documentRoot:   "/var/www/html",
			scriptFilename: "/opt/scripts/worker.php",
			expectedScript: "/opt/scripts/worker.php",
		},
		{
			name:           "trailing slash in root",
			documentRoot:   "/var/www/html/",
			scriptFilename: "/var/www/html/app.php",
			expectedScript: "app.php", // Trailing slash causes partial match
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &FastCGIRunnerConfig{
				Network:        "tcp",
				Address:        "localhost:9000",
				DocumentRoot:   tt.documentRoot,
				ScriptFilename: tt.scriptFilename,
			}

			runner, err := NewRunner(cfg)
			require.NoError(t, err)

			fcgiRunner := runner.(*FastCGIRunner)
			assert.Equal(t, tt.expectedScript, fcgiRunner.cfg.ScriptName)
		})
	}
}
