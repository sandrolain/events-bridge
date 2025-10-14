//nolint:unused,errcheck
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/message"
)

const (
	testModel            = "gpt-4o-mini"
	testTimeout          = 30 * time.Second
	testPrompt           = "Process this message:"
	testMessage          = "Test message"
	baseURL              = "/chat/completions"
	authPrefix           = "Bearer "
	contentType          = "application/json"
	assistantRole        = "assistant"
	testAPIKey           = "test-api-key"
	failedToCreateRunner = "Failed to create runner: %v"
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

func (m *mockSourceMessage) Ack() error {
	return nil
}

func (m *mockSourceMessage) Nak() error {
	return nil
}

func (m *mockSourceMessage) Reply(data *message.ReplyData) error {
	return nil
}

// Simple structures for mock responses
type mockChatRequest struct {
	Model     string                 `json:"model"`
	Messages  []mockChatMessageInput `json:"messages"`
	MaxTokens *int                   `json:"max_tokens,omitempty"`
}

type mockChatMessageInput struct {
	Role    string                `json:"role"`
	Content []mockChatMessagePart `json:"content,omitempty"`
}

type mockChatMessagePart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type mockChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type mockChatResponse struct {
	ID      string           `json:"id"`
	Object  string           `json:"object"`
	Created int64            `json:"created"`
	Model   string           `json:"model"`
	Choices []mockChatChoice `json:"choices"`
}

type mockChatChoice struct {
	Index        int             `json:"index"`
	Message      mockChatMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

type mockErrorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func createMockOpenAIServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Validate request method and path
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if r.URL.Path != baseURL {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// Validate Content-Type
		if r.Header.Get("Content-Type") != contentType {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Validate Authorization header
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, authPrefix) {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(mockErrorResponse{
				Error: struct {
					Message string `json:"message"`
					Type    string `json:"type"`
				}{
					Message: "Incorrect API key provided",
					Type:    "invalid_request_error",
				},
			})
			return
		}

		// Parse request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var req mockChatRequest
		if err := json.Unmarshal(body, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Validate required fields
		if req.Model == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(mockErrorResponse{
				Error: struct {
					Message string `json:"message"`
					Type    string `json:"type"`
				}{
					Message: "Missing required parameter: 'model'",
					Type:    "invalid_request_error",
				},
			})
			return
		}

		// Handle different test scenarios based on request content
		if len(req.Messages) > 0 && len(req.Messages[0].Content) > 0 {
			var content string
			// Extract text from Content parts
			for _, part := range req.Messages[0].Content {
				if part.Type == "text" {
					content += part.Text + " "
				}
			}

			// Test timeout scenario
			if strings.Contains(content, "timeout") {
				time.Sleep(2 * time.Second)
				w.WriteHeader(http.StatusRequestTimeout)
				return
			}

			// Test error response
			if strings.Contains(content, "error") {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(mockErrorResponse{
					Error: struct {
						Message string `json:"message"`
						Type    string `json:"type"`
					}{
						Message: "Invalid request",
						Type:    "invalid_request_error",
					},
				})
				return
			}
		}

		// Return successful response
		var responseContent string
		if len(req.Messages) > 0 && len(req.Messages[0].Content) > 0 {
			// Extract text from Content for response
			var texts []string
			for _, part := range req.Messages[0].Content {
				if part.Type == "text" {
					texts = append(texts, part.Text)
				}
			}
			responseContent = fmt.Sprintf("Response to: %s", strings.Join(texts, " "))
		}

		response := mockChatResponse{
			ID:      "chatcmpl-test123",
			Object:  "chat.completion",
			Created: time.Now().Unix(),
			Model:   req.Model,
			Choices: []mockChatChoice{
				{
					Index: 0,
					Message: mockChatMessage{
						Role:    assistantRole,
						Content: responseContent,
					},
					FinishReason: "stop",
				},
			},
		}

		w.Header().Set("Content-Type", contentType)
		json.NewEncoder(w).Encode(response)
	}))
}

// createMockServerWithTransient429 returns 429 for the first N attempts, then 200
func createMockServerWithTransient429(t *testing.T, failCount int) *httptest.Server {
	var calls int
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != baseURL {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls++
		if calls <= failCount {
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(mockErrorResponse{Error: struct {
				Message string `json:"message"`
				Type    string `json:"type"`
			}{Message: "rate limited", Type: "rate_limit"}})
			return
		}
		// Success payload after failures
		var req mockChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		resp := mockChatResponse{
			ID:      "ok",
			Object:  "chat.completion",
			Created: time.Now().Unix(),
			Model:   req.Model,
			Choices: []mockChatChoice{{
				Index: 0,
				Message: mockChatMessage{
					Role:    assistantRole,
					Content: "Recovered after 429",
				},
				FinishReason: "stop",
			}},
		}
		w.Header().Set("Content-Type", contentType)
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

func createTestMessage(data []byte, metadata map[string]string) *message.RunnerMessage {
	sourceMsg := &mockSourceMessage{
		id:       []byte("test-id"),
		data:     data,
		metadata: metadata,
	}
	return message.NewRunnerMessage(sourceMsg)
}

func TestGPTRunnerSuccessfulCompletion(t *testing.T) {
	server := createMockOpenAIServer(t)
	defer server.Close()

	// Create GPT runner configuration
	cfg := &RunnerConfig{
		ApiURL:    server.URL,
		ApiKey:    testAPIKey,
		Prompt:    testPrompt,
		Model:     testModel,
		MaxTokens: 100,
		Timeout:   testTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(failedToCreateRunner, err)
	}

	// Test data
	testData := []byte("Hello, how are you?")
	msg := createTestMessage(testData, nil)

	// Process message
	err = runner.Process(msg)

	// Assertions
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	resultData, err := msg.GetData()
	if err != nil {
		t.Fatalf("Failed to get target data: %v", err)
	}
	if len(resultData) == 0 {
		t.Fatal("Expected non-empty result data")
	}

	resultStr := string(resultData)
	if !strings.Contains(resultStr, "Response to:") {
		t.Errorf("Expected response to contain 'Response to:', got: %s", resultStr)
	}
	if !strings.Contains(resultStr, "Process this message:") {
		t.Errorf("Expected response to contain prompt, got: %s", resultStr)
	}
}

func TestGPTRunnerInvalidModel(t *testing.T) {
	server := createMockOpenAIServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		ApiURL:    server.URL,
		ApiKey:    testAPIKey,
		Prompt:    testPrompt,
		Model:     "", // Invalid model
		MaxTokens: 100,
		Timeout:   testTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(failedToCreateRunner, err)
	}

	// Test data
	testData := []byte(testMessage)
	msg := createTestMessage(testData, nil)

	err = runner.Process(msg)
	if err == nil {
		t.Fatal("Expected error for invalid model, got nil")
	}
}

func TestGPTRunnerUnauthorizedRequest(t *testing.T) {
	server := createMockOpenAIServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		ApiURL:    server.URL,
		ApiKey:    "", // Invalid API key
		Prompt:    testPrompt,
		Model:     testModel,
		MaxTokens: 100,
		Timeout:   testTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(failedToCreateRunner, err)
	}

	// Test data
	testData := []byte(testMessage)
	msg := createTestMessage(testData, nil)

	err = runner.Process(msg)
	if err == nil {
		t.Fatal("Expected error for unauthorized request, got nil")
	}
}

func TestGPTRunnerErrorResponse(t *testing.T) {
	server := createMockOpenAIServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		ApiURL:    server.URL,
		ApiKey:    testAPIKey,
		Prompt:    "error trigger", // Use error in prompt to trigger error response
		Model:     testModel,
		MaxTokens: 100,
		Timeout:   testTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(failedToCreateRunner, err)
	}

	// Use any message data
	testData := []byte(testMessage)
	msg := createTestMessage(testData, nil)

	err = runner.Process(msg)

	if err == nil {
		t.Fatal("Expected error from error response, got nil")
	}
}

func TestGPTRunnerMetadataPreservation(t *testing.T) {
	server := createMockOpenAIServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		ApiURL:    server.URL,
		ApiKey:    testAPIKey,
		Prompt:    testPrompt,
		Model:     testModel,
		MaxTokens: 100,
		Timeout:   testTimeout,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(failedToCreateRunner, err)
	}

	// Test with metadata
	testData := []byte("Hello with metadata")
	metadata := map[string]string{
		"original_source": "test_system",
		"request_id":      "12345",
	}
	msg := createTestMessage(testData, metadata)

	err = runner.Process(msg)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Check that metadata is preserved in the result
	targetMetadata, err := msg.GetMetadata()
	if err != nil {
		t.Fatalf("Failed to get target metadata: %v", err)
	}

	if targetMetadata["original_source"] != "test_system" {
		t.Errorf("Expected metadata 'original_source' to be preserved")
	}

	if targetMetadata["request_id"] != "12345" {
		t.Errorf("Expected metadata 'request_id' to be preserved")
	}
}

func TestGPTRunnerTimeout(t *testing.T) {
	server := createMockOpenAIServer(t)
	defer server.Close()

	cfg := &RunnerConfig{
		ApiURL:    server.URL,
		ApiKey:    testAPIKey,
		Prompt:    "timeout trigger", // Use timeout in prompt to trigger delay
		Model:     testModel,
		MaxTokens: 100,
		Timeout:   500 * time.Millisecond, // Short timeout
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(failedToCreateRunner, err)
	}

	// Use any message data
	testData := []byte(testMessage)
	msg := createTestMessage(testData, nil)

	err = runner.Process(msg)

	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("Expected timeout-related error, got: %v", err)
	}
}

func TestResolveSecretFromEnv(t *testing.T) {
	const envName = "TEST_OPENAI_KEY"
	const envVal = "from-env-key"
	t.Setenv(envName, envVal)
	val, err := resolveSecret("env:" + envName)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != envVal {
		t.Fatalf("expected %q, got %q", envVal, val)
	}
}

func TestResolveSecretFromFile(t *testing.T) {
	f, err := os.CreateTemp("", "openai-key-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString("secret-from-file\n"); err != nil {
		t.Fatalf("failed writing temp file: %v", err)
	}
	_ = f.Close()

	val, err := resolveSecret("file:" + f.Name())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "secret-from-file" {
		t.Fatalf("expected 'secret-from-file', got %q", val)
	}
}

func TestResolveSecretFileRequiresAbsolute(t *testing.T) {
	_, err := resolveSecret("file:relative-path.txt")
	if err == nil {
		t.Fatal("expected error for relative file path")
	}
}

func TestRetriesOn429ThenSuccess(t *testing.T) {
	server := createMockServerWithTransient429(t, 1)
	defer server.Close()

	cfg := &RunnerConfig{
		ApiURL:       server.URL,
		ApiKey:       testAPIKey,
		Prompt:       testPrompt,
		Model:        testModel,
		MaxTokens:    50,
		Timeout:      2 * time.Second,
		Retries:      2,
		RetryBackoff: 100 * time.Millisecond,
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf(failedToCreateRunner, err)
	}

	msg := createTestMessage([]byte("retry please"), nil)
	if err := runner.Process(msg); err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
}

func TestTLSClientBuildError(t *testing.T) {
	// Provide only cert without key -> BuildClientConfig should fail
	cfg := &RunnerConfig{
		ApiURL:  "http://127.0.0.1",
		ApiKey:  testAPIKey,
		Prompt:  testPrompt,
		Model:   testModel,
		Timeout: 2 * time.Second,
		TLS: &tlsconfig.Config{
			Enabled:    true,
			CertFile:   "/tmp/nonexistent-cert.pem",
			MinVersion: "1.2",
		},
	}
	_, err := NewRunner(cfg)
	if err == nil {
		t.Fatal("expected error when TLS cert/key are incomplete")
	}
}

// (intentionally no additional helpers)
