package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sandrolain/events-bridge/src/common/encdec"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	openai "github.com/sashabaranov/go-openai"
)

// Ensure GPTRunner implements connectors.Runner
var _ connectors.Runner = &GPTRunner{}

const (
	errNoChoicesFromOpenAI = "no choices from openai"
	logNakMessage          = "error naking message"
)

type RunnerConfig struct {
	ApiURL    string        `mapstructure:"apiUrl" validate:"required"`
	ApiKey    string        `mapstructure:"apiKey" validate:"required"`
	Prompt    string        `mapstructure:"prompt" validate:"required"`
	Model     string        `mapstructure:"model" validate:"required"`
	BatchSize int           `mapstructure:"batchSize"`
	BatchWait time.Duration `mapstructure:"batchWait"`
	MaxTokens int           `mapstructure:"maxTokens"`
	Timeout   time.Duration `mapstructure:"timeout" default:"10s" validate:"required"`
	// TLS configuration for connecting to custom/OpenAI-compatible endpoints
	TLS *tlsconfig.Config `mapstructure:"tls"`
	// Number of retries on transient API failures (429/5xx). Defaults to 2.
	Retries int `mapstructure:"retries" default:"2"`
	// Initial backoff between retries
	RetryBackoff time.Duration `mapstructure:"retryBackoff" default:"250ms"`
	// Whether to log full prompts (may contain sensitive data). Defaults to false.
	LogPrompt bool `mapstructure:"logPrompt" default:"false"`
}

type GPTRunner struct {
	cfg     *RunnerConfig
	slog    *slog.Logger
	client  *openai.Client
	decoder encdec.MessageDecoder
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	decoder, err := encdec.NewMessageDecoder("json", "metadata", "data")
	if err != nil {
		return nil, fmt.Errorf("failed to create message decoder: %w", err)
	}

	// Resolve API key: support raw, env:VAR, file:/path
	apiKey, err := resolveSecret(cfg.ApiKey)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve apiKey: %w", err)
	}

	clientConfig := openai.DefaultConfig(apiKey)
	clientConfig.BaseURL = cfg.ApiURL
	// Use a hardened HTTP client with timeouts and optional TLS
	httpClient, err := buildHTTPClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build http client: %w", err)
	}
	clientConfig.HTTPClient = httpClient

	client := openai.NewClientWithConfig(clientConfig)
	return &GPTRunner{
		cfg:     cfg,
		slog:    slog.Default().With("context", "GPT Runner"),
		client:  client,
		decoder: decoder,
	}, nil
}

func (g *GPTRunner) Process(msg *message.RunnerMessage) error {
	if g.cfg.LogPrompt {
		g.slog.Debug("sending prompt to openai", "prompt", g.cfg.Prompt)
	} else {
		g.slog.Debug("sending prompt to openai", "promptLen", len(g.cfg.Prompt))
	}

	jsonData, err := g.decoder.EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message to json: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.Timeout)
	defer cancel()
	req := openai.ChatCompletionRequest{
		Model: g.cfg.Model,
		Messages: []openai.ChatCompletionMessage{{
			Role: openai.ChatMessageRoleUser,
			MultiContent: []openai.ChatMessagePart{
				{Type: openai.ChatMessagePartTypeText, Text: g.cfg.Prompt},
				{Type: openai.ChatMessagePartTypeText, Text: string(jsonData)},
			},
		}},
		MaxTokens: g.cfg.MaxTokens,
	}

	resp, err := g.createChatWithRetry(ctx, req)
	if err != nil {
		return fmt.Errorf("openai error: %w", err)
	}

	g.slog.Debug("openai response received", "choices", len(resp.Choices))
	if len(resp.Choices) == 0 {
		return errors.New(errNoChoicesFromOpenAI)
	}

	result := resp.Choices[0].Message.Content
	if g.cfg.LogPrompt {
		g.slog.Debug("openai response content", "content", result)
	} else {
		g.slog.Debug("openai response received", "contentLen", len(result))
	}

	// Populate basic metadata about the GPT response while preserving existing metadata
	if srcMeta, err := msg.GetSourceMetadata(); err == nil && len(srcMeta) > 0 {
		msg.MergeMetadata(srcMeta)
	}
	if len(g.cfg.Model) > 0 {
		msg.AddMetadata("gpt_model", g.cfg.Model)
	}
	if len(resp.Choices) > 0 {
		// FinishReason may be empty depending on provider
		if fr := resp.Choices[0].FinishReason; string(fr) != "" {
			msg.AddMetadata("gpt_finish_reason", string(fr))
		}
	}
	if resp.Created > 0 {
		msg.AddMetadata("gpt_created", fmt.Sprintf("%d", resp.Created))
	}

	msg.SetData([]byte(result))

	return nil
}

// // ProcessBatch handles batches of messages by batching prompts to the GPT API.
// // It collects data from each message, assigns unique IDs, formats a single batch prompt,
// // sends it to OpenAI, parses the response, and updates each message with its result.
// // If any step fails, it attempts to NAK (negative acknowledge) failed messages.
// func (g *GPTRunner) ProcessBatch(msgs []*message.RunnerMessage) ([]*message.RunnerMessage, error) {
// 	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.Timeout)
// 	defer cancel()
// 	batch := make([]inputItem, 0, len(msgs))
// 	idToMsg := make(map[string]*message.RunnerMessage)
// 	for i, msg := range msgs {
// 		data, err := msg.GetData()
// 		if err != nil {
// 			g.slog.Debug("GetData failed", "index", i, "error", err)
// 			g.tryNak(msg)
// 			continue
// 		}
// 		id := uuid.NewString()
// 		batch = append(batch, inputItem{ID: id, Data: string(data)})
// 		idToMsg[id] = msg
// 	}
// 	g.slog.Debug("batch ready", "size", len(batch))
// 	if len(batch) == 0 {
// 		return nil, fmt.Errorf("no valid messages to process")
// 	}
// 	prompt, err := g.formatPromptItems(batch)
// 	if err != nil {
// 		g.slog.Error("failed to format prompt", "error", err)
// 		for _, msg := range msgs {
// 			g.tryNak(msg)
// 		}
// 		return nil, fmt.Errorf("failed to format prompt: %w", err)
// 	}
// 	g.slog.Debug("sending prompt to openai", "prompt", prompt)
// 	resp, err := g.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
// 		Model: g.cfg.Model,
// 		Messages: []openai.ChatCompletionMessage{{
// 			Role:    openai.ChatMessageRoleUser,
// 			Content: prompt,
// 		}},
// 		MaxTokens: g.cfg.MaxTokens,
// 	})
// 	if err != nil {
// 		g.slog.Error("openai error", "error", err)
// 		for _, msg := range msgs {
// 			g.tryNak(msg)
// 		}
// 		return nil, fmt.Errorf("openai error: %w", err)
// 	}
// 	g.slog.Debug("openai response received", "choices", len(resp.Choices))
// 	if len(resp.Choices) == 0 {
// 		g.slog.Error(errNoChoicesFromOpenAI)
// 		for _, msg := range msgs {
// 			g.tryNak(msg)
// 		}
// 		return nil, errors.New(errNoChoicesFromOpenAI)
// 	}
// 	res := resp.Choices[0].Message.Content
// 	g.slog.Debug("openai response content", "content", res)
// 	results, err := g.parseBatchResponse(res)
// 	if err != nil {
// 		g.slog.Error("failed to parse gpt response", "error", err)
// 		for _, msg := range msgs {
// 			g.tryNak(msg)
// 		}
// 		return nil, fmt.Errorf("failed to parse gpt response: %w", err)
// 	}
// 	g.slog.Debug("parsed gpt results", "results_count", len(results))

// 	// After populating results, ensure each message has a response
// 	for id, msg := range idToMsg {
// 		result, ok := results[id]
// 		if !ok {
// 			g.slog.Debug("no result for id", "id", id)
// 			g.tryNak(msg)
// 			continue
// 		}

// 		msg.SetData(result)
// 	}
// 	return msgs, nil
// }

func (g *GPTRunner) Close() error {
	return nil
}

// buildHTTPClient builds a hardened HTTP client with optional TLS configuration
func buildHTTPClient(cfg *RunnerConfig) (*http.Client, error) {
	// Build TLS config if requested
	var tlsConf *tls.Config
	if cfg.TLS != nil && cfg.TLS.Enabled {
		built, err := cfg.TLS.BuildClientConfig()
		if err != nil {
			return nil, err
		}
		tlsConf = built
	}

	transport := &http.Transport{
		TLSClientConfig:       tlsConf,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// Overall timeout uses cfg.Timeout, add small buffer for network overhead
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout + 2*time.Second,
	}
	return client, nil
}

// resolveSecret supports prefixes:
// - "env:NAME" to read from environment variable NAME
// - "file:/path" to read the contents of a file
// Any other value is returned as-is
func resolveSecret(value string) (string, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return "", nil
	}
	if strings.HasPrefix(v, "env:") {
		name := strings.TrimPrefix(v, "env:")
		return os.Getenv(name), nil
	}
	if strings.HasPrefix(v, "file:") {
		path := strings.TrimPrefix(v, "file:")
		// Basic hardening: require absolute path to avoid traversal of relative locations
		if !strings.HasPrefix(path, "/") {
			return "", fmt.Errorf("file secret path must be absolute")
		}
		b, err := os.ReadFile(path) // #nosec G304 - path is user-provided by configuration and required for file-based secrets; we enforce absolute path above.
		if err != nil {
			return "", fmt.Errorf("read file %s: %w", path, err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return v, nil
}

// createChatWithRetry executes CreateChatCompletion with basic retry/backoff on transient errors
func (g *GPTRunner) createChatWithRetry(ctx context.Context, req openai.ChatCompletionRequest) (openai.ChatCompletionResponse, error) {
	retries := g.cfg.Retries
	if retries < 0 {
		retries = 0
	}
	backoff := g.cfg.RetryBackoff
	if backoff <= 0 {
		backoff = 250 * time.Millisecond
	}
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		resp, err := g.client.CreateChatCompletion(ctx, req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !shouldRetry(err) || attempt == retries {
			break
		}
		// Sleep with linear backoff; keep simple to avoid extra deps
		select {
		case <-time.After(time.Duration(attempt+1) * backoff):
		case <-ctx.Done():
			return openai.ChatCompletionResponse{}, ctx.Err()
		}
	}
	return openai.ChatCompletionResponse{}, lastErr
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *openai.APIError
	if errors.As(err, &apiErr) {
		if apiErr.HTTPStatusCode == 429 || apiErr.HTTPStatusCode >= 500 {
			return true
		}
		return false
	}
	// Network-level transient errors could be retried but we keep conservative defaults
	return false
}
