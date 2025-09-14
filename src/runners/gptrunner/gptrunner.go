package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners"
	openai "github.com/sashabaranov/go-openai"
)

// Ensure GPTRunner implements runners.Runner
var _ runners.Runner = &GPTRunner{}

type GPTRunner struct {
	cfg     *runners.RunnerGPTRunnerConfig
	slog    *slog.Logger
	client  *openai.Client
	timeout time.Duration
	mu      sync.Mutex
	stopCh  chan struct{}
}

type inputItem struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

type resultItem struct {
	ID     string `json:"id"`
	Result string `json:"result"`
}

func New(cfg *runners.RunnerGPTRunnerConfig) (runners.Runner, error) {
	if cfg == nil {
		return nil, fmt.Errorf("gpt runner configuration cannot be nil")
	}
	if cfg.ApiURL == "" && cfg.ApiKey == "" {
		return nil, fmt.Errorf("openai api key is required")
	}
	if cfg.Action == "" {
		return nil, fmt.Errorf("gpt action prompt is required")
	}
	if cfg.Model == "" {
		cfg.Model = openai.GPT3Dot5Turbo
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	log := slog.Default().With("context", "GPTRUNNER")

	clientConfig := openai.DefaultConfig(cfg.ApiKey)
	if cfg.ApiURL != "" {
		clientConfig.BaseURL = cfg.ApiURL
	}
	client := openai.NewClientWithConfig(clientConfig)
	return &GPTRunner{
		cfg:     cfg,
		slog:    log,
		client:  client,
		timeout: timeout,
		stopCh:  make(chan struct{}),
	}, nil
}

func (g *GPTRunner) Process(msg message.Message) (message.Message, error) {
	prompt, err := g.formatPrompt(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to format prompt: %w", err)
	}
	g.slog.Debug("sending prompt to openai", "prompt", prompt)
	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	resp, err := g.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: g.cfg.Model,
		Messages: []openai.ChatCompletionMessage{{
			Role:    openai.ChatMessageRoleUser,
			Content: prompt,
		}},
		MaxTokens: g.cfg.MaxTokens,
	})
	if err != nil {
		return nil, fmt.Errorf("openai error: %w", err)
	}
	g.slog.Debug("openai response received", "choices", len(resp.Choices))
	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("no choices from openai")
	}
	result := resp.Choices[0].Message.Content
	g.slog.Debug("openai response content", "content", result)

	processed := &gptMessage{
		original: msg,
		data:     []byte(result),
	}

	return processed, nil
}

// formatPromptItems builds the prompt for a batch of items
func (g *GPTRunner) formatPrompt(msg message.Message) (string, error) {
	b, err := msg.GetData()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\n\n%s", g.cfg.Action, string(b)), nil
}

// TODO: implement ProcessBatch to handle batches of messages
func (g *GPTRunner) ProcessBatch(msgs []message.Message, out chan<- message.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	batch := make([]inputItem, 0, len(msgs))
	idToMsg := make(map[string]message.Message)
	for i, msg := range msgs {
		data, err := msg.GetData()
		if err != nil {
			g.slog.Debug("GetData failed", "index", i, "error", err)
			if e := msg.Nak()	; e != nil {
				g.slog.Error("error naking message", "err", e)
			}
			continue
		}
		id := uuid.NewString()
		batch = append(batch, inputItem{ID: id, Data: string(data)})
		idToMsg[id] = msg
	}
	g.slog.Debug("batch ready", "size", len(batch))
	if len(batch) == 0 {
		return nil
	}
	prompt, err := g.formatPromptItems(batch)
	if err != nil {
		g.slog.Error("failed to format prompt", "error", err)
		for _, msg := range msgs {
			if e := msg.Nak(); e != nil {
				g.slog.Error("error naking message", "err", e)
			}
		}
		return fmt.Errorf("failed to format prompt: %w", err)
	}
	g.slog.Debug("sending prompt to openai", "prompt", prompt)
	resp, err := g.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: g.cfg.Model,
		Messages: []openai.ChatCompletionMessage{{
			Role:    openai.ChatMessageRoleUser,
			Content: prompt,
		}},
		MaxTokens: g.cfg.MaxTokens,
	})
	if err != nil {
		g.slog.Error("openai error", "error", err)
		for _, msg := range msgs {
			if e := msg.Nak(); e != nil {
				g.slog.Error("error naking message", "err", e)
			}
		}
		return fmt.Errorf("openai error: %w", err)
	}
	g.slog.Debug("openai response received", "choices", len(resp.Choices))
	if len(resp.Choices) == 0 {
		g.slog.Error("no choices from openai")
		for _, msg := range msgs {
			if e := msg.Nak(); e != nil {
				g.slog.Error("error naking message", "err", e)
			}
		}
		return fmt.Errorf("no choices from openai")
	}
	res := resp.Choices[0].Message.Content
	g.slog.Debug("openai response content", "content", res)
	results, err := g.parseBatchResponse(res)
	if err != nil {
		g.slog.Error("failed to parse gpt response", "error", err)
		for _, msg := range msgs {
			if e := msg.Nak(); e != nil {
				g.slog.Error("error naking message", "err", e)
			}
		}
		return fmt.Errorf("failed to parse gpt response: %w", err)
	}
	g.slog.Debug("parsed gpt results", "results_count", len(results))
	// After populating results, ensure each message has a response
	for id, msg := range idToMsg {
		result, ok := results[id]
		if !ok {
			g.slog.Debug("no result for id", "id", id)
			if e := msg.Nak(); e != nil {
				g.slog.Error("error naking message", "err", e)
			}
			continue
		}
		g.slog.Debug("sending out message", "id", id)
		fmt.Printf("result: %v\n", string(result))
		out <- &gptMessage{original: msg, data: result}
	}
	return nil
}

// formatPromptItems builds the prompt for a batch of items
func (g *GPTRunner) formatPromptItems(batch []inputItem) (string, error) {
	b, err := json.Marshal(batch)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\nMESSAGES: %s\nReturn a JSON array of objects with the same 'id' and a 'result' field.", g.cfg.Action, string(b)), nil
}

func (g *GPTRunner) parseBatchResponse(resp string) (map[string][]byte, error) {
	// Try to extract a JSON array from the response
	var arr []resultItem
	dec := json.NewDecoder(bytes.NewReader([]byte(resp)))
	dec.UseNumber()
	if err := dec.Decode(&arr); err != nil {
		// fallback: try to find the first [ ... ]
		start := -1
		end := -1
		for i, c := range resp {
			if c == '[' && start == -1 {
				start = i
			}
			if c == ']' {
				end = i + 1
			}
		}
		if start >= 0 && end > start {
			if err := json.Unmarshal([]byte(resp[start:end]), &arr); err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("no json array found in response")
		}
	}
	results := make(map[string][]byte, len(arr))
	for _, obj := range arr {
		if obj.ID != "" {
			results[obj.ID] = []byte(obj.Result)
		}
	}
	return results, nil
}

func (g *GPTRunner) Close() error {
	g.slog.Info("closing gpt runner")
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.stopCh:
		// already closed
	default:
		close(g.stopCh)
	}
	return nil
}
