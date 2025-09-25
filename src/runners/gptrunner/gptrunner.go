package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/sandrolain/events-bridge/src/utils"
	openai "github.com/sashabaranov/go-openai"
)

// Ensure GPTRunner implements runners.Runner
var _ runners.Runner = &GPTRunner{}

const (
	errNoChoicesFromOpenAI = "no choices from openai"
	logNakMessage          = "error naking message"
)

type Config struct {
	ApiURL    string
	ApiKey    string
	Action    string
	Model     string
	BatchSize int
	BatchWait time.Duration
	MaxTokens int
	Timeout   time.Duration
}

type GPTRunner struct {
	cfg    *Config
	slog   *slog.Logger
	client *openai.Client
}

type inputItem struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

type resultItem struct {
	ID     string `json:"id"`
	Result string `json:"result"`
}

func parseConfig(opts map[string]any) (*Config, error) {
	cfg := &Config{}
	parser := &utils.OptsParser{}
	cfg.ApiURL = parser.OptString(opts, "api_url", "", utils.StringNonEmpty())
	cfg.ApiKey = parser.OptString(opts, "api_key", "", utils.StringNonEmpty())
	cfg.Action = parser.OptString(opts, "action", "", utils.StringNonEmpty())
	cfg.Model = parser.OptString(opts, "model", openai.GPT3Dot5Turbo)
	cfg.BatchSize = parser.OptInt(opts, "batch_size", 0)
	cfg.BatchWait = parser.OptDuration(opts, "batch_wait", 0)
	cfg.MaxTokens = parser.OptInt(opts, "max_tokens", 0)
	cfg.Timeout = parser.OptDuration(opts, "timeout", runners.DefaultTimeout, utils.DurationPositive())
	if err := parser.Error(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func New(opts map[string]any) (runners.Runner, error) {
	cfg, err := parseConfig(opts)
	if err != nil {
		return nil, err
	}
	log := slog.Default().With("context", "GPTRUNNER")

	clientConfig := openai.DefaultConfig(cfg.ApiKey)
	if cfg.ApiURL != "" {
		clientConfig.BaseURL = cfg.ApiURL
	}
	client := openai.NewClientWithConfig(clientConfig)
	return &GPTRunner{
		cfg:    cfg,
		slog:   log,
		client: client,
	}, nil
}

func (g *GPTRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	prompt, err := g.formatPrompt(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to format prompt: %w", err)
	}
	g.slog.Debug("sending prompt to openai", "prompt", prompt)
	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.Timeout)
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
		return nil, errors.New(errNoChoicesFromOpenAI)
	}
	result := resp.Choices[0].Message.Content
	g.slog.Debug("openai response content", "content", result)

	msg.SetData([]byte(result))

	return msg, nil
}

// formatPromptItems builds the prompt for a batch of items
func (g *GPTRunner) formatPrompt(msg *message.RunnerMessage) (string, error) {
	b, err := msg.GetSourceData()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\n\n%s", g.cfg.Action, string(b)), nil
}

// ProcessBatch handles batches of messages by batching prompts to the GPT API.
func (g *GPTRunner) ProcessBatch(msgs []*message.RunnerMessage) ([]*message.RunnerMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.Timeout)
	defer cancel()
	batch := make([]inputItem, 0, len(msgs))
	idToMsg := make(map[string]*message.RunnerMessage)
	for i, msg := range msgs {
		data, err := msg.GetSourceData()
		if err != nil {
			g.slog.Debug("GetData failed", "index", i, "error", err)
			g.tryNak(msg)
			continue
		}
		id := uuid.NewString()
		batch = append(batch, inputItem{ID: id, Data: string(data)})
		idToMsg[id] = msg
	}
	g.slog.Debug("batch ready", "size", len(batch))
	if len(batch) == 0 {
		return nil, fmt.Errorf("no valid messages to process")
	}
	prompt, err := g.formatPromptItems(batch)
	if err != nil {
		g.slog.Error("failed to format prompt", "error", err)
		for _, msg := range msgs {
			g.tryNak(msg)
		}
		return nil, fmt.Errorf("failed to format prompt: %w", err)
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
			g.tryNak(msg)
		}
		return nil, fmt.Errorf("openai error: %w", err)
	}
	g.slog.Debug("openai response received", "choices", len(resp.Choices))
	if len(resp.Choices) == 0 {
		g.slog.Error(errNoChoicesFromOpenAI)
		for _, msg := range msgs {
			g.tryNak(msg)
		}
		return nil, errors.New(errNoChoicesFromOpenAI)
	}
	res := resp.Choices[0].Message.Content
	g.slog.Debug("openai response content", "content", res)
	results, err := g.parseBatchResponse(res)
	if err != nil {
		g.slog.Error("failed to parse gpt response", "error", err)
		for _, msg := range msgs {
			g.tryNak(msg)
		}
		return nil, fmt.Errorf("failed to parse gpt response: %w", err)
	}
	g.slog.Debug("parsed gpt results", "results_count", len(results))

	// After populating results, ensure each message has a response
	for id, msg := range idToMsg {
		result, ok := results[id]
		if !ok {
			g.slog.Debug("no result for id", "id", id)
			g.tryNak(msg)
			continue
		}

		msg.SetData(result)
	}
	return msgs, nil
}

func (g *GPTRunner) tryNak(msg *message.RunnerMessage) {
	if err := msg.Nak(); err != nil {
		g.slog.Error(logNakMessage, "err", err)
	}
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
	return nil
}
