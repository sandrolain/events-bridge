package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/common/encdec"
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

	clientConfig := openai.DefaultConfig(cfg.ApiKey)
	clientConfig.BaseURL = cfg.ApiURL

	client := openai.NewClientWithConfig(clientConfig)
	return &GPTRunner{
		cfg:     cfg,
		slog:    slog.Default().With("context", "GTP Runner"),
		client:  client,
		decoder: decoder,
	}, nil
}

func (g *GPTRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	g.slog.Debug("sending prompt to openai", "prompt", g.cfg.Prompt)

	jsonData, err := g.decoder.EncodeMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode message to json: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.Timeout)
	defer cancel()
	resp, err := g.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: g.cfg.Model,
		Messages: []openai.ChatCompletionMessage{{
			Role: openai.ChatMessageRoleUser,
			MultiContent: []openai.ChatMessagePart{
				{Type: openai.ChatMessagePartTypeText, Text: g.cfg.Prompt},
				{Type: openai.ChatMessagePartTypeText, Text: string(jsonData)},
			},
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
