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
	"github.com/sandrolain/events-bridge/src/utils"
	openai "github.com/sashabaranov/go-openai"
)

// Assicura che GPTRunner implementi runners.Runner
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

func (g *GPTRunner) Ingest(in <-chan message.Message) (<-chan message.Message, error) {
	g.slog.Info("starting gpt ingestion")
	out := make(chan message.Message)
	batchSize := g.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 5
	}
	batchWait := g.cfg.BatchWait
	if batchWait == 0 {
		batchWait = 2 * time.Second
	}
	g.slog.Debug("batcher config", "batchSize", batchSize, "batchWait", batchWait)
	batched := utils.Batcher(in, batchSize, batchWait)
	go func() {
		defer close(out)
		for {
			select {
			case msgs, ok := <-batched:
				g.slog.Debug("received batch", "count", len(msgs), "ok", ok)
				if !ok {
					return
				}
				if err := g.processBatch(msgs, out); err != nil {
					g.slog.Error("gpt batch error", "error", err)
				}
			case <-g.stopCh:
				g.slog.Info("gpt runner stopped via stopCh")
				return
			}
		}
	}()
	return out, nil
}

func (g *GPTRunner) processBatch(msgs []message.Message, out chan<- message.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	batch := make([]inputItem, 0, len(msgs))
	idToMsg := make(map[string]message.Message)
	for i, msg := range msgs {
		data, err := msg.GetData()
		if err != nil {
			g.slog.Debug("GetData failed", "index", i, "error", err)
			msg.Nak()
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
			msg.Nak()
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
			msg.Nak()
		}
		return fmt.Errorf("openai error: %w", err)
	}
	g.slog.Debug("openai response received", "choices", len(resp.Choices))
	if len(resp.Choices) == 0 {
		g.slog.Error("no choices from openai")
		for _, msg := range msgs {
			msg.Nak()
		}
		return fmt.Errorf("no choices from openai")
	}
	res := resp.Choices[0].Message.Content
	g.slog.Debug("openai response content", "content", res)
	results, err := g.parseResponse(res)
	if err != nil {
		g.slog.Error("failed to parse gpt response", "error", err)
		for _, msg := range msgs {
			msg.Nak()
		}
		return fmt.Errorf("failed to parse gpt response: %w", err)
	}
	g.slog.Debug("parsed gpt results", "results_count", len(results))
	// Dopo aver popolato results, verifica che ogni messaggio abbia una risposta
	for id, msg := range idToMsg {
		result, ok := results[id]
		if !ok {
			g.slog.Debug("no result for id", "id", id)
			msg.Nak()
			continue
		}
		g.slog.Debug("sending out message", "id", id)
		fmt.Printf("result: %v\n", string(result))
		out <- &gptMessage{original: msg, data: result}
	}
	return nil
}

// formatPromptItems accetta un batch di item e restituisce il prompt
func (g *GPTRunner) formatPromptItems(batch []inputItem) (string, error) {
	b, err := json.Marshal(batch)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\nMESSAGES: %s\nReturn a JSON array of objects with the same 'id' and a 'result' field.", g.cfg.Action, string(b)), nil
}

func (g *GPTRunner) parseResponse(resp string) (map[string][]byte, error) {
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
		// già chiuso
	default:
		close(g.stopCh)
	}
	return nil
}
