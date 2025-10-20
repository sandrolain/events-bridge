package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/destel/rill"
	"github.com/sandrolain/events-bridge/src/common/expreval"
	"github.com/sandrolain/events-bridge/src/config"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

// connectorPath returns the path to a connector plugin
func connectorPath(connectorType string) string {
	return fmt.Sprintf("./connectors/%s.so", strings.ToLower(connectorType))
}

// RunnerItem holds a runner configuration and its instance
type RunnerItem struct {
	Config connectors.RunnerConfig
	Runner connectors.Runner
}

// EventsBridge encapsulates the full events bridge lifecycle
type EventsBridge struct {
	cfg     *config.Config
	logger  *slog.Logger
	source  connectors.Source
	runners []RunnerItem
	target  connectors.Target
	handler *MessageHandler
}

// MessageHandler consolidates message lifecycle handling (ack/nak) with consistent logging
type MessageHandler struct {
	logger *slog.Logger
}

// NewMessageHandler creates a new MessageHandler instance
func NewMessageHandler(logger *slog.Logger) *MessageHandler {
	return &MessageHandler{logger: logger}
}

// HandleSuccess acknowledges a message successfully and logs at info level
func (h *MessageHandler) HandleSuccess(msg *message.RunnerMessage, operation string, logArgs ...any) {
	h.logger.Info(operation, logArgs...)
	if msg == nil {
		h.logger.Warn("cannot ack nil message in " + operation)
		return
	}
	if ackErr := msg.Ack(); ackErr != nil {
		h.logger.Error("failed to ack message after "+operation, "error", ackErr)
	}
}

// HandleError handles error cases with consistent logging and nak behavior
func (h *MessageHandler) HandleError(msg *message.RunnerMessage, err error, operation string, additionalFields ...any) {
	logArgs := append([]any{"error", err}, additionalFields...)
	h.logger.Error(operation, logArgs...)
	if msg == nil {
		h.logger.Warn("cannot nak nil message in " + operation)
		return
	}
	if nakErr := msg.Nak(); nakErr != nil {
		h.logger.Error("failed to nak message after "+operation, "error", nakErr)
	}
}

// HandleRunnerError is a rill-compatible version of HandleError that returns the message for pipeline processing
func (h *MessageHandler) HandleRunnerError(msg *message.RunnerMessage, err error, operation string, additionalFields ...any) (*message.RunnerMessage, bool, error) {
	h.HandleError(msg, err, operation, additionalFields...)
	return nil, false, nil
}

// NewEventsBridge creates a new EventsBridge instance and initializes all connectors
func NewEventsBridge(cfg *config.Config, logger *slog.Logger) (*EventsBridge, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	bridge := &EventsBridge{
		cfg:     cfg,
		logger:  logger,
		handler: NewMessageHandler(logger),
		runners: make([]RunnerItem, len(cfg.Runners)),
	}

	if err := bridge.initializeSource(); err != nil {
		return nil, fmt.Errorf("source init: %w", err)
	}

	if err := bridge.initializeRunners(); err != nil {
		return nil, fmt.Errorf("runners init: %w", err)
	}

	if err := bridge.initializeTarget(); err != nil {
		return nil, fmt.Errorf("target init: %w", err)
	}

	return bridge, nil
}

// initializeSource creates and configures the source connector
func (b *EventsBridge) initializeSource() error {
	b.logger.Info("creating source", "type", b.cfg.Source.Type, "buffer", b.cfg.Source.Buffer)

	source, err := utils.LoadPluginAndConfig[connectors.Source](
		connectorPath(b.cfg.Source.Type),
		connectors.NewSourceMethodName,
		connectors.NewSourceConfigName,
		b.cfg.Source.Options,
	)
	if err != nil {
		return fmt.Errorf("failed to create source: %w", err)
	}

	b.source = source
	return nil
}

// initializeRunners creates and configures all runner connectors
func (b *EventsBridge) initializeRunners() error {
	if len(b.cfg.Runners) == 0 {
		b.logger.Info("no runner configured, messages will be passed through without processing")
		return nil
	}

	b.logger.Info("creating runners", "count", len(b.cfg.Runners))

	for i, runnerConfig := range b.cfg.Runners {
		b.logger.Info("creating runner", "type", runnerConfig.Type)

		var runner connectors.Runner
		var err error

		if runnerConfig.Type != "pass" {
			runner, err = utils.LoadPluginAndConfig[connectors.Runner](
				connectorPath(runnerConfig.Type),
				connectors.NewRunnerMethodName,
				connectors.NewRunnerConfigName,
				runnerConfig.Options,
			)
			if err != nil {
				return fmt.Errorf("failed to create runner %d: %w", i, err)
			}
		}

		b.runners[i] = RunnerItem{
			Config: runnerConfig,
			Runner: runner,
		}
	}

	return nil
}

// initializeTarget creates and configures the target connector
func (b *EventsBridge) initializeTarget() error {
	if b.cfg.Target.Type == "" || b.cfg.Target.Type == "none" {
		b.logger.Info("no target configured, messages will be replied back to source if supported")
		return nil
	}

	b.logger.Info("creating target", "type", b.cfg.Target.Type)

	target, err := utils.LoadPluginAndConfig[connectors.Target](
		connectorPath(b.cfg.Target.Type),
		connectors.NewTargetMethodName,
		connectors.NewTargetConfigName,
		b.cfg.Target.Options,
	)
	if err != nil {
		return fmt.Errorf("failed to create target: %w", err)
	}

	b.target = target
	b.logger.Info("target created")
	return nil
}

// Run starts the event bridge and processes messages until context is cancelled
func (b *EventsBridge) Run(ctx context.Context) error {
	// Start message production from source
	c, err := b.source.Produce(b.cfg.Source.Buffer)
	if err != nil {
		return fmt.Errorf("failed to produce messages from source: %w", err)
	}

	out := rill.FromChan(c, nil)
	defer rill.Drain(out)

	// Apply runner pipeline if configured
	if len(b.runners) > 0 {
		b.logger.Info("runner starting to consume messages from source")
		out = b.applyRunners(out)
	} else {
		b.logger.Info("no runner configured, passing messages through without processing")
	}

	// Consume messages with target or reply to source
	if b.target != nil {
		return b.consumeWithTarget(out)
	}
	return b.replyToSource(out)
}

// applyRunners applies all configured runners to the message stream
func (b *EventsBridge) applyRunners(stream rill.Stream[*message.RunnerMessage]) rill.Stream[*message.RunnerMessage] {
	out := stream

	for i, runnerItem := range b.runners {
		runner := runnerItem.Runner
		cfg := runnerItem.Config
		routines := min(cfg.Routines, 1)

		ifEval, err := expreval.NewExprEvaluator(cfg.IfExpr)
		if err != nil {
			b.logger.Error("failed to create ifExpr evaluator", "runner", i, "error", err)
			continue
		}

		filterEval, err := expreval.NewExprEvaluator(cfg.FilterExpr)
		if err != nil {
			b.logger.Error("failed to create filterExpr evaluator", "runner", i, "error", err)
			continue
		}

		out = rill.OrderedFilterMap(out, routines, func(msg *message.RunnerMessage) (*message.RunnerMessage, bool, error) {
			// Evaluate if condition
			if ifEval != nil {
				pass, err := ifEval.EvalMessage(msg)
				if err != nil {
					return b.handler.HandleRunnerError(msg, err, "failed to evaluate ifExpr, skipping runner processing", "ifExpr", cfg.IfExpr)
				}
				if !pass {
					b.logger.Debug("ifExpr evaluated to false, skipping runner processing", "ifExpr", cfg.IfExpr)
					return msg, true, nil
				}
			}

			// Process message with runner
			if runner != nil {
				if err := runner.Process(msg); err != nil {
					return b.handler.HandleRunnerError(msg, err, "error processing message")
				}
			}

			// Evaluate filter condition
			if filterEval != nil {
				pass, err := filterEval.EvalMessage(msg)
				if err != nil {
					return b.handler.HandleRunnerError(msg, err, "failed to evaluate filterExpr, skipping message", "filterExpr", cfg.FilterExpr)
				}
				if !pass {
					b.handler.HandleSuccess(msg, "message filtered out by filterExpr", "filterExpr", cfg.FilterExpr)
					return nil, false, nil
				}
			}

			return msg, true, nil
		})
	}

	return out
}

// consumeWithTarget consumes messages using the configured target
func (b *EventsBridge) consumeWithTarget(stream rill.Stream[*message.RunnerMessage]) error {
	routines := min(b.cfg.Target.Routines, 1)
	b.logger.Info("target starting to consume messages", "routines", routines)

	return rill.ForEach(stream, routines, func(msg *message.RunnerMessage) error {
		if err := b.target.Consume(msg); err != nil {
			b.handler.HandleError(msg, err, "failed to consume message with target")
			return nil
		}
		b.handler.HandleSuccess(msg, "message consumed by target")
		return nil
	})
}

// replyToSource replies messages back to the source
func (b *EventsBridge) replyToSource(stream rill.Stream[*message.RunnerMessage]) error {
	return rill.ForEach(stream, 1, func(msg *message.RunnerMessage) error {
		if err := msg.ReplySource(); err != nil {
			b.handler.HandleError(msg, err, "failed to reply message back to source")
		}
		return nil
	})
}

// Close closes all connectors with retry logic
func (b *EventsBridge) Close() error {
	var closeErrors []error

	// Close source
	if b.source != nil {
		if err := closeWithRetry(b.source.Close, 3, time.Second); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("failed to close source: %w", err))
		}
	}

	// Close all runners
	for i, runnerItem := range b.runners {
		if runnerItem.Runner != nil {
			if err := closeWithRetry(runnerItem.Runner.Close, 3, time.Second); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close runner %d: %w", i, err))
			}
		}
	}

	// Close target
	if b.target != nil {
		if err := closeWithRetry(b.target.Close, 3, time.Second); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("failed to close target: %w", err))
		}
	}

	// Log all errors
	for _, err := range closeErrors {
		b.logger.Error("close error", "error", err)
	}

	if len(closeErrors) > 0 {
		return fmt.Errorf("encountered %d close errors", len(closeErrors))
	}

	return nil
}

// closeWithRetry attempts to close with retries on failure
func closeWithRetry(closeFunc func() error, maxRetries int, delay time.Duration) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = closeFunc()
		if err == nil {
			return nil
		}
		if i < maxRetries-1 {
			time.Sleep(delay)
		}
	}
	return err
}

func min(a, b int) int {
	if a < 1 {
		return b
	}
	return a
}
