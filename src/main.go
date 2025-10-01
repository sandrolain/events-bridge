package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/destel/rill"
	"github.com/lmittmann/tint"
	"github.com/sandrolain/events-bridge/src/config"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/utils"
)

func main() {
	w := os.Stdout

	// Set global logger with custom options
	slog.SetDefault(slog.New(
		tint.NewHandler(w, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.Kitchen,
		}),
	))

	l := slog.Default().With("context", "main")

	cfg, err := config.LoadConfig()
	if err != nil {
		fatal(l, err, "failed to load configuration file")
	}

	var source connectors.Source
	var runners = make([]RunnerItem, len(cfg.Runners))
	var target connectors.Target
	var closeErrors []error

	// Setup source, runner, and target
	l.Info("creating source", "type", cfg.Source.Type, "buffer", cfg.Source.Buffer)

	source, err = utils.LoadPluginAndConfig[connectors.Source](
		connectorPath(cfg.Source.Type),
		connectors.NewSourceMethodName,
		connectors.NewSourceConfigName,
		cfg.Source.Options,
	)
	if err != nil {
		fatal(l, err, "failed to create source")
	}
	defer func() {
		if err := closeWithRetry(source.Close, 3, time.Second); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("failed to close source: %w", err))
		}
	}()

	if len(cfg.Runners) > 0 {
		l.Info("creating runners", "count", len(cfg.Runners))

		for i, runnerConfig := range cfg.Runners {
			l.Info("creating runner", "type", runnerConfig.Type)

			runner, err := utils.LoadPluginAndConfig[connectors.Runner](
				connectorPath(runnerConfig.Type),
				connectors.NewRunnerMethodName,
				connectors.NewRunnerConfigName,
				runnerConfig.Options,
			)
			if err != nil {
				fatal(l, err, "failed to create runner")
			}
			defer func() {
				if err := closeWithRetry(runner.Close, 3, time.Second); err != nil {
					closeErrors = append(closeErrors, fmt.Errorf("failed to close runner: %w", err))
				}
			}()
			runners[i] = RunnerItem{
				Config: runnerConfig,
				Runner: runner,
			}
		}
	} else {
		l.Info("no runner configured, messages will be passed through without processing")
	}

	if cfg.Target.Type != "" && cfg.Target.Type != "none" {
		l.Info("creating target", "type", cfg.Target.Type)

		target, err = utils.LoadPluginAndConfig[connectors.Target](
			connectorPath(cfg.Target.Type),
			connectors.NewTargetMethodName,
			connectors.NewTargetConfigName,
			cfg.Target.Options,
		)
		if err != nil {
			fatal(l, err, "failed to create target")
		}
		l.Info("target created, deferring close")
		defer func() {
			if err := closeWithRetry(target.Close, 3, time.Second); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close target: %w", err))
			}
		}()
	} else {
		l.Info("no target configured, messages will be replied back to source if supported")
	}

	// Start the event incoming loop
	c, err := source.Produce(cfg.Source.Buffer)
	if err != nil {
		fatal(l, err, "failed to produce messages from source")
	}
	defer func() {
		if err := source.Close(); err != nil {
			l.Error("failed to close source", "error", err)
		}
	}()

	out := rill.FromChan(c, nil)
	defer func() {
		rill.Drain(out)
	}()

	if len(runners) > 0 {
		l.Info("runner starting to consume messages from source")

		for i, runnerItem := range runners {
			runner := runnerItem.Runner
			cfg := runnerItem.Config

			runnerRoutines := cfg.Routines
			if runnerRoutines < 1 {
				runnerRoutines = 1
			}

			evaluator, err := utils.NewExprEvaluator(cfg.IfExpr)
			if err != nil {
				fatal(l, err, fmt.Sprintf("failed to create expression evaluator for runner %d ifExpr: %s", i, cfg.IfExpr))
			}

			out = rill.OrderedFilterMap(out, runnerRoutines, func(msg *message.RunnerMessage) (*message.RunnerMessage, bool, error) {
				if cfg.IfExpr != "" {
					meta, err := msg.GetTargetMetadata()
					if err != nil {
						l.Error("failed to get message metadata for ifExpr evaluation, skipping runner processing", "ifExpr", cfg.IfExpr, "error", err)
						if err = msg.Nak(); err != nil {
							l.Error("failed to nak message after ifExpr metadata retrieval error", "error", err)
						}
						return msg, false, nil
					}

					data, err := msg.GetTargetData()
					if err != nil {
						l.Error("failed to get message data for ifExpr evaluation, skipping runner processing", "ifExpr", cfg.IfExpr, "error", err)
						if err = msg.Nak(); err != nil {
							l.Error("failed to nak message after ifExpr data retrieval error", "error", err)
						}
						return msg, false, nil
					}

					pass, err := evaluator.Eval(map[string]any{
						"metadata": meta,
						"data":     data,
					})
					if err != nil {
						l.Error("failed to evaluate ifExpr, skipping runner processing", "ifExpr", cfg.IfExpr, "error", err)
						if err = msg.Nak(); err != nil {
							l.Error("failed to nak message after ifExpr evaluation error", "error", err)
						}
						// If expression evaluation fails, we skip processing but do not drop the message
						return msg, false, nil
					}

					if !pass {
						l.Debug("ifExpr evaluated to false, skipping runner processing", "ifExpr", cfg.IfExpr)
						return msg, true, nil
					}
				}

				res, err := runner.Process(msg)
				if err != nil {
					l.Error("error processing message", "error", err)

					if err = msg.Nak(); err != nil {
						l.Error("failed to nak message after processing error", "error", err)
					}
					return nil, false, nil
				}

				return res, true, nil
			})
		}
	} else {
		l.Info("no runner configured, passing messages through without processing")
	}

	if target != nil {
		targetRoutines := cfg.Target.Routines
		if targetRoutines < 1 {
			targetRoutines = 1
		}

		l.Info("target starting to consume messages", "routines", targetRoutines)

		err = rill.ForEach(out, targetRoutines, func(msg *message.RunnerMessage) error {
			err := target.Consume(msg)
			if err != nil {
				l.Error("failed to consume message", "error", err)
				err = msg.Nak()
				if err != nil {
					l.Error("failed to nak message after consume failure", "error", err)
				}
			} else {
				err = msg.Ack()
				if err != nil {
					l.Error("failed to ack message after consume", "error", err)
				}
			}
			return nil
		})
		if err != nil {
			fatal(l, err, "failed to process messages with target")
		}
	} else {
		err = rill.ForEach(out, 1, func(msg *message.RunnerMessage) error {
			err := msg.Reply()
			if err != nil {
				l.Error("failed to reply message", "error", err)
				err = msg.Nak()
				if err != nil {
					l.Error("failed to nak message after reply", "error", err)
				}
			}
			return nil
		})
		if err != nil {
			fatal(l, err, "failed to process messages without target")
		}
	}

	// Log any close errors
	if len(closeErrors) > 0 {
		for _, err := range closeErrors {
			l.Error("close error", "error", err)
		}
	}
}

func connectorPath(connectorType string) string {
	return fmt.Sprintf("./connectors/%s.so", strings.ToLower(connectorType))
}

func fatal(l *slog.Logger, err error, log string) {
	slog.Error(log, "error", err)
	os.Exit(1)
}

type RunnerItem struct {
	Config connectors.RunnerConfig
	Runner connectors.Runner
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
