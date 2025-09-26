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

	runnerRoutines := cfg.Runner.Routines
	if runnerRoutines < 1 {
		runnerRoutines = 1
	}

	targetRoutines := cfg.Target.Routines
	if targetRoutines < 1 {
		targetRoutines = 1
	}

	var source connectors.Source
	var runner connectors.Runner
	var target connectors.Target

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
		if err := source.Close(); err != nil {
			l.Error("failed to close source", "error", err)
		}
	}()

	l.Info("creating runner", "type", cfg.Runner.Type)

	runner, err = utils.LoadPluginAndConfig[connectors.Runner](
		connectorPath(cfg.Runner.Type),
		connectors.NewRunnerMethodName,
		connectors.NewRunnerConfigName,
		cfg.Runner.Options,
	)
	if err != nil {
		fatal(l, err, "failed to create runner")
	}
	if runner != nil {
		l.Info("runner created, deferring close")
		defer func() {
			if err := runner.Close(); err != nil {
				l.Error("failed to close runner", "error", err)
			}
		}()
	} else {
		l.Info("no runner configured, messages will be passed through without processing")
	}

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
	if target != nil {
		l.Info("target created, deferring close")
		defer func() {
			if err := target.Close(); err != nil {
				l.Error("failed to close target", "error", err)
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

	if runner != nil {
		l.Info("runner starting to consume messages from source")

		out = rill.OrderedFilterMap(out, runnerRoutines, func(msg *message.RunnerMessage) (*message.RunnerMessage, bool, error) {
			res, err := runner.Process(msg)
			if err != nil {
				l.Error("error processing message", "error", err)
				err = msg.Nak()
				if err != nil {
					l.Error("failed to nak message after processing error", "error", err)
				}
				return nil, false, nil
			}
			return res, true, nil
		})
	} else {
		l.Info("no runner configured, passing messages through without processing")
	}

	if target != nil {
		l.Info("target starting to consume messages")

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

	rill.Drain(out)

	select {}
}

func connectorPath(connectorType string) string {
	return fmt.Sprintf("./connectors/%s.so", strings.ToLower(connectorType))
}

func fatal(l *slog.Logger, err error, log string) {
	slog.Error(log, "error", err)
	os.Exit(1)
}
