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

	cfg, err := config.LoadConfig()
	if err != nil {
		fatal(err, "failed to load configuration file")
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

	source, err = createSource(cfg.Source)
	if err != nil {
		fatal(err, "failed to create source")
	}
	defer func() {
		if err := source.Close(); err != nil {
			slog.Error("failed to close source", "error", err)
		}
	}()

	runner, err = createRunner(cfg.Runner)
	if err != nil {
		fatal(err, "failed to create runner")
	}
	if runner != nil {
		slog.Info("runner created, deferring close")
		defer func() {
			if err := runner.Close(); err != nil {
				slog.Error("failed to close runner", "error", err)
			}
		}()
	}

	target, err = createTarget(cfg.Target)
	if err != nil {
		fatal(err, "failed to create target")
	}
	if target != nil {
		slog.Info("target created, deferring close")
		defer func() {
			if err := target.Close(); err != nil {
				slog.Error("failed to close target", "error", err)
			}
		}()
	}

	// Start the event incoming loop

	c, err := source.Produce(cfg.Source.Buffer)
	if err != nil {
		fatal(err, "failed to produce messages from source")
	}
	defer func() {
		if err := source.Close(); err != nil {
			slog.Error("failed to close source", "error", err)
		}
	}()

	out := rill.FromChan(c, nil)

	if runner != nil {
		slog.Info("runner starting to consume messages from source")

		out = rill.OrderedFilterMap(out, runnerRoutines, func(msg *message.RunnerMessage) (*message.RunnerMessage, bool, error) {
			res, err := runner.Process(msg)
			if err != nil {
				slog.Error("error processing message", "error", err)
				err = msg.Nak()
				if err != nil {
					slog.Error("failed to nak message after processing error", "error", err)
				}
				return nil, false, nil
			}
			return res, true, nil
		})
	} else {
		slog.Info("no runner configured, passing messages through without processing")
	}

	if target != nil {
		slog.Info("target starting to consume messages")

		err = rill.ForEach(out, targetRoutines, func(msg *message.RunnerMessage) error {
			err := target.Consume(msg)
			if err != nil {
				slog.Error("failed to consume message", "error", err)
				err = msg.Nak()
				if err != nil {
					slog.Error("failed to nak message after consume failure", "error", err)
				}
			} else {
				err = msg.Ack()
				if err != nil {
					slog.Error("failed to ack message after consume", "error", err)
				}
			}
			return nil
		})
		if err != nil {
			fatal(err, "failed to process messages with target")
		}
	} else {
		err = rill.ForEach(out, 1, func(msg *message.RunnerMessage) error {
			err := msg.Reply()
			if err != nil {
				slog.Error("failed to reply message", "error", err)
				err = msg.Nak()
				if err != nil {
					slog.Error("failed to nak message after reply", "error", err)
				}
			}
			return nil
		})
		if err != nil {
			fatal(err, "failed to process messages without target")
		}
	}

	rill.Drain(out)

	select {}
}

func createSource(cfg connectors.SourceConfig) (source connectors.Source, err error) {
	slog.Info("creating source", "type", cfg.Type, "buffer", cfg.Buffer)

	if cfg.Type == "" || cfg.Type == "none" {
		err = fmt.Errorf("no source configured, cannot proceed")
		return
	}

	path := fmt.Sprintf("./connectors/%s.so", strings.ToLower(cfg.Type))

	source, err = utils.LoadPlugin[map[string]any, connectors.Source](path, connectors.NewSourceMethodName, cfg.Options)

	return
}

func createTarget(cfg connectors.TargetConfig) (target connectors.Target, err error) {
	slog.Info("creating target", "type", cfg.Type)

	if cfg.Type == "" || cfg.Type == "none" {
		slog.Info("no target configured, messages will be replyed to source if supported")
		target = nil
		return
	}

	path := fmt.Sprintf("./connectors/%s.so", strings.ToLower(cfg.Type))

	target, err = utils.LoadPlugin[map[string]any, connectors.Target](path, connectors.NewTargetMethodName, cfg.Options)

	return
}

func createRunner(cfg connectors.RunnerConfig) (runner connectors.Runner, err error) {
	slog.Info("creating runner", "type", cfg.Type)

	if cfg.Type == "" || cfg.Type == "none" {
		slog.Info("no runner configured, messages will be passed through without processing")
		runner = nil
		return
	}

	path := fmt.Sprintf("./connectors/%s.so", strings.ToLower(cfg.Type))

	runner, err = utils.LoadPlugin[map[string]any, connectors.Runner](path, connectors.NewRunnerMethodName, cfg.Options)

	return
}

func fatal(err error, log string) {
	slog.Error(log, "error", err)
	os.Exit(1)
}
