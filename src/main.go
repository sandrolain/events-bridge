package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/sandrolain/events-bridge/src/bridge"
	"github.com/sandrolain/events-bridge/src/config"
)

func main() {
	// Setup signal handling for graceful shutdown
	ctx, cancel := setupSignalHandling()
	defer cancel()

	// Setup logging
	logger := setupLogging()

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		fatal(logger, err, "failed to load configuration file")
	}

	// Create events bridge
	evBridge, err := bridge.NewEventsBridge(cfg, logger)
	if err != nil {
		fatal(logger, err, "failed to create events bridge")
	}
	defer func() {
		if err := evBridge.Close(); err != nil {
			logger.Error("failed to close bridge", "error", err)
		}
	}()

	// Run the bridge
	if err := evBridge.Run(ctx); err != nil && err != context.Canceled {
		fatal(logger, err, "bridge stopped with error")
	}

	// Monitor for shutdown signal
	<-ctx.Done()
	logger.Info("shutdown signal received, cleaning up resources")
	logger.Info("graceful shutdown completed")
}

// setupSignalHandling configures signal handling for graceful shutdown
func setupSignalHandling() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("received signal, initiating graceful shutdown", "signal", sig.String())
		cancel()
	}()

	return ctx, cancel
}

// setupLogging configures the global logger with custom options
func setupLogging() *slog.Logger {
	logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.Kitchen,
	}))
	slog.SetDefault(logger)
	return logger.With("context", "main")
}

func fatal(l *slog.Logger, err error, log string) {
	slog.Error(log, "error", err)
	os.Exit(1)
}
