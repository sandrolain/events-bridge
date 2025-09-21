//nolint:errcheck
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

type CommandConfig struct {
	Name         string            `yaml:"name"` // name no longer required
	Cmd          string            `yaml:"cmd" validate:"required"`
	Args         []string          `yaml:"args"`
	RestartTries int               `yaml:"restartTries"` // How many times to restart after death; negative = forever
	RestartAfter string            `yaml:"restartAfter"` // Delay before restarting, Go duration string (e.g., "500ms", "2s")
	Env          map[string]string `yaml:"env"`
	StartAfter   string            `yaml:"startAfter"` // Delay before starting, Go duration string (e.g., "500ms", "2s")
	Silent       bool              `yaml:"silent"`     // If true, suppress command stdout/stderr
	Duration     string            `yaml:"duration"`   // Max run duration before terminating (Go duration string)
}

type Config struct {
	Commands      []CommandConfig `yaml:"commands" validate:"required,dive,required"`
	KillOthers    bool            `yaml:"killOthers"`
	KillTimeout   int             `yaml:"killTimeout"` // Milliseconds to wait before forcing termination
	NoColors      bool            `yaml:"noColors"`
	SetupCommands []CommandConfig `yaml:"setupCommands"`
}

func main() {
	var cfg Config
	if err := yaml.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse config: %v\n", err)
		os.Exit(1)
	}

	// Configure color usage
	color.NoColor = cfg.NoColors

	// Assign names if missing
	assignNames(cfg.Commands)
	assignNames(cfg.SetupCommands)

	validate := validator.New()
	if err := validate.Struct(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		color.New(color.FgRed, color.Bold).Fprintln(os.Stderr, "[goncurrently] Interrupt received, stopping all processes...")
		select {
		case <-stop:
			// already closed, do nothing
		default:
			close(stop)
		}
	}()

	colors := []*color.Color{
		color.New(color.FgCyan),
		color.New(color.FgGreen),
		color.New(color.FgMagenta),
		color.New(color.FgYellow),
		color.New(color.FgBlue),
		color.New(color.FgRed),
	}

	// Run setup commands sequentially (with restart semantics, without global stop handling)
	runSetupSequence(cfg.SetupCommands, colors)

	for i, c := range cfg.Commands {
		wg.Add(1)
		go func(idx int, cc CommandConfig) {
			defer wg.Done()
			requestStop := func() {
				select {
				case <-stop:
				default:
					close(stop)
				}
			}
			runManagedCommand(cc, colors[idx%len(colors)], stop, time.Duration(cfg.KillTimeout)*time.Millisecond, cfg.KillOthers, requestStop)
		}(i, c)
	}
	<-stop
	wg.Wait()
}

func streamOutput(col *color.Color, prefix string, r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fmt.Printf("%s%s\n", col.Sprint(prefix), scanner.Text())
	}
}

// mustParseDurationField parses a duration field from YAML. On invalid value it exits
// with an error message indicating the problematic field and command name.
func mustParseDurationField(field string, value string, commandName string) time.Duration {
	if value == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid duration for %s in command '%s': %v\n", field, commandName, err)
		os.Exit(1)
	}
	return d
}

// assignNames sets the name of each command to the basename of Cmd if empty.
func assignNames(cmds []CommandConfig) {
	for i := range cmds {
		if cmds[i].Name == "" {
			cmdParts := cmds[i].Cmd
			lastSlash := -1
			for j := len(cmdParts) - 1; j >= 0; j-- {
				if cmdParts[j] == '/' {
					lastSlash = j
					break
				}
			}
			if lastSlash != -1 && lastSlash+1 < len(cmdParts) {
				cmds[i].Name = cmdParts[lastSlash+1:]
			} else {
				cmds[i].Name = cmdParts
			}
		}
	}
}

// startProcess prepares and starts the process based on CommandConfig, returning the cmd, context cancel (if any), and pipes.
func startProcess(c CommandConfig) (cmd *exec.Cmd, ctx context.Context, cancel context.CancelFunc, stdout, stderr io.ReadCloser, err error) {
	if dur := mustParseDurationField("duration", c.Duration, c.Name); dur > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), dur)
		cmd = exec.CommandContext(ctx, c.Cmd, c.Args...)
	} else {
		cmd = exec.Command(c.Cmd, c.Args...)
	}
	if c.Env != nil {
		env := os.Environ()
		for k, v := range c.Env {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
		cmd.Env = env
	}
	if c.Silent {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	} else {
		if stdout, err = cmd.StdoutPipe(); err != nil {
			return nil, ctx, cancel, nil, nil, err
		}
		if stderr, err = cmd.StderrPipe(); err != nil {
			return nil, ctx, cancel, nil, nil, err
		}
	}
	if err = cmd.Start(); err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, ctx, nil, nil, nil, err
	}
	return cmd, ctx, cancel, stdout, stderr, nil
}

// executeOnce runs the command until completion or stop is requested. It returns (err, timedOut, interrupted).
func executeOnce(c CommandConfig, col *color.Color, prefix string, stop <-chan struct{}, killTimeout time.Duration) (error, bool, bool) {
	cmd, ctx, cancel, stdout, stderr, err := startProcess(c)
	if err != nil {
		color.New(color.FgRed, color.Bold).Fprintf(os.Stderr, "%sfailed to start: %v\n", prefix, err)
		return err, false, false
	}
	if !c.Silent {
		go streamOutput(col, prefix, stdout)
		go streamOutput(col, prefix, stderr)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		timedOut := ctx != nil && ctx.Err() == context.DeadlineExceeded
		if cancel != nil {
			cancel()
		}
		return err, timedOut, false
	case <-stop:
		color.New(color.FgYellow, color.Bold).Fprintf(os.Stderr, "%sinterrupted\n", prefix)
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			if killTimeout > 0 {
				select {
				case <-done:
				case <-time.After(killTimeout):
					_ = cmd.Process.Kill()
				}
			} else {
				_ = cmd.Process.Kill()
			}
		}
		if cancel != nil {
			cancel()
		}
		return nil, false, true
	}
}

// runSetupSequence runs setup commands sequentially with restart semantics; exits on unrecoverable error.
func runSetupSequence(cmds []CommandConfig, colors []*color.Color) {
	for i, c := range cmds {
		col := colors[i%len(colors)]
		if d := mustParseDurationField("startAfter", c.StartAfter, c.Name); d > 0 {
			time.Sleep(d)
		}
		if !runSetupWithRetries(c, col) {
			os.Exit(1)
		}
	}
}

// runManagedCommand runs a command with restart semantics and stop/kill orchestration.
func runManagedCommand(c CommandConfig, col *color.Color, stop <-chan struct{}, killTimeout time.Duration, killOthers bool, requestStop func()) {
	prefix := fmt.Sprintf("[%s] ", c.Name)
	triesLeft := c.RestartTries
	if waitStartDelay(c, stop) {
		return
	}
	for {
		err, timedOut, interrupted := executeOnce(c, col, prefix, stop, killTimeout)
		if interrupted {
			return
		}
		if !shouldRestart(err, timedOut, &triesLeft, c.RestartTries) {
			if killOthers {
				color.New(color.FgRed, color.Bold).Fprintln(os.Stderr, "[goncurrently] Stopping all processes due to killOnExit...")
				requestStop()
			}
			return
		}
		if waitRestartDelay(c, stop) {
			return
		}
	}
}

// Helper: wait for start delay; returns true if stopped during wait
func waitStartDelay(c CommandConfig, stop <-chan struct{}) bool {
	if d := mustParseDurationField("startAfter", c.StartAfter, c.Name); d > 0 {
		if stop == nil {
			time.Sleep(d)
			return false
		}
		select {
		case <-stop:
			return true
		case <-time.After(d):
			return false
		}
	}
	return false
}

// Helper: wait for restart delay; returns true if stopped during wait
func waitRestartDelay(c CommandConfig, stop <-chan struct{}) bool {
	if d := mustParseDurationField("restartAfter", c.RestartAfter, c.Name); d > 0 {
		if stop == nil {
			time.Sleep(d)
			return false
		}
		select {
		case <-stop:
			return true
		case <-time.After(d):
			return false
		}
	}
	return false
}

// Helper: compute if we should restart and update triesLeft
func shouldRestart(err error, timedOut bool, triesLeft *int, restartTries int) bool {
	if err == nil || timedOut {
		return false
	}
	if restartTries < 0 {
		return true
	}
	if *triesLeft > 0 {
		*triesLeft--
		return true
	}
	return false
}

// Helper: run setup command with retries; returns true on success/timed-out, false on fatal failure
func runSetupWithRetries(c CommandConfig, col *color.Color) bool {
	prefix := fmt.Sprintf("[%s] ", c.Name)
	triesLeft := c.RestartTries
	for {
		err, timedOut, _ := executeOnce(c, col, prefix, nil, 0)
		if err == nil || timedOut {
			return true
		}
		if !shouldRestart(err, timedOut, &triesLeft, c.RestartTries) {
			return false
		}
		_ = waitRestartDelay(c, nil)
	}
}
