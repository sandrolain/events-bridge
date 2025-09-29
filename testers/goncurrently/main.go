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
	"github.com/rivo/tview"
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
	EnableTUI     bool            `yaml:"enableTUI"` // If true, render outputs in a tview-based dashboard
}

const (
	basePanelName  = "system"
	lineJoinFormat = "%s%s\n"
)

var errorOutput io.Writer = os.Stderr

type outputRouter interface {
	BaseWriter() io.Writer
	LineWriter(name string, col *color.Color, prefix string) func(string)
	Stop()
}

func newOutputRouter(enableTUI bool, commands []CommandConfig) (outputRouter, error) {
	if !enableTUI {
		return &consoleRouter{}, nil
	}
	commandNames := make([]string, 0, len(commands))
	for _, c := range commands {
		commandNames = append(commandNames, c.Name)
	}
	return newTUIRouter(basePanelName, commandNames)
}

type consoleRouter struct{}

func (c *consoleRouter) BaseWriter() io.Writer {
	return os.Stderr
}

func (c *consoleRouter) LineWriter(_ string, col *color.Color, prefix string) func(string) {
	coloredPrefix := prefix
	if col != nil {
		coloredPrefix = col.Sprint(prefix)
	}
	return func(line string) {
		fmt.Printf(lineJoinFormat, coloredPrefix, line)
	}
}

// Stop satisfies the outputRouter interface; console output requires no cleanup.
func (c *consoleRouter) Stop() {
	// No cleanup required for console output.
}

type tuiRouter struct {
	app      *tview.Application
	baseName string
	views    map[string]*tview.TextView
	flex     *tview.Flex
	stopOnce sync.Once
	runDone  chan struct{}
	runErr   error
}

func newTUIRouter(baseName string, commandNames []string) (*tuiRouter, error) {
	app := tview.NewApplication()
	flex := tview.NewFlex().SetDirection(tview.FlexRow)
	views := make(map[string]*tview.TextView)
	sectionNames := append([]string{baseName}, commandNames...)
	for _, name := range sectionNames {
		textView := tview.NewTextView()
		textView.SetDynamicColors(true)
		textView.SetBorder(true)
		textView.SetTitle(name)
		flex.AddItem(textView, 0, 1, false)
		views[name] = textView
	}

	t := &tuiRouter{
		app:      app,
		baseName: baseName,
		views:    views,
		flex:     flex,
		runDone:  make(chan struct{}),
	}

	app.SetRoot(flex, true)
	app.SetFocus(flex)

	go func() {
		t.runErr = app.Run()
		close(t.runDone)
	}()

	return t, nil
}

func (t *tuiRouter) BaseWriter() io.Writer {
	return &textViewWriter{app: t.app, view: t.views[t.baseName]}
}

func (t *tuiRouter) LineWriter(name string, _ *color.Color, prefix string) func(string) {
	view, ok := t.views[name]
	if !ok {
		view = t.views[t.baseName]
	}
	return func(line string) {
		t.app.QueueUpdateDraw(func() {
			fmt.Fprintf(view, lineJoinFormat, prefix, line)
			view.ScrollToEnd()
		})
	}
}

func (t *tuiRouter) Stop() {
	t.stopOnce.Do(func() {
		if t.app != nil {
			t.app.Stop()
		}
		if t.runDone != nil {
			<-t.runDone
		}
	})
}

type textViewWriter struct {
	app  *tview.Application
	view *tview.TextView
}

func (w *textViewWriter) Write(p []byte) (int, error) {
	if w == nil || w.view == nil {
		return len(p), nil
	}
	text := string(p)
	w.app.QueueUpdateDraw(func() {
		fmt.Fprint(w.view, text)
		w.view.ScrollToEnd()
	})
	return len(p), nil
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

	router, err := newOutputRouter(cfg.EnableTUI, cfg.Commands)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize output routing: %v\n", err)
		os.Exit(1)
	}
	defer router.Stop()

	if cfg.EnableTUI {
		color.NoColor = true
	}

	errorOutput = router.BaseWriter()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		color.New(color.FgRed, color.Bold).Fprintf(errorOutput, "[goncurrently] Interrupt received, stopping all processes...\n")
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
	runSetupSequence(cfg.SetupCommands, colors, router)

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
			runManagedCommand(cc, colors[idx%len(colors)], router, stop, time.Duration(cfg.KillTimeout)*time.Millisecond, cfg.KillOthers, requestStop)
		}(i, c)
	}
	<-stop
	wg.Wait()
}

func streamOutput(writeLine func(string), r io.Reader) {
	if writeLine == nil {
		_, _ = io.Copy(io.Discard, r)
		return
	}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		writeLine(scanner.Text())
	}
}

func logCommandLine(stdoutWriter, stderrWriter func(string), identifier, message string) {
	switch {
	case stderrWriter != nil:
		stderrWriter(message)
	case stdoutWriter != nil:
		stdoutWriter(message)
	default:
		fmt.Fprintf(errorOutput, "%s%s\n", identifier, message)
	}
}

func terminateProcess(cmd *exec.Cmd, killTimeout time.Duration, done <-chan error) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	if killTimeout <= 0 {
		_ = cmd.Process.Kill()
		return
	}
	select {
	case <-done:
	case <-time.After(killTimeout):
		_ = cmd.Process.Kill()
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
		fmt.Fprintf(errorOutput, "invalid duration for %s in command '%s': %v\n", field, commandName, err)
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
func executeOnce(c CommandConfig, identifier string, stdoutWriter, stderrWriter func(string), stop <-chan struct{}, killTimeout time.Duration) (error, bool, bool) {
	cmd, ctx, cancel, stdout, stderr, err := startProcess(c)
	if err != nil {
		logCommandLine(stdoutWriter, stderrWriter, identifier, fmt.Sprintf("failed to start: %v", err))
		return err, false, false
	}
	if !c.Silent {
		go streamOutput(stdoutWriter, stdout)
		go streamOutput(stderrWriter, stderr)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	select {
	case err := <-done:
		timedOut := ctx != nil && ctx.Err() == context.DeadlineExceeded
		return err, timedOut, false
	case <-stop:
		logCommandLine(stdoutWriter, stderrWriter, identifier, "interrupted")
		terminateProcess(cmd, killTimeout, done)
		return nil, false, true
	}
}

// runSetupSequence runs setup commands sequentially with restart semantics; exits on unrecoverable error.
func runSetupSequence(cmds []CommandConfig, colors []*color.Color, sink outputRouter) {
	for i, c := range cmds {
		col := colors[i%len(colors)]
		identifier := fmt.Sprintf("[setup:%s] ", c.Name)
		stdoutWriter := sink.LineWriter(basePanelName, col, identifier)
		stderrWriter := sink.LineWriter(basePanelName, col, fmt.Sprintf("[setup:%s stderr] ", c.Name))
		if d := mustParseDurationField("startAfter", c.StartAfter, c.Name); d > 0 {
			time.Sleep(d)
		}
		if !runSetupWithRetries(c, identifier, stdoutWriter, stderrWriter) {
			color.New(color.FgRed, color.Bold).Fprintf(errorOutput, "[goncurrently] Setup command '%s' failed after retries\n", c.Name)
			os.Exit(1)
		}
	}
}

// runManagedCommand runs a command with restart semantics and stop/kill orchestration.
func runManagedCommand(c CommandConfig, col *color.Color, sink outputRouter, stop <-chan struct{}, killTimeout time.Duration, killOthers bool, requestStop func()) {
	prefix := fmt.Sprintf("[%s] ", c.Name)
	stdoutWriter := sink.LineWriter(c.Name, col, prefix)
	stderrWriter := sink.LineWriter(c.Name, col, fmt.Sprintf("[%s stderr] ", c.Name))
	alert := color.New(color.FgRed, color.Bold)
	triesLeft := c.RestartTries
	if waitStartDelay(c, stop) {
		return
	}
	for {
		err, timedOut, interrupted := executeOnce(c, prefix, stdoutWriter, stderrWriter, stop, killTimeout)
		if interrupted {
			return
		}
		if !shouldRestart(err, timedOut, &triesLeft, c.RestartTries) {
			if killOthers {
				alert.Fprintf(errorOutput, "[goncurrently] Stopping all processes due to killOnExit triggered by '%s'\n", c.Name)
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
func runSetupWithRetries(c CommandConfig, identifier string, stdoutWriter, stderrWriter func(string)) bool {
	triesLeft := c.RestartTries
	for {
		err, timedOut, _ := executeOnce(c, identifier, stdoutWriter, stderrWriter, nil, 0)
		if err == nil || timedOut {
			return true
		}
		if !shouldRestart(err, timedOut, &triesLeft, c.RestartTries) {
			return false
		}
		_ = waitRestartDelay(c, nil)
	}
}
