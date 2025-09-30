//nolint:errcheck
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/gdamore/tcell/v2"
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
	basePanelName  = "goncurrently"
	lineJoinFormat = "%s%s\n"
)

var errorOutput io.Writer = os.Stderr

type panelAppearance struct {
	BorderColor     tcell.Color
	TitleColor      tcell.Color
	BackgroundColor tcell.Color
}

type stopSignals struct {
	stop      <-chan struct{}
	immediate <-chan struct{}
}

func baseLog(format string, args ...any) {
	if !strings.HasSuffix(format, "\n") {
		format += "\n"
	}
	fmt.Fprintf(errorOutput, format, args...)
}

type outputRouter interface {
	BaseWriter() io.Writer
	LineWriter(name string, col *color.Color, prefix string) func(string)
	Stop()
}

func newOutputRouter(enableTUI bool, commands []CommandConfig, styles map[string]panelAppearance) (outputRouter, error) {
	if !enableTUI {
		return &consoleRouter{}, nil
	}
	commandNames := make([]string, 0, len(commands))
	for _, c := range commands {
		commandNames = append(commandNames, c.Name)
	}
	return newTUIRouter(basePanelName, commandNames, styles)
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
	app         *tview.Application
	baseName    string
	views       map[string]*tview.TextView
	defaultView *tview.TextView
	stopOnce    sync.Once
	runDone     chan struct{}
	runErr      error
}

func newTUIRouter(baseName string, commandNames []string, styles map[string]panelAppearance) (*tuiRouter, error) {
	app := tview.NewApplication()
	sectionNames := make([]string, 0, len(commandNames)+1)
	sectionNames = append(sectionNames, baseName)
	sectionNames = append(sectionNames, commandNames...)
	if len(sectionNames) == 0 {
		sectionNames = []string{baseName}
	}
	seen := make(map[string]struct{}, len(sectionNames))
	unique := make([]string, 0, len(sectionNames))
	for _, name := range sectionNames {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		unique = append(unique, name)
	}
	sectionNames = unique
	layout, views := buildTUILayout(sectionNames, styles)

	defaultView := views[sectionNames[0]]
	if _, ok := views[baseName]; !ok {
		baseName = sectionNames[0]
	}

	t := &tuiRouter{
		app:         app,
		baseName:    baseName,
		views:       views,
		defaultView: defaultView,
		runDone:     make(chan struct{}),
	}

	app.SetRoot(layout, true)
	if focusView, ok := views[baseName]; ok {
		app.SetFocus(focusView)
	}

	go func() {
		t.runErr = app.Run()
		close(t.runDone)
	}()

	return t, nil
}

func buildTUILayout(sectionNames []string, styles map[string]panelAppearance) (*tview.Flex, map[string]*tview.TextView) {
	rows, cols := calculateGridDimensions(len(sectionNames))
	rootFlex := tview.NewFlex().SetDirection(tview.FlexRow)
	rowFlexes := make([]*tview.Flex, rows)
	for r := 0; r < rows; r++ {
		rowFlex := tview.NewFlex().SetDirection(tview.FlexColumn)
		rootFlex.AddItem(rowFlex, 0, 1, false)
		rowFlexes[r] = rowFlex
	}

	views := make(map[string]*tview.TextView, len(sectionNames))
	for idx, name := range sectionNames {
		style := styles[name]
		view := createPanelView(name, style)
		rowFlexes[idx/cols].AddItem(view, 0, 1, idx == 0)
		views[name] = view
	}
	return rootFlex, views
}

func calculateGridDimensions(total int) (rows int, cols int) {
	if total <= 0 {
		return 1, 1
	}
	cols = int(math.Ceil(math.Sqrt(float64(total))))
	if cols < 1 {
		cols = 1
	}
	rows = int(math.Ceil(float64(total) / float64(cols)))
	if rows < 1 {
		rows = 1
	}
	return rows, cols
}

func createPanelView(name string, style panelAppearance) *tview.TextView {
	textView := tview.NewTextView()
	textView.SetDynamicColors(true)
	textView.SetBorder(true)
	textView.SetTitle(name)
	if style.BorderColor != tcell.ColorDefault {
		textView.SetBorderColor(style.BorderColor)
	}
	if style.TitleColor != tcell.ColorDefault {
		textView.SetTitleColor(style.TitleColor)
	}
	if style.BackgroundColor != tcell.ColorDefault {
		textView.SetBackgroundColor(style.BackgroundColor)
	}
	textView.SetScrollable(true)
	textView.SetWrap(true)
	return textView
}

func (t *tuiRouter) BaseWriter() io.Writer {
	view := t.views[t.baseName]
	if view == nil {
		view = t.defaultView
	}
	return &textViewWriter{
		app:         t.app,
		view:        view,
		prefix:      color.New(color.FgHiCyan).Sprint("[gonc] "),
		atLineStart: true,
	}
}

func (t *tuiRouter) LineWriter(name string, col *color.Color, prefix string) func(string) {
	view, ok := t.views[name]
	if !ok || view == nil {
		view = t.views[t.baseName]
	}
	if view == nil {
		view = t.defaultView
	}
	if view == nil {
		return func(string) {
			/* no available view; dropping line */
		}
	}
	coloredPrefix := prefix
	if col != nil {
		coloredPrefix = col.Sprint(prefix)
	}
	return func(line string) {
		t.app.QueueUpdateDraw(func() {
			writer := tview.ANSIWriter(view)
			fmt.Fprintf(writer, lineJoinFormat, coloredPrefix, line)
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
	app         *tview.Application
	view        *tview.TextView
	prefix      string
	atLineStart bool
}

func (w *textViewWriter) Write(p []byte) (int, error) {
	if w == nil || w.view == nil {
		return len(p), nil
	}
	text := string(p)
	w.app.QueueUpdateDraw(func() {
		tvWriter := tview.ANSIWriter(w.view)
		remaining := text
		for len(remaining) > 0 {
			newlineIdx := strings.IndexByte(remaining, '\n')
			if w.atLineStart && w.prefix != "" {
				fmt.Fprint(tvWriter, w.prefix)
			}
			if newlineIdx == -1 {
				fmt.Fprint(tvWriter, remaining)
				w.atLineStart = false
				break
			}
			fmt.Fprint(tvWriter, remaining[:newlineIdx])
			fmt.Fprint(tvWriter, "\n")
			w.atLineStart = true
			remaining = remaining[newlineIdx+1:]
		}
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

	colors := []*color.Color{
		color.New(color.FgCyan),
		color.New(color.FgGreen),
		color.New(color.FgMagenta),
		color.New(color.FgYellow),
		color.New(color.FgBlue),
		color.New(color.FgRed),
	}

	panelColors := []tcell.Color{
		tcell.GetColor("aqua"),
		tcell.GetColor("springgreen"),
		tcell.GetColor("fuchsia"),
		tcell.GetColor("yellow"),
		tcell.GetColor("dodgerblue"),
		tcell.GetColor("indianred"),
	}

	panelStyles := make(map[string]panelAppearance, len(cfg.Commands)+1)
	for i, c := range cfg.Commands {
		panelColor := panelColors[i%len(panelColors)]
		panelStyles[c.Name] = panelAppearance{
			BorderColor:     panelColor,
			TitleColor:      panelColor,
			BackgroundColor: tcell.ColorDefault,
		}
	}
	if _, ok := panelStyles[basePanelName]; !ok {
		panelStyles[basePanelName] = panelAppearance{
			BorderColor:     tcell.ColorDarkGray,
			TitleColor:      tcell.ColorWhite,
			BackgroundColor: tcell.ColorDefault,
		}
	}

	router, err := newOutputRouter(cfg.EnableTUI, cfg.Commands, panelStyles)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize output routing: %v\n", err)
		os.Exit(1)
	}
	defer router.Stop()

	errorOutput = router.BaseWriter()
	baseLog("Initialized goncurrently | commands=%d setup=%d killOthers=%t", len(cfg.Commands), len(cfg.SetupCommands), cfg.KillOthers)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	immediateStop := make(chan struct{})
	var stopOnce sync.Once
	var immediateOnce sync.Once
	closeStop := func() {
		stopOnce.Do(func() {
			close(stop)
		})
	}
	closeImmediate := func() {
		immediateOnce.Do(func() {
			close(immediateStop)
		})
	}
	triggerImmediateStop := func() {
		closeStop()
		closeImmediate()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		color.New(color.FgRed, color.Bold).Fprintf(errorOutput, "Interrupt received, stopping all processes...\n")
		triggerImmediateStop()
	}()

	// Run setup commands sequentially (with restart semantics, without global stop handling)
	runSetupSequence(cfg.SetupCommands, colors, router)
	if len(cfg.SetupCommands) > 0 {
		baseLog("Setup phase completed")
	}

	for i, c := range cfg.Commands {
		wg.Add(1)
		go func(idx int, cc CommandConfig) {
			defer wg.Done()
			signals := stopSignals{stop: stop, immediate: immediateStop}
			requestStop := closeStop
			baseLog("[%s] worker initialized", cc.Name)
			runManagedCommand(cc, colors[idx%len(colors)], router, signals, time.Duration(cfg.KillTimeout)*time.Millisecond, cfg.KillOthers, requestStop)
		}(i, c)
	}

	go func() {
		wg.Wait()
		closeStop()
	}()

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

func terminateProcess(cmd *exec.Cmd, killTimeout time.Duration, done <-chan error, immediate bool) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	if immediate {
		killTimeout = 0
	}
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
func executeOnce(c CommandConfig, identifier string, stdoutWriter, stderrWriter func(string), signals stopSignals, killTimeout time.Duration) (error, bool, bool) {
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
	case <-signals.stop:
		immediate := false
		if signals.immediate != nil {
			select {
			case <-signals.immediate:
				immediate = true
			default:
			}
		}
		logCommandLine(stdoutWriter, stderrWriter, identifier, "interrupted")
		terminateProcess(cmd, killTimeout, done, immediate)
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
		baseLog("[setup:%s] starting", c.Name)
		if !runSetupWithRetries(c, identifier, stdoutWriter, stderrWriter) {
			color.New(color.FgRed, color.Bold).Fprintf(errorOutput, "Setup command '%s' failed after retries\n", c.Name)
			os.Exit(1)
		}
		baseLog("[setup:%s] completed", c.Name)
	}
}

func logCommandOutcome(name string, err error, timedOut bool) {
	switch {
	case err == nil:
		baseLog("[%s] completed successfully", name)
	case timedOut:
		baseLog("[%s] timed out: %v", name, err)
	default:
		baseLog("[%s] exited with error: %v", name, err)
	}
}

func handleNoRestart(name string, killOthers bool, alert *color.Color, requestStop func()) {
	if killOthers {
		alert.Fprintf(errorOutput, "Stopping all processes due to killOnExit triggered by '%s'\n", name)
		requestStop()
	}
	baseLog("[%s] will not restart (killOthers=%t)", name, killOthers)
}

func logRestartSchedule(name string, attempt int, restartTries int, triesLeft int) {
	if restartTries >= 0 {
		baseLog("[%s] scheduling restart (attempt %d of %d, remaining retries %d)", name, attempt, restartTries+1, triesLeft)
		return
	}
	baseLog("[%s] scheduling restart (attempt %d)", name, attempt)
}

// runManagedCommand runs a command with restart semantics and stop/kill orchestration.
func runManagedCommand(c CommandConfig, col *color.Color, sink outputRouter, signals stopSignals, killTimeout time.Duration, killOthers bool, requestStop func()) {
	prefix := fmt.Sprintf("[%s] ", c.Name)
	stdoutWriter := sink.LineWriter(c.Name, col, prefix)
	stderrWriter := sink.LineWriter(c.Name, col, fmt.Sprintf("[%s stderr] ", c.Name))
	alert := color.New(color.FgRed, color.Bold)
	triesLeft := c.RestartTries
	if waitStartDelay(c, signals.stop) {
		baseLog("[%s] start aborted before launch", c.Name)
		return
	}
	baseLog("[%s] starting", c.Name)
	attempt := 1
	for {
		err, timedOut, interrupted := executeOnce(c, prefix, stdoutWriter, stderrWriter, signals, killTimeout)
		if interrupted {
			baseLog("[%s] interrupted", c.Name)
			return
		}
		logCommandOutcome(c.Name, err, timedOut)
		if !shouldRestart(err, timedOut, &triesLeft, c.RestartTries) {
			handleNoRestart(c.Name, killOthers, alert, requestStop)
			return
		}
		attempt++
		logRestartSchedule(c.Name, attempt, c.RestartTries, triesLeft)
		if waitRestartDelay(c, signals.stop) {
			baseLog("[%s] restart aborted due to stop signal", c.Name)
			return
		}
		baseLog("[%s] restarting now", c.Name)
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
		err, timedOut, _ := executeOnce(c, identifier, stdoutWriter, stderrWriter, stopSignals{}, 0)
		if err == nil || timedOut {
			return true
		}
		if !shouldRestart(err, timedOut, &triesLeft, c.RestartTries) {
			return false
		}
		_ = waitRestartDelay(c, nil)
	}
}
