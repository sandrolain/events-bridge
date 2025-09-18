//nolint:errcheck
package main

import (
	"bufio"
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
	RestartAfter int               `yaml:"restartAfter"` // Delay before restarting, in milliseconds
	Env          map[string]string `yaml:"env"`
	StartAfter   int               `yaml:"startAfter"` // Delay before starting, in milliseconds
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

	// Assign the name if missing using the final part of Cmd
	for i := range cfg.Commands {
		if cfg.Commands[i].Name == "" {
			cmdParts := cfg.Commands[i].Cmd
			// Extract only the command name (after the last /)
			lastSlash := -1
			for j := len(cmdParts) - 1; j >= 0; j-- {
				if cmdParts[j] == '/' {
					lastSlash = j
					break
				}
			}
			if lastSlash != -1 && lastSlash+1 < len(cmdParts) {
				cfg.Commands[i].Name = cmdParts[lastSlash+1:]
			} else {
				cfg.Commands[i].Name = cmdParts
			}
		}
	}
	for i := range cfg.SetupCommands {
		if cfg.SetupCommands[i].Name == "" {
			cmdParts := cfg.SetupCommands[i].Cmd
			lastSlash := -1
			for j := len(cmdParts) - 1; j >= 0; j-- {
				if cmdParts[j] == '/' {
					lastSlash = j
					break
				}
			}
			if lastSlash != -1 && lastSlash+1 < len(cmdParts) {
				cfg.SetupCommands[i].Name = cmdParts[lastSlash+1:]
			} else {
				cfg.SetupCommands[i].Name = cmdParts
			}
		}
	}

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
	for i, c := range cfg.SetupCommands {
		prefix := fmt.Sprintf("[%s] ", c.Name)
		col := colors[i%len(colors)]
		triesLeft := c.RestartTries
		// Optional delay before the very first start
		if c.StartAfter > 0 {
			time.Sleep(time.Duration(c.StartAfter) * time.Millisecond)
		}
	SetupLoop:
		for {
			cmd := exec.Command(c.Cmd, c.Args...)
			if c.Env != nil {
				env := os.Environ()
				for k, v := range c.Env {
					env = append(env, fmt.Sprintf("%s=%s", k, v))
				}
				cmd.Env = env
			}
			stdout, _ := cmd.StdoutPipe()
			stderr, _ := cmd.StderrPipe()
			if err := cmd.Start(); err != nil {
				color.New(color.FgRed, color.Bold).Fprintf(os.Stderr, "%sfailed to start: %v\n", prefix, err)
				os.Exit(1)
			}
			go streamOutput(col, prefix, stdout)
			go streamOutput(col, prefix, stderr)
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			err := <-done
			if err != nil {
				color.New(color.FgRed, color.Bold).Fprintf(os.Stderr, "%sprocess exited: %v\n", prefix, err)
			}
			shouldRestart := false
			if err != nil {
				if c.RestartTries < 0 {
					shouldRestart = true
				} else if triesLeft > 0 {
					triesLeft--
					shouldRestart = true
				}
			}
			if !shouldRestart {
				if err != nil {
					// setup command failed and no more retries
					os.Exit(1)
				}
				// success -> move to next setup command
				break SetupLoop
			}
			if c.RestartAfter > 0 {
				time.Sleep(time.Duration(c.RestartAfter) * time.Millisecond)
			}
			// loop to restart
		}
	}

	for i, c := range cfg.Commands {
		wg.Add(1)
		go func(idx int, cc CommandConfig) {
			defer wg.Done()
			prefix := fmt.Sprintf("[%s] ", cc.Name)
			col := colors[idx%len(colors)]
			triesLeft := cc.RestartTries
			// Optional delay before the very first start (cancellable)
			if cc.StartAfter > 0 {
				select {
				case <-stop:
					return
				case <-time.After(time.Duration(cc.StartAfter) * time.Millisecond):
				}
			}
			for {
				cmd := exec.Command(cc.Cmd, cc.Args...)
				if cc.Env != nil {
					env := os.Environ()
					for k, v := range cc.Env {
						env = append(env, fmt.Sprintf("%s=%s", k, v))
					}
					cmd.Env = env
				}
				stdout, _ := cmd.StdoutPipe()
				stderr, _ := cmd.StderrPipe()
				if err := cmd.Start(); err != nil {
					color.New(color.FgRed, color.Bold).Fprintf(os.Stderr, "%sfailed to start: %v\n", prefix, err)
					if cfg.KillOthers {
						color.New(color.FgRed, color.Bold).Fprintln(os.Stderr, "[goncurrently] Stopping all processes due to killOnExit (startup failure)...")
						select {
						case <-stop:
							// already closed
						default:
							close(stop)
						}
					}
					return
				}
				go streamOutput(col, prefix, stdout)
				go streamOutput(col, prefix, stderr)
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				select {
				case err := <-done:
					if err != nil {
						color.New(color.FgRed, color.Bold).Fprintf(os.Stderr, "%sprocess exited: %v\n", prefix, err)
					}
					shouldRestart := false
					if err != nil { // only restart if process died with error
						if cc.RestartTries < 0 {
							shouldRestart = true
						} else if triesLeft > 0 {
							triesLeft--
							shouldRestart = true
						}
					}
					if !shouldRestart {
						if cfg.KillOthers {
							color.New(color.FgRed, color.Bold).Fprintln(os.Stderr, "[goncurrently] Stopping all processes due to killOnExit...")
							select {
							case <-stop:
							default:
								close(stop)
							}
						}
						return
					}
					// don't restart if a global stop was requested meanwhile
					select {
					case <-stop:
						return
					default:
					}
					if cc.RestartAfter > 0 {
						select {
						case <-stop:
							return
						case <-time.After(time.Duration(cc.RestartAfter) * time.Millisecond):
						}
					}
				case <-stop:
					color.New(color.FgYellow, color.Bold).Fprintf(os.Stderr, "%sinterrupted\n", prefix)
					if cmd.Process != nil {
						_ = cmd.Process.Signal(syscall.SIGTERM)
						if cfg.KillTimeout > 0 {
							select {
							case <-done:
							case <-time.After(time.Duration(cfg.KillTimeout) * time.Millisecond):
								_ = cmd.Process.Kill()
							}
						} else {
							_ = cmd.Process.Kill()
						}
					}
					return
				}
			}
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
