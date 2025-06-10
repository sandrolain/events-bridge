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

	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
)

type CommandConfig struct {
	Name    string   `yaml:"name"`
	Cmd     string   `yaml:"cmd"`
	Args    []string `yaml:"args"`
	Restart bool     `yaml:"restart"`
}

type Config struct {
	Commands   []CommandConfig `yaml:"commands"`
	KillOnExit bool            `yaml:"killOnExit"`
}

func main() {
	var cfg Config
	if err := yaml.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse config: %v\n", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	killed := false

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		color.New(color.FgRed, color.Bold).Fprintln(os.Stderr, "[concurrently] Interrupt received, stopping all processes...")
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

	for i, c := range cfg.Commands {
		wg.Add(1)
		go func(idx int, cc CommandConfig) {
			defer wg.Done()
			prefix := fmt.Sprintf("[%s] ", cc.Name)
			col := colors[idx%len(colors)]
			for {
				cmd := exec.Command(cc.Cmd, cc.Args...)
				stdout, _ := cmd.StdoutPipe()
				stderr, _ := cmd.StderrPipe()
				if err := cmd.Start(); err != nil {
					fmt.Fprintf(os.Stderr, "%sfailed to start: %v\n", prefix, err)
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
					if !cc.Restart || killed {
						if cfg.KillOnExit {
							color.New(color.FgRed, color.Bold).Fprintln(os.Stderr, "[concurrently] Stopping all processes due to killOnExit...")
							select {
							case <-stop:
								// already closed
							default:
								close(stop)
							}
						}
						return
					}
					if killed {
						return
					}
				case <-stop:
					color.New(color.FgYellow, color.Bold).Fprintf(os.Stderr, "%sinterrupted\n", prefix)
					_ = cmd.Process.Kill()
					return
				}
			}
		}(i, c)
	}
	<-stop
	killed = true
	wg.Wait()
}

func streamOutput(col *color.Color, prefix string, r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fmt.Printf("%s%s\n", col.Sprint(prefix), scanner.Text())
	}
}
