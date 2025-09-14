package plugin

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
)

type PluginConfig struct {
	Name     string        `yaml:"name" json:"name" validate:"required"`
	Exec     string        `yaml:"exec" json:"exec" validate:"required"`
	Args     []string      `yaml:"args" json:"args" validate:"omitempty"`
	Env      []string      `yaml:"env" json:"env" validate:"omitempty"`
	Protocol string        `yaml:"protocol" json:"protocol" validate:"required,oneof=unix tcp"`
	Delay    time.Duration `yaml:"delay" json:"delay" validate:"omitempty"`
	Retry    int           `yaml:"retry" json:"retry" validate:"omitempty,gt=0"`
	Output   bool          `yaml:"output" json:"output"`
}

type Plugin struct {
	ID      string
	Address string
	Port    int
	Config  PluginConfig
	cmd     *exec.Cmd
	conn    *grpc.ClientConn
	client  proto.PluginServiceClient
	stopped bool
	slog    *slog.Logger
}

func (p *Plugin) Start() (err error) {
	cfg := p.Config

	p.slog.Info("Starting plugin", "name", cfg.Name, "retries", cfg.Retry, "delay", cfg.Delay)

	var address string
	switch cfg.Protocol {
	case "unix":
		// For Unix sockets, we use the ID as the address
		address = fmt.Sprintf("/tmp/%s_%d.sock", p.ID, time.Now().UnixMilli())
		p.Address = fmt.Sprintf("unix://%s", address)
	case "tcp":
		var port int
		port, err = GetFreePort()
		if err != nil {
			err = fmt.Errorf("cannot get free port: %w", err)
			return
		}
		p.Port = port
		// For TCP, we use the ID as the host and a default port
		address = fmt.Sprintf("%s:%d", "0.0.0.0", p.Port)
		p.Address = fmt.Sprintf("%s:%d", "localhost", p.Port)
	default:
		err = fmt.Errorf("unsupported protocol: %s", p.Config.Protocol)
		return
	}

	p.cmd = exec.Command(cfg.Exec, cfg.Args...)
	env := append(os.Environ(), cfg.Env...)
	env = append(env, fmt.Sprintf("PLUGIN_ID=%s", p.ID))
	env = append(env, fmt.Sprintf("PLUGIN_PROTOCOL=%s", cfg.Protocol))
	env = append(env, fmt.Sprintf("PLUGIN_ADDRESS=%s", address))
	p.cmd.Env = env

	p.slog.Debug("Plugin start", "name", cfg.Name, "protocol", cfg.Protocol, "exec", cfg.Exec, "args", cfg.Args, "address", address)

	if cfg.Output {
		var stdout io.ReadCloser
		var stderr io.ReadCloser

		stdout, err = p.cmd.StdoutPipe()
		if err != nil {
			p.slog.Error("Cannot get stdout pipe", "name", cfg.Name, "err", err)
			return
		}

		go func() {
			// print the output of the subprocess
			scanner := bufio.NewScanner(stdout)
			for !p.stopped && scanner.Scan() {
				m := scanner.Text()
				s := fmt.Sprintf("[PLUGIN: %s] %s\n", cfg.Name, m)
				_, err := os.Stdout.WriteString(s)
				if err != nil {
					p.slog.Error("Error writing to stdout", "name", cfg.Name, "err", err)
				}
			}
		}()

		stderr, err = p.cmd.StderrPipe()
		if err != nil {
			p.slog.Error("Cannot get stderr pipe", "name", cfg.Name, "err", err)
			return
		}

		go func() {
			// print the error output of the subprocess
			scanner := bufio.NewScanner(stderr)
			for !p.stopped && scanner.Scan() {
				m := scanner.Text()
				s := fmt.Sprintf("[PLUGIN: %s] %s\n", cfg.Name, m)
				_, err := os.Stderr.WriteString(s)
				if err != nil {
					p.slog.Error("Error writing to stderr", "name", cfg.Name, "err", err)
				}
			}
		}()
	}

	err = p.cmd.Start()
	if err != nil {
		p.slog.Error("Cannot start plugin", "name", cfg.Name, "err", err)
		p.Stop()
		return
	}

	err = p.connect()
	if err != nil {
		p.slog.Error("Cannot connect to plugin", "name", cfg.Name, "err", err, "retries", cfg.Retry, "delay", cfg.Delay)
		p.Stop()
		return
	}

	go p.startCheckStatus()

	return
}

func (p *Plugin) connect() (err error) {
	cfg := p.Config

	r := retrier.New(retrier.ConstantBackoff(cfg.Retry, cfg.Delay), nil)

	err = r.Run(func() (err error) {
		p.slog.Debug("Connecting to plugin", "name", cfg.Name, "addr", p.Address)
		p.conn, err = grpc.NewClient(p.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			p.slog.Error("Error connecting to plugin", "name", cfg.Name, "err", err)
			return
		}
		p.client = proto.NewPluginServiceClient(p.conn)
		sts, err := p.checkStatus()
		if err != nil {
			p.slog.Error("Failed to check status", "name", cfg.Name, "err", err)
			return
		}
		p.slog.Info("Connected to plugin", "name", cfg.Name, "status", sts.Status)

		if sts.Status != proto.Status_STATUS_READY {
			err = fmt.Errorf("plugin not ready")
			return
		}

		return
	})
	return
}

func (p *Plugin) startCheckStatus() {
	cfg := p.Config
	for !p.stopped {
		sts, e := p.checkStatus()
		if e != nil {
			p.slog.Error("Failed to check status", "name", cfg.Name, "err", e)
		}
		p.slog.Debug("Plugin status", "name", cfg.Name, "status", sts.Status)
		// TODO: handle status
		// TODO: duration from config
		time.Sleep(5 * time.Second)
	}
}

func (p *Plugin) checkStatus() (sts *proto.StatusRes, err error) {
	sts, err = p.client.Status(context.Background(), &proto.StatusReq{})
	if err != nil {
		err = fmt.Errorf("failed to check status: %w", err)
		return
	}
	return
}

func (p *Plugin) Stop() {
	cfg := p.Config
	if p.client != nil {
		p.slog.Debug("Shutting down plugin", "name", cfg.Name)
		_, err := p.client.Shutdown(context.Background(), &proto.ShutdownReq{})
		if err != nil {
			p.slog.Error("Error shutting down plugin", "name", cfg.Name, "err", err)
		}
	}

	if p.conn != nil {
		p.slog.Debug("Closing connection to plugin", "name", cfg.Name)
		err := p.conn.Close()
		if err != nil {
			p.slog.Error("Error closing connection to plugin", "name", cfg.Name, "err", err)
		}
	}

	if p.cmd != nil && p.cmd.Process != nil {
		p.slog.Debug("Killing plugin", "name", cfg.Name)
		err := p.cmd.Process.Kill()
		if err != nil {
			p.slog.Error("Error killing plugin", "name", cfg.Name, "err", err)
		}
	}
}
