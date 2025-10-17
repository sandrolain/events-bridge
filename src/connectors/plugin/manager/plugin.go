package manager

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
	"github.com/sandrolain/events-bridge/src/connectors/plugin/proto"
)

type PluginConfig struct {
	Name           string        `mapstructure:"name" validate:"required"`
	Exec           string        `mapstructure:"exec" validate:"required"`
	Args           []string      `mapstructure:"args" validate:"omitempty"`
	Env            []string      `mapstructure:"env" validate:"omitempty"`
	Protocol       string        `mapstructure:"protocol" validate:"required,oneof=unix tcp"`
	Delay          time.Duration `mapstructure:"delay" default:"500ms" validate:"omitempty"`
	Retry          int           `mapstructure:"retry" default:"3" validate:"omitempty,gt=0"`
	Output         bool          `mapstructure:"output"`
	StatusInterval time.Duration `mapstructure:"statusInterval" default:"3s" validate:"omitempty"` // Interval to check status
	Timeout        time.Duration `mapstructure:"timeout" default:"5s" validate:"omitempty"`        // Timeout for plugin operations
	// Security settings
	AllowedPluginsDir string `mapstructure:"allowedPluginsDir" validate:"omitempty"` // Directory where plugins must be located
	ExpectedSHA256    string `mapstructure:"expectedSHA256" validate:"omitempty"`    // Expected SHA256 hash of plugin binary
	VerifyHash        bool   `mapstructure:"verifyHash" default:"false"`             // Whether to verify plugin hash
	StrictValidation  bool   `mapstructure:"strictValidation" default:"true"`        // Enable strict security validation
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
	timeout time.Duration
	slog    *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

func (p *Plugin) Start() (err error) {
	cfg := p.Config

	// Security validations
	if err := p.validateSecurity(); err != nil {
		return fmt.Errorf("security validation failed: %w", err)
	}

	// Initialize context for goroutine management
	p.ctx, p.cancel = context.WithCancel(context.Background())

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

	p.cmd = exec.Command(cfg.Exec, cfg.Args...) // #nosec G204 - plugin execution requires external command execution
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
			defer func() {
				if err := stdout.Close(); err != nil {
					p.slog.Error("Failed to close stdout pipe", "name", cfg.Name, "err", err)
				}
			}()
			// print the output of the subprocess
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				select {
				case <-p.ctx.Done():
					return
				default:
				}

				if p.stopped {
					return
				}

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
			defer func() {
				if err := stderr.Close(); err != nil {
					p.slog.Error("Failed to close stderr pipe", "name", cfg.Name, "err", err)
				}
			}()
			// print the error output of the subprocess
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				select {
				case <-p.ctx.Done():
					return
				default:
				}

				if p.stopped {
					return
				}

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
	ticker := time.NewTicker(cfg.StatusInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			p.slog.Debug("Status check stopped due to context cancellation", "name", cfg.Name)
			return
		case <-ticker.C:
			if p.stopped {
				return
			}

			sts, e := p.checkStatus()
			if e != nil {
				p.slog.Error("Failed to check status", "name", cfg.Name, "err", e)
			} else {
				p.slog.Debug("Plugin status", "name", cfg.Name, "status", sts.Status)
				// TODO: handle status
			}
		}
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

func (p *Plugin) validateSecurity() error {
	cfg := p.Config

	// Validate plugin name
	if err := ValidatePluginName(cfg.Name); err != nil {
		return fmt.Errorf("invalid plugin name: %w", err)
	}

	// Validate plugin path if strict validation is enabled
	if cfg.StrictValidation {
		if err := ValidatePluginPath(cfg.Exec, cfg.AllowedPluginsDir); err != nil {
			return fmt.Errorf("plugin path validation failed: %w", err)
		}
	}

	// Verify plugin hash if enabled
	if cfg.VerifyHash {
		if cfg.ExpectedSHA256 == "" {
			return fmt.Errorf("hash verification enabled but expectedSHA256 not provided")
		}
		if err := VerifyPluginHash(cfg.Exec, cfg.ExpectedSHA256); err != nil {
			return fmt.Errorf("plugin hash verification failed: %w", err)
		}
		p.slog.Info("Plugin hash verified successfully", "name", cfg.Name)
	}

	// Validate environment variables
	if err := SanitizePluginEnv(cfg.Env); err != nil {
		return fmt.Errorf("invalid environment variables: %w", err)
	}

	// Validate arguments
	if err := ValidatePluginArgs(cfg.Args); err != nil {
		return fmt.Errorf("invalid plugin arguments: %w", err)
	}

	return nil
}

func (p *Plugin) Stop() {
	cfg := p.Config

	// Cancel context to stop all goroutines
	if p.cancel != nil {
		p.cancel()
	}

	// Mark as stopped
	p.stopped = true

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
