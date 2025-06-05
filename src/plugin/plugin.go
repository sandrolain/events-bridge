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
	ID      string   `yaml:"id" json:"id" validate:"required"`
	Exec    string   `yaml:"exec" json:"exec" validate:"required"`
	Args    []string `yaml:"args" json:"args" validate:"omitempty"`
	Env     []string `yaml:"env" json:"env" validate:"omitempty"`
	Delay   string   `yaml:"delay" json:"delay" default:"1s" validate:"omitempty,duration"`
	Retry   int      `yaml:"retry" json:"retry" default:"3" validate:"omitempty,gt=0"`
	Marshal string   `yaml:"marshal" json:"marshal" default:"msgpack" validate:"required,oneof=json msgpack gob"`
	Output  bool     `yaml:"output" json:"output" default:"true"`
}

type Plugin struct {
	AppPort   int
	ID        string
	Host      string
	Port      int
	Exec      string
	Name      string
	Args      []string
	Env       []string
	Config    PluginConfig
	Output    bool
	cmd       *exec.Cmd
	conn      *grpc.ClientConn
	client    proto.PluginServiceClient
	ConnRetry int
	ConnDelay time.Duration
	stopped   bool
	slog      *slog.Logger
}

func (p *Plugin) Start() (err error) {
	slog.Info("Starting plugin", "name", p.Name, "retries", p.ConnRetry, "delay", p.ConnDelay)

	p.cmd = exec.Command(p.Exec, p.Args...)
	env := append(os.Environ(), p.Env...)
	env = append(env, fmt.Sprintf("APP_PORT=%d", p.AppPort))
	env = append(env, fmt.Sprintf("PLUGIN_ID=%s", p.ID))
	env = append(env, fmt.Sprintf("PLUGIN_PORT=%d", p.Port))
	env = append(env, fmt.Sprintf("CONN_RETRY=%d", p.ConnRetry))
	env = append(env, fmt.Sprintf("CONN_DELAY=%s", p.ConnDelay))
	p.cmd.Env = env

	var stdout io.ReadCloser

	if p.Output {
		stdout, err = p.cmd.StdoutPipe()
		if err != nil {
			slog.Error("Cannot get stdout pipe", "name", p.Name, "err", err)
			return
		}

		go func() {
			// print the output of the subprocess
			scanner := bufio.NewScanner(stdout)
			for !p.stopped && scanner.Scan() {
				m := scanner.Text()
				fmt.Printf(`[PLUGIN: %s] %s\n`, p.Name, m)
			}
		}()
	}

	err = p.cmd.Start()
	if err != nil {
		slog.Error("Cannot start plugin", "name", p.Name, "err", err)
		p.Stop()
		return
	}

	err = p.connect()
	if err != nil {
		slog.Error("Cannot connect to plugin", "name", p.Name, "err", err, "retries", p.ConnRetry, "delay", p.ConnDelay)
		p.Stop()
		return
	}

	go p.startCheckStatus()

	return
}

func (p *Plugin) connect() (err error) {
	addr := fmt.Sprintf("%s:%d", p.Host, p.Port)

	r := retrier.New(retrier.ConstantBackoff(p.ConnRetry, p.ConnDelay), nil)

	err = r.Run(func() (err error) {
		slog.Debug("Connecting to plugin", "name", p.Name, "addr", addr)
		p.conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("Error connecting to plugin", "name", p.Name, "err", err)
			return
		}
		p.client = proto.NewPluginServiceClient(p.conn)
		sts, err := p.checkStatus()
		if err != nil {
			slog.Error("Failed to check status", "name", p.Name, "err", err)
			return
		}
		p.slog.Info("Connected to plugin", "name", p.Name, "status", sts.Status)

		if sts.Status != proto.Status_STATUS_READY {
			err = fmt.Errorf("plugin not ready")
			return
		}

		return
	})
	return
}

func (p *Plugin) startCheckStatus() {
	for !p.stopped {
		sts, e := p.checkStatus()
		if e != nil {
			slog.Error("Failed to check status", "name", p.Name, "err", e)
		}
		slog.Debug("Plugin status", "name", p.Name, "status", sts.Status)
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
	if p.client != nil {
		slog.Debug("Shutting down plugin", "name", p.Name)
		_, err := p.client.Shutdown(context.Background(), &proto.ShutdownReq{})
		if err != nil {
			slog.Error("Error shutting down plugin", "name", p.Name, "err", err)
		}
	}

	if p.conn != nil {
		slog.Debug("Closing connection to plugin", "name", p.Name)
		err := p.conn.Close()
		if err != nil {
			slog.Error("Error closing connection to plugin", "name", p.Name, "err", err)
		}
	}

	if p.cmd != nil && p.cmd.Process != nil {
		slog.Debug("Killing plugin", "name", p.Name)
		err := p.cmd.Process.Kill()
		if err != nil {
			slog.Error("Error killing plugin", "name", p.Name, "err", err)
		}
	}
}
