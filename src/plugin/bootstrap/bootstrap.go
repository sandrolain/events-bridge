package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StartOptions struct {
	Status   StatusFn
	Shutdown ShutdownFn
	Source   SourceFn
	Runner   RunnerFn
	Target   TargetFn
	Setup    SetupFn
}

type SetupFn func() error

type Config struct {
	ID       string `env:"PLUGIN_ID" validate:"required"`
	Protocol string `env:"PLUGIN_PROTOCOL" validate:"required,oneof=tcp unix"`
	Address  string `env:"PLUGIN_ADDRESS" validate:"required"`
}

var cfg Config
var lis net.Listener
var pluginStatus proto.Status = proto.Status_STATUS_STARTUP
var err error

func Start(opts StartOptions) {
	e := runStart(opts)
	if e != nil {
		slog.Error("failed to bootstrap", "error", e)
	}
}

func runStart(opts StartOptions) (err error) {
	e := env.Parse(&cfg)
	if e != nil {
		err = fmt.Errorf("cannot parse config: %w", e)
		return
	}

	validate := validator.New(validator.WithRequiredStructEnabled())
	e = validate.Struct(cfg)
	if e != nil {
		err = fmt.Errorf("config validation failed: %w", e)
		return
	}

	slog.Info("listening", "protocol", cfg.Protocol, "address", cfg.Address)
	lis, e = net.Listen(cfg.Protocol, cfg.Address)
	if e != nil {
		err = fmt.Errorf("failed to listen: %w", err)
		return
	}

	// Create a new gRPC server
	server := &server{
		status:   opts.Status,
		shutdown: opts.Shutdown,
		source:   opts.Source,
		runner:   opts.Runner,
		target:   opts.Target,
	}

	s := grpc.NewServer()
	s.RegisterService(&proto.PluginService_ServiceDesc, server)

	// Start the gRPC server
	errCh := make(chan error)

	go func() {
		slog.Info("serving")
		e := s.Serve(lis)
		if e != nil {
			slog.Error("failed to serve", "error", e)
			errCh <- fmt.Errorf("failed to serve: %w", err)
		}
	}()

	if opts.Setup != nil {
		go func() {
			slog.Info("executing setup")
			if e := opts.Setup(); e != nil {
				slog.Error("setup failed", "error", e)
				SetError(fmt.Errorf("setup failed: %w", err))
			}
		}()
	} else {
		SetReady()
	}

	err = <-errCh

	return
}

func SetError(e error) {
	err = e
	pluginStatus = proto.Status_STATUS_ERROR
}

func SetReady() bool {
	if pluginStatus == proto.Status_STATUS_STARTUP {
		pluginStatus = proto.Status_STATUS_READY
		return true
	}
	return false
}

func GetStatus() proto.Status {
	return pluginStatus
}

func GetStatusResponse() *proto.StatusRes {
	var errMsg *string
	if err != nil {
		m := err.Error()
		errMsg = &m
	}
	// TODO: implememt self-kill after timeout without status requests
	return &proto.StatusRes{
		Status: pluginStatus,
		Error:  errMsg,
	}
}

func Shutdown(delay *string) *proto.ShutdownRes {
	d := "0s"
	if delay != nil {
		d = *delay
	}
	dl, err := time.ParseDuration(d)
	if err != nil {
		slog.Error("failed to parse duration", "error", err)
	}
	pluginStatus = proto.Status_STATUS_SHUTDOWN
	go func() {
		time.Sleep(dl)
		if lis != nil {
			if err := lis.Close(); err != nil {
				slog.Error("failed to close listener", "error", err)
			}
		}
		os.Exit(0)
	}()
	return &proto.ShutdownRes{}
}

func ResponseMessage(meta message.MessageMetadata, data []byte) *proto.PluginMessage {
	uid := uuid.New().String()

	var metadata []*proto.Metadata
	for k, v := range meta {
		metadata = append(metadata, &proto.Metadata{
			Name:  k,
			Value: v,
		})
	}

	return &proto.PluginMessage{
		Uuid:     uid,
		Metadata: metadata,
		Data:     data,
	}
}

type StatusFn func(ctx context.Context, in *proto.StatusReq) (*proto.StatusRes, error)
type ShutdownFn func(ctx context.Context, in *proto.ShutdownReq) (*proto.ShutdownRes, error)
type SourceFn func(*proto.SourceReq, proto.PluginService_SourceServer) error
type RunnerFn func(context.Context, *proto.PluginMessage) (*proto.PluginMessage, error)
type TargetFn func(context.Context, *proto.PluginMessage) (*proto.TargetRes, error)

type server struct {
	proto.UnimplementedPluginServiceServer
	status   StatusFn
	shutdown ShutdownFn
	source   SourceFn
	runner   RunnerFn
	target   TargetFn
}

func (s *server) Status(ctx context.Context, in *proto.StatusReq) (*proto.StatusRes, error) {
	if s.status != nil {
		return s.status(ctx, in)
	}
	return GetStatusResponse(), nil
}

func (s *server) Shutdown(ctx context.Context, in *proto.ShutdownReq) (*proto.ShutdownRes, error) {
	if s.shutdown != nil {
		return s.shutdown(ctx, in)
	}
	return Shutdown(in.Wait), nil
}

func (s *server) Source(ctx *proto.SourceReq, srv proto.PluginService_SourceServer) error {
	if s.source != nil {
		return s.source(ctx, srv)
	}
	return status.Errorf(codes.Unimplemented, "method Source not implemented")
}

func (s *server) Runner(ctx context.Context, in *proto.PluginMessage) (*proto.PluginMessage, error) {
	if s.runner != nil {
		return s.runner(ctx, in)
	}
	return nil, status.Errorf(codes.Unimplemented, "method Runner not implemented")
}

func (s *server) Target(ctx context.Context, in *proto.PluginMessage) (*proto.TargetRes, error) {
	if s.target != nil {
		return s.target(ctx, in)
	}
	return nil, status.Errorf(codes.Unimplemented, "method Target not implemented")
}
