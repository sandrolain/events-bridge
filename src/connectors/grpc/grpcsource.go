package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/grpc/proto"
	"github.com/sandrolain/events-bridge/src/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var _ connectors.Source = (*GRPCSource)(nil)

// SourceConfig defines the configuration for the gRPC source connector.
type SourceConfig struct {
	// Address is the TCP address to listen on (e.g., "0.0.0.0:50051")
	Address string `mapstructure:"address" validate:"required"`

	// TLS configuration for secure connections
	TLS tlsconfig.Config `mapstructure:"tls"`

	// MaxConcurrentStreams limits the number of concurrent streams per connection
	MaxConcurrentStreams uint32 `mapstructure:"maxConcurrentStreams" default:"100"`

	// MaxReceiveMessageSize limits the maximum message size in bytes (default: 4MB)
	MaxReceiveMessageSize int `mapstructure:"maxReceiveMessageSize" default:"4194304" validate:"gt=0"`
}

// NewSourceConfig creates a new SourceConfig instance.
func NewSourceConfig() any {
	return new(SourceConfig)
}

// NewSource creates a new gRPC source connector from the provided configuration.
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	logger := slog.Default().With("context", "gRPC Source")

	return &GRPCSource{
		cfg:  cfg,
		slog: logger,
	}, nil
}

// GRPCSource implements the gRPC source connector.
type GRPCSource struct {
	proto.UnimplementedEventBridgeServiceServer
	cfg    *SourceConfig
	slog   *slog.Logger
	c      chan *message.RunnerMessage
	server *grpc.Server
}

// Produce starts the gRPC server and returns a channel for incoming messages.
func (s *GRPCSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting gRPC server", "address", s.cfg.Address, "tls", s.cfg.TLS.Enabled)

	// Create listener
	listener, err := net.Listen("tcp", s.cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	// Configure gRPC server options
	var opts []grpc.ServerOption

	// Add TLS credentials if enabled
	if s.cfg.TLS.Enabled {
		tlsConfig, err := s.cfg.TLS.BuildServerConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	// Add server configuration
	opts = append(opts,
		grpc.MaxConcurrentStreams(s.cfg.MaxConcurrentStreams),
		grpc.MaxRecvMsgSize(s.cfg.MaxReceiveMessageSize),
		grpc.MaxSendMsgSize(s.cfg.MaxReceiveMessageSize),
	)

	// Create gRPC server
	s.server = grpc.NewServer(opts...)
	proto.RegisterEventBridgeServiceServer(s.server, s)

	// Start serving in a goroutine
	go func() {
		if err := s.server.Serve(listener); err != nil {
			s.slog.Error("gRPC server error", "error", err)
		}
	}()

	return s.c, nil
}

// SendEvent handles a single event message.
func (s *GRPCSource) SendEvent(ctx context.Context, msg *proto.EventMessage) (*proto.EventResponse, error) {
	s.slog.Debug("received single event", "uuid", msg.Uuid, "metadata", msg.Metadata)

	grpcMsg := NewGRPCMessage(msg)
	runnerMsg := message.NewRunnerMessage(grpcMsg)

	// Send message to the channel
	select {
	case s.c <- runnerMsg:
	case <-ctx.Done():
		return &proto.EventResponse{
			Success: false,
			Error:   strPtr("context canceled"),
		}, nil
	}

	// Wait for acknowledgment
	select {
	case status := <-grpcMsg.done:
		if status == message.ResponseStatusAck {
			return &proto.EventResponse{Success: true}, nil
		}
		return &proto.EventResponse{
			Success: false,
			Error:   strPtr("message was not acknowledged"),
		}, nil
	case <-grpcMsg.reply:
		return &proto.EventResponse{Success: true}, nil
	case <-ctx.Done():
		return &proto.EventResponse{
			Success: false,
			Error:   strPtr("context canceled"),
		}, nil
	}
}

// StreamEvents handles multiple event messages via client streaming.
func (s *GRPCSource) StreamEvents(stream proto.EventBridgeService_StreamEventsServer) error {
	s.slog.Debug("received streaming events")

	messageCount := 0
	errorCount := 0

	for {
		msg, err := stream.Recv()
		if err != nil {
			// End of stream
			s.slog.Debug("stream ended", "messageCount", messageCount, "errorCount", errorCount)
			if errorCount > 0 {
				return stream.SendAndClose(&proto.EventResponse{
					Success: false,
					Error:   strPtr(fmt.Sprintf("processed %d messages with %d errors", messageCount, errorCount)),
				})
			}
			return stream.SendAndClose(&proto.EventResponse{Success: true})
		}

		messageCount++

		grpcMsg := NewGRPCMessage(msg)
		runnerMsg := message.NewRunnerMessage(grpcMsg)

		// Send message to the channel
		select {
		case s.c <- runnerMsg:
			// Wait for acknowledgment in a non-blocking way
			select {
			case status := <-grpcMsg.done:
				if status != message.ResponseStatusAck {
					errorCount++
					s.slog.Warn("message not acknowledged", "uuid", msg.Uuid)
				}
			case <-grpcMsg.reply:
				// Message acknowledged with reply
			case <-stream.Context().Done():
				return fmt.Errorf("stream context canceled")
			}
		case <-stream.Context().Done():
			return fmt.Errorf("stream context canceled")
		}
	}
}

// Close stops the gRPC server and releases resources.
func (s *GRPCSource) Close() error {
	if s.server != nil {
		s.slog.Info("shutting down gRPC server")
		s.server.GracefulStop()
	}
	if s.c != nil {
		close(s.c)
	}
	return nil
}

// strPtr is a helper function to create a string pointer.
func strPtr(s string) *string {
	return &s
}
