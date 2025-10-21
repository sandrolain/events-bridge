package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/connectors/plugin/bootstrap"
	"github.com/sandrolain/events-bridge/src/connectors/plugin/proto"
)

// This test plugin is used only for unit tests of the plugin connector.
// It is built via task build-test-plugin-connector.
// Behaviour summary:
//  - Source: emits N (default 3) messages at 50ms interval then exits.
//  - Runner: wraps JSON data into CBOR with metadata passthrough.
//  - Target: records last received message timestamp (used only for success path) and returns empty response.
// Configuration is passed through environment variables or metadata not needed here; kept simple intentionally.

var totalMessages = 3
var interval = 50 * time.Millisecond

func main() {
	bootstrap.Start(bootstrap.StartOptions{
		Source: source,
		Runner: runner,
		Target: target,
		Setup: func() error {
			bootstrap.SetReady()
			return nil
		},
	})
}

var source bootstrap.SourceFn = func(req *proto.SourceReq, stream proto.PluginService_SourceServer) error {
	for i := 0; i < totalMessages; i++ {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return fmt.Errorf("failed to generate random id: %w", err)
		}
		msgMap := map[string]any{"i": i, "ts": time.Now().UnixNano()}
		data, err := json.Marshal(msgMap)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		resp := bootstrap.ResponseMessage(id, map[string]string{"idx": fmt.Sprintf("%d", i)}, data)
		if err := stream.Send(resp); err != nil {
			return err
		}
		time.Sleep(interval)
	}
	return nil
}

var runner bootstrap.RunnerFn = func(ctx context.Context, req *proto.PluginMessage) (*proto.PluginMessage, error) {
	// Convert JSON data to CBOR - ignoring errors for test simplicity
	var tmp map[string]any
	if err := json.Unmarshal(req.GetData(), &tmp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	cborBytes, err := cbor.Marshal(tmp)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CBOR: %w", err)
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("failed to generate random id: %w", err)
	}
	return bootstrap.ResponseMessage(id, map[string]string{"processed": "true"}, cborBytes), nil
}

var target bootstrap.TargetFn = func(ctx context.Context, req *proto.PluginMessage) (*proto.TargetRes, error) {
	slog.Info("target received", "uuid", req.GetUuid())
	return &proto.TargetRes{}, nil
}
