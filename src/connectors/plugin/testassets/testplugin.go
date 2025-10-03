package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/message"
	plugin "github.com/sandrolain/events-bridge/src/plugin/bootstrap"
	proto "github.com/sandrolain/events-bridge/src/plugin/proto"
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
	plugin.Start(plugin.StartOptions{
		Source: source,
		Runner: runner,
		Target: target,
		Setup: func() error {
			plugin.SetReady()
			return nil
		},
	})
}

var source plugin.SourceFn = func(req *proto.SourceReq, stream proto.PluginService_SourceServer) error {
	for i := 0; i < totalMessages; i++ {
		msgMap := map[string]any{"i": i, "ts": time.Now().UnixNano()}
		data, _ := json.Marshal(msgMap)
		resp := plugin.ResponseMessage(message.MessageMetadata{"idx": fmt.Sprintf("%d", i)}, data)
		if err := stream.Send(resp); err != nil {
			return err
		}
		time.Sleep(interval)
	}
	return nil
}

var runner plugin.RunnerFn = func(ctx context.Context, req *proto.PluginMessage) (*proto.PluginMessage, error) {
	// Convert JSON data to CBOR - ignoring errors for test simplicity
	var tmp map[string]any
	_ = json.Unmarshal(req.GetData(), &tmp)
	cborBytes, _ := cbor.Marshal(tmp)
	return plugin.ResponseMessage(message.MessageMetadata{"processed": "true"}, cborBytes), nil
}

var target plugin.TargetFn = func(ctx context.Context, req *proto.PluginMessage) (*proto.TargetRes, error) {
	slog.Info("target received", "uuid", req.GetUuid())
	return &proto.TargetRes{}, nil
}
