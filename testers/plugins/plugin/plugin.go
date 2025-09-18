package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/message"
	plugin "github.com/sandrolain/events-bridge/src/plugin/bootstrap"
	proto "github.com/sandrolain/events-bridge/src/plugin/proto"
)

func main() {
	plugin.Start(plugin.StartOptions{
		Source: source,
		Runner: runner,
		Target: target,
		Setup: func() error {

			time.Sleep(2 * time.Second)

			plugin.SetReady()

			return nil
		},
	})
}

var source plugin.SourceFn = func(req *proto.SourceReq, stream proto.PluginService_SourceServer) error {
	timer := time.NewTicker(time.Second)

	for {
		select {
		// Exit on stream context done
		case <-stream.Context().Done():
			return nil
		case <-timer.C:

			slog.Info("sending hardware stats")

			dataMap := map[string]interface{}{
				"cpu":    "Intel Core i7",
				"memory": "16GB",
				"disk":   "512GB SSD",
				"time":   time.Now().Format(time.RFC3339),
			}

			data, err := json.Marshal(&dataMap)
			if err != nil {
				slog.Error("failed to marshal data", "error", err)
				continue
			}

			res := plugin.ResponseMessage(message.MessageMetadata{
				"time": time.Now().String(),
			}, data)

			// Send the Hardware stats on the stream
			err = stream.Send(res)
			if err != nil {
				slog.Error("failed to send hardware stats", "error", err)
			}
		}
	}
}

var target plugin.TargetFn = func(ctx context.Context, req *proto.PluginMessage) (*proto.TargetRes, error) {
	slog.Info("target received message", "uuid", req.GetUuid(), "metadata", req.GetMetadata(), "data", string(req.GetData()))

	// Simulate some processing
	time.Sleep(500 * time.Millisecond)

	// Return a response
	return &proto.TargetRes{}, nil
}

var runner plugin.RunnerFn = func(ctx context.Context, req *proto.PluginMessage) (*proto.PluginMessage, error) {
	data := req.GetData()

	var jsonData map[string]interface{}
	err := json.Unmarshal(data, &jsonData)
	if err != nil {
		return nil, err
	}

	jsonBytes, err := cbor.Marshal(jsonData)
	if err != nil {
		return nil, err
	}

	return plugin.ResponseMessage(
		message.MessageMetadata{
			"content-type": "application/json",
			"uuid":         req.GetUuid(),
		},
		jsonBytes,
	), nil
}
