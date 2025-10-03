package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/sandrolain/events-bridge/src/connectors/plugin/bootstrap"
	"github.com/sandrolain/events-bridge/src/connectors/plugin/proto"
)

func main() {
	bootstrap.Start(bootstrap.StartOptions{
		Source: source,
		Runner: runner,
		Target: target,
		Setup: func() error {

			time.Sleep(2 * time.Second)

			bootstrap.SetReady()

			return nil
		},
	})
}

var source bootstrap.SourceFn = func(req *proto.SourceReq, stream proto.PluginService_SourceServer) error {
	timer := time.NewTicker(time.Second)
	var mem runtime.MemStats

	for {
		select {
		// Exit on stream context done
		case <-stream.Context().Done():
			return nil
		case <-timer.C:
			runtime.ReadMemStats(&mem)

			slog.Info("sending hardware stats")

			id, err := bootstrap.GenerateId()
			if err != nil {
				slog.Error("failed to generate ID", "error", err)
				continue
			}

			dataMap := map[string]interface{}{
				"cpu":  runtime.NumCPU(),
				"mem":  map[string]uint64{"Alloc": mem.Alloc, "TotalAlloc": mem.TotalAlloc, "Sys": mem.Sys, "NumGC": uint64(mem.NumGC)},
				"time": time.Now().String(),
			}

			data, err := json.Marshal(&dataMap)
			if err != nil {
				slog.Error("failed to marshal data", "error", err)
				continue
			}

			res := bootstrap.ResponseMessage(id, map[string]string{
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

var target bootstrap.TargetFn = func(ctx context.Context, req *proto.PluginMessage) (*proto.TargetRes, error) {
	slog.Info("target received message", "uuid", req.GetUuid(), "metadata", req.GetMetadata(), "data", string(req.GetData()))

	// Simulate some processing
	time.Sleep(500 * time.Millisecond)

	// Return a response
	return &proto.TargetRes{}, nil
}

var runner bootstrap.RunnerFn = func(ctx context.Context, req *proto.PluginMessage) (*proto.PluginMessage, error) {
	data := req.GetData()

	oldId := req.GetUuid()
	oldIdHex := fmt.Sprintf("%x", oldId)

	id, err := bootstrap.GenerateId()
	if err != nil {
		return nil, fmt.Errorf("failed to generate ID: %w", err)
	}

	var jsonData map[string]interface{}
	err = json.Unmarshal(data, &jsonData)
	if err != nil {
		return nil, err
	}

	jsonBytes, err := cbor.Marshal(jsonData)
	if err != nil {
		return nil, err
	}

	return bootstrap.ResponseMessage(
		id,
		map[string]string{
			"content-type": "application/json",
			"uuid":         oldIdHex,
		},
		jsonBytes,
	), nil
}
