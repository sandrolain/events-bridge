package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/fxamacker/cbor/v2"
	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if len(os.Args) < 4 {
		logger.Error("Usage: <host:port> <path> <json-payload>", "args", os.Args)
		fmt.Printf("Usage: %s <host:port> <path> <json-payload>\n", os.Args[0])
		os.Exit(1)
	}
	addr := os.Args[1]
	path := os.Args[2]
	jsonPayload := os.Args[3]

	logger.Info("Preparing payload", "address", addr, "path", path)

	var data interface{}
	err := json.Unmarshal([]byte(jsonPayload), &data)
	if err != nil {
		logger.Error("Invalid JSON payload", "error", err)
		os.Exit(1)
	}

	cborPayload, err := cbor.Marshal(data)
	if err != nil {
		logger.Error("Failed to encode CBOR", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger.Info("Connecting to CoAP server", "address", addr)
	client, err := coapudp.Dial(addr)
	if err != nil {
		logger.Error("Failed to dial CoAP server", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	logger.Info("Sending POST request", "path", path, "payload_size", len(cborPayload))
	resp, err := client.Post(ctx, path, coapmessage.AppCBOR, bytes.NewReader(cborPayload))
	if err != nil {
		logger.Error("Failed to send POST request", "error", err)
		os.Exit(1)
	}

	logger.Info("Response received", "code", resp.Code())
	if resp.Body() != nil {
		body, _ := io.ReadAll(resp.Body())
		logger.Info("Response body", "body", string(body))
	}
}
