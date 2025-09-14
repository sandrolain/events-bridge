package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/fxamacker/cbor/v2"
	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"
)

func main() {
	address := flag.String("address", "localhost:5683", "CoAP server address:port")
	path := flag.String("path", "/test", "CoAP resource path")
	payload := flag.String("payload", "{}", "JSON payload da inviare")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	logger.Info("Preparing payload", "address", *address, "path", *path)

	var data interface{}
	err := json.Unmarshal([]byte(*payload), &data)
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

	logger.Info("Connecting to CoAP server", "address", *address)
	client, err := coapudp.Dial(*address)
	if err != nil {
		logger.Error("Failed to dial CoAP server", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Error("Failed to close CoAP client", "error", err)
		}
	}()

	logger.Info("Sending POST request", "path", *path, "payload_size", len(cborPayload))
	resp, err := client.Post(ctx, *path, coapmessage.AppCBOR, bytes.NewReader(cborPayload))
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
