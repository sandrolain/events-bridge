package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	coap "github.com/plgd-dev/go-coap/v3"
	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
	coaptcp "github.com/plgd-dev/go-coap/v3/tcp"
	coapudp "github.com/plgd-dev/go-coap/v3/udp"

	testpayload "github.com/sandrolain/events-bridge/testers/tools/testpayload"
	toolutil "github.com/sandrolain/events-bridge/testers/tools/toolutil"
)

func main() {
	root := &cobra.Command{
		Use:   "coapcli",
		Short: "CoAP client/server tester",
		Long:  "A simple CoAP client/server CLI with send and serve commands.",
	}

	// SEND command
	var (
		sendAddress  string
		sendPath     string
		sendPayload  string
		sendInterval string
		sendProto    string
		sendMIME     string
	)

	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Send periodic CoAP POST requests",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := toolutil.Logger()
			dur, err := time.ParseDuration(sendInterval)
			if err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}
			ticker := time.NewTicker(dur)
			defer ticker.Stop()

			logger.Info("Sending CoAP POST periodically", "proto", sendProto, "addr", sendAddress, "path", sendPath, "every", dur)

			sendOnce := func() {
				var body []byte
				var ct string

				b, err := testpayload.Interpolate(sendPayload)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to interpolate payload: %v\n", err)
					return
				}
				body = b
				ct = sendMIME

				if ct == "" {
					ct = toolutil.CTJSON
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				var code any
				var respBody []byte

				mt := MimeToCoapMediaType(ct)

				switch sendProto {
				case "udp":
					client, err := coapudp.Dial(sendAddress)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to dial CoAP (udp): %v\n", err)
						return
					}
					defer client.Close()
					resp, err := client.Post(ctx, sendPath, mt, bytes.NewReader(body))
					if err != nil {
						fmt.Fprintf(os.Stderr, "POST error: %v\n", err)
						return
					}
					code = resp.Code()
					if resp.Body() != nil {
						b, _ := io.ReadAll(resp.Body())
						respBody = b
					}
				case "tcp":
					client, err := coaptcp.Dial(sendAddress)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to dial CoAP (tcp): %v\n", err)
						return
					}
					defer client.Close()
					resp, err := client.Post(ctx, sendPath, mt, bytes.NewReader(body))
					if err != nil {
						fmt.Fprintf(os.Stderr, "POST error: %v\n", err)
						return
					}
					code = resp.Code()
					if resp.Body() != nil {
						b, _ := io.ReadAll(resp.Body())
						respBody = b
					}
				default:
					fmt.Fprintf(os.Stderr, "Unknown proto: %s (use udp or tcp)\n", sendProto)
					return
				}

				logger.Info("Response received", "code", code, "len", len(respBody))
				if len(respBody) > 0 {
					logger.Info("Response body", "body", string(respBody))
				}
			}

			for range ticker.C {
				go sendOnce()
			}
			select {}
		},
	}

	sendCmd.Flags().StringVar(&sendAddress, "address", "localhost:5683", "CoAP server address:port")
	toolutil.AddPathFlag(sendCmd, &sendPath, "/event", "CoAP resource path")
	toolutil.AddPayloadFlags(sendCmd, &sendPayload, "{}", &sendMIME, toolutil.CTJSON)
	toolutil.AddIntervalFlag(sendCmd, &sendInterval, "5s")
	sendCmd.Flags().StringVar(&sendProto, "proto", "udp", "CoAP transport protocol: udp or tcp")

	// SERVE command
	var (
		serveAddr  string
		serveProto string
	)
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Run a CoAP server that logs requests",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
			logger.Info("Starting coapdbg", "proto", serveProto, "addr", serveAddr)

			router := coapmux.NewRouter()
			if err := router.Handle("/", SimpleOKHandler(serveProto)); err != nil {
				return err
			}
			return Serve(serveProto, serveAddr, router)
		},
	}
	serveCmd.Flags().StringVar(&serveAddr, "address", ":5683", "Listen address (e.g.: :5683)")
	serveCmd.Flags().StringVar(&serveProto, "proto", "udp", "CoAP transport protocol: udp or tcp")

	root.AddCommand(sendCmd, serveCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// --- CoAP-specific helpers that used to live in coaputil ---

// PrintCoAPRequest logs details about an incoming CoAP request.
func PrintCoAPRequest(proto, remote string, req *coapmux.Message) {
	// Build sections and delegate to shared formatter
	path, _ := req.Options().Path()
	// Build query
	var query string
	for _, opt := range req.Options() {
		if opt.ID == coapmessage.URIQuery {
			if query != "" {
				query += "&"
			}
			query += string(opt.Value)
		}
	}
	// Build options dump
	var optionItems []toolutil.KV
	for _, opt := range req.Options() {
		optionItems = append(optionItems, toolutil.KV{Key: fmt.Sprintf("%v", opt.ID), Value: fmt.Sprintf("%v", opt.Value)})
	}
	sections := []toolutil.MessageSection{
		{Title: "Request", Items: []toolutil.KV{{Key: "From", Value: fmt.Sprintf("%s (%s)", remote, proto)}, {Key: "Code", Value: fmt.Sprintf("%v", req.Code())}, {Key: "Path", Value: path}, {Key: "Query", Value: query}, {Key: "Token", Value: fmt.Sprintf("%v", req.Token())}}},
		{Title: "Options", Items: optionItems},
	}
	var mime string
	if mt, err := req.Options().ContentFormat(); err == nil {
		mime = CoapMediaTypeToMIME(coapmessage.MediaType(mt))
	}
	var bodyBytes []byte
	if req.Body() != nil {
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(req.Body()); err == nil {
			bodyBytes = buf.Bytes()
		}
	}
	toolutil.PrintColoredMessage("CoAP", sections, bodyBytes, mime)
}

// SimpleOKHandler builds a handler that prints and responds with 2.05 Content and text/plain OK.
func SimpleOKHandler(proto string) coapmux.Handler {
	return coapmux.HandlerFunc(func(w coapmux.ResponseWriter, req *coapmux.Message) {
		PrintCoAPRequest(proto, w.Conn().RemoteAddr().String(), req)
		if err := w.SetResponse(coapcodes.Content, coapmessage.TextPlain, bytes.NewReader([]byte("OK"))); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to set response: %v\n", err)
		}
	})
}

// Serve runs a mux router on chosen proto (udp or tcp).
func Serve(proto, addr string, router *coapmux.Router) error {
	switch proto {
	case "udp", "tcp":
		return coap.ListenAndServe(proto, addr, router)
	default:
		return fmt.Errorf("unknown mode: %s (use udp or tcp)", proto)
	}
}

// MimeToCoapMediaType maps common MIME types to CoAP media types.
func MimeToCoapMediaType(ct string) coapmessage.MediaType {
	switch ct {
	case toolutil.CTJSON:
		return coapmessage.AppJSON
	case toolutil.CTCBOR:
		return coapmessage.AppCBOR
	case toolutil.CTText:
		return coapmessage.TextPlain
	default:
		return coapmessage.AppOctets
	}
}

// CoapMediaTypeToMIME maps CoAP media types to MIME strings.
func CoapMediaTypeToMIME(mt coapmessage.MediaType) string {
	switch mt {
	case coapmessage.AppJSON:
		return toolutil.CTJSON
	case coapmessage.AppCBOR:
		return toolutil.CTCBOR
	case coapmessage.TextPlain:
		return toolutil.CTText
	default:
		return "application/octet-stream"
	}
}
