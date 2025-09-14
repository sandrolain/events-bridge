package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/TylerBrock/colorjson"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"

	testpayload "github.com/sandrolain/events-bridge/testers/tools/testpayload"
)

const (
	ctJSON = "application/json"
	ctCBOR = "application/cbor"
	ctText = "text/plain"
)

func main() {
	root := &cobra.Command{
		Use:   "httpcli",
		Short: "HTTP client/server tester",
		Long:  "A simple HTTP client/server CLI with send and serve commands.",
	}

	// SEND command (client)
	var (
		sendAddress     string
		sendMethod      string
		sendPath        string
		sendPayload     string
		sendInterval    string
		sendMIME        string
		sendTestPayload string
	)
	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Send periodic HTTP requests",
		RunE: func(cmd *cobra.Command, args []string) error {
			url := sendAddress + sendPath

			dur, err := time.ParseDuration(sendInterval)
			if err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}
			ticker := time.NewTicker(dur)
			defer ticker.Stop()

			fmt.Printf("Sending %s requests to %s every %s\n", sendMethod, url, dur)

			sendRequest := func() {
				var reqBody []byte
				var contentType string

				if sendTestPayload != "" {
					typ := testpayload.TestPayloadType(sendTestPayload)
					if !typ.IsValid() {
						fmt.Fprintf(os.Stderr, "Unknown testpayload type: %s\n", sendTestPayload)
						return
					}
					b, err := testpayload.Generate(typ)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to generate test payload: %v\n", err)
						return
					}
					reqBody = b
					// TODO: use a testpayload package function for content-type
					switch typ {
					case testpayload.TestPayloadJSON:
						contentType = ctJSON
					case testpayload.TestPayloadCBOR:
						contentType = ctCBOR
					default:
						contentType = ctText
					}
				} else if sendPayload != "" {
					// Allow placeholder interpolation within provided payload string
					b, err := testpayload.Interpolate(sendPayload)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to interpolate payload: %v\n", err)
						return
					}
					reqBody = b
					contentType = sendMIME
				}

				r := fasthttp.AcquireRequest()
				w := fasthttp.AcquireResponse()
				defer func() {
					fasthttp.ReleaseRequest(r)
					fasthttp.ReleaseResponse(w)
				}()

				r.Header.SetMethod(sendMethod)
				r.SetRequestURI(url)
				if contentType != "" {
					r.Header.Set("Content-Type", contentType)
				}
				if len(reqBody) > 0 {
					r.SetBody(reqBody)
				}

				var client fasthttp.Client
				if err := client.Do(r, w); err != nil {
					fmt.Fprintf(os.Stderr, "Request error: %v\n", err)
					return
				}
				fmt.Printf("Response: %d\n", w.StatusCode())
			}

			for range ticker.C {
				go sendRequest()
			}

			select {}
		},
	}
	sendCmd.Flags().StringVar(&sendAddress, "address", "http://localhost:8080", "HTTP server base address, e.g. http://localhost:8080")
	sendCmd.Flags().StringVar(&sendMethod, "method", "POST", "HTTP method (POST, PUT, PATCH)")
	sendCmd.Flags().StringVar(&sendPath, "path", "/test", "HTTP request path")
	sendCmd.Flags().StringVar(&sendPayload, "payload", "{}", "Payload to send (supports placeholders: {json},{cbor},{sentiment},{sentence},{datetime},{nowtime})")
	sendCmd.Flags().StringVar(&sendInterval, "interval", "5s", "Interval between requests, e.g. 2s, 500ms, 1m")
	sendCmd.Flags().StringVar(&sendMIME, "mime", "application/json", "Payload MIME type (application/json, text/plain, application/xml, ...)")
	sendCmd.Flags().StringVar(&sendTestPayload, "testpayload", "", "Test payload generator: json, cbor, sentiment, sentence, datetime, nowtime")

	// SERVE command (server)
	var serveAddr string
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Run an HTTP server that logs requests",
		RunE: func(cmd *cobra.Command, args []string) error {
			slog.Info("Starting httpdbg", "addr", serveAddr)

			black := color.New(color.FgBlack).Add(color.ResetUnderline).PrintfFunc()
			blue := color.New(color.FgHiBlue).Add(color.Underline).PrintfFunc()
			white := color.New(color.FgWhite).Add(color.ResetUnderline).PrintfFunc()

			handler := func(ctx *fasthttp.RequestCtx) {
				black("\n----------------------------------------\n")
				black(time.Now().Format(time.RFC3339) + "\n")
				blue("Request:\n")
				white("  %s %s\n", ctx.Method(), ctx.RequestURI())
				blue("Query:\n")
				for key, value := range ctx.QueryArgs().All() {
					white("  %s: %s\n", key, value)
				}
				blue("Remote address:\n")
				white("  %s\n", ctx.RemoteAddr().String())
				blue("Headers:\n")
				for key, value := range ctx.Request.Header.All() {
					white("  %s: %s\n", key, value)
				}
				blue("Body:\n")

				if strings.Contains(string(ctx.Request.Header.ContentType()), ctJSON) {
					var obj interface{}
					if err := json.Unmarshal(ctx.Request.Body(), &obj); err != nil {
						fmt.Fprintf(os.Stderr, "Failed to unmarshal JSON: %v\n", err)
					}
					f := colorjson.NewFormatter()
					f.Indent = 2
					s, err := f.Marshal(obj)
					if err != nil {
						s = ctx.Request.Body()
					}
					white("%s\n\n", s)
				} else {
					white("%s\n\n", ctx.Request.Body())
				}
			}

			if err := fasthttp.ListenAndServe(serveAddr, handler); err != nil {
				slog.Error("error serving httpdbg", "err", err)
				return err
			}
			return nil
		},
	}
	serveCmd.Flags().StringVar(&serveAddr, "address", "0.0.0.0:8080", "HTTP listen address")

	root.AddCommand(sendCmd, serveCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
