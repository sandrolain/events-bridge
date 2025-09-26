package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	toolutil "github.com/sandrolain/events-bridge/testers/toolutil"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
)

func main() {
	root := &cobra.Command{
		Use:   "httpcli",
		Short: "HTTP client/server tester",
		Long:  "A simple HTTP client/server CLI with send and serve commands.",
	}

	// SEND command (client)
	var (
		sendAddress  string
		sendMethod   string
		sendPath     string
		sendPayload  string
		sendInterval string
		sendMIME     string
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
				reqBody, contentType, err := toolutil.BuildPayload(sendPayload, sendMIME)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return
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
	toolutil.AddMethodFlag(sendCmd, &sendMethod, "POST", "HTTP method (POST, PUT, PATCH)")
	toolutil.AddPathFlag(sendCmd, &sendPath, "/event", "HTTP request path")
	toolutil.AddPayloadFlags(sendCmd, &sendPayload, "{}", &sendMIME, toolutil.CTJSON)
	toolutil.AddIntervalFlag(sendCmd, &sendInterval, "5s")

	// SERVE command (server)
	var serveAddr string
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Run an HTTP server that logs requests",
		RunE: func(cmd *cobra.Command, args []string) error {
			slog.Info("Starting httpdbg", "addr", serveAddr)

			handler := func(ctx *fasthttp.RequestCtx) {
				// Build sections
				var queryItems []toolutil.KV
				for key, value := range ctx.QueryArgs().All() {
					queryItems = append(queryItems, toolutil.KV{Key: string(key), Value: string(value)})
				}
				var headerItems []toolutil.KV
				for key, value := range ctx.Request.Header.All() {
					headerItems = append(headerItems, toolutil.KV{Key: string(key), Value: string(value)})
				}
				sections := []toolutil.MessageSection{
					{Title: "Request", Items: []toolutil.KV{{Key: "Method", Value: string(ctx.Method())}, {Key: "URI", Value: string(ctx.RequestURI())}}},
					{Title: "Query", Items: queryItems},
					{Title: "Remote", Items: []toolutil.KV{{Key: "Addr", Value: ctx.RemoteAddr().String()}}},
					{Title: "Headers", Items: headerItems},
				}
				ct := string(ctx.Request.Header.ContentType())
				toolutil.PrintColoredMessage("HTTP", sections, ctx.Request.Body(), ct)
			}

			if err := fasthttp.ListenAndServe(serveAddr, handler); err != nil {
				slog.Error("error serving httpdbg", "err", err)
				return err
			}
			return nil
		},
	}
	serveCmd.Flags().StringVar(&serveAddr, "address", "0.0.0.0:9090", "HTTP listen address")

	root.AddCommand(sendCmd, serveCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
