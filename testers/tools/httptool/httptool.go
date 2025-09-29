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

func printHTTPResponse(method, url string, resp *fasthttp.Response) {
	var headerItems []toolutil.KV
	resp.Header.VisitAll(func(key, value []byte) {
		headerItems = append(headerItems, toolutil.KV{Key: string(key), Value: string(value)})
	})

	statusText := fasthttp.StatusMessage(resp.StatusCode())
	sections := []toolutil.MessageSection{
		{Title: "Request", Items: []toolutil.KV{{Key: "Method", Value: method}, {Key: "URL", Value: url}}},
		{Title: "Response", Items: []toolutil.KV{{Key: "Status", Value: fmt.Sprintf("%d %s", resp.StatusCode(), statusText)}}},
		{Title: "Headers", Items: headerItems},
	}

	mime := string(resp.Header.ContentType())
	if mime == "" {
		mime = toolutil.GuessMIME(resp.Body())
	}

	toolutil.PrintColoredMessage("HTTP Response", sections, resp.Body(), mime)
}

type sendOptions struct {
	address  string
	method   string
	path     string
	payload  string
	interval string
	mime     string
}

func (o *sendOptions) run() error {
	url := o.address + o.path

	dur, err := time.ParseDuration(o.interval)
	if err != nil {
		return fmt.Errorf("invalid interval: %w", err)
	}
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	fmt.Printf("Sending %s requests to %s every %s\n", o.method, url, dur)

	sendRequest := func() {
		reqBody, contentType, err := toolutil.BuildPayload(o.payload, o.mime)
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

		r.Header.SetMethod(o.method)
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

		printHTTPResponse(o.method, url, w)
	}

	for range ticker.C {
		go sendRequest()
	}

	select {}
}

func main() {
	root := newRootCommand()
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "httpcli",
		Short: "HTTP client/server tester",
		Long:  "A simple HTTP client/server CLI with send and serve commands.",
	}

	root.AddCommand(newSendCommand(), newServeCommand())
	return root
}

func newSendCommand() *cobra.Command {
	opts := &sendOptions{}
	cmd := &cobra.Command{
		Use:   "send",
		Short: "Send periodic HTTP requests",
		RunE: func(cmd *cobra.Command, args []string) error {
			return opts.run()
		},
	}

	cmd.Flags().StringVar(&opts.address, "address", "http://localhost:8080", "HTTP server base address, e.g. http://localhost:8080")
	toolutil.AddMethodFlag(cmd, &opts.method, "POST", "HTTP method (POST, PUT, PATCH)")
	toolutil.AddPathFlag(cmd, &opts.path, "/event", "HTTP request path")
	toolutil.AddPayloadFlags(cmd, &opts.payload, "{}", &opts.mime, toolutil.CTJSON)
	toolutil.AddIntervalFlag(cmd, &opts.interval, "5s")

	return cmd
}

func newServeCommand() *cobra.Command {
	var serveAddr string

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run an HTTP server that logs requests",
		RunE: func(cmd *cobra.Command, args []string) error {
			slog.Info("Starting httpdbg", "addr", serveAddr)

			handler := func(ctx *fasthttp.RequestCtx) {
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

	cmd.Flags().StringVar(&serveAddr, "address", "0.0.0.0:9090", "HTTP listen address")
	return cmd
}
