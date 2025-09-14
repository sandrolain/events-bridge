package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/TylerBrock/colorjson"
	"github.com/fatih/color"
	"github.com/valyala/fasthttp"
)

func main() {
	addr := flag.String("address", "0.0.0.0:8080", "HTTP listen address")
	flag.Parse()

	slog.Info("Starting httpdbg", "addr", *addr)

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

		if strings.Contains(string(ctx.Request.Header.ContentType()), "application/json") {
			var obj interface{}
			err := json.Unmarshal(ctx.Request.Body(), &obj)
			if err != nil {
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
	if err := fasthttp.ListenAndServe(*addr, handler); err != nil {
		slog.Error("error serving httpdbg", "err", err)
		panic(err)
	}
}
