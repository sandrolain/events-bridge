package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/TylerBrock/colorjson"
	"github.com/fatih/color"
	"github.com/valyala/fasthttp"
)

func main() {
	addr := "0.0.0.0:8989"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	slog.Info("Starting httpdbg", "addr", addr)

	black := color.New(color.FgBlack).Add(color.ResetUnderline).PrintfFunc()
	blue := color.New(color.FgHiBlue).Add(color.Underline).PrintfFunc()
	white := color.New(color.FgWhite).Add(color.ResetUnderline).PrintfFunc()

	handler := func(ctx *fasthttp.RequestCtx) {
		black("\n----------------------------------------\n")
		black(time.Now().Format(time.RFC3339) + "\n")
		blue("Request:\n")
		white("  %s %s\n", ctx.Method(), ctx.RequestURI())
		blue("Query:\n")
		ctx.QueryArgs().VisitAll(func(key, value []byte) {
			white("  %s: %s\n", key, value)
		})
		blue("Remote address:\n")
		white("  %s\n", ctx.RemoteAddr().String())
		blue("Headers:\n")
		ctx.Request.Header.VisitAll(func(key, value []byte) {
			white("  %s: %s\n", key, value)
		})
		blue("Body:\n")

		if strings.Contains(string(ctx.Request.Header.ContentType()), "application/json") {
			var obj interface{}
			json.Unmarshal(ctx.Request.Body(), &obj)
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
	if err := fasthttp.ListenAndServe(addr, handler); err != nil {
		slog.Error("error serving httpdbg", "err", err)
		panic(err)
	}
}
