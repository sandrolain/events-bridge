package toolutil

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/TylerBrock/colorjson"
	"github.com/fatih/color"
	"github.com/fxamacker/cbor/v2"
	testpayload "github.com/sandrolain/events-bridge/testers/tools/testpayload"
	"github.com/spf13/cobra"
)

const (
	CTJSON = "application/json"
	CTCBOR = "application/cbor"
	CTText = "text/plain"
)

// Logger returns a slog logger to stdout.
func Logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// PrettyBodyByMIME pretty-prints JSON/CBOR bodies based on MIME, otherwise returns original body.
func PrettyBodyByMIME(mime string, body []byte) []byte {
	if len(body) == 0 {
		return body
	}
	m := strings.ToLower(mime)
	switch {
	case strings.Contains(m, "json"):
		var obj any
		if err := json.Unmarshal(body, &obj); err == nil {
			f := colorjson.NewFormatter()
			f.Indent = 2
			if s, err := f.Marshal(obj); err == nil {
				return s
			}
		}
		return body
	case strings.Contains(m, "cbor"):
		var obj any
		if err := cbor.Unmarshal(body, &obj); err == nil {
			f := colorjson.NewFormatter()
			f.Indent = 2
			if s, err := f.Marshal(obj); err == nil {
				return s
			}
		}
		return body
	default:
		return body
	}
}

// EncodeCBORFromJSON parses a JSON string and encodes it as CBOR bytes.
func EncodeCBORFromJSON(jsonStr string) ([]byte, error) {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON payload: %w", err)
	}
	b, err := cbor.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode CBOR: %w", err)
	}
	return b, nil
}

// BuildPayload builds request payload bytes and content-type from either a testpayload type or a raw payload with MIME.
// Priority: if testType is provided, it's used; otherwise raw payload with MIME is used; returns (nil, "") if neither provided.
func BuildPayload(rawPayload string, mime string) ([]byte, string, error) {
	b, err := testpayload.Interpolate(rawPayload)
	if err != nil {
		return nil, "", fmt.Errorf("failed to interpolate payload: %w", err)
	}
	return b, mime, nil
}

// GuessMIME tries to guess a content type from raw body.
// It detects JSON by leading '{' or '[' and CBOR by first byte 0xA0-0xBF/0x80-0x9F/0x60-0x7F heuristics.
// Falls back to text/plain.
func GuessMIME(body []byte) string {
	if len(body) == 0 {
		return CTText
	}
	b := strings.TrimSpace(string(body))
	if strings.HasPrefix(b, "{") || strings.HasPrefix(b, "[") {
		return CTJSON
	}
	// Simple CBOR heuristic: detect major types for map/array/text
	// Not perfect, but ok for debugging tool.
	first := body[0]
	if (first&0xE0) == 0xA0 || (first&0xE0) == 0x80 || (first&0xE0) == 0x60 {
		return CTCBOR
	}
	return CTText
}

// --- Colored message printer (shared across tools) ---

// KV represents a single key-value pair to print under a section.
type KV struct {
	Key   string
	Value string
}

// MessageSection groups related key-value pairs under a titled section.
type MessageSection struct {
	Title string
	Items []KV
}

var printCounter int = 0
var printCountMutex = sync.Mutex{}

func getNextPrintCount() int {
	printCountMutex.Lock()
	defer printCountMutex.Unlock()
	printCounter++
	return printCounter
}

// PrintColoredMessage prints a colored, consistently formatted message with sections and body.
// Title and section titles are highlighted; items are aligned as key: value; body is pretty-printed by MIME.
func PrintColoredMessage(title string, sections []MessageSection, body []byte, mime string) {
	black := color.New(color.FgBlack).Add(color.ResetUnderline).PrintfFunc()
	blue := color.New(color.FgHiBlue).Add(color.Underline).PrintfFunc()
	white := color.New(color.FgWhite).Add(color.ResetUnderline).PrintfFunc()

	count := getNextPrintCount()
	black("\n-------- Message %d --------\n", count)
	black(time.Now().Format(time.RFC3339) + "\n")
	if title != "" {
		blue("%s:\n", title)
	}

	for _, s := range sections {
		if s.Title != "" {
			blue("%s:\n", s.Title)
		}
		for _, kv := range s.Items {
			white("  %s: %s\n", kv.Key, kv.Value)
		}
	}

	blue("Body:\n")
	pretty := PrettyBodyByMIME(mime, body)
	white("%s\n\n", pretty)
}

// --- Shared CLI flag helpers ---

// AddMethodFlag adds a common HTTP method flag.
func AddMethodFlag(cmd *cobra.Command, method *string, def string, usage string) {
	if def == "" {
		def = "POST"
	}
	if usage == "" {
		usage = "HTTP method (POST, PUT, PATCH)"
	}
	cmd.Flags().StringVar(method, "method", def, usage)
}

// AddPathFlag adds a common path/resource flag.
func AddPathFlag(cmd *cobra.Command, path *string, def string, usage string) {
	if def == "" {
		def = "/event"
	}
	if usage == "" {
		usage = "Request path/resource"
	}
	cmd.Flags().StringVar(path, "path", def, usage)
}

// AddPayloadFlags adds payload, mime and testpayload flags.
func AddPayloadFlags(cmd *cobra.Command, payload *string, payloadDef string, mime *string, mimeDef string) {
	if payloadDef == "" {
		payloadDef = "{}"
	}
	if mimeDef == "" {
		mimeDef = CTJSON
	}
	cmd.Flags().StringVar(payload, "payload", payloadDef, "Payload to send (supports placeholders: {json},{cbor},{sentiment},{sentence},{datetime},{nowtime})")
	cmd.Flags().StringVar(mime, "mime", mimeDef, "Payload MIME type (application/json, application/cbor, text/plain)")
}

// AddIntervalFlag adds a common interval flag for periodic actions.
func AddIntervalFlag(cmd *cobra.Command, interval *string, def string) {
	if def == "" {
		def = "5s"
	}
	cmd.Flags().StringVar(interval, "interval", def, "Interval between actions, e.g. 2s, 500ms, 1m")
}

// Note: tool-specific flags (e.g. MQTT broker/topic) should be defined in the tool files.
