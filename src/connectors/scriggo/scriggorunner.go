// Package main implements the Scriggo/Go interpreter connector for the Events Bridge.
//
// The Scriggo connector enables execution of Go code as message processors
// using the Scriggo embeddable Go interpreter.
//
// Key features:
//   - Full Go language support (interpreted)
//   - Isolated program instances per message (no shared state)
//   - Configurable execution timeout via context cancellation
//   - Optional script integrity verification via SHA256
//   - Custom packages can be exposed to scripts
//
// Security architecture:
//   - Each message runs in a fresh Scriggo program instance
//   - Context-based timeout enforcement
//   - Panic recovery prevents process crashes
//   - Script integrity verification (optional)
//   - Only explicitly provided packages are accessible
//
// Example configuration:
//
//	runners:
//	  - name: go-processor
//	    type: scriggo
//	    config:
//	      path: ./processor.go
//	      timeout: 5s
//	      verifyScriptHash: true
//	      expectedSHA256: "abc123..."
//
// For script integrity verification, generate hash with:
//
//	sha256sum processor.go
//
// The script must be a valid Go program with package main and a main() function.
// The message object is available via the "events" package.
//
// Example script:
//
//	package main
//
//	import "events"
//
//	func main() {
//	    data, _ := events.Message.GetData()
//	    events.Message.SetData(append([]byte("processed: "), data...))
//	    events.Message.AddMetadata("processed", "true")
//	}
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/open2b/scriggo"
	"github.com/open2b/scriggo/native"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure ScriggoRunner implements connectors.Runner
var _ connectors.Runner = &ScriggoRunner{}

// RunnerConfig defines the configuration for the Scriggo/Go interpreter runner with security enhancements.
//
// Security features:
//   - Script integrity verification via SHA256 hash
//   - Context-based timeout for execution cancellation
//   - Only explicitly provided packages are accessible
type RunnerConfig struct {
	// Path is the filesystem path to the Go script file
	Path string `mapstructure:"path" validate:"required"`

	// Timeout is the maximum execution time for scripts
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`

	// VerifyScriptHash enables script integrity verification
	VerifyScriptHash bool `mapstructure:"verifyScriptHash" default:"false"`

	// ExpectedSHA256 is the expected hash of the script (required if VerifyScriptHash is true)
	ExpectedSHA256 string `mapstructure:"expectedSHA256" validate:"required_if=VerifyScriptHash true"`

	// AllowGoStmt allows the use of the go statement in scripts
	AllowGoStmt bool `mapstructure:"allowGoStmt" default:"false"`
}

// ScriggoRunner executes Go code to process messages using the Scriggo interpreter.
//
// Security considerations:
//   - Scripts run in isolated Scriggo program instances
//   - Context-based timeout prevents runaway scripts
//   - Optional script integrity verification via SHA256
//   - Only explicitly provided packages are accessible
//
// Known limitations:
//   - Scripts must be valid Go programs with package main
//   - The message variable is injected via the events package
type ScriggoRunner struct {
	cfg      *RunnerConfig
	slog     *slog.Logger
	src      []byte
	fileName string
}

// NewRunnerConfig creates a new RunnerConfig instance with security defaults.
//
// Default values:
//   - Timeout: 5s
//   - VerifyScriptHash: false
//   - AllowGoStmt: false
//
// Returns:
//   - any: A pointer to RunnerConfig that can be populated via mapstructure
func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// NewRunner creates a new ScriggoRunner instance from the provided configuration.
// It loads the Go script, optionally verifying its integrity.
//
// Security features:
//   - Script integrity verification via SHA256 (if enabled)
//   - Validation of all configuration parameters
//
// Parameters:
//   - anyCfg: Configuration object (must be *RunnerConfig)
//
// Returns:
//   - connectors.Runner: The configured ScriggoRunner
//   - error: Configuration errors or file read errors
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "Scriggo Runner")
	log.Info("loading scriggo program", "path", cfg.Path)

	src, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read go file: %w", err)
	}

	// Verify script integrity if enabled
	if cfg.VerifyScriptHash {
		if err := VerifyScriptIntegrity(src, cfg.ExpectedSHA256); err != nil {
			log.Error("script integrity verification failed", "error", err)
			return nil, fmt.Errorf("script integrity verification failed: %w", err)
		}
		log.Info("script integrity verified", "hash", cfg.ExpectedSHA256)
	}

	return &ScriggoRunner{
		cfg:      cfg,
		slog:     log,
		src:      src,
		fileName: filepath.Base(cfg.Path),
	}, nil
}

// Process executes the Go program to process a message.
//
// Security measures:
//   - Each message runs in a new program instance
//   - Context-based timeout enforcement
//   - Panic recovery prevents crashes
//
// Execution flow:
//  1. Create context with timeout
//  2. Create file system with script source
//  3. Build program with message as global via events package
//  4. Execute program with context for cancellation
//
// Parameters:
//   - msg: The message to process
//
// Returns:
//   - error: Timeout, build errors, or execution errors
func (s *ScriggoRunner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	defer cancel()

	// Create a file system with the program source
	fsys := scriggo.Files{s.fileName: s.src}

	// Create native packages with the message object and standard library packages
	//nolint:forbidigo // fmt.Print* are exposed for script usage, not direct logging
	packages := native.Packages{
		"events": native.Package{
			Name: "events",
			Declarations: native.Declarations{
				"Message": msg,
			},
		},
		"fmt": native.Package{
			Name: "fmt",
			Declarations: native.Declarations{
				"Errorf":   fmt.Errorf,
				"Print":    fmt.Print,
				"Printf":   fmt.Printf,
				"Println":  fmt.Println,
				"Sprint":   fmt.Sprint,
				"Sprintf":  fmt.Sprintf,
				"Sprintln": fmt.Sprintln,
			},
		},
		"strings": native.Package{
			Name: "strings",
			Declarations: native.Declarations{
				"Contains":    strings.Contains,
				"HasPrefix":   strings.HasPrefix,
				"HasSuffix":   strings.HasSuffix,
				"Index":       strings.Index,
				"Join":        strings.Join,
				"Replace":     strings.Replace,
				"ReplaceAll":  strings.ReplaceAll,
				"Split":       strings.Split,
				"ToLower":     strings.ToLower,
				"ToUpper":     strings.ToUpper,
				"Trim":        strings.Trim,
				"TrimPrefix":  strings.TrimPrefix,
				"TrimSuffix":  strings.TrimSuffix,
				"TrimSpace":   strings.TrimSpace,
				"NewReader":   strings.NewReader,
				"NewReplacer": strings.NewReplacer,
			},
		},
		"strconv": native.Package{
			Name: "strconv",
			Declarations: native.Declarations{
				"Atoi":       strconv.Atoi,
				"Itoa":       strconv.Itoa,
				"FormatInt":  strconv.FormatInt,
				"FormatBool": strconv.FormatBool,
				"ParseInt":   strconv.ParseInt,
				"ParseFloat": strconv.ParseFloat,
				"ParseBool":  strconv.ParseBool,
			},
		},
		"time": native.Package{
			Name: "time",
			Declarations: native.Declarations{
				"Now":           time.Now,
				"Since":         time.Since,
				"Until":         time.Until,
				"Parse":         time.Parse,
				"ParseDuration": time.ParseDuration,
				"Date":          time.Date,
				"Unix":          time.Unix,
				"UnixMilli":     time.UnixMilli,
				"Sleep":         time.Sleep,
				"Duration":      reflect.TypeOf(time.Duration(0)),
				"Time":          reflect.TypeOf(time.Time{}),
				"RFC3339":       time.RFC3339,
				"RFC3339Nano":   time.RFC3339Nano,
				"RFC1123":       time.RFC1123,
				"Second":        time.Second,
				"Minute":        time.Minute,
				"Hour":          time.Hour,
				"Millisecond":   time.Millisecond,
				"Microsecond":   time.Microsecond,
				"Nanosecond":    time.Nanosecond,
			},
		},
		"bytes": native.Package{
			Name: "bytes",
			Declarations: native.Declarations{
				"Buffer":     reflect.TypeOf(bytes.Buffer{}),
				"NewBuffer":  bytes.NewBuffer,
				"Contains":   bytes.Contains,
				"Equal":      bytes.Equal,
				"HasPrefix":  bytes.HasPrefix,
				"HasSuffix":  bytes.HasSuffix,
				"Index":      bytes.Index,
				"Join":       bytes.Join,
				"Split":      bytes.Split,
				"ToLower":    bytes.ToLower,
				"ToUpper":    bytes.ToUpper,
				"Trim":       bytes.Trim,
				"TrimSpace":  bytes.TrimSpace,
				"TrimPrefix": bytes.TrimPrefix,
				"TrimSuffix": bytes.TrimSuffix,
			},
		},
		"encoding/json": native.Package{
			Name: "json",
			Declarations: native.Declarations{
				"Marshal":       json.Marshal,
				"Unmarshal":     json.Unmarshal,
				"MarshalIndent": json.MarshalIndent,
				"Valid":         json.Valid,
			},
		},
		"encoding/base64": native.Package{
			Name: "base64",
			Declarations: native.Declarations{
				"StdEncoding":    base64.StdEncoding,
				"URLEncoding":    base64.URLEncoding,
				"RawStdEncoding": base64.RawStdEncoding,
				"RawURLEncoding": base64.RawURLEncoding,
			},
		},
		"math": native.Package{
			Name: "math",
			Declarations: native.Declarations{
				"Abs":   math.Abs,
				"Ceil":  math.Ceil,
				"Floor": math.Floor,
				"Max":   math.Max,
				"Min":   math.Min,
				"Pow":   math.Pow,
				"Round": math.Round,
				"Sqrt":  math.Sqrt,
			},
		},
		"sort": native.Package{
			Name: "sort",
			Declarations: native.Declarations{
				"Strings":  sort.Strings,
				"Ints":     sort.Ints,
				"Float64s": sort.Float64s,
			},
		},
		"regexp": native.Package{
			Name: "regexp",
			Declarations: native.Declarations{
				"Compile":     regexp.Compile,
				"MustCompile": regexp.MustCompile,
				"Match":       regexp.Match,
				"MatchString": regexp.MatchString,
				"QuoteMeta":   regexp.QuoteMeta,
				"Regexp":      reflect.TypeOf((*regexp.Regexp)(nil)),
			},
		},
	}

	// Build options
	opts := &scriggo.BuildOptions{
		AllowGoStmt: s.cfg.AllowGoStmt,
		Packages:    packages,
	}

	// Build the program
	program, err := scriggo.Build(fsys, opts)
	if err != nil {
		s.slog.Error("scriggo build failed", "error", err)
		return fmt.Errorf("scriggo build error: %w", err)
	}

	// Execute with panic recovery
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.slog.Error("scriggo panic recovered", "panic", r)
				done <- fmt.Errorf("scriggo panic: %v", r)
			}
		}()

		// Run with context for cancellation
		runOpts := &scriggo.RunOptions{
			Context: ctx,
		}
		err := program.Run(runOpts)
		if err != nil {
			s.slog.Error("scriggo execution failed", "error", err)
		}
		done <- err
	}()

	// Wait for completion or timeout
	select {
	case <-ctx.Done():
		s.slog.Warn("scriggo execution timeout", "timeout", s.cfg.Timeout)
		return fmt.Errorf("scriggo execution timeout after %v", s.cfg.Timeout)
	case err := <-done:
		if err != nil {
			// Check if it's a context cancellation
			if ctx.Err() != nil {
				return fmt.Errorf("scriggo execution timeout after %v", s.cfg.Timeout)
			}
			return fmt.Errorf("scriggo execution error: %w", err)
		}
	}

	return nil
}

// Close performs cleanup when the runner is no longer needed.
//
// Currently, ScriggoRunner has no resources to release, but this method
// is required by the connectors.Runner interface.
//
// Returns:
//   - error: Always nil
func (s *ScriggoRunner) Close() error {
	s.slog.Info("closing scriggo runner")
	return nil
}

// VerifyScriptIntegrity checks if the script matches the expected SHA256 hash.
//
// This prevents execution of modified or tampered scripts by comparing
// the actual hash with the expected one from configuration.
//
// Parameters:
//   - script: The Go source code to verify
//   - expectedHash: The expected SHA256 hash in hex format
//
// Returns:
//   - error: nil if hash matches, error otherwise
func VerifyScriptIntegrity(script []byte, expectedHash string) error {
	actualHash := sha256.Sum256(script)
	actualHashHex := hex.EncodeToString(actualHash[:])

	if actualHashHex != expectedHash {
		return fmt.Errorf("script integrity check failed: expected %s, got %s", expectedHash, actualHashHex)
	}

	return nil
}
