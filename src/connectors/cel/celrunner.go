package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/interpreter/functions"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Ensure CELRunner implements connectors.Runner
var _ connectors.Runner = &CELRunner{}

type RunnerConfig struct {
	Path     string                        `mapstructure:"path" validate:"required"`
	Timeout  time.Duration                 `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Services map[string]connectors.Service `mapstructure:"services,omitempty"`
}

type CELRunner struct {
	cfg        *RunnerConfig
	slog       *slog.Logger
	env        *cel.Env
	program    cel.Program
	scriptName string
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// New creates a new instance of CELRunner
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "CEL Runner")
	log.Info("loading cel program", "path", cfg.Path)

	src, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read cel file: %w", err)
	}

	declsList := []*exprpb.Decl{
		decls.NewVar("metadata", decls.NewMapType(decls.String, decls.String)),
		decls.NewVar("data_bytes", decls.Bytes),
		decls.NewVar("data_string", decls.String),
		decls.NewFunction("bytes_to_string", decls.NewOverload("bytes_to_string", []*exprpb.Type{decls.Bytes}, decls.String)),
		decls.NewFunction("string_to_bytes", decls.NewOverload("string_to_bytes", []*exprpb.Type{decls.String}, decls.Bytes)),
		decls.NewFunction("log_info", decls.NewOverload("log_info_string", []*exprpb.Type{decls.String}, decls.Bool)),
		decls.NewFunction("log_warn", decls.NewOverload("log_warn_string", []*exprpb.Type{decls.String}, decls.Bool)),
		decls.NewFunction("log_error", decls.NewOverload("log_error_string", []*exprpb.Type{decls.String}, decls.Bool)),
	}

	for name := range cfg.Services {
		declsList = append(declsList,
			decls.NewFunction(serviceFuncName(name), decls.NewOverload(serviceFuncName(name)+"_list", []*exprpb.Type{decls.NewListType(decls.Dyn)}, decls.Bytes)),
		)
	}

	serviceOverloads := buildServiceOverloads(cfg.Services)
	utilityOverloads := buildUtilityOverloads()
	logOverloads := buildLogOverloads(log.With("script", cfg.Path))
	allOverloads := append(append(utilityOverloads, logOverloads...), serviceOverloads...)

	env, err := cel.NewEnv(
		cel.Declarations(declsList...),
		cel.Functions(allOverloads...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create cel environment: %w", err)
	}

	ast, issues := env.Compile(string(src))
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile cel program: %w", issues.Err())
	}

	if ast == nil {
		return nil, fmt.Errorf("failed to compile cel program: empty AST")
	}

	prog, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate cel program: %w", err)
	}

	return &CELRunner{
		cfg:        cfg,
		slog:       log,
		env:        env,
		program:    prog,
		scriptName: filepath.Base(cfg.Path),
	}, nil
}

// Process evaluates the CEL program for the incoming message
func (c *CELRunner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.Timeout)
	defer cancel()

	metadata, data, err := msg.GetMetadataAndData()
	if err != nil {
		return fmt.Errorf("failed to get metadata and data: %w", err)
	}

	evalCh := make(chan evalResult, 1)
	go func() {
		defer close(evalCh)
		res, _, evalErr := c.program.Eval(map[string]any{
			"metadata":    metadata,
			"data_bytes":  data,
			"data_string": string(data),
		})
		evalCh <- evalResult{val: res, err: evalErr}
	}()

	select {
	case <-ctx.Done():
		c.slog.Warn("cel execution timeout", "script", c.scriptName)
		return fmt.Errorf("cel execution timeout")
	case result := <-evalCh:
		if result.err != nil {
			return fmt.Errorf("cel execution error: %w", result.err)
		}
		if types.IsError(result.val) {
			return fmt.Errorf("cel execution error: %v", result.val)
		}
		if err := c.applyResult(msg, result.val); err != nil {
			return fmt.Errorf("failed to apply cel result: %w", err)
		}
	}

	return nil
}

func (c *CELRunner) Close() error {
	return nil
}

type evalResult struct {
	val ref.Val
	err error
}

func (c *CELRunner) buildFunctionOverloads(msg *message.RunnerMessage) []*functions.Overload {
	overloads := make([]*functions.Overload, 0, 8+len(c.cfg.Services))
	overloads = append(overloads, c.dataOverloads(msg)...)
	overloads = append(overloads, c.metadataOverloads(msg)...)
	overloads = append(overloads, c.utilityOverloads()...)
	overloads = append(overloads, c.loggingOverloads()...)
	overloads = append(overloads, c.serviceOverloads()...)
	return overloads
}

func (c *CELRunner) dataOverloads(msg *message.RunnerMessage) []*functions.Overload {
	return []*functions.Overload{
		{
			Operator: "set_data_bytes",
			Unary: func(val ref.Val) ref.Val {
				b, ok := val.(types.Bytes)
				if !ok {
					return types.NewErr("set_data_bytes expects bytes argument")
				}
				msg.SetData([]byte(b))
				return types.True
			},
		},
		{
			Operator: "set_data_string",
			Unary: func(val ref.Val) ref.Val {
				s, ok := val.(types.String)
				if !ok {
					return types.NewErr("set_data_string expects string argument")
				}
				msg.SetData([]byte(s))
				return types.True
			},
		},
	}
}

func (c *CELRunner) metadataOverloads(msg *message.RunnerMessage) []*functions.Overload {
	return []*functions.Overload{
		{
			Operator: "set_metadata",
			Binary: func(key, value ref.Val) ref.Val {
				k, ok := key.(types.String)
				if !ok {
					return types.NewErr("set_metadata expects string key")
				}
				v, ok := value.(types.String)
				if !ok {
					return types.NewErr("set_metadata expects string value")
				}
				msg.AddMetadata(string(k), string(v))
				return types.True
			},
		},
		{
			Operator: "merge_metadata",
			Unary: func(val ref.Val) ref.Val {
				mapper, ok := val.(traits.Mapper)
				if !ok {
					return types.NewErr("merge_metadata expects map argument")
				}
				if err := mergeMetadataFromVal(msg, mapper); err != nil {
					return types.NewErr("merge_metadata error: %v", err)
				}
				return types.True
			},
		},
	}
}

func (c *CELRunner) utilityOverloads() []*functions.Overload {
	return []*functions.Overload{
		{
			Operator: "bytes_to_string",
			Unary: func(val ref.Val) ref.Val {
				b, ok := val.(types.Bytes)
				if !ok {
					return types.NewErr("bytes_to_string expects bytes")
				}
				return types.String(string(b))
			},
		},
		{
			Operator: "string_to_bytes",
			Unary: func(val ref.Val) ref.Val {
				s, ok := val.(types.String)
				if !ok {
					return types.NewErr("string_to_bytes expects string")
				}
				return types.Bytes([]byte(s))
			},
		},
	}
}

func (c *CELRunner) loggingOverloads() []*functions.Overload {
	return []*functions.Overload{
		c.logOverload("log_info", func(msg string) {
			c.slog.Info(msg, "script", c.scriptName)
		}),
		c.logOverload("log_warn", func(msg string) {
			c.slog.Warn(msg, "script", c.scriptName)
		}),
		c.logOverload("log_error", func(msg string) {
			c.slog.Error(msg, "script", c.scriptName)
		}),
	}
}

func (c *CELRunner) serviceOverloads() []*functions.Overload {
	overloads := make([]*functions.Overload, 0, len(c.cfg.Services))
	for name, svc := range c.cfg.Services {
		serviceName := name
		service := svc
		overloads = append(overloads, &functions.Overload{
			Operator: serviceFuncName(serviceName),
			Unary: func(val ref.Val) ref.Val {
				list, ok := val.(traits.Lister)
				if !ok {
					return types.NewErr("%s expects list argument", serviceFuncName(serviceName))
				}
				method, args, err := extractServiceCallParams(list)
				if err != nil {
					return types.NewErr("%s argument error: %v", serviceFuncName(serviceName), err)
				}
				if !service.IsValidMethod(method, args) {
					return types.NewErr("%s invalid method %s", serviceFuncName(serviceName), method)
				}
				res, callErr := service.Call(method, args)
				if callErr != nil {
					return types.NewErr("%s call failed: %v", serviceFuncName(serviceName), callErr)
				}
				return types.Bytes(res)
			},
		})
	}
	return overloads
}

func (c *CELRunner) logOverload(name string, logger func(string)) *functions.Overload {
	return &functions.Overload{
		Operator: name,
		Unary: func(val ref.Val) ref.Val {
			s, ok := val.(types.String)
			if !ok {
				return types.NewErr("%s expects string argument", name)
			}
			logger(string(s))
			return types.True
		},
	}
}

func (c *CELRunner) applyResult(msg *message.RunnerMessage, val ref.Val) error {
	if val == nil || val == types.NullValue {
		return nil
	}

	switch typed := val.(type) {
	case types.Bytes:
		msg.SetData([]byte(typed))
		return nil
	case types.String:
		msg.SetData([]byte(typed))
		return nil
	case traits.Mapper:
		native, err := celValToInterface(typed)
		if err != nil {
			return err
		}
		return applyNativeResult(msg, native)
	default:
		return applyNativeResult(msg, typed.Value())
	}
}

func extractServiceCallParams(list traits.Lister) (string, []any, error) {
	sizeVal := list.Size()
	sizeInt, ok := sizeVal.(types.Int)
	if !ok {
		return "", nil, fmt.Errorf("service call list size must be int")
	}
	size := int(sizeInt)
	if size == 0 {
		return "", nil, fmt.Errorf("service call requires method name")
	}
	methodVal := list.Get(types.Int(0))
	method, ok := methodVal.(types.String)
	if !ok {
		return "", nil, fmt.Errorf("service call method must be string")
	}
	args := make([]any, 0, size-1)
	for i := 1; i < size; i++ {
		argVal := list.Get(types.Int(i))
		converted, err := celValToInterface(argVal)
		if err != nil {
			return "", nil, err
		}
		args = append(args, converted)
	}
	return string(method), args, nil
}

func celValToInterface(val ref.Val) (any, error) {
	switch v := val.(type) {
	case types.String:
		return string(v), nil
	case types.Bytes:
		return []byte(v), nil
	case types.Int:
		return int64(v), nil
	case types.Double:
		return float64(v), nil
	case types.Bool:
		return bool(v), nil
	case traits.Lister:
		return listValToInterfaces(v)
	case traits.Mapper:
		return mapValToInterface(v)
	case types.Null:
		return nil, nil
	default:
		return v.Value(), nil
	}
}

func listValToInterfaces(list traits.Lister) ([]any, error) {
	sizeVal := list.Size()
	sizeInt, ok := sizeVal.(types.Int)
	if !ok {
		return nil, fmt.Errorf("list size is not int")
	}
	size := int(sizeInt)
	out := make([]any, 0, size)
	for i := 0; i < size; i++ {
		elem := list.Get(types.Int(i))
		converted, err := celValToInterface(elem)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func mapValToInterface(mapper traits.Mapper) (map[string]any, error) {
	iter := mapper.Iterator()
	out := make(map[string]any)
	for iter.HasNext() == types.True {
		key := iter.Next()
		keyStr, ok := key.(types.String)
		if !ok {
			return nil, fmt.Errorf("map key must be string")
		}
		value := mapper.Get(keyStr)
		converted, err := celValToInterface(value)
		if err != nil {
			return nil, err
		}
		out[string(keyStr)] = converted
	}
	return out, nil
}

func applyNativeResult(msg *message.RunnerMessage, native any) error {
	switch v := native.(type) {
	case nil:
		return nil
	case []byte:
		msg.SetData(v)
	case string:
		msg.SetData([]byte(v))
	case map[string]string:
		for k, val := range v {
			msg.AddMetadata(k, val)
		}
	case map[string]any:
		return applyMapResult(msg, v)
	default:
		// Ignore other types; expression may return bool/number for chaining
	}
	return nil
}

func applyMapResult(msg *message.RunnerMessage, data map[string]any) error {
	if raw, ok := data["data"]; ok {
		if err := applyNativeResult(msg, raw); err != nil {
			return err
		}
	}
	if raw, ok := data["metadata"]; ok {
		if err := addMetadataFromAny(msg, raw); err != nil {
			return err
		}
	}
	return nil
}

func addMetadataFromAny(msg *message.RunnerMessage, value any) error {
	switch meta := value.(type) {
	case map[string]string:
		for k, v := range meta {
			msg.AddMetadata(k, v)
		}
		return nil
	case map[string]any:
		for k, raw := range meta {
			str, ok := raw.(string)
			if !ok {
				return fmt.Errorf("metadata value for key %s must be string", k)
			}
			msg.AddMetadata(k, str)
		}
		return nil
	case traits.Mapper:
		native, err := celValToInterface(meta)
		if err != nil {
			return err
		}
		return addMetadataFromAny(msg, native)
	case nil:
		return nil
	default:
		return fmt.Errorf("metadata must be a map, got %T", value)
	}
}

func mergeMetadataFromVal(msg *message.RunnerMessage, mapper traits.Mapper) error {
	native, err := celValToInterface(mapper)
	if err != nil {
		return err
	}
	return addMetadataFromAny(msg, native)
}

func serviceFuncName(name string) string {
	return fmt.Sprintf("%s_call", name)
}
