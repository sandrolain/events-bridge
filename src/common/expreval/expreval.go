package expreval

import (
	"fmt"
	"reflect"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/sandrolain/events-bridge/src/message"
)

type ExprEvaluator struct {
	program *vm.Program
}

func NewExprEvaluator(expression string) (*ExprEvaluator, error) {
	if expression != "" {
		program, err := expr.Compile(expression)
		if err != nil {
			return nil, fmt.Errorf("failed to compile expression: %w", err)
		}
		return &ExprEvaluator{program: program}, nil
	}
	return nil, nil
}

func (e *ExprEvaluator) Eval(input map[string]any) (bool, error) {
	result, err := vm.Run(e.program, input)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate expression: %w", err)
	}
	return toBool(result), nil
}

func (e *ExprEvaluator) EvalMessage(msg *message.RunnerMessage) (bool, error) {
	meta, err := msg.GetMetadata()
	if err != nil {
		return false, fmt.Errorf("failed to get message metadata: %w", err)
	}
	return e.Eval(map[string]any{
		"metadata": meta,
	})
}

func toBool(v any) bool {
	if v == nil {
		return false
	}

	switch val := v.(type) {
	case bool:
		return val
	case string:
		return val != "" && val != "0" && val != "false"
	case int:
		return val != int(0)
	case int8:
		return val != int8(0)
	case int16:
		return val != int16(0)
	case int32:
		return val != int32(0)
	case int64:
		return val != int64(0)
	case uint:
		return val != uint(0)
	case uint8:
		return val != uint8(0)
	case uint16:
		return val != uint16(0)
	case uint32:
		return val != uint32(0)
	case uint64:
		return val != uint64(0)
	case float32:
		return val != float32(0)
	case float64:
		return val != float64(0)
	case uintptr:
		return val != uintptr(0)
	}

	// handle empty collections
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() > 0
	case reflect.Struct:
		// for struct you can decide whether to check "zero value"
		zero := reflect.Zero(rv.Type())
		return !reflect.DeepEqual(rv.Interface(), zero.Interface())
	}

	// fallback: if not nil nor empty, consider it true
	return true
}
