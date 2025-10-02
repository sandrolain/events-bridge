package expreval

import (
	"fmt"
	"reflect"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
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
	if e.program == nil {
		return true, nil // If no expression is set, always return true
	}

	result, err := vm.Run(e.program, input)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate expression: %w", err)
	}

	return toBool(result), nil
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
	case int, int8, int16, int32, int64:
		return val != 0
	case uint, uint8, uint16, uint32, uint64:
		return val != 0
	case float32, float64:
		return val != 0
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
