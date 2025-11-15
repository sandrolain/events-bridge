package expreval

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/sandrolain/events-bridge/src/message"
)

const (
	// Maximum expression length to prevent DoS
	DefaultMaxExpressionLength = 10000
	// Maximum complexity score (number of operators/functions)
	DefaultMaxComplexity = 1000
	// Maximum nesting depth
	DefaultMaxNestingDepth = 50
)

// Config holds the configuration for expression evaluation with security settings
type Config struct {
	Expression          string
	MaxExpressionLength int
	AllowedFunctions    []string
	DisableBuiltins     bool
	AllowUndefined      bool
}

type ExprEvaluator struct {
	program *vm.Program
}

// NewExprEvaluator creates a new ExprEvaluator from an expression string
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

// NewExprEvaluatorWithProgram creates an ExprEvaluator from a pre-compiled program
func NewExprEvaluatorWithProgram(program *vm.Program) *ExprEvaluator {
	return &ExprEvaluator{program: program}
}

func (e *ExprEvaluator) Eval(input map[string]any) (bool, error) {
	result, err := vm.Run(e.program, input)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate expression: %w", err)
	}
	return toBool(result), nil
}

func (e *ExprEvaluator) EvalMessage(msg *message.RunnerMessage) (bool, error) {
	meta, err := msg.GetAllMetadata()
	if err != nil {
		return false, fmt.Errorf("failed to get message metadata: %w", err)
	}
	return e.Eval(map[string]any{
		"metadata": meta,
	})
}

// validateExpression performs security checks on an expression string
func validateExpression(expression string, maxLength int) error {
	if maxLength == 0 {
		maxLength = DefaultMaxExpressionLength
	}

	// Check length
	if len(expression) > maxLength {
		return fmt.Errorf("expression exceeds maximum length of %d characters", maxLength)
	}

	// Check for empty expression
	if strings.TrimSpace(expression) == "" {
		return fmt.Errorf("expression cannot be empty")
	}

	// Estimate complexity by counting operators and function calls
	complexity := estimateComplexity(expression)
	if complexity > DefaultMaxComplexity {
		return fmt.Errorf("expression too complex (score: %d, max: %d)", complexity, DefaultMaxComplexity)
	}

	return nil
}

// estimateComplexity returns a rough complexity score for an expression
func estimateComplexity(expr string) int {
	score := 0

	// Count operators
	operators := []string{
		"+", "-", "*", "/", "%", "^",
		"==", "!=", "<", ">", "<=", ">=",
		"&&", "||", "!",
		"in", "not in", "contains",
		"matches", "startsWith", "endsWith",
	}
	for _, op := range operators {
		score += strings.Count(expr, op)
	}

	// Count function-like patterns (word followed by opening parenthesis)
	funcPattern := regexp.MustCompile(`\w+\s*\(`)
	matches := funcPattern.FindAllString(expr, -1)
	score += len(matches) * 2 // Functions are weighted more

	// Count array/map operations
	score += strings.Count(expr, "[")
	score += strings.Count(expr, "{")

	// Count ternary operators
	score += strings.Count(expr, "?") * 3

	return score
}

// validateAllowedFunctions checks if the expression only uses allowed functions
func validateAllowedFunctions(expression string, allowedFunctions []string) error {
	if len(allowedFunctions) == 0 {
		return nil // No restrictions
	}

	// Extract function names from expression
	funcPattern := regexp.MustCompile(`\b([a-zA-Z_]\w*)\s*\(`)
	matches := funcPattern.FindAllStringSubmatch(expression, -1)

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		funcName := match[1]

		// Check if function is in allowlist
		allowed := false
		for _, allowedFunc := range allowedFunctions {
			if funcName == allowedFunc {
				allowed = true
				break
			}
		}

		if !allowed {
			return fmt.Errorf("function '%s' is not in the allowlist", funcName)
		}
	}

	return nil
}

// detectDangerousPatterns checks for potentially dangerous expression patterns
func detectDangerousPatterns(expression string) error {
	// Check for infinite loop patterns
	if strings.Contains(expression, "while(true)") || strings.Contains(expression, "for(;;)") {
		return fmt.Errorf("expression contains infinite loop pattern")
	}

	// Check for excessive nesting (count parentheses depth)
	maxDepth := 0
	currentDepth := 0
	for _, char := range expression {
		switch char {
		case '(', '[', '{':
			currentDepth++
			if currentDepth > maxDepth {
				maxDepth = currentDepth
			}
		case ')', ']', '}':
			currentDepth--
		}
	}

	if maxDepth > DefaultMaxNestingDepth {
		return fmt.Errorf("expression has excessive nesting depth: %d (max: %d)", maxDepth, DefaultMaxNestingDepth)
	}

	return nil
}

// NewExprEvaluatorWithConfig creates a new ExprEvaluator with security validation
func NewExprEvaluatorWithConfig(cfg Config) (*ExprEvaluator, error) {
	// Validate expression
	if err := validateExpression(cfg.Expression, cfg.MaxExpressionLength); err != nil {
		return nil, fmt.Errorf("expression validation failed: %w", err)
	}

	// Check for dangerous patterns
	if err := detectDangerousPatterns(cfg.Expression); err != nil {
		return nil, fmt.Errorf("dangerous expression pattern detected: %w", err)
	}

	// Validate allowed functions
	if len(cfg.AllowedFunctions) > 0 {
		if err := validateAllowedFunctions(cfg.Expression, cfg.AllowedFunctions); err != nil {
			return nil, fmt.Errorf("expression uses disallowed functions: %w", err)
		}
	}

	// Build expr options for security
	var exprOptions []expr.Option
	if !cfg.AllowUndefined {
		exprOptions = append(exprOptions, expr.AllowUndefinedVariables())
	}

	// Compile expression with options
	program, err := expr.Compile(cfg.Expression, exprOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to compile expression: %w", err)
	}

	return &ExprEvaluator{program: program}, nil
}

func toBool(v any) bool {
	if v == nil {
		return false
	}

	// Handle pointer types - check if pointer is nil using reflection
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		return !rv.IsNil()
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
