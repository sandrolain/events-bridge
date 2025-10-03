package expreval

import (
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
)

// TestNewExprEvaluator covers creation success and empty expression behavior.
func TestNewExprEvaluator(t *testing.T) {
	cases := []struct {
		name       string
		expression string
		wantNil    bool
		wantErr    bool
	}{
		{"empty returns nil", "", true, false},
		{"simple ok", "1 == 1", false, false},
		{"compile error", "this is not valid", true, true},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			eval, err := NewExprEvaluator(c.expression)
			if c.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.wantNil && eval != nil {
				t.Fatalf("expected nil evaluator, got %#v", eval)
			}
			if !c.wantNil && eval == nil {
				t.Fatalf("expected evaluator instance, got nil")
			}
		})
	}
}

// TestEvalBasic ensures basic boolean logic and error propagation.
func TestEvalBasic(t *testing.T) {
	eval, err := NewExprEvaluator("a > 10 && b == 'ok'")
	if err != nil || eval == nil {
		panic("compilation failed in test setup")
	}

	cases := []struct {
		name  string
		input map[string]any
		want  bool
	}{
		{"true case", map[string]any{"a": 11, "b": "ok"}, true},
		{"false case threshold", map[string]any{"a": 10, "b": "ok"}, false},
		{"false case value", map[string]any{"a": 11, "b": "no"}, false},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := eval.Eval(c.input)
			if err != nil {
				t.Fatalf("unexpected eval error: %v", err)
			}
			if got != c.want {
				t.Fatalf("expected %v, got %v", c.want, got)
			}
		})
	}
}

// TestEvalError checks unknown identifiers produce an error.
func TestEvalError(t *testing.T) {
	eval, err := NewExprEvaluator("unknown + 1 == 2")
	if err != nil || eval == nil {
		panic("compilation failed in test setup")
	}
	// The expression will reference an identifier not present in the input map; expr runtime should error.
	_, runErr := eval.Eval(map[string]any{})
	if runErr == nil {
		t.Fatalf("expected evaluation error for missing identifier")
	}
}

// TestEvalMessage validates metadata injection path.
// fakeSourceMessage provides minimal implementation for message.SourceMessage.
type fakeSourceMessage struct {
	id       []byte
	metadata map[string]string
	data     []byte
}

func (f *fakeSourceMessage) GetID() []byte                           { return f.id }
func (f *fakeSourceMessage) GetMetadata() (map[string]string, error) { return f.metadata, nil }
func (f *fakeSourceMessage) GetData() ([]byte, error)                { return f.data, nil }
func (f *fakeSourceMessage) Ack() error                              { return nil }
func (f *fakeSourceMessage) Nak() error                              { return nil }
func (f *fakeSourceMessage) Reply(d *message.ReplyData) error        { return nil }

func TestEvalMessage(t *testing.T) {
	eval, err := NewExprEvaluator("metadata.kind == 'test' && int(metadata.count) > 1")
	if err != nil || eval == nil {
		panic("compilation failed in test setup")
	}

	base := &fakeSourceMessage{metadata: map[string]string{"kind": "test", "count": "2"}}
	msg := message.NewRunnerMessage(base)
	res, err := eval.EvalMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res {
		t.Fatalf("expected true, got false")
	}

	base2 := &fakeSourceMessage{metadata: map[string]string{"kind": "test", "count": "1"}}
	msg2 := message.NewRunnerMessage(base2)
	res, err = eval.EvalMessage(msg2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res {
		t.Fatalf("expected false, got true")
	}
}

// fake type to validate struct / collection conversions for toBool

type zeroStruct struct{ A int }
type nonZeroStruct struct{ A int }

// TestToBoolCasts exercises the internal toBool conversion rules via exported API.
func TestToBoolCasts(t *testing.T) {
	// Build an expression that explicitly coerces each value to boolean via comparisons / length checks
	eval, err := NewExprEvaluator("a && b != '' && c != 0 && d != 0 && e != 0 && len(f) > 0 && len(g) > 0 && len(h) > 0 && i.A != 0 && j != '' && k != 0")
	if err != nil || eval == nil {
		panic("compilation failed in test setup")
	}
	// Build all truthy values except one each turn to verify behavior. We'll run subcases.
	cases := []struct {
		name string
		vals map[string]any
		want bool
	}{
		{"all truthy", map[string]any{"a": true, "b": "x", "c": 1, "d": uint(1), "e": 1.5, "f": []int{1}, "g": map[string]int{"k": 1}, "h": []int{2}, "i": nonZeroStruct{A: 1}, "j": "err", "k": 123}, true},
		{"one falsy string empty", map[string]any{"a": true, "b": "", "c": 1, "d": uint(1), "e": 1.5, "f": []int{1}, "g": map[string]int{"k": 1}, "h": []int{2}, "i": nonZeroStruct{A: 1}, "j": "err", "k": 123}, false},
		{"one falsy zero int", map[string]any{"a": true, "b": "x", "c": 0, "d": uint(1), "e": 1.5, "f": []int{1}, "g": map[string]int{"k": 1}, "h": []int{2}, "i": nonZeroStruct{A: 1}, "j": "err", "k": 123}, false},
		{"one falsy empty slice", map[string]any{"a": true, "b": "x", "c": 1, "d": uint(1), "e": 1.5, "f": []int{}, "g": map[string]int{"k": 1}, "h": []int{2}, "i": nonZeroStruct{A: 1}, "j": "err", "k": 123}, false},
		{"one falsy empty map", map[string]any{"a": true, "b": "x", "c": 1, "d": uint(1), "e": 1.5, "f": []int{1}, "g": map[string]int{}, "h": []int{2}, "i": nonZeroStruct{A: 1}, "j": "err", "k": 123}, false},
		{"one falsy zero struct", map[string]any{"a": true, "b": "x", "c": 1, "d": uint(1), "e": 1.5, "f": []int{1}, "g": map[string]int{"k": 1}, "h": []int{2}, "i": zeroStruct{}, "j": "err", "k": 123}, false},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := eval.Eval(c.vals)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("expected %v, got %v", c.want, got)
			}
		})
	}
}
