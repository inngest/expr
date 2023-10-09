package expr

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	"github.com/stretchr/testify/require"
)

func newAggregateEvaluator(t *testing.T) AggregateEvaluator {
	t.Helper()
	return NewAggregateEvaluator(newParser(t))
}

func newParser(t *testing.T) TreeParser {
	env, err := cel.NewEnv(
		cel.Variable("event", cel.AnyType),
		cel.Variable("async", cel.AnyType),
	)
	require.NoError(t, err)
	parser, err := NewTreeParser(env)
	require.NoError(t, err)
	return parser
}

func TestParse(t *testing.T) {
	ctx := context.Background()

	t.Run("Base case", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			{
				input: `!(event == "no") && !(!(event == "yes")) && event.data > 50 && (event.ok >= 'true' && event.continue != 'no')`,
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(50),
								Ident:    "event.data",
								Operator: operators.Greater,
							},
							{
								Depth:    0,
								Literal:  "yes",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, actual, test.input)
		}
	})

	t.Run("It handles basic expressions", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			{
				input: "event == 'foo'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
			{
				input: "event.data.run_id == 'xyz'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "xyz",
								Ident:    "event.data.run_id",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
			{
				input: "event.data.id == 'foo' && event.data.value > 100",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event.data.id",
								Operator: operators.Equals,
							},
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.Greater,
							},
						},
					},
				},
			},
			{
				input: "event.data.id == 'foo' && event.data.value > 100 && event.data.float <= 3.141 ",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  float64(3.141),
								Ident:    "event.data.float",
								Operator: operators.LessEquals,
							},
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event.data.id",
								Operator: operators.Equals,
							},
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.Greater,
							},
						},
					},
				},
			},
			// Dupes should be, well, deduped
			{
				input: "event == 'foo' && event == 'foo'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, actual, test.input)
		}
	})

	t.Run("It negates expressions", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			{
				input:    "!(event.data.a == 'a')",
				expected: []PredicateGroup{},
			},
			{
				input: "!(!(event.data.a == 'a'))",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "a",
								Ident:    "event.data.a",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, actual, test.input)
		}
	})

	t.Run("It doesn't handle non equality matching for strings", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			{
				input:    "event.data.a >= 'a'",
				expected: []PredicateGroup{},
			},
			{
				input:    "event.data.a != 'a'",
				expected: []PredicateGroup{},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, actual)
		}
	})

	t.Run("It handles OR branching", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			{
				input: "event == 'foo' || event == 'bar'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "bar",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
			{
				input: "(event == 'foo' || event == 'bar')",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "bar",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
			{
				input: "event == 'foo' || event == 'bar' || event == 'baz'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "bar",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "baz",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
			{
				input: "(event.data.type == 'order' && event.data.value > 500) || event.data.type == 'preorder'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "order",
								Ident:    "event.data.type",
								Operator: operators.Equals,
							},
							{
								Depth:    0,
								Literal:  int64(500),
								Ident:    "event.data.value",
								Operator: operators.Greater,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "preorder",
								Ident:    "event.data.type",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			byt, _ := json.MarshalIndent(actual, "", "  ")
			require.EqualValues(t, test.expected, actual, string(byt))
		}
	})

	t.Run("It handles AND branching", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			{
				input: "event.data.a == 'a' && event.data.b == 'b' && event.data.c == 'c' && event.data.d  == 'd' && event.ts > 123",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "c",
								Ident:    "event.data.c",
								Operator: operators.Equals,
							},
							{
								Depth:    0,
								Literal:  "d",
								Ident:    "event.data.d",
								Operator: operators.Equals,
							},
							{
								Depth:    0,
								Literal:  int64(123),
								Ident:    "event.ts",
								Operator: operators.Greater,
							},
							{
								Depth:    0,
								Literal:  "a",
								Ident:    "event.data.a",
								Operator: operators.Equals,
							},
							{
								Depth:    0,
								Literal:  "b",
								Ident:    "event.data.b",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, actual)
		}
	})

	t.Run("It handles OR branching", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			{
				input: "event == 'foo' || event == 'bar'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "bar",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
			{
				input: "(event == 'foo' || event == 'bar')",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "bar",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
			{
				input: "event == 'foo' || event == 'bar' || event == 'baz'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "bar",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0, // RHS is evaluated first.
								Literal:  "baz",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
			{
				input: "(event.data.type == 'order' && event.data.value > 500) || event.data.type == 'preorder'",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "order",
								Ident:    "event.data.type",
								Operator: operators.Equals,
							},
							{
								Depth:    0,
								Literal:  int64(500),
								Ident:    "event.data.value",
								Operator: operators.Greater,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "preorder",
								Ident:    "event.data.type",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			byt, _ := json.MarshalIndent(actual, "", "  ")
			require.EqualValues(t, test.expected, actual, string(byt))
		}
	})

	t.Run("It normalizes GT/LT(e) operators", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			// Normalizing to literal on RHS
			{
				input: "100 < event.data.value",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.Greater,
							},
						},
					},
				},
			},
			{
				input: "100 <= event.data.value",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.GreaterEquals,
							},
						},
					},
				},
			},
			{
				input: "100 > event.data.value",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.Less,
							},
						},
					},
				},
			},
			{
				input: "100 >= event.data.value",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.LessEquals,
							},
						},
					},
				},
			},
			// Normal
			{
				input: "event.data.value < 100",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.Less,
							},
						},
					},
				},
			},
			{
				input: "event.data.value <= 100",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.LessEquals,
							},
						},
					},
				},
			},
			{
				input: "event.data.value > 100",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.Greater,
							},
						},
					},
				},
			},
			{
				input: "event.data.value >= 100",
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(100),
								Ident:    "event.data.value",
								Operator: operators.GreaterEquals,
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			require.EqualValues(t, test.expected, actual)
		}
	})

	t.Run("Complex queries with many branches", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []PredicateGroup
		}{
			{
				// NOTE: It is expected that this ignores the nested if branches, currently.
				// We still evaluate expressions that match predicate groups. Over time, we will
				// improve nested branches.
				input: `
						((event.data.type == 'order' || event.data.type == "preorder") && event.data.value > 500)
						|| event.data.type == 'refund'`,
				expected: []PredicateGroup{
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  int64(500),
								Ident:    "event.data.value",
								Operator: operators.Greater,
							},
						},
					},
					{
						Predicates: []Predicate{
							{
								Depth:    0,
								Literal:  "refund",
								Ident:    "event.data.type",
								Operator: operators.Equals,
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			byt, _ := json.MarshalIndent(actual, "", "  ")
			require.EqualValues(t, test.expected, actual, string(byt))
		}
	})

}
