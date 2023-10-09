package expr

import (
	"context"
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

type parseTestInput struct {
	input    string
	expected []PredicateGroup
}

func TestParse(t *testing.T) {
	ctx := context.Background()

	// helper function to assert each case.
	assert := func(t *testing.T, tests []parseTestInput) {
		for _, test := range tests {
			actual, err := newParser(t).Parse(ctx, test.input)
			require.NoError(t, err)
			require.Equal(t, len(test.expected), len(actual))

			// Assert all predicate groups are the same, and the ID has the
			// correct count embedded.
			//
			// We can't compare predicate groups as the ID is random.
			for n, item := range actual {
				expected := test.expected[n]
				require.EqualValues(t, expected.Predicates, item.Predicates)
				require.EqualValues(t, len(item.Predicates), item.GroupID.Size())
			}
		}
	}

	t.Run("Base case", func(t *testing.T) {
		tests := []parseTestInput{
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

		assert(t, tests)
	})

	t.Run("It handles basic expressions", func(t *testing.T) {
		tests := []parseTestInput{
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

		assert(t, tests)
	})

	t.Run("It negates expressions", func(t *testing.T) {
		tests := []parseTestInput{
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

		assert(t, tests)
	})

	t.Run("It doesn't handle non equality matching for strings", func(t *testing.T) {
		tests := []parseTestInput{
			{
				input:    "event.data.a >= 'a'",
				expected: []PredicateGroup{},
			},
			{
				input:    "event.data.a != 'a'",
				expected: []PredicateGroup{},
			},
		}

		assert(t, tests)
	})

	t.Run("It handles OR branching", func(t *testing.T) {
		tests := []parseTestInput{
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

		assert(t, tests)
	})

	t.Run("It handles AND branching", func(t *testing.T) {
		tests := []parseTestInput{
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

		assert(t, tests)
	})

	t.Run("It handles OR branching", func(t *testing.T) {
		tests := []parseTestInput{
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

		assert(t, tests)
	})

	t.Run("It normalizes GT/LT(e) operators", func(t *testing.T) {
		tests := []parseTestInput{
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

		assert(t, tests)
	})

	t.Run("Complex queries with many branches", func(t *testing.T) {
		tests := []parseTestInput{
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

		assert(t, tests)
	})
}

func TestParseGroupIDs(t *testing.T) {
	t.Run("It creates new group IDs when parsing the same expression", func(t *testing.T) {
		ctx := context.Background()
		a, err := newParser(t).Parse(ctx, "event == 'foo'")
		require.NoError(t, err)
		b, err := newParser(t).Parse(ctx, "event == 'foo'")
		require.NoError(t, err)
		c, err := newParser(t).Parse(ctx, "event == 'foo'")

		require.NotEqual(t, a[0].GroupID, b[0].GroupID)
		require.NotEqual(t, b[0].GroupID, c[0].GroupID)
		require.NotEqual(t, a[0].GroupID, c[0].GroupID)
	})
}
