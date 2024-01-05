package expr

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	"github.com/stretchr/testify/require"
)

func newEnv() *cel.Env {
	env, _ := cel.NewEnv(
		cel.Variable("event", cel.AnyType),
		cel.Variable("async", cel.AnyType),
		cel.Variable("vars", cel.AnyType),
	)
	return env
}

func newParser() (TreeParser, error) {
	return NewTreeParser(EnvParser(newEnv()))
}

type parseTestInput struct {
	input    string
	output   string
	expected ParsedExpression
}

func TestParse(t *testing.T) {
	ctx := context.Background()

	// helper function to assert each case.
	assert := func(t *testing.T, tests []parseTestInput) {
		t.Helper()

		for _, test := range tests {
			parser, err := newParser()
			require.NoError(t, err)

			eval := tex(test.input)
			actual, err := parser.Parse(ctx, eval)

			// Shortcut to ensure the evaluable instance matches
			if test.expected.Evaluable == nil {
				test.expected.Evaluable = eval
			}

			require.NoError(t, err)
			require.NotNil(t, actual)

			require.EqualValues(t, test.output, actual.Root.String(), "String() does not match expected output")

			a, _ := json.MarshalIndent(test.expected, "", " ")
			b, _ := json.MarshalIndent(actual, "", " ")

			require.EqualValues(
				t,
				test.expected,
				*actual,
				"Invalid strucutre:\n%s\nExpected: %s\n\nGot: %s\nGroups: %d",
				test.input,
				string(a),
				string(b),
				len(actual.RootGroups()),
			)
		}
	}

	t.Run("It handles array indexing", func(t *testing.T) {
		tests := []parseTestInput{
			{
				input:  `event.data.ids[2] == "a"`,
				output: `event.data.ids[2] == "a"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Ident:    "event.data.ids[2]",
							Literal:  "a",
							Operator: operators.Equals,
						},
					},
				},
			},
			{
				input:  `event.data.ids[2].id == "a"`,
				output: `event.data.ids[2].id == "a"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Ident:    "event.data.ids[2].id",
							Literal:  "a",
							Operator: operators.Equals,
						},
					},
				},
			},
		}

		assert(t, tests)
	})

	t.Run("It handles ident matching", func(t *testing.T) {
		ident := "vars.a"

		tests := []parseTestInput{
			{
				input:  "event == vars.a",
				output: `event == vars.a`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Ident:        "event",
							LiteralIdent: &ident,
							Operator:     operators.Equals,
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
				input:  "event == 'foo'",
				output: `event == "foo"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "foo",
							Ident:    "event",
							Operator: operators.Equals,
						},
					},
				},
			},
			{
				input:  "event.data.run_id == 'xyz'",
				output: `event.data.run_id == "xyz"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "xyz",
							Ident:    "event.data.run_id",
							Operator: operators.Equals,
						},
					},
				},
			},

			{
				input:  "event.data.id == 'foo' && event.data.value > 100",
				output: `event.data.id == "foo" && event.data.value > 100`,
				expected: ParsedExpression{
					Root: Node{
						Ands: []*Node{
							{
								Predicate: &Predicate{
									Literal:  "foo",
									Ident:    "event.data.id",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  int64(100),
									Ident:    "event.data.value",
									Operator: operators.Greater,
								},
							},
						},
					},
				},
			},
			{
				input:  "event.data.id == 'foo' && event.data.value > 100 && event.data.float <= 3.141 ",
				output: `event.data.float <= 3.141 && event.data.id == "foo" && event.data.value > 100`,
				expected: ParsedExpression{
					Root: Node{
						Ands: []*Node{
							{
								Predicate: &Predicate{
									Literal:  3.141,
									Ident:    "event.data.float",
									Operator: operators.LessEquals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  "foo",
									Ident:    "event.data.id",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  int64(100),
									Ident:    "event.data.value",
									Operator: operators.Greater,
								},
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
				input:  "!(event.data.a == 'a')",
				output: `event.data.a != "a"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "a",
							Ident:    "event.data.a",
							Operator: operators.NotEquals,
						},
					},
				},
			},
			{
				input:  "!(!(event.data.a == 'a'))",
				output: `event.data.a == "a"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "a",
							Ident:    "event.data.a",
							Operator: operators.Equals,
						},
					},
				},
			},
		}

		assert(t, tests)
	})

	t.Run("It handles non equality matching for strings", func(t *testing.T) {
		tests := []parseTestInput{
			{
				input:  "event.data.id >= 'ulid'",
				output: `event.data.id >= "ulid"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "ulid",
							Ident:    "event.data.id",
							Operator: operators.GreaterEquals,
						},
					},
				},
			},
			{
				input:  "event.data.id < 'ulid'",
				output: `event.data.id < "ulid"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "ulid",
							Ident:    "event.data.id",
							Operator: operators.Less,
						},
					},
				},
			},
			{
				input:  "event.data.a != 'a'",
				output: `event.data.a != "a"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "a",
							Ident:    "event.data.a",
							Operator: operators.NotEquals,
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
				input:  "event == 'foo' || event == 'bar'",
				output: `event == "foo" || event == "bar"`,
				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							{
								Predicate: &Predicate{
									Literal:  "foo",
									Ident:    "event",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  "bar",
									Ident:    "event",
									Operator: operators.Equals,
								},
							},
						},
					},
				},
			},
			{
				input:  "(event == 'foo' || event == 'bar')",
				output: `event == "foo" || event == "bar"`,
				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							{
								Predicate: &Predicate{
									Literal:  "foo",
									Ident:    "event",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  "bar",
									Ident:    "event",
									Operator: operators.Equals,
								},
							},
						},
					},
				},
			},
			{
				input:  "a == 1 || b == 2 && b != 3",
				output: `a == 1 || (b == 2 && b != 3)`,
				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							// Either
							{
								Predicate: &Predicate{
									Literal:  int64(1),
									Ident:    "a",
									Operator: operators.Equals,
								},
							},
							{
								Ands: []*Node{
									{
										Predicate: &Predicate{
											Literal:  int64(2),
											Ident:    "b",
											Operator: operators.Equals,
										},
									},
									{
										Predicate: &Predicate{
											Literal:  int64(3),
											Ident:    "b",
											Operator: operators.NotEquals,
										},
									},
								},
							},
						},
					},
				},
			},
			{
				input:  "event == 'foo' || event == 'bar' || event == 'baz'",
				output: `event == "baz" || event == "foo" || event == "bar"`,
				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							{
								Predicate: &Predicate{
									Literal:  "baz",
									Ident:    "event",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  "foo",
									Ident:    "event",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  "bar",
									Ident:    "event",
									Operator: operators.Equals,
								},
							},
						},
					},
				},
			},
			{
				input:  `(event.data.type == 'order' && event.data.value > 500) || event.data.type == "preorder"`,
				output: `(event.data.type == "order" && event.data.value > 500) || event.data.type == "preorder"`,

				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							{
								Ands: []*Node{
									{
										Predicate: &Predicate{
											Literal:  "order",
											Ident:    "event.data.type",
											Operator: operators.Equals,
										},
									},
									{
										Predicate: &Predicate{
											Literal:  int64(500),
											Ident:    "event.data.value",
											Operator: operators.Greater,
										},
									},
								},
							},
							{
								Predicate: &Predicate{
									Literal:  "preorder",
									Ident:    "event.data.type",
									Operator: operators.Equals,
								},
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
				input:  "100 < event.data.value",
				output: "event.data.value > 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Greater,
						},
					},
				},
			},
			{
				input:  "100 <= event.data.value",
				output: "event.data.value >= 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.GreaterEquals,
						},
					},
				},
			},
			{
				input:  "100 > event.data.value",
				output: "event.data.value < 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Less,
						},
					},
				},
			},
			{
				input:  "100 >= event.data.value",
				output: "event.data.value <= 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.LessEquals,
						},
					},
				},
			},
			// Normal
			{
				input:  "event.data.value < 100",
				output: "event.data.value < 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Less,
						},
					},
				},
			},
			{
				input:  "event.data.value <= 100",
				output: "event.data.value <= 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.LessEquals,
						},
					},
				},
			},
			{
				input:  "event.data.value > 100",
				output: "event.data.value > 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Greater,
						},
					},
				},
			},
			{
				input:  "event.data.value >= 100",
				output: "event.data.value >= 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.GreaterEquals,
						},
					},
				},
			},
		}

		assert(t, tests)
	})

	t.Run("It negates GT/LT(e) operators", func(t *testing.T) {
		tests := []parseTestInput{
			// Normalizing to literal on RHS
			{
				input:  "!(100 < event.data.value)",
				output: "event.data.value <= 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.LessEquals,
						},
					},
				},
			},
			{
				input:  "!(100 <= event.data.value)",
				output: "event.data.value < 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Less,
						},
					},
				},
			},
			{
				input:  "!(100 > event.data.value)",
				output: "event.data.value >= 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.GreaterEquals,
						},
					},
				},
			},
			{
				input:  "!(100 >= event.data.value)",
				output: "event.data.value > 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Greater,
						},
					},
				},
			},
			// RHS normalized already
			{
				input:  "!(event.data.value <= 100)",
				output: "event.data.value > 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Greater,
						},
					},
				},
			},
			{
				input:  "!(event.data.value > 100)",
				output: "event.data.value <= 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.LessEquals,
						},
					},
				},
			},
			{
				input:  "!(event.data.value >= 100)",
				output: "event.data.value < 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Less,
						},
					},
				},
			},
			// Double negate
			{
				input:  "!(!(event.data.value < 100))",
				output: "event.data.value < 100",
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  int64(100),
							Ident:    "event.data.value",
							Operator: operators.Less,
						},
					},
				},
			},
		}

		assert(t, tests)
	})

	t.Run("Queries with nested branches", func(t *testing.T) {
		tests := []parseTestInput{
			{
				input:  `a == 1 || b == 2 || c == 3`,
				output: `c == 3 || a == 1 || b == 2`,
				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							{
								Predicate: &Predicate{
									Literal:  int64(3),
									Ident:    "c",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  int64(1),
									Ident:    "a",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  int64(2),
									Ident:    "b",
									Operator: operators.Equals,
								},
							},
						},
					},
				},
			},
			{
				// This evaluates the same as `(a == 1 && b == 2) || c == 3`,
				input:  `a == 1 && b == 2 || c == 3`,
				output: `(a == 1 && b == 2) || c == 3`,
				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							{
								Ands: []*Node{
									{
										Predicate: &Predicate{
											Literal:  int64(1),
											Ident:    "a",
											Operator: operators.Equals,
										},
									},
									{
										Predicate: &Predicate{
											Literal:  int64(2),
											Ident:    "b",
											Operator: operators.Equals,
										},
									},
								},
							},
							{
								Predicate: &Predicate{
									Literal:  int64(3),
									Ident:    "c",
									Operator: operators.Equals,
								},
							},
						},
					},
				},
			},
			{
				// This evaluates the same as `a == 1 || (b == 2 && c == 3)`,
				input:  `a == 1 || b == 2 && c == 3`,
				output: `a == 1 || (b == 2 && c == 3)`,
				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							{
								Predicate: &Predicate{
									Literal:  int64(1),
									Ident:    "a",
									Operator: operators.Equals,
								},
							},
							{
								Ands: []*Node{
									{
										Predicate: &Predicate{
											Literal:  int64(2),
											Ident:    "b",
											Operator: operators.Equals,
										},
									},
									{
										Predicate: &Predicate{
											Literal:  int64(3),
											Ident:    "c",
											Operator: operators.Equals,
										},
									},
								},
							},
						},
					},
				},
			},
			{
				// And parenthesis to amend the order.
				input:  `(a == 1 || b == 2) && c == 3`,
				output: `c == 3 && (a == 1 || b == 2)`,
				expected: ParsedExpression{
					Root: Node{
						Ands: []*Node{
							{
								Predicate: &Predicate{
									Literal:  int64(3),
									Ident:    "c",
									Operator: operators.Equals,
								},
							},
						},
						Ors: []*Node{
							{
								Predicate: &Predicate{
									Literal:  int64(1),
									Ident:    "a",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  int64(2),
									Ident:    "b",
									Operator: operators.Equals,
								},
							},
						},
					},
				},
			},
			{
				// Swapping the order of the expression
				input:  `a == 1 && b == 2 && (c == 3 || d == 4)`,
				output: `a == 1 && b == 2 && (c == 3 || d == 4)`,
				expected: ParsedExpression{
					Root: Node{
						Ands: []*Node{
							{
								Predicate: &Predicate{
									Literal:  int64(1),
									Ident:    "a",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  int64(2),
									Ident:    "b",
									Operator: operators.Equals,
								},
							},
						},
						Ors: []*Node{
							{
								Predicate: &Predicate{
									Literal:  int64(3),
									Ident:    "c",
									Operator: operators.Equals,
								},
							},
							{
								Predicate: &Predicate{
									Literal:  int64(4),
									Ident:    "d",
									Operator: operators.Equals,
								},
							},
						},
					},
				},
			},
			{
				input: `
					 			a == 1
					 			&& (b == 2 && (c == 3 || d == 4))
					 			|| z == 3
					 			&& (e == 5 && (f == 6 || g == 7))
					 			|| zz == 4
					 			`,
				output: `zz == 4 || (a == 1 && b == 2 && (c == 3 || d == 4)) || (z == 3 && e == 5 && (f == 6 || g == 7))`,
				expected: ParsedExpression{
					Root: Node{
						Ors: []*Node{
							{
								Predicate: &Predicate{
									Literal:  int64(4),
									Ident:    "zz",
									Operator: operators.Equals,
								},
							},
							{
								Ands: []*Node{
									{
										Predicate: &Predicate{
											Literal:  int64(1),
											Ident:    "a",
											Operator: operators.Equals,
										},
									},
									{
										Predicate: &Predicate{
											Literal:  int64(2),
											Ident:    "b",
											Operator: operators.Equals,
										},
									},
								},
								Ors: []*Node{
									{
										Predicate: &Predicate{
											Literal:  int64(3),
											Ident:    "c",
											Operator: operators.Equals,
										},
									},
									{
										Predicate: &Predicate{
											Literal:  int64(4),
											Ident:    "d",
											Operator: operators.Equals,
										},
									},
								},
							},
							{
								Ands: []*Node{
									{
										Predicate: &Predicate{
											Literal:  int64(3),
											Ident:    "z",
											Operator: operators.Equals,
										},
									},
									{
										Predicate: &Predicate{
											Literal:  int64(5),
											Ident:    "e",
											Operator: operators.Equals,
										},
									},
								},
								Ors: []*Node{
									{
										Predicate: &Predicate{
											Literal:  int64(6),
											Ident:    "f",
											Operator: operators.Equals,
										},
									},
									{
										Predicate: &Predicate{
											Literal:  int64(7),
											Ident:    "g",
											Operator: operators.Equals,
										},
									},
								},
							},
						},
					},
				},
			},
		}

		assert(t, tests)
	})

	// TODO
	/*
		t.Run("It deduplicates expressions", func(t *testing.T) {
			tests := []parseTestInput{
				{
					input: "event == 'foo' && event == 'foo'",
					expected: ParsedExpression{
						Root: Node{
							Predicate: &Predicate{
								Literal:  "foo",
								Ident:    "event",
								Operator: operators.Equals,
							},
						},
					},
				},
			}
			assert(t, tests)
		})
	*/

}

func TestParse_LiftedVars(t *testing.T) {
	ctx := context.Background()

	cachingCelParser := NewCachingParser(newEnv(), nil)

	assert := func(t *testing.T, tests []parseTestInput) {
		t.Helper()

		for _, test := range tests {
			parser, err := NewTreeParser(cachingCelParser)
			require.NoError(t, err)
			eval := tex(test.input)
			actual, err := parser.Parse(ctx, eval)

			// Shortcut to ensure the evaluable instance matches
			if test.expected.Evaluable == nil {
				test.expected.Evaluable = eval
			}

			// Convert the lifted arg interfaces to the same map values
			actual.Vars = regularArgMap(actual.Vars.Map())

			require.NoError(t, err)
			require.NotNil(t, actual)

			require.EqualValues(t, test.output, actual.Root.String(), "String() does not match expected output")

			a, _ := json.MarshalIndent(test.expected, "", " ")
			b, _ := json.MarshalIndent(actual, "", " ")

			require.EqualValues(
				t,
				test.expected,
				*actual,
				"Invalid strucutre:\n%s\nExpected: %s\n\nGot: %s\nGroups: %d",
				test.input,
				string(a),
				string(b),
				len(actual.RootGroups()),
			)
		}
	}

	t.Run("It handles basic expressions", func(t *testing.T) {
		tests := []parseTestInput{
			{
				input:  `event == "foo"`,
				output: `event == "foo"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "foo",
							Ident:    "event",
							Operator: operators.Equals,
						},
					},
					Vars: regularArgMap{
						"a": "foo",
					},
				},
			},
			{
				input:  `event == "bar"`,
				output: `event == "bar"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "bar",
							Ident:    "event",
							Operator: operators.Equals,
						},
					},
					Vars: regularArgMap{
						"a": "bar",
					},
				},
			},
			{
				input:  `"bar" == event`,
				output: `event == "bar"`,
				expected: ParsedExpression{
					Root: Node{
						Predicate: &Predicate{
							Literal:  "bar",
							Ident:    "event",
							Operator: operators.Equals,
						},
					},
					Vars: regularArgMap{
						"a": "bar",
					},
				},
			},
		}

		assert(t, tests)

		// We should have had one hit, as `event == "bar"` and `event == "foo"`
		// were lifted into the same expression `event == vars.a`
		require.EqualValues(t, 1, cachingCelParser.(*cachingParser).Hits())
	})
}

func TestRootGroups(t *testing.T) {
	r := require.New(t)
	ctx := context.Background()
	parser, err := newParser()

	r.NoError(err)

	t.Run("With single groups", func(t *testing.T) {
		actual, err := parser.Parse(ctx, tex("a == 1"))
		r.NoError(err)
		r.Equal(1, len(actual.RootGroups()))
		r.Equal(&actual.Root, actual.RootGroups()[0])

		actual, err = parser.Parse(ctx, tex("a == 1 && b == 2"))
		r.NoError(err)
		r.Equal(1, len(actual.RootGroups()))
		r.Equal(&actual.Root, actual.RootGroups()[0])

		actual, err = parser.Parse(ctx, tex("root == 'yes' && (a == 1 || b == 2)"))
		r.NoError(err)
		r.Equal(1, len(actual.RootGroups()))
		r.Equal(&actual.Root, actual.RootGroups()[0])
	})

	t.Run("With an or", func(t *testing.T) {
		actual, err := parser.Parse(ctx, tex("a == 1 || b == 2"))
		r.NoError(err)
		r.Equal(2, len(actual.RootGroups()))

		actual, err = parser.Parse(ctx, tex("a == 1 || b == 2 || c == 3"))
		r.NoError(err)
		r.Equal(3, len(actual.RootGroups()))

		actual, err = parser.Parse(ctx, tex("a == 1 && b == 2 || c == 3"))
		r.NoError(err)
		r.Equal(2, len(actual.RootGroups()))
	})

}
