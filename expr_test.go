package expr

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/require"
)

func BenchmarkCachingEvaluate1_000(b *testing.B) {
	benchEval(1_000, NewCachingParser(newEnv(), nil), b)
}

// func BenchmarkNonCachingEvaluate1_000(b *testing.B) { benchEval(1_000, EnvParser(newEnv()), b) }
func benchEval(i int, p CELParser, b *testing.B) {
	for n := 0; n < b.N; n++ {
		parser := NewTreeParser(p)
		_ = evaluate(b, i, parser)
	}
}

func evaluate(b *testing.B, i int, parser TreeParser) error {
	b.StopTimer()
	ctx := context.Background()
	e := NewAggregateEvaluator(parser, testBoolEvaluator)

	// Insert the match we want to see.
	expected := tex(`event.data.account_id == "yes" && event.data.match == "true"`)
	_, _ = e.Add(ctx, expected)

	addOtherExpressions(i, e)

	b.StartTimer()

	results, _, _ := e.Evaluate(ctx, map[string]any{
		"event": map[string]any{
			"data": map[string]any{
				"account_id": "yes",
				"match":      "true",
			},
		},
	})

	if len(results) != 1 {
		return fmt.Errorf("unexpected number of results: %d", results)
	}
	return nil
}

func TestEvaluate_Strings(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingParser(newEnv(), nil))
	e := NewAggregateEvaluator(parser, testBoolEvaluator)

	expected := tex(`event.data.account_id == "yes" && event.data.match == "true"`)
	_, err := e.Add(ctx, expected)
	require.NoError(t, err)

	n := 100_000

	addOtherExpressions(n, e)

	require.EqualValues(t, n+1, e.Len())

	t.Run("It matches items", func(t *testing.T) {
		pre := time.Now()
		evals, matched, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"account_id": "yes",
					"match":      "true",
				},
			},
		})
		total := time.Since(pre)
		fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
		fmt.Printf("Matched in %v ms\n", total.Milliseconds())

		require.NoError(t, err)
		require.EqualValues(t, 1, matched)
		require.EqualValues(t, []Evaluable{expected}, evals)
	})

	t.Run("It handles non-matching data", func(t *testing.T) {
		pre := time.Now()
		evals, matched, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"account_id": "yes",
					"match":      "no",
				},
			},
		})
		total := time.Since(pre)
		fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
		fmt.Printf("Matched in %v ms\n", total.Milliseconds())

		require.NoError(t, err)
		require.EqualValues(t, 0, len(evals))
		require.EqualValues(t, 0, matched) // We still ran one expression
	})
}

func TestEvaluate_Concurrently(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingParser(newEnv(), nil))
	e := NewAggregateEvaluator(parser, testBoolEvaluator)

	expected := tex(`event.data.account_id == "yes" && event.data.match == "true"`)
	_, err := e.Add(ctx, expected)
	require.NoError(t, err)

	addOtherExpressions(100_000, e)

	t.Run("It matches items", func(t *testing.T) {
		wg := sync.WaitGroup{}
		for i := 0; i <= 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				evals, matched, err := e.Evaluate(ctx, map[string]any{
					"event": map[string]any{
						"data": map[string]any{
							"account_id": "yes",
							"match":      "true",
						},
					},
				})
				require.NoError(t, err)
				require.EqualValues(t, 1, matched)
				require.EqualValues(t, []Evaluable{expected}, evals)
			}()
		}
		wg.Wait()
	})

}

func TestEvaluate_ArrayIndexes(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingParser(newEnv(), nil))
	e := NewAggregateEvaluator(parser, testBoolEvaluator)

	expected := tex(`event.data.ids[1] == "id-b" && event.data.ids[2] == "id-c"`)
	_, err := e.Add(ctx, expected)
	require.NoError(t, err)

	t.Run("It doesn't return if arrays contain non-matching data", func(t *testing.T) {
		pre := time.Now()
		evals, matched, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"ids": []string{"none-match", "nope"},
				},
			},
		})
		total := time.Since(pre)
		fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
		fmt.Printf("Matched in %v ms\n", total.Milliseconds())

		require.NoError(t, err)
		require.EqualValues(t, 0, len(evals))
		require.EqualValues(t, 0, matched)
	})

	t.Run("It matches arrays", func(t *testing.T) {
		pre := time.Now()
		evals, matched, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"ids": []string{"id-a", "id-b", "id-c"},
				},
			},
		})
		total := time.Since(pre)
		fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
		fmt.Printf("Matched in %v ms\n", total.Milliseconds())

		require.NoError(t, err)
		require.EqualValues(t, 1, len(evals))
		require.EqualValues(t, 1, matched)
	})
}

func TestEvaluate_Compound(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingParser(newEnv(), nil))
	e := NewAggregateEvaluator(parser, testBoolEvaluator)

	expected := tex(`event.data.a == "ok" && event.data.b == "yes" && event.data.c == "please"`)
	ok, err := e.Add(ctx, expected)
	require.True(t, ok)
	require.NoError(t, err)

	t.Run("It matches items", func(t *testing.T) {
		evals, matched, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"a": "ok",
					"b": "yes",
					"c": "please",
				},
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, matched) // We only perform one eval
		require.EqualValues(t, []Evaluable{expected}, evals)
	})

	t.Run("It skips if less than the group length is found", func(t *testing.T) {
		evals, matched, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"a": "ok",
					"b": "yes",
					"c": "no - no match",
				},
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 0, matched)
		require.EqualValues(t, []Evaluable{}, evals)
	})

}

func TestAggregateMatch(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)
	e := NewAggregateEvaluator(parser, testBoolEvaluator)

	// Add three expressions matching on "a", "b", "c" respectively.
	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		ok, err := e.Add(ctx, tex(fmt.Sprintf(`event.data.%s == "yes"`, k)))
		require.True(t, ok)
		require.NoError(t, err)
	}

	// When passing input.data.a as "yes", we should find the match,
	// as the expression's variable (event.data.a) matches the literal ("yes").
	t.Run("It matches when the ident and literal match", func(t *testing.T) {
		input := map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"a":         "yes",
					"not-found": "no",
				},
			},
		}

		matched, err := e.AggregateMatch(ctx, input)
		require.NoError(t, err)
		require.EqualValues(t, 1, len(matched))
		require.EqualValues(t,
			`event.data.a == "yes"`,
			matched[0].Parsed.Evaluable.GetExpression(),
		)
	})

	// When passing input.data.b, we should match only one expression.
	t.Run("It doesn't match if the literal changes", func(t *testing.T) {
		input := map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"b": "no",
				},
			},
		}

		matched, err := e.AggregateMatch(ctx, input)
		require.NoError(t, err)
		require.EqualValues(t, 0, len(matched))
	})

	// When passing input.data.a, we should match only one expression.
	t.Run("It skips data with no expressions in the tree", func(t *testing.T) {
		input := map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"none": "yes",
				},
			},
		}

		matched, err := e.AggregateMatch(ctx, input)
		require.NoError(t, err)
		require.EqualValues(t, 0, len(matched))
	})
}

func TestAddRemove(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	t.Run("With a basic aggregateable expression", func(t *testing.T) {
		e := NewAggregateEvaluator(parser, testBoolEvaluator)

		firstExpr := tex(`event.data.foo == "yes"`, "first-id")

		ok, err := e.Add(ctx, firstExpr)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 1, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 1, e.AggregateableLen())

		// Add the same expression again.
		ok, err = e.Add(ctx, tex(`event.data.foo == "yes"`, "second-id"))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 2, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 2, e.AggregateableLen())

		t.Run("It removes duplicate expressions with different IDs", func(t *testing.T) {
			// Matching this expr should work before removal.
			eval, count, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{"foo": "yes"},
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 2, len(eval))
			require.EqualValues(t, 2, count)

			err = e.Remove(ctx, tex(`event.data.foo == "yes"`, "second-id"))
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, 1, e.Len())
			require.Equal(t, 0, e.ConstantLen())
			require.Equal(t, 1, e.AggregateableLen())

			// Matching this expr should now fail.
			eval, count, err = e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{"foo": "yes"},
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 1, len(eval))
			require.EqualValues(t, 1, count)
			require.EqualValues(t, firstExpr.GetID(), eval[0].GetID())
		})

		// Add a new expression
		ok, err = e.Add(ctx, tex(`event.data.another == "no"`))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 2, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 2, e.AggregateableLen())

		// Remove all expressions
		t.Run("It removes an aggregateable expression", func(t *testing.T) {
			// Matching this expr should work before removal.
			eval, count, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{"another": "no"},
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 1, len(eval))
			require.EqualValues(t, 1, count)

			err = e.Remove(ctx, tex(`event.data.another == "no"`))
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, 1, e.Len()) // The first expr is remaining.
			require.Equal(t, 0, e.ConstantLen())
			require.Equal(t, 1, e.AggregateableLen())

			// Matching this expr should now fail.
			eval, count, err = e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{"another": "no"},
				},
			})
			require.NoError(t, err)
			require.Empty(t, eval)
			require.EqualValues(t, 0, count)
		})

		// And yeet a non-existent aggregateable expr.
		err = e.Remove(ctx, tex(`event.data.another == "i'm not here"`))
		require.Error(t, ErrEvaluableNotFound, err)
		require.Equal(t, 1, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 1, e.AggregateableLen())
	})

	t.Run("With a non-aggregateable expression due to inequality/GTE on strings", func(t *testing.T) {
		e := NewAggregateEvaluator(parser, testBoolEvaluator)

		ok, err := e.Add(ctx, tex(`event.data.foo != "no"`))
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, 1, e.Len())
		require.Equal(t, 1, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())

		// Add the same expression again.
		ok, err = e.Add(ctx, tex(`event.data.foo >= "no"`))
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, 2, e.Len())
		require.Equal(t, 2, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())

		// Add a new expression
		ok, err = e.Add(ctx, tex(`event.data.another < "no"`))
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, 3, e.Len())
		require.Equal(t, 3, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())

		// And remove.
		err = e.Remove(ctx, tex(`event.data.another < "no"`))
		require.NoError(t, err)
		require.Equal(t, 2, e.Len())
		require.Equal(t, 2, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())

		// And yeet out another non-existent expression
		err = e.Remove(ctx, tex(`event.data.another != "i'm not here" && a != "b"`))
		require.Error(t, ErrEvaluableNotFound, err)
		require.Equal(t, 2, e.Len())
		require.Equal(t, 2, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())
	})
}

func TestEmptyExpressions(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	e := NewAggregateEvaluator(parser, testBoolEvaluator)

	empty := tex(``, "id-1")

	t.Run("Adding an empty expression succeeds", func(t *testing.T) {
		ok, err := e.Add(ctx, empty)
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, 1, e.Len())
		require.Equal(t, 1, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())
	})

	t.Run("Empty expressions always match", func(t *testing.T) {
		// Matching this expr should now fail.
		eval, count, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{"any": true},
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
		require.EqualValues(t, 1, len(eval))
		require.EqualValues(t, empty, eval[0])
	})

	t.Run("Removing an empty expression succeeds", func(t *testing.T) {
		err := e.Remove(ctx, empty)
		require.NoError(t, err)
		require.Equal(t, 0, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())
	})
}

func TestEvaluate_Null(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	e := NewAggregateEvaluator(parser, testBoolEvaluator)
	notNull := tex(`event.ts != null`, "id-1")
	isNull := tex(`event.ts == null`, "id-2")

	t.Run("Adding a `null` check succeeds and is aggregateable", func(t *testing.T) {
		ok, err := e.Add(ctx, notNull)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = e.Add(ctx, isNull)
		require.NoError(t, err)
		require.True(t, ok)

		require.Equal(t, 2, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 2, e.AggregateableLen())
	})

	t.Run("Not null checks succeed", func(t *testing.T) {
		// Matching this expr should now fail.
		eval, count, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"ts": time.Now().UnixMilli(),
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, len(eval))
		require.EqualValues(t, 1, count)
		require.EqualValues(t, notNull, eval[0])
	})

	t.Run("Is null checks succeed", func(t *testing.T) {
		// Matching this expr should work, as "ts" is nil
		eval, count, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"ts": nil,
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, len(eval))
		require.EqualValues(t, 1, count)
		require.EqualValues(t, isNull, eval[0])
	})

	t.Run("It removes null checks", func(t *testing.T) {
		err := e.Remove(ctx, notNull)
		require.NoError(t, err)

		require.Equal(t, 1, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 1, e.AggregateableLen())

		// We should still match on `isNull`
		t.Run("Is null checks succeed", func(t *testing.T) {
			// Matching this expr should work, as "ts" is nil
			eval, count, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"ts": nil,
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 1, len(eval))
			require.EqualValues(t, 1, count)
			require.EqualValues(t, isNull, eval[0])
		})

		err = e.Remove(ctx, isNull)
		require.NoError(t, err)
		require.Equal(t, 0, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())

		// We should no longer match on `isNull`
		t.Run("Is null checks succeed", func(t *testing.T) {
			// Matching this expr should work, as "ts" is nil
			eval, count, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"ts": nil,
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 0, len(eval))
			require.EqualValues(t, 0, count)
		})
	})

	t.Run("Two idents aren't treated as nulls", func(t *testing.T) {
		e := NewAggregateEvaluator(parser, testBoolEvaluator)
		idents := tex("event.data.a == event.data.b")
		ok, err := e.Add(ctx, idents)
		require.NoError(t, err)
		require.False(t, ok)

		require.Equal(t, 1, e.Len())
		require.Equal(t, 1, e.ConstantLen())
		require.Equal(t, 0, e.AggregateableLen())

		eval, count, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"a": 1,
					"b": 1,
				},
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, len(eval))
		require.EqualValues(t, 1, count)
	})
}

// tex represents a test Evaluable expression
func tex(expr string, ids ...string) Evaluable {
	return testEvaluable{
		expr: expr,
		id:   strings.Join(ids, ","),
	}
}

type testEvaluable struct {
	expr string
	id   string
}

func (e testEvaluable) GetExpression() string { return e.expr }
func (e testEvaluable) GetID() string         { return e.expr + e.id }

func testBoolEvaluator(ctx context.Context, e Evaluable, input map[string]any) (bool, error) {
	env, _ := cel.NewEnv(
		cel.Variable("event", cel.AnyType),
		cel.Variable("async", cel.AnyType),
	)
	ast, _ := env.Parse(e.GetExpression())

	// Create the program, refusing to short circuit if a match is found.
	//
	// This will add all functions from functions.StandardOverloads as we
	// created the environment with our custom library.
	program, err := env.Program(
		ast,
		cel.EvalOptions(cel.OptExhaustiveEval, cel.OptTrackState, cel.OptPartialEval), // Exhaustive, always, right now.
	)
	if err != nil {
		return false, err
	}
	result, _, err := program.Eval(input)
	if result == nil {
		return false, nil
	}
	if types.IsUnknown(result) {
		// When evaluating to a strict result this should never happen.  We inject a decorator
		// to handle unknowns as values similar to null, and should always get a value.
		return false, nil
	}
	if types.IsError(result) {
		return false, fmt.Errorf("invalid type comparison: %v", result)
	}
	if err != nil {
		// This shouldn't be handled, as we should get an Error type in result above.
		return false, fmt.Errorf("error evaluating expression: %w", err)
	}
	return result.Value().(bool), nil
}

func addOtherExpressions(n int, e AggregateEvaluator) {
	ctx := context.Background()
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		//nolint:all
		go func() {
			defer wg.Done()
			byt := make([]byte, 8)
			_, err := rand.Read(byt)
			if err != nil {
				panic(err)
			}
			str := hex.EncodeToString(byt)

			_, err = e.Add(ctx, tex(fmt.Sprintf(`event.data.account_id == "%s"`, str)))
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
}
