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
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func BenchmarkCachingEvaluate1_000(b *testing.B) {
	benchEval(1_000, NewCachingCompiler(newEnv(), nil), b)
}

// func BenchmarkNonCachingEvaluate1_000(b *testing.B) { benchEval(1_000, EnvParser(newEnv()), b) }
func benchEval(i int, p CELCompiler, b *testing.B) {
	for n := 0; n < b.N; n++ {
		parser := NewTreeParser(p)
		_ = evaluate(b, i, parser)
	}
}

func newEvalLoader() *evalLoader {
	return &evalLoader{
		l: &sync.Mutex{},
		d: map[uuid.UUID]Evaluable{},
	}
}

type evalLoader struct {
	l *sync.Mutex
	d map[uuid.UUID]Evaluable
}

func (e *evalLoader) Load(ctx context.Context, evalID ...uuid.UUID) ([]Evaluable, error) {
	e.l.Lock()
	defer e.l.Unlock()
	evals := []Evaluable{}
	for _, id := range evalID {
		eval, ok := e.d[id]
		if !ok {
			continue
		}
		evals = append(evals, eval)
	}
	return evals, nil
}

func (e *evalLoader) AddEval(eval Evaluable) Evaluable {
	e.l.Lock()
	defer e.l.Unlock()
	e.d[eval.GetID()] = eval
	return eval
}

func (e *evalLoader) AddStr(expr string) Evaluable {
	e.l.Lock()
	defer e.l.Unlock()
	eval := tex(expr)
	e.d[eval.GetID()] = eval
	return eval
}

func evaluate(b *testing.B, i int, parser TreeParser) error {
	b.StopTimer()
	ctx := context.Background()

	expected := tex(`event.data.account_id == "yes" && event.data.match == "true"`)
	loader := newEvalLoader()
	loader.AddEval(expected)

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)
	_, _ = e.Add(ctx, expected)

	addOtherExpressions(i, e, loader)

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

func TestAdd(t *testing.T) {
	ctx := context.Background()

	parser := NewTreeParser(NewCachingCompiler(newEnv(), nil))
	loader := newEvalLoader()

	expr := tex(`event.data == {"a":1}`)
	loader.AddEval(expr)

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)
	_, err := e.Add(ctx, expr)

	require.NoError(t, err)
	require.Equal(t, 1, e.SlowLen())

}

func TestEvaluate_Strings(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingCompiler(newEnv(), nil))

	expected := tex(`event.data.account_id == "yes" && event.data.match == "true"`)
	loader := newEvalLoader()
	loader.AddEval(expected)

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

	_, err := e.Add(ctx, expected)
	require.NoError(t, err)

	n := 100_000

	addOtherExpressions(n, e, loader)

	require.EqualValues(t, n+1, e.Len())
	// These should all be fast matches.
	require.EqualValues(t, n+1, e.FastLen())
	require.EqualValues(t, 0, e.MixedLen())
	require.EqualValues(t, 0, e.SlowLen())

	t.Run("It matches items", func(t *testing.T) {
		pre := time.Now()
		evals, executed, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"account_id": "yes",
					"match":      "true",
				},
			},
		})
		total := time.Since(pre)
		fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
		fmt.Printf("Matched in %v ms (%d)\n", total.Milliseconds(), executed)

		require.NoError(t, err)
		require.EqualValues(t, []Evaluable{expected}, evals)
		// We may match more than 1 as the string matcher engine
		// returns false positives
		require.Equal(t, executed, int32(1))
	})

	t.Run("It handles non-matching data", func(t *testing.T) {
		pre := time.Now()
		evals, executed, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"account_id": "yes",
					"match":      "no",
				},
			},
		})
		total := time.Since(pre)
		fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
		fmt.Printf("Matched in %v ms (%d)\n", total.Milliseconds(), executed)

		require.NoError(t, err)
		require.EqualValues(t, 0, len(evals))
		require.EqualValues(t, 0, executed)
	})
}

func TestEvaluate_Strings_Inequality(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingCompiler(newEnv(), nil))

	expected := tex(`event.data.account_id == "yes" && event.data.neq != "neq"`)
	loader := newEvalLoader()
	loader.AddEval(expected)

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

	_, err := e.Add(ctx, expected)
	require.NoError(t, err)

	n := 100_000

	addOtherExpressions(n, e, loader)

	require.EqualValues(t, n+1, e.Len())

	t.Run("It matches items", func(t *testing.T) {
		pre := time.Now()
		evals, matched, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"account_id": "yes",
					"match":      "true",
					"neq":        "nah",
				},
			},
		})
		total := time.Since(pre)
		fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
		fmt.Printf("Matched in %v ms (%d)\n", total.Milliseconds(), matched)

		require.NoError(t, err)
		require.EqualValues(t, 1, len(evals))
		require.EqualValues(t, []Evaluable{expected}, evals)
		// We may match more than 1 as the string matcher engine
		// returns false positives
		require.GreaterOrEqual(t, matched, int32(1))
	})

	t.Run("It handles non-matching data", func(t *testing.T) {
		pre := time.Now()
		evals, matched, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"account_id": "yes",
					"match":      "no",
					"neq":        "nah",
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

func TestEvaluate_Numbers(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingCompiler(newEnv(), nil))

	t.Run("With annoying floats", func(t *testing.T) {
		// This is the expected epression
		expected := tex(`4.797009e+06 == event.data.id && (event.data.ts == null || event.data.ts > 1715211850340)`)
		// expected := tex(`event.data.id == 25`)
		loader := newEvalLoader()
		loader.AddEval(expected)

		e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

		_, err := e.Add(ctx, expected)
		require.NoError(t, err)

		n := 1

		addOtherExpressions(n, e, loader)

		require.EqualValues(t, n+1, e.Len())

		t.Run("It matches items", func(t *testing.T) {
			pre := time.Now()
			evals, matched, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"id": 4797009,
						"ts": 2015211850340,
					},
				},
			})
			total := time.Since(pre)
			fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
			fmt.Printf("Matched in %v ms (%d)\n", total.Milliseconds(), matched)

			require.NoError(t, err)
			require.EqualValues(t, []Evaluable{expected}, evals)

			// Assert that we only evaluate one expression.
			require.Equal(t, matched, int32(1))
		})

		t.Run("It handles non-matching data", func(t *testing.T) {
			pre := time.Now()
			evals, matched, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"account_id": "yes",
						"ts":         "???",
						"match":      "no",
					},
				},
			})
			total := time.Since(pre)
			fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
			fmt.Printf("Matched in %v ms\n", total.Milliseconds())

			require.NoError(t, err)
			require.EqualValues(t, 0, len(evals))
			// require.EqualValues(t, 0, matched) // We still ran one expression
			_ = matched
		})
	})

	t.Run("With floats", func(t *testing.T) {

		// This is the expected epression
		expected := tex(`326909.0 == event.data.account_id && (event.data.ts == null || event.data.ts > 1714000000000)`)
		// expected := tex(`event.data.id == 25`)
		loader := newEvalLoader()
		loader.AddEval(expected)

		e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

		_, err := e.Add(ctx, expected)
		require.NoError(t, err)

		n := 1

		addOtherExpressions(n, e, loader)

		require.EqualValues(t, n+1, e.Len())

		t.Run("It matches items", func(t *testing.T) {
			pre := time.Now()
			evals, matched, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"account_id": 326909,
						"ts":         1714000000001,
					},
				},
			})
			total := time.Since(pre)
			fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
			fmt.Printf("Matched in %v ms (%d)\n", total.Milliseconds(), matched)

			require.NoError(t, err)
			require.EqualValues(t, []Evaluable{expected}, evals)

			// Assert that we only evaluate one expression.
			require.Equal(t, matched, int32(1))
		})

		t.Run("It handles non-matching data", func(t *testing.T) {
			pre := time.Now()
			evals, matched, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"account_id": "yes",
						"ts":         "???",
						"match":      "no",
					},
				},
			})
			total := time.Since(pre)
			fmt.Printf("Matched in %v ns\n", total.Nanoseconds())
			fmt.Printf("Matched in %v ms\n", total.Milliseconds())

			require.NoError(t, err)
			require.EqualValues(t, 0, len(evals))
			// require.EqualValues(t, 0, matched) // We still ran one expression
			_ = matched
		})
	})
}

func TestEvaluate_Concurrently(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingCompiler(newEnv(), nil))

	expected := tex(`event.data.account_id == "yes" && event.data.match == "true"`)
	loader := newEvalLoader()
	loader.AddEval(expected)

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

	_, err := e.Add(ctx, expected)
	require.NoError(t, err)

	addOtherExpressions(1_000, e, loader)

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
	parser := NewTreeParser(NewCachingCompiler(newEnv(), nil))

	expected := tex(`event.data.ids[1] == "id-b" && event.data.ids[2] == "id-c"`)
	loader := newEvalLoader()
	loader.AddEval(expected)

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

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
	parser := NewTreeParser(NewCachingCompiler(newEnv(), nil))

	expected := tex(`event.data.a == "ok" && event.data.b == "yes" && event.data.c == "please"`)
	loader := newEvalLoader()
	loader.AddEval(expected)

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

	ok, err := e.Add(ctx, expected)
	require.Greater(t, ok, float64(0))
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

	// Note: we do not use group IDs for optimization right now.
	//
	// t.Run("It skips if less than the group length is found", func(t *testing.T) {
	// 	evals, matched, err := e.Evaluate(ctx, map[string]any{
	// 		"event": map[string]any{
	// 			"data": map[string]any{
	// 				"a": "ok",
	// 				"b": "yes",
	// 				"c": "no - no match",
	// 			},
	// 		},
	// 	})
	// 	require.NoError(t, err)
	// 	require.EqualValues(t, 0, matched)
	// 	require.EqualValues(t, []Evaluable{}, evals)
	// })

}

func TestAggregateMatch(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	loader := newEvalLoader()

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

	// Add three expressions matching on "a", "b", "c" respectively.
	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		eval := tex(fmt.Sprintf(`event.data.%s == "yes"`, k))
		loader.AddEval(eval)
		ok, err := e.Add(ctx, eval)
		require.Greater(t, ok, float64(0))
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
		// False positives increase matches.
		// require.EqualValues(t, 1, len(matched))
		found := false
		for _, item := range matched {
			eval, err := loader.Load(ctx, item.Parsed.EvaluableID)
			require.Nil(t, err)
			require.EqualValues(t, 1, len(eval))

			if eval[0].GetExpression() == `event.data.a == "yes"` {
				found = true
				break
			}
		}
		require.True(t, found)
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

func TestMacros(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	loader := newEvalLoader()
	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)
	eval := tex(`event.data.ok == "true" || event.data.ids.exists(id, id == 'c')`)
	loader.AddEval(eval)
	ok, err := e.Add(ctx, eval)
	require.NoError(t, err)
	require.Equal(t, ok, float64(-1)) // Not supported.

	t.Run("It doesn't evaluate macros", func(t *testing.T) {

		input := map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"ok":  nil,
					"ids": []string{"a", "b", "c"},
				},
			},
		}
		evals, matched, err := e.Evaluate(ctx, input)
		require.NoError(t, err)
		require.EqualValues(t, 1, len(evals))
		require.EqualValues(t, 1, matched)

		t.Run("Failing match", func(t *testing.T) {
			input = map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"ok":  nil,
						"ids": []string{"nope"},
					},
				},
			}
			evals, matched, err = e.Evaluate(ctx, input)
			require.NoError(t, err)
			require.EqualValues(t, 0, len(evals))
			require.EqualValues(t, 1, matched)
		})

		t.Run("Partial macro", func(t *testing.T) {
			input = map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"ok":  "true",
						"ids": []string{"nope"},
					},
				},
			}
			evals, matched, err = e.Evaluate(ctx, input)
			require.NoError(t, err)
			require.EqualValues(t, 1, len(evals), evals)
			require.EqualValues(t, 1, matched)
		})
	})
}

func TestAddRemove(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	loader := newEvalLoader()

	t.Run("With a basic aggregateable expression", func(t *testing.T) {
		e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

		firstExpr := tex(`event.data.foo == "yes"`, "first-id")
		loader.AddEval(firstExpr)

		ok, err := e.Add(ctx, firstExpr)
		require.NoError(t, err)
		require.Greater(t, ok, float64(0))
		require.Equal(t, 1, e.Len())
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 1, e.FastLen())

		// Add the same expression again.
		ok, err = e.Add(ctx, loader.AddEval(tex(`event.data.foo == "yes"`, "second-id")))
		require.NoError(t, err)
		require.Greater(t, ok, float64(0))
		require.Equal(t, 2, e.Len())
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 2, e.FastLen())

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
			fmt.Println("DONE")

			err = e.Remove(ctx, tex(`event.data.foo == "yes"`, "second-id"))
			fmt.Println("REMO")
			require.NoError(t, err)
			require.Greater(t, ok, float64(0))
			fmt.Println("DONE")

			require.Equal(t, 1, e.Len())
			require.Equal(t, 0, e.SlowLen())
			require.Equal(t, 1, e.FastLen())

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
			fmt.Println("DONE")
		})

		// Add a new expression
		ok, err = e.Add(ctx, loader.AddEval(tex(`event.data.another == "no"`)))
		require.NoError(t, err)
		require.Greater(t, ok, float64(0))

		require.Equal(t, 2, e.Len())
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 2, e.FastLen())

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
			require.Greater(t, ok, float64(0))

			require.Equal(t, 1, e.Len()) // The first expr is remaining.
			require.Equal(t, 0, e.SlowLen())
			require.Equal(t, 1, e.FastLen())

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
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 1, e.FastLen())
	})

	t.Run("With a non-aggregateable expression due to inequality/GTE on strings", func(t *testing.T) {
		e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

		ok, err := e.Add(ctx, loader.AddEval(tex(`event.data.foo != "no"`)))
		require.NoError(t, err)
		require.Equal(t, ok, float64(0))
		require.Equal(t, 1, e.Len())
		require.Equal(t, 1, e.SlowLen())
		require.Equal(t, 0, e.FastLen())
		require.Equal(t, 0, e.MixedLen())

		// Add the same expression again.
		ok, err = e.Add(ctx, loader.AddEval(tex(`event.data.foo >= "no"`)))
		require.NoError(t, err)
		require.Equal(t, ok, float64(0))
		require.Equal(t, 2, e.Len())
		require.Equal(t, 2, e.SlowLen())
		require.Equal(t, 0, e.FastLen())

		// Add a new expression
		ok, err = e.Add(ctx, loader.AddEval(tex(`event.data.another < "no"`)))
		require.NoError(t, err)
		require.Equal(t, ok, float64(0))
		require.Equal(t, 3, e.Len())
		require.Equal(t, 3, e.SlowLen())
		require.Equal(t, 0, e.FastLen())

		// And remove.
		err = e.Remove(ctx, loader.AddEval(tex(`event.data.another < "no"`)))
		require.NoError(t, err)
		require.Equal(t, 2, e.SlowLen())
		require.Equal(t, 2, e.Len())
		require.Equal(t, 0, e.FastLen())

		// And yeet out another non-existent expression
		err = e.Remove(ctx, loader.AddEval(tex(`event.data.another != "i'm not here" && a != "b"`)))
		require.Error(t, ErrEvaluableNotFound, err)
		require.Equal(t, 2, e.Len())
		require.Equal(t, 2, e.SlowLen())
		require.Equal(t, 0, e.FastLen())
	})

	t.Run("Partial aggregates", func(t *testing.T) {

		e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)
		ok, err := e.Add(ctx, loader.AddEval(tex(`event.data.foo == "yea" && event.data.bar != "baz"`)))
		require.NoError(t, err)
		// now fully aggregated
		require.Equal(t, ok, float64(1))
		require.Equal(t, 1, e.Len())
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 1, e.FastLen())
		require.Equal(t, 0, e.MixedLen())

		// Matching this expr should now fail.
		eval, count, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"foo": "yea",
					"bar": "lol",
				},
			},
		})

		require.EqualValues(t, 1, count)
		require.EqualValues(t, 1, len(eval))
		require.NoError(t, err)

		// Matching this expr should now fail.
		eval, count, err = e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"foo": "yea",
					"bar": "baz",
				},
			},
		})

		require.EqualValues(t, 0, count)
		require.EqualValues(t, 0, len(eval))
		require.NoError(t, err)
	})
}

func TestEmptyExpressions(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	loader := newEvalLoader()

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)

	empty := loader.AddEval(tex(``, "id-1"))

	t.Run("Adding an empty expression succeeds", func(t *testing.T) {
		ok, err := e.Add(ctx, empty)
		require.NoError(t, err)
		require.Equal(t, ok, float64(-1)) // TODO Check this failing case
		require.Equal(t, 1, e.Len())
		require.Equal(t, 1, e.SlowLen())
		require.Equal(t, 0, e.FastLen())
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
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 0, e.FastLen())
	})
}

func TestEvaluate_Null(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	loader := newEvalLoader()

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)
	notNull := loader.AddEval(tex(`event.ts != null`, "id-1"))
	isNull := loader.AddEval(tex(`event.ts == null`, "id-2"))

	t.Run("Adding a `null` check succeeds and is aggregateable", func(t *testing.T) {
		ok, err := e.Add(ctx, notNull)
		require.NoError(t, err)
		require.Greater(t, ok, float64(0))

		ok, err = e.Add(ctx, isNull)
		require.NoError(t, err)
		require.Greater(t, ok, float64(0))

		require.Equal(t, 2, e.Len())
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 2, e.FastLen())
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
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 1, e.FastLen())

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
		require.Equal(t, 0, e.SlowLen())
		require.Equal(t, 0, e.FastLen())

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
		e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0)
		idents := loader.AddEval(tex("event.data.a == event.data.b"))
		ok, err := e.Add(ctx, idents)
		require.NoError(t, err)
		require.Equal(t, ok, float64(0))

		require.Equal(t, 1, e.Len())
		require.Equal(t, 1, e.SlowLen())
		require.Equal(t, 0, e.FastLen())

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

func TestMixedEngines(t *testing.T) {
	ctx := context.Background()
	parser := NewTreeParser(NewCachingCompiler(newEnv(), nil))

	loader := newEvalLoader()

	e := NewAggregateEvaluator(parser, testBoolEvaluator, loader.Load, 0).(*aggregator)

	t.Run("Assert mixed engines", func(t *testing.T) {
		exprs := []string{
			// each id has 1, 2, 3 as a TS
			`event.data.id == "a" && (event.ts == null || event.ts > 1)`,
			`event.data.id == "a" && (event.ts == null || event.ts > 2)`,
			`event.data.id == "a" && (event.ts == null || event.ts > 3)`,

			`event.data.id == "b" && (event.ts == null || event.ts > 1)`,
			`event.data.id == "b" && (event.ts == null || event.ts > 2)`,
			`event.data.id == "b" && (event.ts == null || event.ts > 3)`,

			`event.data.id == "c" && (event.ts == null || event.ts > 1)`,
			`event.data.id == "c" && (event.ts == null || event.ts > 2)`,
			`event.data.id == "c" && (event.ts == null || event.ts > 3)`,
		}

		for n, expr := range exprs {
			eval := tex(expr, fmt.Sprintf("id-%d", n))
			loader.AddEval(eval)
			ratio, err := e.Add(ctx, eval)
			require.NoError(t, err)
			require.EqualValues(t, 1.0, ratio)
		}

		t.Run("Success matches", func(t *testing.T) {
			// Should match no expressions - event.ts <= 1, 2, and 3.
			eval, count, err := e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"id": "a",
					},
					"ts": 1,
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 0, len(eval))
			require.EqualValues(t, 0, count)

			// Should match just the first "a" expression
			eval, count, err = e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"id": "a",
					},
					"ts": 2,
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 1, len(eval))
			require.EqualValues(t, 1, count)
			require.Equal(t, `event.data.id == "a" && (event.ts == null || event.ts > 1)`, eval[0].GetExpression())

			// Should match the first and second "a" expr.
			eval, count, err = e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"id": "a",
					},
					"ts": 3,
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 2, len(eval))
			require.EqualValues(t, 2, count)

			// Null matches
			eval, count, err = e.Evaluate(ctx, map[string]any{
				"event": map[string]any{
					"data": map[string]any{
						"id": "a",
					},
					"ts": nil,
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, 3, len(eval))
			require.EqualValues(t, 3, count)
		})
	})

	t.Run("Fail matches", func(t *testing.T) {
		eval, count, err := e.Evaluate(ctx, map[string]any{
			"event": map[string]any{
				"data": map[string]any{
					"id": "z",
				},
				"ts": 5,
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 0, len(eval))
		require.EqualValues(t, 0, count)
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
func (e testEvaluable) GetID() uuid.UUID {
	// deterministic IDs based off of expressions in testing.
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(e.expr+e.id))
}

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

func addOtherExpressions(n int, e AggregateEvaluator, loader *evalLoader) {

	r := rand.New(rand.NewSource(123))
	var l sync.Mutex

	ctx := context.Background()
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		//nolint:all
		wg.Add(1)
		go func() {
			defer wg.Done()
			byt := make([]byte, 8)
			l.Lock()
			_, err := r.Read(byt)
			l.Unlock()
			if err != nil {
				panic(err)
			}
			str := hex.EncodeToString(byt)

			expr := tex(fmt.Sprintf(`event.data.account_id == "%s" && event.data.neq != "neq"`, str))
			loader.AddEval(expr)
			_, err = e.Add(ctx, expr)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
}
