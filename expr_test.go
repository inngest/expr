package expr

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/require"
)

// tex represents a test Evaluable expression
type tex string

func (e tex) Expression() string { return string(e) }

func TestEvaluate(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)
	e := NewAggregateEvaluator(parser, testBoolEvaluator)

	expected := tex(`event.data.account_id == "yes" && event.data.match == "true"`)

	_, err = e.Add(ctx, expected)
	require.NoError(t, err)

	// Insert 100k random matches.
	for i := 0; i < 100_000; i++ {
		byt := make([]byte, 8)
		_, err := rand.Read(byt)
		require.NoError(t, err)
		str := hex.EncodeToString(byt)

		_, err = e.Add(ctx, tex(fmt.Sprintf(`event.data.account_id == "%s"`, str)))
		require.NoError(t, err)
	}

	require.EqualValues(t, 100_001, e.Len())

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
		total := time.Now().Sub(pre)
		fmt.Printf("Matched in %v ns\n", total.Nanoseconds())

		require.NoError(t, err)
		require.EqualValues(t, 1, matched)
		require.EqualValues(t, []Evaluable{expected}, evals)
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
			matched[0].Evaluable.Expression(),
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

func TestAdd(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	t.Run("With a basic aggregateable expression", func(t *testing.T) {
		e := NewAggregateEvaluator(parser, testBoolEvaluator)

		ok, err := e.Add(ctx, tex(`event.data.foo == "yes"`))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 1, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 1, e.AggregateableLen())

		// Add the same expression again.
		ok, err = e.Add(ctx, tex(`event.data.foo == "yes"`))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 2, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 2, e.AggregateableLen())

		// Add a new expression
		ok, err = e.Add(ctx, tex(`event.data.another == "no"`))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 3, e.Len())
		require.Equal(t, 0, e.ConstantLen())
		require.Equal(t, 3, e.AggregateableLen())
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
	})
}

func testBoolEvaluator(ctx context.Context, e Evaluable, input map[string]any) (bool, error) {
	env, _ := cel.NewEnv(
		cel.Variable("event", cel.AnyType),
		cel.Variable("async", cel.AnyType),
	)
	ast, _ := env.Parse(e.Expression())

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
