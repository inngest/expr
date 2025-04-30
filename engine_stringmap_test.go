package expr

import (
	"context"
	"testing"

	"github.com/google/cel-go/common/operators"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEngineStringmap(t *testing.T) {
	ctx := context.Background()
	s := newStringEqualityMatcher(testConcurrency).(*stringLookup)

	gid := newGroupID(4, 2) // optimized to 2 == matches.
	exp := &ParsedExpression{
		EvaluableID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("eq-neq")),
	}
	// a, c, and d belong to the same expression 'eq-neq':
	// "async.data.id == '123' && async.data.another == '456' && asnc.data.neq != 'neq-1'"
	a := ExpressionPart{
		Parsed:  exp,
		GroupID: gid,
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  "123",
			Operator: operators.Equals,
		},
	}
	c := ExpressionPart{
		Parsed:  exp,
		GroupID: gid,
		Predicate: &Predicate{
			Ident:    "async.data.another",
			Literal:  "456",
			Operator: operators.Equals,
		},
	}
	// Test inequality
	d := ExpressionPart{
		Parsed:  exp,
		GroupID: gid,
		Predicate: &Predicate{
			Ident:    "async.data.neq",
			Literal:  "neq-1",
			Operator: operators.NotEquals,
		},
	}

	// b is a new expression.
	b := ExpressionPart{
		Parsed:  &ParsedExpression{EvaluableID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("eq-single"))},
		GroupID: newGroupID(1, 0), // This belongs to a "different" expression, but is the same pred.
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  "123",
			Operator: operators.Equals,
		},
	}

	// e is a new expression.
	e := ExpressionPart{
		Parsed:  &ParsedExpression{EvaluableID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("neq-single"))},
		GroupID: newGroupID(1, 0), // This belongs to a "different" expression, but is the same pred.
		Predicate: &Predicate{
			Ident:    "async.data.neq",
			Literal:  "neq-2",
			Operator: operators.NotEquals,
		},
	}

	// Adding expressions works
	var err error

	err = s.Add(ctx, a)
	require.NoError(t, err)

	t.Run("Adding the same string twice", func(t *testing.T) {
		err = s.Add(ctx, b)
		require.NoError(t, err)
		require.Equal(t, 2, len(s.equality[s.hash("123")]))
	})

	// A different expression
	err = s.Add(ctx, c)
	require.NoError(t, err)

	t.Run("It searches strings", func(t *testing.T) {
		result := NewMatchResult()
		s.Search(ctx, "async.data.id", "123", result)
		require.Equal(t, 2, result.Len())

		// As this only has two results, this should be easy. Contain just A and B.
		require.Contains(t, result.Result, a.Parsed.EvaluableID)
		require.Contains(t, result.Result, b.Parsed.EvaluableID)

		t.Run("It handles variable names", func(t *testing.T) {
			result = NewMatchResult()
			s.Search(ctx, "this doesn't matter", "123", result)
			require.Equal(t, 0, result.Len())
		})

		result = NewMatchResult()
		s.Search(ctx, "async.data.another", "456", result)
		require.Equal(t, 1, result.Len())
	})

	// Inequality
	err = s.Add(ctx, d)
	require.NoError(t, err)
	err = s.Add(ctx, e)
	require.NoError(t, err)

	t.Run("inequality", func(t *testing.T) {
		t.Run("first case: neq-1", func(t *testing.T) {
			result := NewMatchResult()
			err := s.Match(ctx, map[string]any{
				"async": map[string]any{
					"data": map[string]any{"neq": "neq-1"},
				},
			}, result)
			require.NoError(t, err)
			require.Equal(t, 1, result.Len())
			require.Contains(t, result.Result, e.Parsed.EvaluableID)
		})

		t.Run("second case: neq-1", func(t *testing.T) {
			result := NewMatchResult()
			err := s.Match(ctx, map[string]any{
				"async": map[string]any{
					"data": map[string]any{"neq": "neq-2"},
				},
			}, result)
			require.NoError(t, err)
			require.Equal(t, 1, result.Len())
			require.Contains(t, result.Result, d.Parsed.EvaluableID)
		})

		t.Run("third case: both", func(t *testing.T) {
			result := NewMatchResult()
			err := s.Match(ctx, map[string]any{
				"async": map[string]any{
					"data": map[string]any{"neq": "both"},
				},
			}, result)
			require.NoError(t, err)
			require.Equal(t, 2, result.Len())
		})
	})

	t.Run("It matches data, including neq", func(t *testing.T) {
		result := NewMatchResult()
		err := s.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"id":  "123", // A and B
					"neq": "lol", // D
				},
			},
		}, result)
		require.NoError(t, err)

		// This matches all 3 expressions, but the compound expression a+c+d will not have
		// enough group IDs to match.
		require.Equal(t, 3, result.Len())
	})

	t.Run("It matches data with null neq", func(t *testing.T) {
		result := NewMatchResult()
		err := s.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"id": "123",
					// by not including neq, we ensure we test against null matches.
				},
			},
		}, result)
		require.NoError(t, err)

		// This matches all 3 expressions, but the compound expression a+c+d will not have
		// enough group IDs to match.
		require.Equal(t, 3, result.Len())
	})

	t.Run("It matches data with expression optimizations in group ID", func(t *testing.T) {
		result := NewMatchResult()
		err := s.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"id":      "123",
					"another": "456",
					"neq":     "lol",
				},
			},
		}, result)
		require.NoError(t, err)

		// We only have 3 expressions, but all should have each group ID matching.
		require.Equal(t, 3, result.Len())
		require.Equal(t, 3, result.Result[exp.EvaluableID][gid]) //  3 parts to this expr
		require.Equal(t, 1, result.Result[e.Parsed.EvaluableID][e.GroupID])
		require.Equal(t, 1, result.Result[b.Parsed.EvaluableID][b.GroupID])
	})
}

func TestEngineStringmap_DuplicateValues(t *testing.T) {
	ctx := context.Background()
	s := newStringEqualityMatcher(testConcurrency).(*stringLookup)
	a := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.var_a",
			Literal:  "123",
			Operator: operators.Equals,
		},
	}
	b := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.var_b",
			Literal:  "123",
			Operator: operators.Equals,
		},
	}
	err := s.Add(ctx, a)
	require.NoError(t, err)
	err = s.Add(ctx, b)
	require.NoError(t, err)

	// It only matches var B
	result := NewMatchResult()
	s.Search(ctx, "async.data.var_b", "123", result)
	require.Equal(t, 1, result.Len())
}

func TestEngineStringmap_DuplicateNeq(t *testing.T) {
	ctx := context.Background()
	s := newStringEqualityMatcher(testConcurrency).(*stringLookup)
	a := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.var_a",
			Literal:  "a",
			Operator: operators.Equals,
		},
		Parsed: &ParsedExpression{EvaluableID: uuid.New()},
	}
	b := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.var_b",
			Literal:  "b",
			Operator: operators.Equals,
		},
		Parsed: &ParsedExpression{EvaluableID: uuid.New()},
	}
	c := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.var_c",
			Literal:  "123",
			Operator: operators.NotEquals,
		},
		Parsed: &ParsedExpression{EvaluableID: uuid.New()},
	}
	err := s.Add(ctx, a)
	require.NoError(t, err)
	err = s.Add(ctx, b)
	require.NoError(t, err)
	err = s.Add(ctx, c)
	require.NoError(t, err)

	result := NewMatchResult()
	err = s.Match(ctx, map[string]any{
		"async": map[string]any{
			"data": map[string]any{
				"var_a": "a",
				"var_b": "nah",
			},
		},
	}, result)

	require.NoError(t, err)
	require.Equal(t, 2, result.Len())

	// Never matches B, as B isn't complete.
	require.NotContains(t, result.Result, b.Parsed.EvaluableID)
}
