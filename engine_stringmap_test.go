package expr

import (
	"context"
	"testing"

	"github.com/google/cel-go/common/operators"
	"github.com/stretchr/testify/require"
)

func TestEngineStringmap(t *testing.T) {
	ctx := context.Background()
	s := newStringEqualityMatcher().(*stringLookup)

	a := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  "123",
			Operator: operators.Equals,
		},
	}
	b := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  "123",
			Operator: operators.Equals,
		},
	}
	c := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.another",
			Literal:  "456",
			Operator: operators.Equals,
		},
	}

	// Test inequality
	d := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.neq",
			Literal:  "neq-1",
			Operator: operators.NotEquals,
		},
	}
	e := ExpressionPart{
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
		parts := s.Search(ctx, "async.data.id", "123")
		require.Equal(t, 2, len(parts))

		for _, part := range parts {
			require.EqualValues(t, part.PredicateID, a.Hash())
			require.EqualValues(t, part.PredicateID, b.Hash())
		}

		t.Run("It handles variable names", func(t *testing.T) {
			parts = s.Search(ctx, "this doesn't matter", "123")
			require.Equal(t, 0, len(parts))
		})

		parts = s.Search(ctx, "async.data.another", "456")
		require.Equal(t, 1, len(parts))
	})

	// Inequality
	err = s.Add(ctx, d)
	require.NoError(t, err)
	err = s.Add(ctx, e)
	require.NoError(t, err)

	t.Run("inequality", func(t *testing.T) {
		t.Run("first case: neq-1", func(t *testing.T) {
			parts, err := s.Match(ctx, map[string]any{
				"async": map[string]any{
					"data": map[string]any{"neq": "neq-1"},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(parts))
			require.EqualValues(t, parts[0].PredicateID, e.Hash())
		})

		t.Run("second case: neq-1", func(t *testing.T) {
			parts, err := s.Match(ctx, map[string]any{
				"async": map[string]any{
					"data": map[string]any{"neq": "neq-2"},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(parts))
			require.EqualValues(t, parts[0].PredicateID, d.Hash())
		})

		t.Run("third case: both", func(t *testing.T) {
			parts, err := s.Match(ctx, map[string]any{
				"async": map[string]any{
					"data": map[string]any{"neq": "both"},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 2, len(parts))
		})
	})

	t.Run("It matches data, including neq", func(t *testing.T) {
		found, err := s.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"id":  "123",
					"neq": "lol",
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 4, len(found)) // matching plus inequality
	})

	t.Run("It matches data with null neq", func(t *testing.T) {
		found, err := s.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"id": "123",
					// by not including neq, we ensure we test against null matches.
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 4, len(found)) // matching plus inequality
	})

}

func TestEngineStringmap_DuplicateValues(t *testing.T) {
	ctx := context.Background()
	s := newStringEqualityMatcher().(*stringLookup)
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
	parts := s.Search(ctx, "async.data.var_b", "123")
	require.Equal(t, 1, len(parts))

}

func TestEngineStringmap_DuplicateNeq(t *testing.T) {
	ctx := context.Background()
	s := newStringEqualityMatcher().(*stringLookup)
	a := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.var_a",
			Literal:  "a",
			Operator: operators.Equals,
		},
	}
	b := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.var_b",
			Literal:  "b",
			Operator: operators.Equals,
		},
	}
	c := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.var_c",
			Literal:  "123",
			Operator: operators.NotEquals,
		},
	}
	err := s.Add(ctx, a)
	require.NoError(t, err)
	err = s.Add(ctx, b)
	require.NoError(t, err)
	err = s.Add(ctx, c)
	require.NoError(t, err)

	parts, err := s.Match(ctx, map[string]any{
		"async": map[string]any{
			"data": map[string]any{
				"var_a": "a",
				"var_b": "nah",
			},
		},
	})

	require.NoError(t, err)
	require.Equal(t, 2, len(parts))
	for _, v := range parts {
		// Never matches B, as B isn't complete.
		require.NotEqualValues(t, v.PredicateID, b.Hash())
		require.Contains(t, []uint64{
			a.Hash(),
			c.Hash(),
		}, v.PredicateID)
	}

}
