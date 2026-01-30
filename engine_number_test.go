package expr

import (
	"context"
	"testing"

	"github.com/google/cel-go/common/operators"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEngineNumber(t *testing.T) {
	ctx := context.Background()
	n := newNumberMatcher().(*numbers)

	// int64
	a := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  int64(123),
			Operator: operators.Equals,
		},
		Parsed: &ParsedExpression{EvaluableID: uuid.New()},
	}
	b := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.pi",
			Literal:  float64(1.131),
			Operator: operators.Equals,
		},
		Parsed: &ParsedExpression{EvaluableID: uuid.New()},
	}
	c := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  25,
			Operator: operators.GreaterEquals,
		},
		Parsed: &ParsedExpression{EvaluableID: uuid.New()},
	}
	d := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  9999,
			Operator: operators.Greater,
		},
		Parsed: &ParsedExpression{EvaluableID: uuid.New()},
	}
	e := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  -100,
			Operator: operators.Less,
		},
		Parsed: &ParsedExpression{EvaluableID: uuid.New()},
	}

	t.Run("It adds numbers", func(t *testing.T) {
		var err error

		err = n.Add(ctx, a)
		require.NoError(t, err)

		err = n.Add(ctx, b)
		require.NoError(t, err)

		err = n.Add(ctx, c)
		require.NoError(t, err)

		err = n.Add(ctx, d)
		require.NoError(t, err)

		err = n.Add(ctx, e)
		require.NoError(t, err)
	})

	t.Run("It searches >=", func(t *testing.T) {
		t.Run("with ints", func(t *testing.T) {
			// Expect only the >= id match.
			result := NewMatchResult()
			n.Search(ctx, "async.data.id", 999, result)
			require.Equal(t, 1, result.Len())

			foundC := false
			for key := range result.Result {
				if key.evalID == c.Parsed.EvaluableID {
					foundC = true
					break
				}
			}
			require.True(t, foundC, "expected to find expression c")
		})
		t.Run("with float64", func(t *testing.T) {
			// Expect only the >= id match.
			result := NewMatchResult()
			n.Search(ctx, "async.data.id", float64(999), result)
			require.Equal(t, 1, result.Len())
			foundC := false
			for key := range result.Result {
				if key.evalID == c.Parsed.EvaluableID {
					foundC = true
					break
				}
			}
			require.True(t, foundC, "expected to find expression c")
		})
	})

	t.Run("It matches == && >=", func(t *testing.T) {
		t.Run("with ints", func(t *testing.T) {
			// Expect only the >= id match.
			result := NewMatchResult()
			n.Search(ctx, "async.data.id", 123, result)
			require.Equal(t, 2, result.Len())

			foundA := false
			foundB := false
			foundC := false
			for key := range result.Result {
				if key.evalID == a.Parsed.EvaluableID {
					foundA = true
				}
				if key.evalID == b.Parsed.EvaluableID {
					foundB = true
				}
				if key.evalID == c.Parsed.EvaluableID {
					foundC = true
				}
			}
			require.True(t, foundA, "expected to find expression a")
			require.False(t, foundB, "should not find expression b")
			require.True(t, foundC, "expected to find expression c")
		})

		t.Run("with float64", func(t *testing.T) {
			// Expect only the >= id match.
			result := NewMatchResult()
			n.Search(ctx, "async.data.id", float64(123), result)
			require.Equal(t, 2, result.Len())

			foundA := false
			foundB := false
			foundC := false
			for key := range result.Result {
				if key.evalID == a.Parsed.EvaluableID {
					foundA = true
				}
				if key.evalID == b.Parsed.EvaluableID {
					foundB = true
				}
				if key.evalID == c.Parsed.EvaluableID {
					foundC = true
				}
			}
			require.True(t, foundA, "expected to find expression a")
			require.False(t, foundB, "should not find expression b")
			require.True(t, foundC, "expected to find expression c")
		})

		t.Run("with a low number", func(t *testing.T) {
			// Expect only the >= id match.
			result := NewMatchResult()
			n.Search(ctx, "async.data.id", float64(1.00001), result)
			require.Equal(t, 0, result.Len(), "returned parts: %#v")

			result = NewMatchResult()
			n.Search(ctx, "async.data.id", float64(24.999), result)
			require.Equal(t, 0, result.Len(), "returned parts: %#v")

			result = NewMatchResult()
			n.Search(ctx, "async.data.id", float64(25.0001), result)
			require.Equal(t, 1, result.Len(), "returned parts: %#v")
		})

		t.Run("matches pi", func(t *testing.T) {
			result := NewMatchResult()
			n.Search(ctx, "async.data.pi", 1.131, result)
			require.Equal(t, 1, result.Len())

			foundB := false
			for key := range result.Result {
				if key.evalID == b.Parsed.EvaluableID {
					foundB = true
					break
				}
			}
			require.True(t, foundB, "expected to find expression b")
		})

		t.Run("gt", func(t *testing.T) {
			result := NewMatchResult()
			n.Search(ctx, "async.data.id", 999999, result)
			require.Equal(t, 2, result.Len())

			foundA := false
			foundB := false
			foundC := false
			foundD := false
			for key := range result.Result {
				if key.evalID == a.Parsed.EvaluableID {
					foundA = true
				}
				if key.evalID == b.Parsed.EvaluableID {
					foundB = true
				}
				if key.evalID == c.Parsed.EvaluableID {
					foundC = true
				}
				if key.evalID == d.Parsed.EvaluableID {
					foundD = true
				}
			}
			require.False(t, foundA, "should not find expression a")
			require.False(t, foundB, "should not find expression b")
			require.True(t, foundC, "expected to find expression c")
			require.True(t, foundD, "expected to find expression d")
		})

		t.Run("lt", func(t *testing.T) {
			result := NewMatchResult()
			n.Search(ctx, "async.data.id", -999999, result)
			require.Equal(t, 1, result.Len())
			foundE := false
			for key := range result.Result {
				if key.evalID == e.Parsed.EvaluableID {
					foundE = true
					break
				}
			}
			require.True(t, foundE, "expected to find expression e")
		})
	})

	t.Run("It matches data", func(t *testing.T) {
		result := NewMatchResult()
		err := n.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"id": 123,
				},
			},
		}, result)
		require.NoError(t, err)
		require.Equal(t, 2, result.Len())

		foundA := false
		foundC := false
		for key := range result.Result {
			if key.evalID == a.Parsed.EvaluableID {
				foundA = true
			}
			if key.evalID == c.Parsed.EvaluableID {
				foundC = true
			}
		}
		require.True(t, foundA, "expected to find expression a")
		require.True(t, foundC, "expected to find expression c")
	})

	err := n.Add(ctx, a)
	require.NoError(t, err, "re-adding expression failed")
}
