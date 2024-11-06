package expr

import (
	"context"
	"testing"

	"github.com/google/cel-go/common/operators"
	"github.com/stretchr/testify/require"
)

const testConcurrency = 1

func TestEngineNumber(t *testing.T) {
	ctx := context.Background()
	n := newNumberMatcher(testConcurrency).(*numbers)

	// int64
	a := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  int64(123),
			Operator: operators.Equals,
		},
	}
	b := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.pi",
			Literal:  float64(1.131),
			Operator: operators.Equals,
		},
	}
	c := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  25,
			Operator: operators.GreaterEquals,
		},
	}
	d := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  9999,
			Operator: operators.Greater,
		},
	}
	e := ExpressionPart{
		Predicate: &Predicate{
			Ident:    "async.data.id",
			Literal:  -100,
			Operator: operators.Less,
		},
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
			parts := n.Search(ctx, "async.data.id", 999)
			require.Equal(t, 1, len(parts))
			for _, part := range parts {
				require.EqualValues(t, part.PredicateID, c.Hash())
			}
		})
		t.Run("with float64", func(t *testing.T) {
			// Expect only the >= id match.
			parts := n.Search(ctx, "async.data.id", float64(999))
			require.Equal(t, 1, len(parts))
			for _, part := range parts {
				require.EqualValues(t, part.PredicateID, c.Hash())
			}
		})
	})

	t.Run("It matches == && >=", func(t *testing.T) {
		t.Run("with ints", func(t *testing.T) {
			// Expect only the >= id match.
			parts := n.Search(ctx, "async.data.id", 123)
			require.Equal(t, 2, len(parts))
			require.EqualValues(t, parts[0].PredicateID, a.Hash())
			require.EqualValues(t, parts[1].PredicateID, c.Hash())
		})

		t.Run("with float64", func(t *testing.T) {
			// Expect only the >= id match.
			parts := n.Search(ctx, "async.data.id", float64(123))
			require.Equal(t, 2, len(parts))
			require.EqualValues(t, parts[0].PredicateID, a.Hash())
			require.EqualValues(t, parts[1].PredicateID, c.Hash())
		})

		t.Run("with a low number", func(t *testing.T) {
			// Expect only the >= id match.
			parts := n.Search(ctx, "async.data.id", float64(1.00001))
			require.Equal(t, 0, len(parts), "returned parts: %#v")

			parts = n.Search(ctx, "async.data.id", float64(24.999))
			require.Equal(t, 0, len(parts), "returned parts: %#v")

			parts = n.Search(ctx, "async.data.id", float64(25.0001))
			require.Equal(t, 1, len(parts), "returned parts: %#v")
		})

		t.Run("matches pi", func(t *testing.T) {
			parts := n.Search(ctx, "async.data.pi", 1.131)
			require.Equal(t, 1, len(parts))
			require.EqualValues(t, parts[0].PredicateID, b.Hash())
		})

		t.Run("gt", func(t *testing.T) {
			parts := n.Search(ctx, "async.data.id", 999999)
			require.Equal(t, 2, len(parts))
			require.EqualValues(t, parts[0].PredicateID, c.Hash())
			require.EqualValues(t, parts[1].PredicateID, d.Hash())
		})

		t.Run("lt", func(t *testing.T) {
			parts := n.Search(ctx, "async.data.id", -999999)
			require.Equal(t, 1, len(parts))
			require.EqualValues(t, parts[0].PredicateID, e.Hash())
		})
	})

	t.Run("It matches data", func(t *testing.T) {
		found, err := n.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"id": 123,
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 2, len(found))
		require.EqualValues(t, found[0].PredicateID, a.Hash())
		require.EqualValues(t, found[1].PredicateID, c.Hash())
	})

	err := n.Add(ctx, a)
	require.NoError(t, err, "re-adding expression failed")

}
