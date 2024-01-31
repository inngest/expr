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

	t.Run("It adds strings", func(t *testing.T) {
		var err error

		err = s.Add(ctx, ExpressionPart{
			Predicate: Predicate{
				Ident:    "async.data.id",
				Literal:  "123",
				Operator: operators.Equals,
			},
		})
		require.NoError(t, err)

		err = s.Add(ctx, ExpressionPart{
			Predicate: Predicate{
				Ident:    "async.data.another",
				Literal:  "456",
				Operator: operators.Equals,
			},
		})
		require.NoError(t, err)

		t.Run("Adding the same string twice", func(t *testing.T) {
			err = s.Add(ctx, ExpressionPart{
				Predicate: Predicate{
					Ident:    "async.data.id",
					Literal:  "123",
					Operator: operators.Equals,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 2, len(s.strings[s.hash("123")]))
		})
	})

	t.Run("It searches strings", func(t *testing.T) {
		parts := s.Search(ctx, "async.data.id", "123")
		require.Equal(t, 2, len(parts))
		for _, part := range parts {
			require.EqualValues(t, "123", part.Predicate.Literal)
		}

		t.Run("It ignores variable names (for now)", func(t *testing.T) {
			parts = s.Search(ctx, "this doesn't matter", "123")
			require.Equal(t, 2, len(parts))
			for _, part := range parts {
				require.EqualValues(t, "123", part.Predicate.Literal)
			}
		})

		parts = s.Search(ctx, "async.data.another", "456")
		require.Equal(t, 1, len(parts))
	})

	t.Run("It matches data", func(t *testing.T) {
		found, err := s.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"id": "123",
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 2, len(found))
	})
}
