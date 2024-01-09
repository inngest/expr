package expr

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
)

func TestCachingParser_CachesSame(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	a := `event.data.a == "cache"`
	b := `event.data.b == "cache"`

	var (
		prevAST    *cel.Ast
		prevIssues *cel.Issues
		prevVars   LiftedArgs
	)

	t.Run("With an uncached expression", func(t *testing.T) {
		prevAST, prevIssues, prevVars = c.Compile(a)
		require.NotNil(t, prevAST)
		require.Nil(t, prevIssues)
		require.NotNil(t, prevVars)
		require.EqualValues(t, 0, c.Hits())
		require.EqualValues(t, 1, c.Misses())
	})

	t.Run("With a cached expression", func(t *testing.T) {
		ast, issues, vars := c.Compile(a)
		require.NotNil(t, ast)
		require.Nil(t, issues)

		require.Equal(t, prevAST, ast)
		require.Equal(t, prevIssues, issues)
		require.Equal(t, prevVars, vars)

		require.EqualValues(t, 1, c.Hits())
		require.EqualValues(t, 1, c.Misses())
	})

	t.Run("With another uncached expression", func(t *testing.T) {
		prevAST, prevIssues, prevVars = c.Compile(b)
		require.NotNil(t, prevAST)
		require.Nil(t, prevIssues)
		// This misses the cache, as the vars have changed - not the
		// literals.
		require.EqualValues(t, 1, c.Hits())
		require.EqualValues(t, 2, c.Misses())
	})
}

func TestCachingCompile(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	a := `event.data.a == "literal-a" && event.data.b == "yes-1"`
	b := `event.data.a == "literal-b" && event.data.b == "yes-2"`

	var (
		prevAST    *cel.Ast
		prevIssues *cel.Issues
		prevVars   LiftedArgs
	)

	t.Run("With an uncached expression", func(t *testing.T) {
		prevAST, prevIssues, prevVars = c.Compile(a)
		require.NotNil(t, prevAST)
		require.Nil(t, prevIssues)
		require.EqualValues(t, 0, c.Hits())
		require.EqualValues(t, 1, c.Misses())
	})

	t.Run("With a cached expression", func(t *testing.T) {
		ast, issues, vars := c.Compile(a)
		require.NotNil(t, ast)
		require.Nil(t, issues)

		require.Equal(t, prevAST, ast)
		require.Equal(t, prevIssues, issues)
		require.Equal(t, prevVars, vars)

		require.EqualValues(t, 1, c.Hits())
		require.EqualValues(t, 1, c.Misses())
	})

	t.Run("With a cached expression having different literals ONLY", func(t *testing.T) {
		prevAST, prevIssues, _ = c.Compile(b)
		require.NotNil(t, prevAST)
		require.Nil(t, prevIssues)
		// This misses the cache.
		require.EqualValues(t, 2, c.Hits())
		require.EqualValues(t, 1, c.Misses())
	})
}
