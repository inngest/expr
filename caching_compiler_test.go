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

func TestCachingCompile_IntegerLiteralDedup(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	a := `event.data.id == 1`
	b := `event.data.id == 2`

	astA, issA, varsA := c.Compile(a)
	require.NotNil(t, astA)
	require.Nil(t, issA)
	require.EqualValues(t, int64(1), mustGet(t, varsA, "a"))
	require.EqualValues(t, 0, c.Hits())
	require.EqualValues(t, 1, c.Misses())

	// Both normalise to "event.data.id == vars.a"; AST pointer must be identical.
	astB, issB, varsB := c.Compile(b)
	require.NotNil(t, astB)
	require.Nil(t, issB)
	require.Equal(t, astA, astB)
	require.EqualValues(t, int64(2), mustGet(t, varsB, "a"))
	require.EqualValues(t, 1, c.Hits())
	require.EqualValues(t, 1, c.Misses())
}

func TestCachingCompile_FloatLiteralDedup(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	a := `event.data.score >= 1.5`
	b := `event.data.score >= 99.9`

	astA, issA, varsA := c.Compile(a)
	require.NotNil(t, astA)
	require.Nil(t, issA)
	require.EqualValues(t, float64(1.5), mustGet(t, varsA, "a"))
	require.EqualValues(t, 0, c.Hits())
	require.EqualValues(t, 1, c.Misses())

	astB, issB, varsB := c.Compile(b)
	require.NotNil(t, astB)
	require.Nil(t, issB)
	require.Equal(t, astA, astB)
	require.EqualValues(t, float64(99.9), mustGet(t, varsB, "a"))
	require.EqualValues(t, 1, c.Hits())
	require.EqualValues(t, 1, c.Misses())
}

// Scientific notation must be consumed whole; leaving "e10" produces "vars.ae10" (field access).
func TestCachingCompile_ScientificNotationDedup(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	a := `event.data.count > 1e6`
	b := `event.data.count > 2.5e3`

	astA, issA, varsA := c.Compile(a)
	require.NotNil(t, astA)
	require.Nil(t, issA)
	require.EqualValues(t, float64(1e6), mustGet(t, varsA, "a"))
	require.EqualValues(t, 0, c.Hits())
	require.EqualValues(t, 1, c.Misses())

	astB, issB, varsB := c.Compile(b)
	require.NotNil(t, astB)
	require.Nil(t, issB)
	require.Equal(t, astA, astB)
	require.EqualValues(t, float64(2.5e3), mustGet(t, varsB, "a"))
	require.EqualValues(t, 1, c.Hits())
	require.EqualValues(t, 1, c.Misses())

	// explicit positive sign in exponent
	astC, issC, varsC := c.Compile(`event.data.count > 1e+6`)
	require.NotNil(t, astC)
	require.Nil(t, issC)
	require.Equal(t, astA, astC)
	require.EqualValues(t, float64(1e+6), mustGet(t, varsC, "a"))
	require.EqualValues(t, 2, c.Hits())
	require.EqualValues(t, 1, c.Misses())
}

func TestCachingCompile_IdentExprDedup(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	expr := `event.data.id == async.data.id`

	astA, _, _ := c.Compile(expr)
	require.EqualValues(t, 0, c.Hits())
	require.EqualValues(t, 1, c.Misses())

	astB, _, _ := c.Compile(expr)
	require.Equal(t, astA, astB)
	require.EqualValues(t, 1, c.Hits())
	require.EqualValues(t, 1, c.Misses())
}

func TestCachingCompile_MixedLiteralsDedup(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	// Both normalise to: event.name == vars.a && event.data.amount > vars.b
	a := `event.name == "order/created" && event.data.amount > 100`
	b := `event.name == "item/shipped" && event.data.amount > 9999`

	astA, issA, varsA := c.Compile(a)
	require.NotNil(t, astA)
	require.Nil(t, issA)
	require.EqualValues(t, "order/created", mustGet(t, varsA, "a"))
	require.EqualValues(t, int64(100), mustGet(t, varsA, "b"))
	require.EqualValues(t, 0, c.Hits())
	require.EqualValues(t, 1, c.Misses())

	astB, issB, varsB := c.Compile(b)
	require.NotNil(t, astB)
	require.Nil(t, issB)
	require.Equal(t, astA, astB)
	require.EqualValues(t, "item/shipped", mustGet(t, varsB, "a"))
	require.EqualValues(t, int64(9999), mustGet(t, varsB, "b"))
	require.EqualValues(t, 1, c.Hits())
	require.EqualValues(t, 1, c.Misses())
}

// Unary minus stays in the expression; only the positive digit is lifted.
// "-5" and "-3" both normalise to "-vars.a".
func TestCachingCompile_NegativeIntegerDedup(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	a := `event.data.offset > -5`
	b := `event.data.offset > -100`

	astA, issA, varsA := c.Compile(a)
	require.NotNil(t, astA)
	require.Nil(t, issA)
	require.EqualValues(t, int64(5), mustGet(t, varsA, "a"))
	require.EqualValues(t, 0, c.Hits())
	require.EqualValues(t, 1, c.Misses())

	astB, issB, varsB := c.Compile(b)
	require.NotNil(t, astB)
	require.Nil(t, issB)
	require.Equal(t, astA, astB)
	require.EqualValues(t, int64(100), mustGet(t, varsB, "a"))
	require.EqualValues(t, 1, c.Hits())
	require.EqualValues(t, 1, c.Misses())
}

// Digits within an identifier (e.g. "version2") must not be lifted.
func TestCachingCompile_IdentifierDigitsNotLifted(t *testing.T) {
	c := cachingCompiler{env: newEnv()}

	a := `event.data.version2 == "v1"`
	b := `event.data.version2 == "v2"`

	astA, issA, _ := c.Compile(a)
	require.NotNil(t, astA)
	require.Nil(t, issA)
	require.EqualValues(t, 0, c.Hits())
	require.EqualValues(t, 1, c.Misses())

	astB, issB, _ := c.Compile(b)
	require.NotNil(t, astB)
	require.Nil(t, issB)
	require.Equal(t, astA, astB)
	require.EqualValues(t, 1, c.Hits())
	require.EqualValues(t, 1, c.Misses())
}

func mustGet(t *testing.T, args LiftedArgs, key string) any {
	t.Helper()
	val, ok := args.Get(key)
	require.True(t, ok, "expected lifted variable %q to be present", key)
	return val
}
