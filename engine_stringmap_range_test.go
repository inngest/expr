package expr

import (
	"context"
	"testing"
	"time"

	"github.com/google/cel-go/common/operators"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helpers

func newRangeEngine() *stringBTree {
	return newStringBTreeMatcher().(*stringBTree)
}

func rangePart(ident, literal, op string) ExpressionPart {
	return ExpressionPart{
		Parsed:  &ParsedExpression{EvaluableID: uuid.New()},
		GroupID: newGroupID(1, 0),
		Predicate: &Predicate{
			Ident:    ident,
			Literal:  literal,
			Operator: op,
		},
	}
}

func searchOne(t *testing.T, s *stringBTree, variable, val string) int {
	t.Helper()
	result := NewMatchResult()
	s.Search(context.Background(), variable, val, result)
	return result.Len()
}

// ── GTE (>=) ─────────────────────────────────────────────────────────────────

func TestStringRange_GTE(t *testing.T) {
	ctx := context.Background()
	s := newRangeEngine()

	p := rangePart("event.data.version", "v5", operators.GreaterEquals)
	require.NoError(t, s.Add(ctx, p))

	require.Equal(t, 0, searchOne(t, s, "event.data.version", "v3"))  // below: no match
	require.Equal(t, 1, searchOne(t, s, "event.data.version", "v5"))  // exact: match via exact btree
	require.Equal(t, 1, searchOne(t, s, "event.data.version", "v9"))  // above: match via gt scan
	require.Equal(t, 0, searchOne(t, s, "event.data.other", "v9"))    // wrong variable: no match
}

// ── GT (>) ───────────────────────────────────────────────────────────────────

func TestStringRange_GT(t *testing.T) {
	ctx := context.Background()
	s := newRangeEngine()

	p := rangePart("event.data.version", "v5", operators.Greater)
	require.NoError(t, s.Add(ctx, p))

	require.Equal(t, 0, searchOne(t, s, "event.data.version", "v3"))  // below: no match
	require.Equal(t, 0, searchOne(t, s, "event.data.version", "v5"))  // equal: no match (strict)
	require.Equal(t, 1, searchOne(t, s, "event.data.version", "v6"))  // above: match
}

// ── LTE (<=) ─────────────────────────────────────────────────────────────────

func TestStringRange_LTE(t *testing.T) {
	ctx := context.Background()
	s := newRangeEngine()

	p := rangePart("event.data.version", "v5", operators.LessEquals)
	require.NoError(t, s.Add(ctx, p))

	require.Equal(t, 0, searchOne(t, s, "event.data.version", "v9"))  // above: no match
	require.Equal(t, 1, searchOne(t, s, "event.data.version", "v5"))  // exact: match via exact btree
	require.Equal(t, 1, searchOne(t, s, "event.data.version", "v1"))  // below: match via lt scan
}

// ── LT (<) ───────────────────────────────────────────────────────────────────

func TestStringRange_LT(t *testing.T) {
	ctx := context.Background()
	s := newRangeEngine()

	p := rangePart("event.data.version", "v5", operators.Less)
	require.NoError(t, s.Add(ctx, p))

	require.Equal(t, 0, searchOne(t, s, "event.data.version", "v9"))  // above: no match
	require.Equal(t, 0, searchOne(t, s, "event.data.version", "v5"))  // equal: no match (strict)
	require.Equal(t, 1, searchOne(t, s, "event.data.version", "v3"))  // below: match
}

// ── Date-string range (realistic) ────────────────────────────────────────────

func TestStringRange_DateStrings(t *testing.T) {
	ctx := context.Background()
	s := newRangeEngine()

	threshold := "2025-10-28T23:21:37.821Z"
	p := rangePart("async.data.createdAt", threshold, operators.GreaterEquals)
	require.NoError(t, s.Add(ctx, p))

	require.Equal(t, 1, searchOne(t, s, "async.data.createdAt", "2025-10-28T23:21:37.821Z")) // exact
	require.Equal(t, 1, searchOne(t, s, "async.data.createdAt", "2025-11-01T00:00:00.000Z")) // after
	require.Equal(t, 0, searchOne(t, s, "async.data.createdAt", "2025-10-01T00:00:00.000Z")) // before
	require.Equal(t, 0, searchOne(t, s, "async.data.createdAt", "2024-01-01T00:00:00.000Z")) // much earlier
}

// ── Range window: GTE + LTE on same variable ─────────────────────────────────

func TestStringRange_Window(t *testing.T) {
	ctx := context.Background()
	s := newRangeEngine()

	lower := rangePart("event.data.ts", "2025-01-01", operators.GreaterEquals)
	upper := rangePart("event.data.ts", "2025-12-31", operators.LessEquals)
	require.NoError(t, s.Add(ctx, lower))
	require.NoError(t, s.Add(ctx, upper))

	// Inside window: both match.
	result := NewMatchResult()
	s.Search(ctx, "event.data.ts", "2025-06-15", result)
	require.Equal(t, 2, result.Len())

	// At lower bound: both match.
	result = NewMatchResult()
	s.Search(ctx, "event.data.ts", "2025-01-01", result)
	require.Equal(t, 2, result.Len())

	// Below lower: only upper (LTE) matches.
	result = NewMatchResult()
	s.Search(ctx, "event.data.ts", "2024-12-31", result)
	require.Equal(t, 1, result.Len())
	for k := range result.Result {
		require.Equal(t, upper.Parsed.EvaluableID, k.evalID)
	}

	// Above upper: only lower (GTE) matches.
	result = NewMatchResult()
	s.Search(ctx, "event.data.ts", "2026-01-01", result)
	require.Equal(t, 1, result.Len())
	for k := range result.Result {
		require.Equal(t, lower.Parsed.EvaluableID, k.evalID)
	}
}

// ── Multiple variables: no cross-contamination ───────────────────────────────

func TestStringRange_VariableIsolation(t *testing.T) {
	ctx := context.Background()
	s := newRangeEngine()

	a := rangePart("event.data.foo", "m", operators.GreaterEquals)
	b := rangePart("event.data.bar", "m", operators.GreaterEquals)
	require.NoError(t, s.Add(ctx, a))
	require.NoError(t, s.Add(ctx, b))

	result := NewMatchResult()
	s.Search(ctx, "event.data.foo", "z", result)
	require.Equal(t, 1, result.Len())
	for k := range result.Result {
		require.Equal(t, a.Parsed.EvaluableID, k.evalID)
	}

	result = NewMatchResult()
	s.Search(ctx, "event.data.bar", "z", result)
	require.Equal(t, 1, result.Len())
	for k := range result.Result {
		require.Equal(t, b.Parsed.EvaluableID, k.evalID)
	}
}

// ── Match() with nested event map ────────────────────────────────────────────

func TestStringRange_Match(t *testing.T) {
	ctx := context.Background()
	s := newRangeEngine()

	p := rangePart("async.data.createdAt", "2025-10-28", operators.GreaterEquals)
	require.NoError(t, s.Add(ctx, p))

	match := func(createdAt string) int {
		result := NewMatchResult()
		_ = s.Match(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{"createdAt": createdAt},
			},
		}, result)
		return result.Len()
	}

	require.Equal(t, 0, match("2025-10-01"))
	require.Equal(t, 1, match("2025-10-28"))
	require.Equal(t, 1, match("2025-11-15"))
}

// ── Remove ───────────────────────────────────────────────────────────────────

func TestStringRange_Remove(t *testing.T) {
	ctx := context.Background()

	t.Run("GTE", func(t *testing.T) {
		s := newRangeEngine()
		p := rangePart("event.data.v", "v5", operators.GreaterEquals)
		require.NoError(t, s.Add(ctx, p))

		require.Equal(t, 1, searchOne(t, s, "event.data.v", "v5"))
		require.Equal(t, 1, searchOne(t, s, "event.data.v", "v9"))

		count, err := s.Remove(ctx, []ExpressionPart{p})
		require.NoError(t, err)
		require.Equal(t, 1, count)

		require.Equal(t, 0, searchOne(t, s, "event.data.v", "v5"))
		require.Equal(t, 0, searchOne(t, s, "event.data.v", "v9"))
	})

	t.Run("GT", func(t *testing.T) {
		s := newRangeEngine()
		p := rangePart("event.data.v", "v5", operators.Greater)
		require.NoError(t, s.Add(ctx, p))

		require.Equal(t, 1, searchOne(t, s, "event.data.v", "v9"))

		count, err := s.Remove(ctx, []ExpressionPart{p})
		require.NoError(t, err)
		require.Equal(t, 1, count)

		require.Equal(t, 0, searchOne(t, s, "event.data.v", "v9"))
	})

	t.Run("LTE", func(t *testing.T) {
		s := newRangeEngine()
		p := rangePart("event.data.v", "v5", operators.LessEquals)
		require.NoError(t, s.Add(ctx, p))

		require.Equal(t, 1, searchOne(t, s, "event.data.v", "v5"))
		require.Equal(t, 1, searchOne(t, s, "event.data.v", "v1"))

		count, err := s.Remove(ctx, []ExpressionPart{p})
		require.NoError(t, err)
		require.Equal(t, 1, count)

		require.Equal(t, 0, searchOne(t, s, "event.data.v", "v5"))
		require.Equal(t, 0, searchOne(t, s, "event.data.v", "v1"))
	})
}

// ── No double-counting for >= and <= ─────────────────────────────────────────

func TestStringRange_NoDoubleCount(t *testing.T) {
	ctx := context.Background()

	t.Run("GTE at threshold", func(t *testing.T) {
		s := newRangeEngine()
		p := rangePart("event.data.v", "v5", operators.GreaterEquals)
		require.NoError(t, s.Add(ctx, p))

		result := NewMatchResult()
		s.Search(ctx, "event.data.v", "v5", result)
		require.Equal(t, 1, result.Len())

		key := matchKey{evalID: p.Parsed.EvaluableID, groupID: p.GroupID}
		require.Equal(t, 1, result.Result[key], "group count must be 1, not 2")
	})

	t.Run("LTE at threshold", func(t *testing.T) {
		s := newRangeEngine()
		p := rangePart("event.data.v", "v5", operators.LessEquals)
		require.NoError(t, s.Add(ctx, p))

		result := NewMatchResult()
		s.Search(ctx, "event.data.v", "v5", result)
		require.Equal(t, 1, result.Len())

		key := matchKey{evalID: p.Parsed.EvaluableID, groupID: p.GroupID}
		require.Equal(t, 1, result.Result[key], "group count must be 1, not 2")
	})
}

// ── AggregateEvaluator: string range expressions are Fast ────────────────────

func TestStringRange_AggregateEvaluator_Classification(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	e := NewAggregateEvaluator(AggregateEvaluatorOpts[testEvaluable]{
		Parser:      parser,
		Eval:        testBoolEvaluator,
		Concurrency: 0,
	})
	defer e.Close()

	for _, expr := range []string{
		`event.data.ts >= "2025-01-01"`,
		`event.data.ts > "2025-01-01"`,
		`event.data.ts <= "2025-12-31"`,
		`event.data.ts < "2025-12-31"`,
	} {
		ratio, err := e.Add(ctx, tex(expr))
		require.NoError(t, err, "expr: %s", expr)
		require.Equal(t, float64(1), ratio, "expected fast (ratio=1) for: %s", expr)
	}

	require.Equal(t, 4, e.FastLen())
	require.Equal(t, 0, e.MixedLen())
	require.Equal(t, 0, e.SlowLen())
}

// ── AggregateEvaluator: compound expression (equality + range) ───────────────

func TestStringRange_AggregateEvaluator_CompoundMatch(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	e := NewAggregateEvaluator(AggregateEvaluatorOpts[testEvaluable]{
		Parser:      parser,
		Eval:        testBoolEvaluator,
		Concurrency: 0,
	})
	defer e.Close()

	expr := `"sub_1" == async.data.subscriptionId && "2025-10-28T00:00:00.000Z" <= async.data.createdAt && "v1" == async.data.version`
	ratio, err := e.Add(ctx, tex(expr, "pause-1"))
	require.NoError(t, err)
	require.Equal(t, float64(1), ratio)
	require.Equal(t, 1, e.FastLen())
	require.Equal(t, 0, e.MixedLen())

	eval := func(subscriptionId, createdAt, version string) []testEvaluable {
		evals, _, err := e.Evaluate(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"subscriptionId": subscriptionId,
					"createdAt":      createdAt,
					"version":        version,
				},
			},
		})
		require.NoError(t, err)
		return evals
	}

	require.Len(t, eval("sub_1", "2025-10-28T00:00:00.000Z", "v1"), 1) // exact threshold
	require.Len(t, eval("sub_1", "2025-11-01T00:00:00.000Z", "v1"), 1) // after threshold
	require.Len(t, eval("sub_2", "2025-10-28T00:00:00.000Z", "v1"), 0) // wrong subscriptionId
	require.Len(t, eval("sub_1", "2025-10-27T23:59:59.999Z", "v1"), 0) // before threshold
	require.Len(t, eval("sub_1", "2025-10-28T00:00:00.000Z", "v2"), 0) // wrong version
}

// ── AggregateEvaluator: equality + range on same variable both work ───────────

func TestStringRange_AggregateEvaluator_MixedOperators(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	e := NewAggregateEvaluator(AggregateEvaluatorOpts[testEvaluable]{
		Parser:      parser,
		Eval:        testBoolEvaluator,
		Concurrency: 0,
	})
	defer e.Close()

	eqExpr := tex(`event.data.score == "v5"`, "eq")
	gteExpr := tex(`event.data.score >= "v3"`, "gte")
	_, err = e.Add(ctx, eqExpr)
	require.NoError(t, err)
	_, err = e.Add(ctx, gteExpr)
	require.NoError(t, err)
	require.Equal(t, 2, e.FastLen())

	input := func(score string) map[string]any {
		return map[string]any{"event": map[string]any{"data": map[string]any{"score": score}}}
	}

	// "v5" matches both
	evals, _, err := e.Evaluate(ctx, input("v5"))
	require.NoError(t, err)
	require.Len(t, evals, 2)

	// "v4" matches only gte
	evals, _, err = e.Evaluate(ctx, input("v4"))
	require.NoError(t, err)
	require.Len(t, evals, 1)
	require.Equal(t, gteExpr.ID, evals[0].ID)

	// "v1" matches neither
	evals, _, err = e.Evaluate(ctx, input("v1"))
	require.NoError(t, err)
	require.Len(t, evals, 0)
}

// ── AggregateEvaluator: equality + inequality + string range all required ─────

func TestStringRange_AggregateEvaluator_EqNeqRange(t *testing.T) {
	// Expression: subscriptionId == "sub_1" && status != "deleted" && createdAt >= "2025-01-01"
	// All three predicates must hold; failing any one must suppress the match.
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	e := NewAggregateEvaluator(AggregateEvaluatorOpts[testEvaluable]{
		Parser:      parser,
		Eval:        testBoolEvaluator,
		Concurrency: 0,
	})
	defer e.Close()

	expr := tex(
		`async.data.subscriptionId == "sub_1" && async.data.status != "deleted" && async.data.createdAt >= "2025-01-01"`,
		"eq-neq-range",
	)
	ratio, err := e.Add(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, float64(1), ratio, "expression should be fully aggregatable")

	eval := func(subID, status, createdAt string) []testEvaluable {
		evals, _, err := e.Evaluate(ctx, map[string]any{
			"async": map[string]any{
				"data": map[string]any{
					"subscriptionId": subID,
					"status":         status,
					"createdAt":      createdAt,
				},
			},
		})
		require.NoError(t, err)
		return evals
	}

	// All three conditions met → match
	require.Len(t, eval("sub_1", "active", "2025-06-01"), 1, "all conditions met should match")
	require.Len(t, eval("sub_1", "active", "2025-01-01"), 1, "exact threshold should match")

	// Wrong equality → no match
	require.Len(t, eval("sub_2", "active", "2025-06-01"), 0, "wrong subscriptionId should not match")

	// Inequality violated (status == "deleted") → no match
	require.Len(t, eval("sub_1", "deleted", "2025-06-01"), 0, "deleted status should not match")

	// Range violated (createdAt before threshold) → no match
	require.Len(t, eval("sub_1", "active", "2024-12-31"), 0, "createdAt before threshold should not match")

	// Two conditions wrong → no match
	require.Len(t, eval("sub_2", "deleted", "2024-01-01"), 0, "all wrong should not match")
}

// ── AggregateEvaluator: remove works for range expressions ───────────────────

func TestStringRange_AggregateEvaluator_Remove(t *testing.T) {
	ctx := context.Background()
	parser, err := newParser()
	require.NoError(t, err)

	e := NewAggregateEvaluator(AggregateEvaluatorOpts[testEvaluable]{
		Parser:      parser,
		Eval:        testBoolEvaluator,
		Concurrency: 0,
		GCThreshold: 1,
	})
	defer e.Close()

	expr := tex(`event.data.score >= "m"`, "score-expr")
	_, err = e.Add(ctx, expr)
	require.NoError(t, err)

	input := map[string]any{"event": map[string]any{"data": map[string]any{"score": "z"}}}

	evals, _, err := e.Evaluate(ctx, input)
	require.NoError(t, err)
	require.Len(t, evals, 1)

	err = e.Remove(ctx, expr)
	require.NoError(t, err)

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.Equal(ct, 0, e.Len())
	}, 300*time.Millisecond, 10*time.Millisecond)

	evals, _, err = e.Evaluate(ctx, input)
	require.NoError(t, err)
	require.Len(t, evals, 0)
}
