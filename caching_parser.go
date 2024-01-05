package expr

import (
	"sync"
	"sync/atomic"

	"github.com/google/cel-go/cel"
	// "github.com/karlseguin/ccache/v2"
)

var (
	replace = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"}
)

// NewCachingParser returns a CELParser which lifts quoted literals out of the expression
// as variables and uses caching to cache expression parsing, resulting in improved
// performance when parsing expressions.
func NewCachingParser(env *cel.Env) CELParser {
	return &cachingParser{
		env: env,
	}
}

type cachingParser struct {
	// cache is a global cache of precompiled expressions.
	// cache *ccache.Cache
	stupidNoInternetCache sync.Map

	env *cel.Env

	hits   int64
	misses int64
}

func (c *cachingParser) Parse(expr string) (*cel.Ast, *cel.Issues, LiftedArgs) {
	expr, vars := liftLiterals(expr)

	// TODO: ccache, when I have internet.
	if cached, ok := c.stupidNoInternetCache.Load(expr); ok {
		p := cached.(ParsedCelExpr)
		atomic.AddInt64(&c.hits, 1)
		return p.AST, p.Issues, vars
	}

	ast, issues := c.env.Parse(expr)

	c.stupidNoInternetCache.Store(expr, ParsedCelExpr{
		Expr:   expr,
		AST:    ast,
		Issues: issues,
	})

	atomic.AddInt64(&c.misses, 1)
	return ast, issues, vars
}

func (c *cachingParser) Hits() int64 {
	return atomic.LoadInt64(&c.hits)
}

func (c *cachingParser) Misses() int64 {
	return atomic.LoadInt64(&c.misses)
}

type ParsedCelExpr struct {
	Expr   string
	AST    *cel.Ast
	Issues *cel.Issues
}
