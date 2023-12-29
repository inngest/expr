package expr

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/cel-go/cel"
	// "github.com/karlseguin/ccache/v2"
)

var (
	doubleQuoteMatch *regexp.Regexp
	replace          = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
)

func init() {
	doubleQuoteMatch = regexp.MustCompile(`"[^"]*"`)
}

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

// liftLiterals lifts quoted literals into variables, allowing us to normalize
// expressions to increase cache hit rates.
func liftLiterals(expr string) (string, map[string]any) {
	// TODO: Optimize this please.  Use strconv.Unquote as the basis, and perform
	// searches across each index quotes.

	// If this contains an escape sequence (eg. `\` or `\'`), skip the lifting
	// of literals out of the expression.
	if strings.Contains(expr, `\"`) || strings.Contains(expr, `\'`) {
		return expr, nil
	}

	var (
		counter int
		vars    = map[string]any{}
	)

	rewrite := func(str string) string {
		if counter > len(replace) {
			return str
		}

		idx := replace[counter]
		if val, err := strconv.Unquote(str); err == nil {
			str = val
		}
		vars[idx] = str

		counter++
		return VarPrefix + idx
	}

	expr = doubleQuoteMatch.ReplaceAllStringFunc(expr, rewrite)
	return expr, vars
}

func (c *cachingParser) Parse(expr string) (*cel.Ast, *cel.Issues, map[string]any) {
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
