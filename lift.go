package expr

import (
	"fmt"
	"strconv"
	"strings"
)

// LiftedArgs represents a set of variables that have been lifted from expressions and
// replaced with identifiers, eg `id == "foo"` becomes `id == vars.a`, with "foo" lifted
// as "vars.a".
type LiftedArgs interface {
	Get(val string) (any, bool)
	Map() map[string]any
}

// liftLiterals lifts quoted literals into variables, allowing us to normalize
// expressions to increase cache hit rates.
func liftLiterals(expr string) (string, LiftedArgs) {
	// TODO: Lift numeric literals out of expressions.
	// If this contains an escape sequence (eg. `\` or `\'`), skip the lifting
	// of literals out of the expression.
	if strings.Contains(expr, `\"`) || strings.Contains(expr, `\'`) {
		return expr, nil
	}

	lp := liftParser{expr: expr}
	return lp.lift()
}

type liftParser struct {
	expr string
	idx  int

	rewritten *strings.Builder

	// varCounter counts the number of variables lifted.
	varCounter int

	vars pointerArgMap
}

func (l *liftParser) lift() (string, LiftedArgs) {
	l.vars = pointerArgMap{
		expr: l.expr,
		vars: map[string]argMapValue{},
	}

	l.rewritten = &strings.Builder{}

	for l.idx < len(l.expr) {
		char := l.expr[l.idx]

		l.idx++

		switch char {
		case '"':
			// Consume the string arg.
			val := l.consumeString('"')
			l.addLiftedVar(val)

		case '\'':
			val := l.consumeString('\'')
			l.addLiftedVar(val)
		default:
			l.rewritten.WriteByte(char)
		}
	}

	return l.rewritten.String(), l.vars
}

func (l *liftParser) addLiftedVar(val argMapValue) {
	if l.varCounter >= len(replace) {
		// Do nothing.
		str := val.get(l.expr)
		l.rewritten.WriteString(strconv.Quote(str.(string)))
		return
	}

	letter := replace[l.varCounter]

	l.vars.vars[letter] = val
	l.varCounter++

	l.rewritten.WriteString(VarPrefix + letter)
}

func (l *liftParser) consumeString(quoteChar byte) argMapValue {
	offset := l.idx
	length := 0
	for l.idx < len(l.expr) {
		char := l.expr[l.idx]

		// Grab the next char for evaluation.
		l.idx++

		if char == '\\' && l.peek() == quoteChar {
			// If we're escaping the quote character, ignore it.
			l.idx++
			length++
			continue
		}

		if char == quoteChar {
			return argMapValue{offset, length}
		}

		// Only now has the length of the inner quote increased.
		length++
	}

	// Should never happen:  we should always find the ending string quote, as the
	// expression should have already been validated.
	panic(fmt.Sprintf("unable to parse quoted string: `%s` (offset %d)", l.expr, offset))
}

func (l *liftParser) peek() byte {
	if (l.idx + 1) >= len(l.expr) {
		return 0x0
	}
	return l.expr[l.idx+1]
}

// pointerArgMap takes the original expression, and adds pointers to the original expression
// in order to grab variables.
//
// It does this by pointing to the offset and length of data within the expression, as opposed
// to extracting the value into a new string.  This greatly reduces memory growth & heap allocations.
type pointerArgMap struct {
	expr string
	vars map[string]argMapValue
}

func (p pointerArgMap) Map() map[string]any {
	res := map[string]any{}
	for k, v := range p.vars {
		res[k] = v.get(p.expr)
	}
	return res
}

func (p pointerArgMap) Get(key string) (any, bool) {
	val, ok := p.vars[key]
	if !ok {
		return nil, false
	}
	data := val.get(p.expr)
	return data, true
}

// argMapValue represents an offset and length for an argument in an expression string
type argMapValue [2]int

func (a argMapValue) get(expr string) any {
	data := expr[a[0] : a[0]+a[1]]
	return data
}

type regularArgMap map[string]any

func (p regularArgMap) Get(key string) (any, bool) {
	val, ok := p[key]
	return val, ok
}

func (p regularArgMap) Map() map[string]any {
	return p
}
