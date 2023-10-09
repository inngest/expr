package expr

import (
	"context"
	"crypto/md5"
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
)

func NewTreeParser(env *cel.Env) (TreeParser, error) {
	parser := &parser{
		env: env,
	}
	folder, err := cel.NewConstantFoldingOptimizer()
	if err != nil {
		return nil, err
	}
	parser.optimizer = cel.NewStaticOptimizer(folder, parser)
	return parser, nil
}

// TreeParser parses
type TreeParser interface {
	Parse(ctx context.Context, expr string) ([]PredicateGroup, error)
}

type parser struct {
	env       *cel.Env
	optimizer *cel.StaticOptimizer

	parsed []PredicateGroup
}

func (p *parser) Parse(ctx context.Context, expr string) ([]PredicateGroup, error) {
	ast, issues := p.env.Parse(expr)
	if issues != nil {
		return nil, issues.Err()
	}

	// Parse the AST, which allows us to iterate through the expression to check operators
	// and calculate trees.
	_, _ = p.optimizer.Optimize(p.env, ast)

	return p.parsed, nil
}

func (p *parser) Optimize(ctx *cel.OptimizerContext, ast *celast.AST) *celast.AST {
	nav := celast.NavigateAST(ast)
	p.parsed = navigateAST(expr{NavigableExpr: nav}, 0)
	// Add an issue to prevent the optimizer from working further.  We no longer care.
	ctx.Issues.Append(cel.NewIssues(common.NewErrors(common.NewStringSource("halt", ""))))
	return ast
}

type expr struct {
	celast.NavigableExpr

	// negated is true when this expr is part of a logical not branch,
	// ie !($expr)
	negated bool
}

func navigateAST(nav expr, depth int) []PredicateGroup {
	stack := []expr{nav}

	// found tracks all found groups
	found := []PredicateGroup{}
	// group tracks all groups found in the current branch.
	group := PredicateGroup{}

	// evaluate or expressions after all other branches, eg. "&&" operators or equality checks.
	orExpressions := []expr{}

	// Iterate through the stack, recursing down into each function call (eg. && branches).
	for len(stack) > 0 {
		item := stack[0]
		stack = stack[1:]

		switch item.Kind() {
		case celast.LiteralKind:
			// This is a literal string.  Do nothing.
		case celast.IdentKind:
			// This is a variable. Do nothing.
		case celast.CallKind:
			// Call kinds are the actual comparator operators, eg. >=, or &&.  These are specifically
			// what we're trying to parse, by taking the LHS and RHS of each opeartor then bringing
			// this up into a tree.

			// Everything within this branch is negated, if this is a logical not:
			// !(a == b).  This flips the negated field, ie !(foo == bar) becomes foo != bar,
			// whereas !(!(foo == bar)) stays the same.
			negated := item.AsCall().FunctionName() == operators.LogicalNot && !item.negated

			if item.AsCall().FunctionName() == operators.LogicalOr {
				// Handle or expressions after all other types.
				orExpressions = append(orExpressions, item)
				continue
			}

			// If any of the arguments are functions, recurse into this.  This ensures that we call
			// eg "&&" expressions correctly.
			for _, arg := range item.Children() {
				if arg.Kind() == celast.CallKind {
					stack = append(stack, expr{NavigableExpr: arg, negated: negated})
				}
			}

			// This is a function call, ie. a binary op equality check with two
			// arguments, or a ternary operator.
			//
			// We assume that this is being called with an ident as a comparator.
			// Dependign on the LHS/RHS type, we want to organize the kind into
			// a specific type of tree.
			predicate := callToPredicate(item, depth)
			if predicate == nil {
				continue
			}

			group.Predicates = append(group.Predicates, *predicate)
		}

		for _, child := range item.Children() {
			stack = append(stack, expr{NavigableExpr: child, negated: item.negated})
		}
	}

	// Handle or expressions towards the end, ensuring that we process each item within the leaf nodes first.
	// This lets us skip nested or expressions.
	for len(orExpressions) > 0 {
		item := orExpressions[0]
		orExpressions = orExpressions[1:]

		if depth > 0 || len(group.Predicates) > 0 {
			continue
		}

		// This is an or, branching.  Dive into each branch and handle each subexpression
		// separately.
		for _, arg := range item.Children() {
			// If we have items that have already been found or in the group, append
			// to the depth.  This way, top-level branches don't increase depth.
			nextDepth := depth
			if len(group.Predicates) > 0 {
				nextDepth += 1
			}
			found = append(found, navigateAST(expr{NavigableExpr: arg, negated: item.negated}, nextDepth)...)
		}
	}

	if len(group.Predicates) > 0 {
		// Deduplicate groups so that duplicate expressions don't count.
		seen := map[string]struct{}{}
		actual := PredicateGroup{}
		for _, g := range group.Predicates {
			if _, ok := seen[g.hash()]; ok {
				continue
			}
			seen[g.hash()] = struct{}{}
			actual.Predicates = append(actual.Predicates, g)
		}

		return append(found, actual)
	}

	return found
}

func callToPredicate(call expr, depth int) *Predicate {
	args := call.AsCall().Args()

	if len(args) != 2 {
		return nil
	}

	var (
		ident   string
		literal any
	)

	for _, item := range args {
		switch item.Kind() {
		case celast.IdentKind:
			ident = item.AsIdent()
		case celast.LiteralKind:
			literal = item.AsLiteral().Value()
		case celast.SelectKind:
			// This is an expression, ie. "event.data.foo"  Iterate from the root field upwards
			// to get the full ident.
			for item.Kind() == celast.SelectKind {
				sel := item.AsSelect()
				if ident == "" {
					ident = sel.FieldName()
				} else {
					ident = sel.FieldName() + "." + ident
				}
				item = sel.Operand()
			}
			ident = item.AsIdent() + "." + ident
		}
	}

	if ident == "" || literal == nil {
		return nil
	}

	fn := call.AsCall().FunctionName()

	// If this is in a negative expression (ie. `!(foo == bar)`), then invert the expression.
	if call.negated {
		fn = invert(fn)
	}

	// We always assume that the ident is on the LHS.  In the case of comparisons,
	// we need to switch these and the operator if the literal is on the RHS.  This lets
	// us normalize all expressions and ensure correct ordering within Predicates.
	//
	// NOTE: If we passed the specific function into a predicate result we would not have to do this;
	// we could literally call the function with its binary args.  All we have is the AST, and
	// we don't want to pass the raw AST into Predicate as it contains too much data.

	switch fn {
	case operators.Equals:
		// NOTE: NotEquals is _not_ supported.  This requires selecting all leaf nodes _except_
		// a given leaf, iterating over a tree.  We may as well execute every expressiona s the difference
		// is negligible.
	case operators.Greater, operators.GreaterEquals, operators.Less, operators.LessEquals:
		// We only support these operators for ints and floats, right now.
		// In the future we need to support scanning trees from a specific key
		// onwards.
		switch literal.(type) {
		case int64, float64:
			// Allowed
		default:
			return nil
		}

		// Must be LHS/RHS normalized.  The literal must be last.
		if args[1].Kind() == celast.LiteralKind {
			// The RHS is the value/literal.  This is safe.
			break
		}
		// Switch the operators to ensure evaluation of predicates is correct and consistent.
		fn = invert(fn)
	default:
		return nil
	}

	return &Predicate{
		Depth:    depth,
		Literal:  literal,
		Ident:    ident,
		Operator: fn,
	}
}

func invert(op string) string {
	switch op {
	case operators.Equals:
		return operators.NotEquals
	case operators.NotEquals:
		return operators.Equals
	case operators.Greater:
		return operators.Less
	case operators.GreaterEquals:
		return operators.LessEquals
	case operators.Less:
		return operators.Greater
	case operators.LessEquals:
		return operators.GreaterEquals
	default:
		return op
	}
}

// PredicateGroup represents a group of predicates that must all pass in order to execute the
// given expression.  For example, this might contain two predicates representing an expression
// with two operators combined with "&&".
//
// TODO: Change this to a struct with a tree for nested expressions (foo && (bar || baz).  This
// allows us to continue tree matching in a nested fashion, if we ever desire.
type PredicateGroup struct {
	// GroupID represents a unique ID for this group.  All predicates within this group
	// mst match in order for the expression to be evaluated;  we use the group ID to ensure
	// that all expressions are matched individually.
	GroupID groupID
	// Predicates are the predicates within this group.
	Predicates []Predicate
}

// Predicate represents a predicate that must evaluate to true in order for an expression to
// be considered as viable when checking an event.
type Predicate struct {
	Depth int
	// Literal represents the literal value that the operator compares against.
	Literal any
	// Ident is the ident we're comparing to, eg. the variable.
	Ident string
	// Operator is the binary operator being used.  NOTE:  This always assumes that the
	// ident is to the left of the operator, eg "event.data.value > 100".  If the value
	// is to the left of the operator, the operator will be switched
	// (ie. 100 > event.data.value becomes event.data.value < 100)
	Operator string
}

func (p Predicate) hash() string {
	sum := md5.Sum([]byte(fmt.Sprintf("%v", p)))
	return string(sum[:])
}

func (p Predicate) LiteralAsString() string {
	str, _ := p.Literal.(string)
	return str
}

func (p Predicate) TreeType() TreeType {
	// switch on type of literal AND operator type.  int64/float64 literals require
	// btrees, texts require ARTs.
	switch p.Literal.(type) {
	case string:
		return TreeTypeART
	case int64, float64:
		return TreeTypeBTree
	default:
		return TreeTypeNone
	}
}
