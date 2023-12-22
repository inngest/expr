package expr

import (
	"context"
	"crypto/md5"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
)

// TreeParser parses an expression into a tree, with a root node and branches for
// each subsequent OR or AND expression.
type TreeParser interface {
	Parse(ctx context.Context, expr string) (*ParsedExpression, error)
}

// NewTreeParser returns a new tree parser for a given *cel.Env
func NewTreeParser(env *cel.Env) (TreeParser, error) {
	parser := &parser{
		env: env,
	}
	return parser, nil
}

type parser struct{ env *cel.Env }

func (p *parser) Parse(ctx context.Context, expression string) (*ParsedExpression, error) {
	ast, issues := p.env.Parse(expression)
	if issues != nil {
		return nil, issues.Err()
	}

	node := newNode()
	_, err := navigateAST(
		expr{
			NavigableExpr: celast.NavigateAST(ast.NativeRep()),
		},
		node,
	)
	if err != nil {
		return nil, err
	}
	node.normalize()
	return &ParsedExpression{Root: *node}, nil
}

// ParsedExpression represents a parsed CEL expression into our higher-level AST.
//
// Expressions are simplified and canonicalized, eg. !(a == b) becomes a != b and
// !(b <= a) becomes (a > b).
type ParsedExpression struct {
	Root Node

	// Exhaustive represents whether the parsing is exhaustive, or whether
	// specific CEL macros or functions were used which are not supported during
	// parsing.
	//
	// TODO: Allow parsing of all macros/overloads/functions and filter during
	// traversal.
	//
	// Exhaustive bool
}

// RootGroups returns the top-level matching groups within an expression.  This is a small
// utility to check the number of matching groups easily.
func (p ParsedExpression) RootGroups() []*Node {
	if len(p.Root.Ands) == 0 && len(p.Root.Ors) > 1 {
		return p.Root.Ors
	}
	return []*Node{&p.Root}
}

// PredicateGroup represents a group of predicates that must all pass in order to execute the
// given expression.  For example, this might contain two predicates representing an expression
// with two operators combined with "&&".
//
// MATCHING & EVALUATION
//
// A node evaluates to true if ALL of the following conditions are met:
//
// - All of the ANDS are truthy.
// - One or more of the ORs are truthy
//
// In essence, if there are ANDs and ORs, the ORs are implicitly added to ANDs:
//
//	(A && (B || C))
//
// This requres A *and* either B or C, and so we require all ANDs plus at least one node
// from OR to evaluate to true
type Node struct {
	// Ands contains predicates at this level of the expression that are joined together
	// with an && operator.  All nodes in this set must evaluate to true in order for this
	// node in the expression to be truthy.
	//
	// Note that if any on of the Ors nodes evaluates to true, this node is truthy, regardless
	// of whether the Ands set evaluates to true.
	Ands []*Node `json:"and,omitempty"`

	// Ors represents matching OR groups within this expression.  For example, in
	// the expression `a == b && (c == 1 || d == 1)` the top-level predicate group will
	// have a child group containing the parenthesis sub-expression.
	//
	// At least one of the Or node's sub-trees must evaluate to true for the node to
	// be truthy, alongside all Ands.
	Ors []*Node `json:"or,omitempty"`

	// Predicate represents the predicate for this node.  This must evaluate to true in order
	// for the expression to be truthy.
	//
	// If this is nil, this is a parent container for a series of AND or Or checks.
	Predicate *Predicate
}

func (n Node) HasPredicate() bool {
	if n.Predicate == nil {
		return false
	}
	return n.Predicate.Operator != ""
}

func (n *Node) normalize() {
	if n.Predicate != nil {
		return
	}
	if len(n.Ands) == 0 {
		n.Ands = nil
	}
	if len(n.Ors) == 0 {
		n.Ors = nil
	}
	if len(n.Ands) == 1 && len(n.Ors) == 0 {
		// Check to see if the child is an orphan.
		child := n.Ands[0]
		if len(child.Ands) == 0 && len(child.Ors) == 0 && child.Predicate != nil {
			n.Predicate = child.Predicate
			n.Ands = nil
			return
		}
	}
}

func (n *Node) String() string {
	return n.string(0)
}

func (n *Node) string(depth int) string {
	builder := strings.Builder{}

	// If there are both ANDs and ORs in this node, wrap the entire
	// thing in parenthesis to minimize ambiguity.
	writeOuterParen := (len(n.Ands) >= 1 && len(n.Ors) >= 1 && depth > 0) ||
		(len(n.Ands) > 1 && depth > 0) // Chain multiple joined ands together when nesting.

	if writeOuterParen {
		builder.WriteString("(")
	}

	for i, and := range n.Ands {
		builder.WriteString(and.string(depth + 1))
		if i < len(n.Ands)-1 {
			// If this is not the last and, write an ampersand.
			builder.WriteString(" && ")
		}
	}

	// Tie the ANDs and ORs together with an and operand.
	if len(n.Ands) > 0 && len(n.Ors) > 0 {
		builder.WriteString(" && ")
	}

	// Write the "or" groups out, concatenated each with an Or operand..
	//
	// We skip this for the top-level node to remove extra meaningless
	// parens that wrap the entire expression

	writeOrParen := len(n.Ors) > 1 && depth > 0 || // Always chain nested ors
		len(n.Ors) > 1 && len(n.Ands) >= 1

	if writeOrParen {
		builder.WriteString("(")
	}
	for i, or := range n.Ors {
		builder.WriteString(or.string(depth + 1))
		if i < len(n.Ors)-1 {
			// If this is not the last and, write an Or operand..
			builder.WriteString(" || ")
		}
	}
	if writeOrParen {
		builder.WriteString(")")
	}

	// Write the actual clause.
	if n.Predicate != nil {
		builder.WriteString(n.Predicate.String())
	}

	// And finally, the outer paren.
	if writeOuterParen {
		builder.WriteString(")")
	}

	return builder.String()
}

func newNode() *Node {
	return &Node{}
}

// Predicate represents a predicate that must evaluate to true in order for an expression to
// be considered as viable when checking an event.
//
// This is equivalent to a CEL overload/function/macro.
type Predicate struct {
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

func (p Predicate) String() string {
	switch str := p.Literal.(type) {
	case string:
		return fmt.Sprintf("%s %s %v", p.Ident, strings.ReplaceAll(p.Operator, "_", ""), strconv.Quote(str))
	default:
		return fmt.Sprintf("%s %s %v", p.Ident, strings.ReplaceAll(p.Operator, "_", ""), p.Literal)
	}

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

// expr is wrapper around the CEL AST which stores parsing-related data.
type expr struct {
	celast.NavigableExpr

	// negated is true when this expr is part of a logical not branch,
	// ie !($expr)
	negated bool
}

// navigateAST iterates through an expression AST, parsing predicates into groups.
//
// It does this by iterating through the expression, amending the current `group` until
// an or expression is found.  When an or expression is found, we create another group which
// is mutated by the iteration.
func navigateAST(nav expr, parent *Node) ([]*Node, error) {
	// on the very first call to navigateAST, ensure that we set the first node
	// inside the nodemap.
	result := []*Node{}

	// Iterate through the stack, recursing down into each function call (eg. && branches).
	stack := []expr{nav}
	for len(stack) > 0 {
		item := stack[0]
		stack = stack[1:]

		switch item.Kind() {
		case celast.LiteralKind:
			// This is a literal. Do nothing, as this is always true.
		case celast.IdentKind:
			// This is a variable. DO nothing.
			// predicate := Predicate{
			// 	Literal:  true,
			// 	Ident:    item.AsIdent(),
			// 	Operator: operators.Equals,
			// }
			// current.Predicates = append(current.Predicates, predicate)
		case celast.CallKind:
			// Call kinds are the actual comparator operators, eg. >=, or &&.  These are specifically
			// what we're trying to parse, by taking the LHS and RHS of each opeartor then bringing
			// this up into a tree.

			fn := item.AsCall().FunctionName()

			// Firstly, if this is a logical not, everything within this branch is negated:
			// !(a == b).  This flips the negated field, ie !(foo == bar) becomes foo != bar,
			// whereas !(!(foo == bar)) stays the same.
			if fn == operators.LogicalNot {
				// Immediately navigate into this single expression.
				child := item.Children()[0]
				stack = append(stack, expr{
					NavigableExpr: child,
					negated:       !item.negated,
				})
				continue
			}

			if fn == operators.LogicalOr {
				for _, or := range peek(item, operators.LogicalOr) {
					// Ors modify new nodes.  Assign a new Node to each
					// Or entry.
					newParent := newNode()

					// For each item in the stack, recurse into that AST.
					_, err := navigateAST(or, newParent)
					if err != nil {
						return nil, err
					}

					// Ensure that we remove any redundant parents generated.
					newParent.normalize()
					if parent.Ors == nil {
						parent.Ors = []*Node{}
					}
					parent.Ors = append(parent.Ors, newParent)
				}
				continue
			}

			// For each &&, create a new child node in the .And field of the current
			// high-level AST.
			if item.AsCall().FunctionName() == operators.LogicalAnd {
				stack = append(stack, peek(item, operators.LogicalAnd)...)
				continue
			}

			// This is a function call, ie. a binary op equality check with two
			// arguments, or a ternary operator.
			//
			// We assume that this is being called with an ident as a comparator.
			// Dependign on the LHS/RHS type, we want to organize the kind into
			// a specific type of tree.
			predicate := callToPredicate(item.NavigableExpr, item.negated)
			if predicate == nil {
				continue
			}

			child := &Node{
				Predicate: predicate,
			}
			child.normalize()
			result = append(result, child)
		}
	}

	parent.Ands = result
	return result, nil
}

// peek recurses through nested operators (eg. a && b && c), grouping all operators
// together into an array.  This stops after exhausting matching operators.
func peek(nav expr, operator string) []expr {
	// Recurse into the children matching all consecutive child types,
	// eg. all ANDs, or all ORs.
	//
	// For each non-operator found, add it to a return list.
	stack := []expr{nav}
	result := []expr{}
	for len(stack) > 0 {
		item := stack[0]
		stack = stack[1:]

		if item.AsCall().FunctionName() == operator {
			children := item.Children()
			stack = append(
				stack,
				expr{
					NavigableExpr: children[0],
					negated:       nav.negated,
				},
				expr{
					NavigableExpr: children[1],
					negated:       nav.negated,
				},
			)
			continue
		}
		// This is not an AND or OR call, so don't recurse into it - leave this
		// as a result value for handling.
		//
		// In this case, we either have operators (>=) or OR tests.
		result = append(result, item)
	}
	return result
}

// callToPredicate transforms a function call within an expression (eg `>`) into
// a Predicate struct for our matching engine.  It ahandles normalization of
// LHS/RHS plus inversions.
func callToPredicate(item celast.Expr, negated bool) *Predicate {
	fn := item.AsCall().FunctionName()
	if fn == operators.LogicalAnd || fn == operators.LogicalOr {
		// Quit early, as we descend into these while iterating through the tree when calling this.
		return nil
	}

	args := item.AsCall().Args()
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

	// If this is in a negative expression (ie. `!(foo == bar)`), then invert the expression.
	if negated {
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
	case operators.Equals, operators.NotEquals:
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
		case string:
			// Also allowed, eg. for matching datetime strings or filtering ULIDs after
			// a specific string.
		default:
			return nil
		}

		// Ensure we normalize `a > 100` and `100 < a` so that the literal is last.
		// This ensures we treat all expressions the same.
		if args[0].Kind() == celast.LiteralKind {
			// Switch the operators to ensure evaluation of predicates is correct and consistent.
			fn = normalize(fn)
		}
	default:
		return nil
	}

	return &Predicate{
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
		// NOTE: Negating a > turns this into <=.  5 >= 5 == true, and only 5 < 5
		// negates this.
		return operators.LessEquals
	case operators.GreaterEquals:
		return operators.Less
	case operators.Less:
		return operators.GreaterEquals
	case operators.LessEquals:
		return operators.Greater
	default:
		return op
	}
}

func normalize(op string) string {
	switch op {
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
