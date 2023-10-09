package expr

import "context"

type TreeType int

const (
	TreeTypeNone TreeType = iota

	TreeTypeART
	TreeTypeBTree
)

// PredicateTree represents a tree which matches a predicate over
// N expressions.
//
// For example, an expression may check string equality using an
// ART tree, while LTE operations may check against a b+-tree.
type PredicateTree interface {
	Add(ctx context.Context, p Predicate) error
}

// leaf represents the leaf within a tree.  This stores all expressions
// which match the given expression.
//
// For example, adding two expressions each matching "event.data == 'foo'"
// in an ART creates a leaf node with both evaluable expressions stored
// in Evals
//
// Note that there are many sub-clauses which need to be matched.  Each
// leaf is a subset of a full expression.  Therefore,

type Leaf struct {
	Evals []ExpressionPart
}

type ExpressionPart struct {
	GroupID   int32
	Evaluable Evaluable
	// TODO: How do we know this is part of a set
}
