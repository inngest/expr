package expr

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
}
