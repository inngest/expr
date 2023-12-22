package expr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/cel-go/common/operators"
)

// errTreeUnimplemented is used while we develop the aggregate tree library when trees
// are not yet implemented.
var errTreeUnimplemented = fmt.Errorf("tree type unimplemented")

// ExpressionEvaluator is a function which evalues an expression given input data, returning
// a boolean and error.
type ExpressionEvaluator func(ctx context.Context, e Evaluable, input map[string]any) (bool, error)

// AggregateEvaluator represents a group of expressions that must be evaluated for a single
// event received.
//
// An AggregateEvaluator instance exists for every event name being matched.
type AggregateEvaluator interface {
	// Add adds an expression to the tree evaluator.  This returns true
	// if the expression is aggregateable, or false if the expression will be
	// evaluated each time an event is received.
	Add(ctx context.Context, eval Evaluable) (bool, error)

	// Remove removes an expression from the aggregate evaluator
	Remove(ctx context.Context, eval Evaluable) error

	// AggregateMatch returns all expression parts which are evaluable given the input data.
	AggregateMatch(ctx context.Context, data map[string]any) ([]ExpressionPart, error)

	// Evaluate checks input data against all exrpesssions in the aggregate in an optimal
	// manner, only evaluating expressions when necessary (based off of tree matching).
	//
	// Note that any expressions added that cannot be evaluated optimally by trees
	// are evaluated every time this function is called.
	//
	// Evaluate returns all matching Evaluables, plus the total number of evaluations
	// executed.
	Evaluate(ctx context.Context, data map[string]any) ([]Evaluable, int32, error)

	// Len returns the total number of aggregateable and constantly matched expressions
	// stored in the evaluator.
	Len() int

	// AggregateableLen returns the number of expressions being matched by aggregated trees.
	AggregateableLen() int

	// ConstantLen returns the total number of expressions that must constantly
	// be matched due to non-aggregateable clauses in their expressions.
	ConstantLen() int
}

func NewAggregateEvaluator(parser TreeParser, eval ExpressionEvaluator) AggregateEvaluator {
	return &aggregator{
		eval:      eval,
		parser:    parser,
		artIdents: map[string]PredicateTree{},
		lock:      &sync.RWMutex{},
	}
}

type Evaluable interface {
	// Expression returns an expression as a raw string.
	Expression() string
}

type aggregator struct {
	eval   ExpressionEvaluator
	parser TreeParser

	artIdents map[string]PredicateTree
	lock      *sync.RWMutex

	len int32

	// constants tracks evaluable instances that must always be evaluated, due to
	// the expression containing non-aggregateable clauses.
	constants []Evaluable
}

// Len returns the total number of aggregateable and constantly matched expressions
// stored in the evaluator.
func (a aggregator) Len() int {
	return int(a.len) + len(a.constants)
}

// AggregateableLen returns the number of expressions being matched by aggregated trees.
func (a aggregator) AggregateableLen() int {
	return int(a.len)
}

// ConstantLen returns the total number of expressions that must constantly
// be matched due to non-aggregateable clauses in their expressions.
func (a aggregator) ConstantLen() int {
	return len(a.constants)
}

func (a *aggregator) Evaluate(ctx context.Context, data map[string]any) ([]Evaluable, int32, error) {
	// on event entered:
	//
	// 1. load pauses
	// 2. pass event, pauses to aggregator
	// 3. load nodes for pause, if none, run expression
	// 4. evaluate tree nodes for pause against data, if ok, run expression

	var (
		err     error
		matched = int32(0)
		result  = []Evaluable{}
	)

	// TODO: Concurrently match constant expressions using a semaphore for capacity.
	for _, expr := range a.constants {
		atomic.AddInt32(&matched, 1)
		ok, evalerr := a.eval(ctx, expr, data)
		if evalerr != nil {
			err = errors.Join(err, evalerr)
			continue
		}
		if ok {
			result = append(result, expr)
		}
	}

	matches, merr := a.AggregateMatch(ctx, data)
	if merr != nil {
		err = errors.Join(err, merr)
	}

	// TODO: Each match here is a success.  When other trees and operators which are walkable
	// are added (eg. >= operators on strings), ensure that we find the correct number of matches
	// for each group ID and then skip evaluating expressions if so.
	for _, match := range matches {
		atomic.AddInt32(&matched, 1)
		ok, evalerr := a.eval(ctx, match.Evaluable, data)
		if evalerr != nil {
			err = errors.Join(err, evalerr)
			continue
		}
		if ok {
			result = append(result, match.Evaluable)
		}
	}

	return result, matched, nil
}

func (a *aggregator) AggregateMatch(ctx context.Context, data map[string]any) ([]ExpressionPart, error) {
	return a.aggregateMatch(ctx, data, "")
}

func (a *aggregator) aggregateMatch(ctx context.Context, data map[string]any, prefix string) ([]ExpressionPart, error) {
	result := []ExpressionPart{}
	for k, v := range data {
		switch cast := v.(type) {
		case map[string]any:
			// Recurse into the map to pluck out nested idents, eg. "event.data.account.id"
			evals, err := a.aggregateMatch(ctx, cast, prefix+k+".")
			if err != nil {
				return nil, err
			}
			if len(evals) > 0 {
				result = append(result, evals...)
			}
		case string:
			a.lock.RLock()
			tree, ok := a.artIdents[prefix+k]
			a.lock.RUnlock()
			if !ok {
				continue
			}
			found, ok := tree.Search(ctx, cast)
			if !ok {
				continue
			}
			result = append(result, found.Evals...)
		default:
			continue
		}

	}
	return result, nil
}

func (a *aggregator) Add(ctx context.Context, eval Evaluable) (bool, error) {
	parsed, err := a.parser.Parse(ctx, eval.Expression())
	if err != nil {
		return false, err
	}

	aggregateable := true
	for _, g := range parsed.RootGroups() {
		ok, err := a.addGroup(ctx, g, eval)
		if err != nil {
			return false, err
		}
		if !ok && aggregateable {
			// Add this expression as a constant once.
			a.constants = append(a.constants, eval)
			aggregateable = false
		}
	}

	// Track the number of added expressions correctly.
	if aggregateable {
		atomic.AddInt32(&a.len, 1)
	}
	return aggregateable, nil
}

func (a *aggregator) addGroup(ctx context.Context, node *Node, eval Evaluable) (bool, error) {
	if len(node.Ors) > 0 {
		// If there are additional branches, don't bother to add this to the aggregate tree.
		// Mark this as a non-exhaustive addition and skip immediately.
		//
		// TODO: Allow ORs _only if_ the ORs are not nested, eg. the ORs are basic predicate
		// groups that themselves have no branches.
		return false, nil
	}

	// Merge all of the nodes together and check whether each node is aggregateable.
	all := append(node.Ands, node)
	for _, n := range all {
		if !n.HasPredicate() || len(n.Ors) > 0 {
			// Don't handle sub-branching for now.
			return false, nil
		}
		if !isAggregateable(n) {
			return false, nil
		}
	}

	// Create a new group ID which tracks the number of expressions that must match
	// within this group in order for the group to pass.
	//
	// This includes ALL ands, plus at least one OR.
	//
	// When checking an incoming event, we match the event against each node's
	// ident/variable.  Using the group ID, we can see if we've matched N necessary
	// items from the same identifier.  If so, the evaluation is true.
	groupID := newGroupID(uint16(len(all)))
	for _, n := range all {
		err := a.addNode(ctx, n, groupID, eval)
		if err == errTreeUnimplemented {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (a *aggregator) addNode(ctx context.Context, n *Node, gid groupID, eval Evaluable) error {
	// Don't allow anything to update in parallel.
	a.lock.Lock()
	defer a.lock.Unlock()

	// Each node is aggregateable, so add this to the map for fast filtering.
	switch n.Predicate.TreeType() {
	case TreeTypeART:
		tree, ok := a.artIdents[n.Predicate.Ident]
		if !ok {
			tree = newArtTree()
		}
		err := tree.Add(ctx, ExpressionPart{
			GroupID:   gid,
			Predicate: *n.Predicate,
			Evaluable: eval,
		})
		if err != nil {
			return err
		}
		a.artIdents[n.Predicate.Ident] = tree
		return nil
	}
	return errTreeUnimplemented
}

func (a *aggregator) Remove(ctx context.Context, eval Evaluable) error {
	// TODO
	return fmt.Errorf("not implemented")
}

func isAggregateable(n *Node) bool {
	if n.Predicate == nil {
		return true
	}
	switch n.Predicate.Literal.(type) {
	case string:
		if n.Predicate.Operator == operators.NotEquals {
			// NOTE: NotEquals is _not_ supported.  This requires selecting all leaf nodes _except_
			// a given leaf, iterating over a tree.  We may as well execute every expressiona s the difference
			// is negligible.
			return false
		}
		// Right now, we only support equality checking.
		//
		// TODO: Add GT(e)/LT(e) matching with tree iteration.
		return n.Predicate.Operator == operators.Equals
	case int64, float64:
		// TODO: Add binary tree matching for ints/floats
		return false
	default:
		return false
	}
}
