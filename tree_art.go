package expr

import (
	"context"
	"fmt"
	"sync"
	"unsafe"

	art "github.com/plar/go-adaptive-radix-tree"
)

var (
	ErrInvalidType            = fmt.Errorf("invalid type for tree")
	ErrExpressionPartNotFound = fmt.Errorf("expression part not found")
)

func newArtTree() MatchingEngine {
	return &artTree{
		lock: &sync.RWMutex{},
		Tree: art.New(),
	}
}

type artTree struct {
	lock *sync.RWMutex
	art.Tree
	// TODO: Store a list of variable names used within adding items.
}

func (a *artTree) Type() EngineType {
	// TODO
	return EngineTypeNone
}

func (a *artTree) Match(ctx context.Context, input map[string]any) ([]*ExpressionPart, error) {
	/*
		// Iterate through all known variables/idents in the aggregate tree to see if
		// the data has those keys set.  If so, we can immediately evaluate the data with
		// the tree.
		//
		// TODO: we should iterate through the expression in a top-down order, ensuring that if
		// any of the top groups fail to match we quit early.
		for n, item := range a.artIdents {
			tree := item
			path := n
			eg.Go(func() error {
				x, err := jp.ParseString(path)
				if err != nil {
					return err
				}
				res := x.Get(data)
				if len(res) != 1 {
					return nil
				}

				cast, ok := res[0].(string)
				if !ok {
					// This isn't a string, so we can't compare within the radix tree.
					return nil
				}

				add(tree.Search(ctx, path, cast))
				return nil
			})
		}
	*/
	return nil, nil
}

func (a *artTree) Add(ctx context.Context, p ExpressionPart) error {
	str, ok := p.Predicate.Literal.(string)
	if !ok {
		return ErrInvalidType
	}

	key := artKeyFromString(str)

	// Don't allow multiple gorutines to modify the tree simultaneously.
	a.lock.Lock()
	defer a.lock.Unlock()

	val, ok := a.Tree.Search(key)
	if !ok {
		// Insert the ExpressionPart as-is.
		a.Insert(key, art.Value(&Leaf{
			Evals: []*ExpressionPart{&p},
		}))
		return nil
	}

	// Add the expressionpart as an expression matched by the already-existing
	// value.  Many expressions may match on the same string, eg. a user may set
	// up 3 matches for order ID "abc".  All 3 matches must be evaluated.
	next := val.(*Leaf)
	next.Evals = append(next.Evals, &p)
	a.Insert(key, next)
	return nil
}

func (a *artTree) Remove(ctx context.Context, p ExpressionPart) error {
	str, ok := p.Predicate.Literal.(string)
	if !ok {
		return ErrInvalidType
	}

	key := artKeyFromString(str)

	// Don't allow multiple gorutines to modify the tree simultaneously.
	a.lock.Lock()
	defer a.lock.Unlock()

	val, ok := a.Tree.Search(key)
	if !ok {
		return ErrExpressionPartNotFound
	}

	next := val.(*Leaf)
	// Remove the expression part from the leaf.
	for n, eval := range next.Evals {
		if p.Equals(*eval) {
			next.Evals = append(next.Evals[:n], next.Evals[n+1:]...)
			a.Insert(key, next)
			return nil
		}
	}

	return ErrExpressionPartNotFound
}

func (a *artTree) Search(ctx context.Context, variable string, input any) []*ExpressionPart {
	leaf, ok := a.searchLeaf(ctx, input)
	if !ok || leaf == nil {
		return nil
	}
	return leaf.Evals
}

func (a *artTree) searchLeaf(ctx context.Context, input any) (*Leaf, bool) {
	var key art.Key

	switch val := input.(type) {
	case art.Key:
		key = val
	case []byte:
		key = val
	case string:
		key = artKeyFromString(val)
	}

	if len(key) == 0 {
		return nil, false
	}

	val, ok := a.Tree.Search(key)
	if !ok {
		return nil, false
	}
	return val.(*Leaf), true
}

func artKeyFromString(str string) art.Key {
	// Zero-allocation string to byte conversion for speed.
	strd := unsafe.StringData(str)
	return art.Key(unsafe.Slice(strd, len(str)))

}
