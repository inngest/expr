package expr

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/cel-go/common/operators"
	"github.com/ohler55/ojg/jp"
	"golang.org/x/sync/errgroup"
)

func newStringEqualityMatcher() MatchingEngine {
	return &stringLookup{
		lock:       &sync.RWMutex{},
		vars:       map[string]struct{}{},
		equality:   variableMap{},
		inequality: inequalityMap{},
	}
}

type variableMap map[string][]*StoredExpressionPart
type inequalityMap map[string]variableMap

// stringLookup represents a very dumb lookup for string equality matching within
// expressions.
//
// This does nothing fancy:  it takes strings from expressions then adds them a hashmap.
// For any incoming event, we take all strings and store them in a hashmap pointing to
// the ExpressionPart they match.
//
// Note that strings are (obviuously) hashed to store in a hashmap, leading to potential
// false postivies.  Because the aggregate merging filters invalid expressions, this is
// okay:  we still evaluate potential matches at the end of filtering.
//
// Due to this, we do not care about variable names for each string.  Matching on string
// equality alone down the cost of evaluating non-matchingexpressions by orders of magnitude.
type stringLookup struct {
	lock *sync.RWMutex

	// vars stores variable names seen within expressions.
	vars map[string]struct{}
	// equality stores all strings referenced within expressions, mapped to the expression part.
	// this performs string equality lookups.
	equality variableMap

	// inequality stores all variables referenced within inequality checks mapped to the value,
	// which is then mapped to expression parts.
	//
	// this lets us quickly map neq in a fast manner
	inequality    inequalityMap
	inequalityLen int64
}

func (s stringLookup) Type() EngineType {
	return EngineTypeStringHash
}

func (n *stringLookup) Match(ctx context.Context, input map[string]any) ([]*StoredExpressionPart, []*StoredExpressionPart, error) {
	l := &sync.Mutex{}

	matched, denied := map[uint64]*StoredExpressionPart{}, map[uint64]*StoredExpressionPart{}
	eg := errgroup.Group{}

	search := func(ctx context.Context, path, value string) {
		m, d := n.Search(ctx, path, value)
		l.Lock()

		// Only match each predicate once.  Not equals may match on any particular
		// key.
		for _, v := range m {
			matched[v.PredicateID] = v
		}
		for _, v := range d {
			denied[v.PredicateID] = v
		}

		l.Unlock()
	}

	paths := map[string]struct{}{}

	// First, handle equality amtching.
	for item := range n.vars {
		path := item
		paths[path] = struct{}{}
		eg.Go(func() error {
			x, err := jp.ParseString(path)
			if err != nil {
				return err
			}

			res := x.Get(input)
			if len(res) == 0 {
				search(ctx, path, "")
				return nil
			}

			str, ok := res[0].(string)
			if !ok {
				search(ctx, path, "")
				return nil
			}

			search(ctx, path, str)
			return nil
		})
	}

	// Then, iterate through the

	err := eg.Wait()

	matchS := make([]*StoredExpressionPart, len(matched))
	i := 0
	for _, v := range matched {
		matchS[i] = v
		i++
	}

	deniedS := make([]*StoredExpressionPart, len(denied))
	for _, v := range denied {
		deniedS[i] = v
		i++
	}

	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("matchS")
	spew.Dump(matchS)
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")

	return matchS, deniedS, err
}

// Search returns all ExpressionParts which match the given input, ignoring the variable name
// entirely.  It also returns any inequality matches
func (n *stringLookup) Search(ctx context.Context, variable string, input any) (matched, denied []*StoredExpressionPart) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	str, ok := input.(string)
	if !ok {
		return nil, nil
	}

	hashedInput := n.hash(str)

	// Iterate through all matching values, and only take those expressions which match our
	// current variable name.
	filtered := make([]*StoredExpressionPart, len(n.equality))
	i := 0
	for _, part := range n.equality[hashedInput] {
		if part.Ident != nil && *part.Ident != variable {
			// The variables don't match.
			continue
		}
		filtered[i] = part
		i++
	}
	filtered = filtered[0:i]

	// With !=, we want every expression *other than* the match to be valid.
	inequality := make(
		[]*StoredExpressionPart,
		// Utility to make the right sized array - the total number of
		// stored inequality expressions minus the ignored (matching) size.
		int(n.inequalityLen)-len(n.inequality[hashedInput]),
	)

	i = 0
	for k, v := range n.inequality {
		if k == hashedInput {
			continue
		}
		for _, part := range v {
			if part.Ident != nil && *part.Ident != variable {
				// The variables don't match.
				continue
			}
			inequality[i] = part
			i++
		}
	}

	return append(filtered, inequality[0:i]...), n.inequality[n.hash(str)]
}

// hash hashes strings quickly via xxhash.  this provides a _somewhat_ collision-free
// lookup while reducing memory for strings.  note that internally, go maps store the
// raw key as a string, which uses extra memory.  by compressing all strings via this
// hash, memory usage grows predictably even with long strings.
func (n *stringLookup) hash(input string) string {
	ui := xxhash.Sum64String(input)
	return strconv.FormatUint(ui, 36)
}

func (n *stringLookup) Add(ctx context.Context, p ExpressionPart) error {
	// Primarily, we match `$string == lit` and `$string != lit`.
	//
	// Equality operators are easy:  link the matching string to
	// expressions that are candidates.
	switch p.Predicate.Operator {
	case operators.Equals:
		n.lock.Lock()
		defer n.lock.Unlock()
		val := n.hash(p.Predicate.LiteralAsString())

		n.vars[p.Predicate.Ident] = struct{}{}

		if _, ok := n.equality[val]; !ok {
			n.equality[val] = []*StoredExpressionPart{p.ToStored()}
			return nil
		}
		n.equality[val] = append(n.equality[val], p.ToStored())

	case operators.NotEquals:
		n.lock.Lock()
		defer n.lock.Unlock()
		val := n.hash(p.Predicate.LiteralAsString())

		n.vars[p.Predicate.Ident] = struct{}{}

		atomic.AddInt64(&n.inequalityLen, 1)

		if _, ok := n.inequality[val]; !ok {
			n.inequality[val] = []*StoredExpressionPart{p.ToStored()}
			return nil
		}
		n.inequality[val] = append(n.inequality[val], p.ToStored())

		return nil
	default:
		return fmt.Errorf("StringHash engines only support string equality/inequality")
	}

	return nil
}

func (n *stringLookup) Remove(ctx context.Context, p ExpressionPart) error {
	switch p.Predicate.Operator {
	case operators.Equals:
		n.lock.Lock()
		defer n.lock.Unlock()

		val := n.hash(p.Predicate.LiteralAsString())

		coll, ok := n.equality[val]
		if !ok {
			// This could not exist as there's nothing mapping this variable for
			// the given event name.
			return ErrExpressionPartNotFound
		}

		// Remove the expression part from the leaf.
		for i, eval := range coll {
			if p.EqualsStored(eval) {
				coll = append(coll[:i], coll[i+1:]...)
				n.equality[val] = coll
				return nil
			}
		}

		return ErrExpressionPartNotFound

	case operators.NotEquals:
		n.lock.Lock()
		defer n.lock.Unlock()

		val := n.hash(p.Predicate.LiteralAsString())

		coll, ok := n.inequality[val]
		if !ok {
			// This could not exist as there's nothing mapping this variable for
			// the given event name.
			return ErrExpressionPartNotFound
		}

		// Remove the expression part from the leaf.
		for i, eval := range coll {
			if p.EqualsStored(eval) {
				atomic.AddInt64(&n.inequalityLen, -1)
				coll = append(coll[:i], coll[i+1:]...)
				n.inequality[val] = coll
				return nil
			}
		}
		return ErrExpressionPartNotFound

	default:
		return fmt.Errorf("StringHash engines only support string equality/inequality")
	}
}
