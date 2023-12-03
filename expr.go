package expr

import (
	"context"
	"fmt"
)

// AggregateEvaluator represents a group of expressions that must be evaluated for a single
// event received.
type AggregateEvaluator interface {
	// Add adds an expression to the tree evaluator
	Add(ctx context.Context, eval Evaluable) error
	// Remove removes an expression from the aggregate evaluator
	Remove(ctx context.Context, eval Evaluable) error

	// Evaluate checks input data against all exrpesssions in the aggregate in an optimal
	// manner, only evaluating expressions when necessary (based off of tree matching).
	//
	// This returns a list of evaluable expressions that match the given input.
	Evaluate(ctx context.Context, data map[string]any) ([]Evaluable, error)
}

func NewAggregateEvaluator(parser TreeParser) AggregateEvaluator {
	return &aggregator{
		parser: parser,
	}
}

type Evaluable interface {
	// Expression returns an expression as a raw string.
	Expression() string
}

type aggregator struct {
	parser TreeParser
}

func (a *aggregator) Evaluate(ctx context.Context, data map[string]any) ([]Evaluable, error) {
	return nil, nil
}

func (a *aggregator) Add(ctx context.Context, eval Evaluable) error {
	parsed, err := a.parser.Parse(ctx, eval.Expression())
	if err != nil {
		return err
	}

	_ = parsed

	// TODO: Iterate through each group and add the expression to tree
	// types specified.

	// TODO: Add each group to a tree.  The leaf node should point to the
	// expressions that match this leaf node (pause?)

	// TODO: Pointer of checksums -> groups

	// on event entered:
	//
	// 1. load pauses
	// 2. pass event, pauses to aggregator
	// 3. load nodes for pause, if none, run expression
	// 4. evaluate tree nodes for pause against data, if ok, run expression

	fmt.Printf("%#v\n", parsed)
	return fmt.Errorf("not implemented")
}

func (a *aggregator) Remove(ctx context.Context, eval Evaluable) error {
	return fmt.Errorf("not implemented")
}
