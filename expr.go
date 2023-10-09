package expr

import (
	"context"
	"fmt"
)

type AggregateEvaluator interface {
	// Add adds an expression to the tree evaluator
	Add(ctx context.Context, eval Evaluable) error
	// Remove removes an expression from the aggregate evaluator
	Remove(ctx context.Context, eval Evaluable) error
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

func (a *aggregator) Add(ctx context.Context, eval Evaluable) error {
	groups, err := a.parser.Parse(ctx, eval.Expression())
	if err != nil {
		return err
	}

	for _, predicates := range groups {
		_ = predicates
	}

	// TODO: Iterate through each group and add the expression to tree
	// types specified.

	// TODO: Add each group to a tree.  The leaf node should point to the
	// expressions that match this leaf node (pause?)
	//
	// TODO: Pointer of checksums -> groups

	// on event entered:
	//
	// 1. load pauses
	// 2. pass event, pauses to aggregator
	// 3. load nodes for pause, if none, run expression
	// 4. evaluate tree nodes for pause against data, if ok, run expression

	fmt.Printf("%#v\n", groups)
	return fmt.Errorf("not implemented")
}

func (a *aggregator) Remove(ctx context.Context, eval Evaluable) error {
	return fmt.Errorf("not implemented")
}
