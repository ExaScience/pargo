/*
Package pargo provides functions and data structures for expressing
parallel algorithms. While Go is primarily designed for concurrent
programming, it is also usable to some extent for parallel
programming, and this library provides convenince functionality to
turn otherwise sequential algorithms into parallel algorithms, with
the goal to improve performance.

Pargo provides the following subpackages:

pargo/parallel provides simple functions for executing series of
thunks or predicates, as well as thunks or predicates over ranges in
parallel.

pargo/speculative provides speculative implementations of most of
the functions from pargo/parallel. These implementations not only
execute in parallel, but also attempt to terminate early as soon as
the final result is known.

pargo/sequential provides sequential implementations of all
functions from pargo/parallel, for testing and debugging purposes.

pargo/sort provides parallel sorting algorithms.

pargo/sync provides an efficient parallel map implementation.

pargo/pipeline provides functions and data structures to construct
and execute parallel pipelines.
*/
package pargo

import (
	"fmt"
	"runtime"
)

type (
	// A Thunk is a function that neither receives nor returns any
	// parameters.
	Thunk func()

	// An ErrThunk is a function that receives no parameters and returns
	// only an error value or nil.
	ErrThunk func() error

	// A RangeFunc is a function that receives a range from low to high,
	// with 0 <= low <= high.
	RangeFunc func(low, high int)

	// An ErrRangeFunc is a function that receives a range from low to
	// high, with 0 <= low <= high, and returns an error value or nil.
	ErrRangeFunc func(low, high int) error

	// A Predicate is a function that receives no paramaters and returns
	// a bool.
	Predicate func() bool

	// An ErrPredicate is a function that receives no parameters and
	// returns a bool, and an error value or nil.
	ErrPredicate func() (bool, error)

	// A RangePredicate is a function that receives a range from low to
	// high, with 0 <= low <= high, and returns a bool.
	RangePredicate func(low, high int) bool

	// An ErrRangePredicate is a function that receives a range from low
	// to high, with 0 <= low <= high, and returns a bool, and an error
	// value or nil.
	ErrRangePredicate func(low, high int) (bool, error)
)

/*
ComputeEffectiveThreshold determines a threshold for the
parallel.Range, speculative.Range, and sequential.Range groups of
functions.

It takes a low and high integer as input, with 0 <= low <= high, as
well as an input threshold designator.

If the input threshold is > 0, the return value is min(1, (high - low)
/ (threshold * runtime.GOMAXPROCS(0))).

If the input threshold is == 0, the return value is 1.

If the input threshold is < 0, the return value is abs(threshold).
*/
func ComputeEffectiveThreshold(low, high, threshold int) int {
	if (low < 0) || (high < low) {
		panic(fmt.Sprintf("invalid range: %v:%v", low, high))
	}
	if threshold > 0 {
		threshold = (high - low) / (threshold * runtime.GOMAXPROCS(0))
	} else if threshold < 0 {
		return -1 * threshold
	}
	if threshold == 0 {
		threshold = 1
	}
	return threshold
}
