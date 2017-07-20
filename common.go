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

Pargo has been influenced to various extents by ideas from Cilk,
Threading Building Blocks, and Java's java.util.concurrent and
java.util.stream packages. See
http://supertech.csail.mit.edu/papers/steal.pdf for some theoretical
background, and the sample chapter at
https://mitpress.mit.edu/books/introduction-algorithms for a more
practical overview of the underlying concepts.
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
ComputeNofBatches is an internal function used by the parallel.Range,
speculative.Range, and sequential.Range groups of functions.
*/
func ComputeNofBatches(low, high, n int) (batches int) {
	switch size := high - low; {
	case size > 0:
		switch {
		case n == 0:
			batches = 2 * runtime.GOMAXPROCS(0)
		case n > 0:
			batches = n
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
		if batches > size {
			batches = size
		}
	case size == 0:
		batches = 1
	default:
		panic(fmt.Sprintf("invalid range: %v:%v", low, high))
	}
	return
}
