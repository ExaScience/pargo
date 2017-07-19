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
ComputeEffectiveThreshold determines a threshold for the
parallel.Range, speculative.Range, and sequential.Range groups of
functions.

It takes a low and high integer as input, with 0 <= low <= high, as
well as an input threshold designator.

Useful threshold parameter values are 1 to evenly divide up the range
across the avaliable logical CPUs (as determined by
runtime.GOMAXPROCS(0)); or 2 or higher to additionally divide that
number by the threshold parameter. Use 1 if you expect no load
imbalance, between 2 and 10 if you expect some load imbalance, or 10
or more if you expect even more load imbalance.

A threshold parameter value of 0 divides up the input range into
subranges of size 1 and yields the most fine-grained parallelism.
Fine-grained parallelism (with a threshold parameter of 0, or 2 or
higher) only pays off if the work per subrange is sufficiently large
to compensate for the scheduling overhead.

A threshold parameter value below zero can be used to specify the
subrange size directly, which becomes the absolute value of the
threshold parameter value.

More specifically:

If the input threshold is > 0, the return value is ceiling((high -
low) / (threshold * runtime.GOMAXPROCS(0))).

If the input threshold is == 0, the return value is 1.

If the input threshold is < 0, the return value is abs(threshold).
*/
func ComputeEffectiveThreshold(low, high, threshold int) int {
	if (low < 0) || (high < low) {
		panic(fmt.Sprintf("invalid range: %v:%v", low, high))
	}
	if threshold > 0 {
		threshold = ((high - low - 1) / (threshold * runtime.GOMAXPROCS(0))) + 1
	} else if threshold < 0 {
		return -1 * threshold
	}
	if threshold == 0 {
		threshold = 1
	}
	return threshold
}
