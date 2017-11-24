// Package pargo provides functions and data structures for expressing
// parallel algorithms. While Go is primarily designed for concurrent
// programming, it is also usable to some extent for parallel
// programming, and this library provides convenience functionality to
// turn otherwise sequential algorithms into parallel algorithms, with
// the goal to improve performance.
//
// For documentation that provides a more structured overview than is
// possible with Godoc, see the wiki at
// https://github.com/exascience/pargo/wiki
//
// Pargo provides the following subpackages:
//
// pargo/parallel provides simple functions for executing series of
// thunks or predicates, as well as thunks, predicates, or reducers
// over ranges in parallel. See also
// https://github.com/ExaScience/pargo/wiki/TaskParallelism
//
// pargo/speculative provides speculative implementations of most of
// the functions from pargo/parallel. These implementations not only
// execute in parallel, but also attempt to terminate early as soon as
// the final result is known. See also
// https://github.com/ExaScience/pargo/wiki/TaskParallelism
//
// pargo/sequential provides sequential implementations of all
// functions from pargo/parallel, for testing and debugging purposes.
//
// pargo/sort provides parallel sorting algorithms.
//
// pargo/sync provides an efficient parallel map implementation.
//
// pargo/pipeline provides functions and data structures to construct
// and execute parallel pipelines.
//
// Pargo has been influenced to various extents by ideas from Cilk,
// Threading Building Blocks, and Java's java.util.concurrent and
// java.util.stream packages. See
// http://supertech.csail.mit.edu/papers/steal.pdf for some
// theoretical background, and the sample chapter at
// https://mitpress.mit.edu/books/introduction-algorithms for a more
// practical overview of the underlying concepts.
package pargo

type (
	// A Thunk is a function that neither receives nor returns any
	// parameters.
	Thunk func()

	// An ErrThunk is a function that receives no parameters and returns
	// only an error value or nil.
	ErrThunk func() error

	// A RangeFunc is a function that receives a range from low to high,
	// with 0 <= low <= high. It is expected to cover the half-open
	// interval from low to high, including low but excluding high.
	RangeFunc func(low, high int)

	// An ErrRangeFunc is a function that receives a range from low to
	// high, with 0 <= low <= high, and returns an error value or nil.
	// It is expected to cover the half-open interval from low to high,
	// including low but excluding high.
	ErrRangeFunc func(low, high int) error

	// A Predicate is a function that receives no paramaters and returns
	// a bool.
	Predicate func() bool

	// An ErrPredicate is a function that receives no parameters and
	// returns a bool, and an error value or nil.
	ErrPredicate func() (bool, error)

	// A RangePredicate is a function that receives a range from low to
	// high, with 0 <= low <= high, and returns a bool. It is expected
	// to cover the half-open interval from low to high, including low
	// but excluding high.
	RangePredicate func(low, high int) bool

	// An ErrRangePredicate is a function that receives a range from low
	// to high, with 0 <= low <= high, and returns a bool, and an error
	// value or nil. It is expected to cover the half-open interval from
	// low to high, including low but excluding high.
	ErrRangePredicate func(low, high int) (bool, error)

	// A RangeReducer is a function that receives a range from low to
	// high, with 0 <= low <= high, and returns a result. It is expected
	// to cover the half-open interval from low to high, including low
	// but excluding high.
	RangeReducer func(low, high int) interface{}

	// An ErrRangeReducer is a function that receives a range from low
	// to high, with 0 <= low <= high, and returns a result, and an
	// error value or nil. It is expected to cover the half-open
	// interval from low to high, including low but excluding high.
	ErrRangeReducer func(low, high int) (interface{}, error)

	// A PairReducer is function that receives two values and returns a
	// result.
	PairReducer func(x, y interface{}) interface{}

	// An ErrPairReducer is a function that receives two values and
	// returns a result, and an error value or nil.
	ErrPairReducer func(x, y interface{}) (interface{}, error)

	// An IntRangeReducer is a function that receives a range from low
	// to high, with 0 <= low <= high, and returns an int result. It is
	// expected to cover the half-open interval from low to high,
	// including low but excluding high.
	IntRangeReducer func(low, high int) int

	// An ErrIntRangeReducer is a function that receives a range from low
	// to high, with 0 <= low <= high, and returns an int result, and an
	// error value or nil. It is expected to cover the half-open
	// interval from low to high, including low but excluding high.
	ErrIntRangeReducer func(low, high int) (int, error)

	// An IntPairReducer is function that receives two int values and
	// returns an int result.
	IntPairReducer func(x, y int) int

	// An ErrIntPairReducer is a function that receives two int values
	// and returns an int result, and an error value or nil.
	ErrIntPairReducer func(x, y int) (int, error)

	// A Float64RangeReducer is a function that receives a range from
	// low to high, with 0 <= low <= high, and returns a float64
	// result. It is expected to cover the half-open interval from low
	// to high, including low but excluding high.
	Float64RangeReducer func(low, high int) float64

	// An ErrFloat64RangeReducer is a function that receives a range
	// from low to high, with 0 <= low <= high, and returns a float64
	// result, and an error value or nil. It is expected to cover the
	// half-open interval from low to high, including low but excluding
	// high.
	ErrFloat64RangeReducer func(low, high int) (float64, error)

	// A Float64PairReducer is function that receives two float64 values
	// and returns a float64 result.
	Float64PairReducer func(x, y float64) float64

	// An ErrFloat64PairReducer is a function that receives two float64
	// values and returns a float64 result, and an error value or nil.
	ErrFloat64PairReducer func(x, y float64) (float64, error)

	// A StringRangeReducer is a function that receives a range from low
	// to high, with 0 <= low <= high, and returns a string result. It
	// is expected to cover the half-open interval from low to high,
	// including low but excluding high.
	StringRangeReducer func(low, high int) string

	// An ErrStringRangeReducer is a function that receives a range from
	// low to high, with 0 <= low <= high, and returns a string result,
	// and an error value or nil. It is expected to cover the half-open
	// interval from low to high, including low but excluding high.
	ErrStringRangeReducer func(low, high int) (string, error)

	// A StringPairReducer is function that receives two string values
	// and returns a string result.
	StringPairReducer func(x, y string) string

	// An ErrStringPairReducer is a function that receives two string
	// values and returns a string result, and an error value or nil.
	ErrStringPairReducer func(x, y string) (string, error)
)
