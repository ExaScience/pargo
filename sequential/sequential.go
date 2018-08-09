// Package sequential provides sequential implementations of the
// functions provided by the parallel and speculative packages. This
// is useful for testing and debugging.
//
// It is not recommended to use the implementations of this package
// for any other purpose, because they are almost certainly too
// inefficient for regular sequential programs.
package sequential

import (
	"fmt"

	"github.com/exascience/pargo/internal"
)

// Do receives zero or more thunks and executes them sequentially.
func Do(thunks ...func()) {
	for _, thunk := range thunks {
		thunk()
	}
}

// ErrDo receives zero or more thunks and executes them sequentially.
func ErrDo(thunks ...func() error) (err error) {
	for _, thunk := range thunks {
		nerr := thunk()
		if err == nil {
			err = nerr
		}
	}
	return
}

// And receives zero or more predicate functions and executes them
// sequentially, combining all return values with the && operator,
// with true as the default return value.
func And(predicates ...func() bool) (result bool) {
	result = true
	for _, predicate := range predicates {
		result = result && predicate()
	}
	return
}

// Or receives zero or more predicate functions and executes them
// sequentially, combining all return values with the || operator,
// with false as the default return value.
func Or(predicates ...func() bool) (result bool) {
	result = false
	for _, predicate := range predicates {
		result = result || predicate()
	}
	return
}

// ErrAnd receives zero or more predicate functions and executes
// them sequentially, combining all return values with the &&
// operator, with true as the default return value. ErrAnd also
// returns the left-most error value that is different from nil as a
// second return value.
func ErrAnd(predicates ...func() (bool, error)) (result bool, err error) {
	result = true
	for _, predicate := range predicates {
		res, nerr := predicate()
		result = result && res
		if err == nil {
			err = nerr
		}
	}
	return
}

// ErrOr receives zero or more predicate functions and executes
// them sequentially, combining all return values with the ||
// operator, with false as the default return value.  ErrOr also
// returns the left-most error value that is different from nil as a
// second return value.
func ErrOr(predicates ...func() (bool, error)) (result bool, err error) {
	result = false
	for _, predicate := range predicates {
		res, nerr := predicate()
		result = result || res
		if err == nil {
			err = nerr
		}
	}
	return
}

// Range receives a range, a batch count n, and a range function f,
// divides the range into batches, and invokes the range function for
// each of these batches sequentially, covering the half-open interval
// from low to high, including low but excluding high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// Range panics if high < low, or if n < 0.
func Range(
	low, high, n int,
	f func(low, high int),
) {
	var recur func(int, int, int)
	recur = func(low, high, n int) {
		switch {
		case n == 1:
			f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				f(low, high)
			} else {
				recur(low, mid, half)
				recur(mid, high, n-half)
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrRange receives a range, a batch count n, and a range function f,
// divides the range into batches, and invokes the range function for
// each of these batches sequentially, covering the half-open interval
// from low to high, including low but excluding high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// ErrRange returns the left-most error value that is different from
// nil.
//
// ErrRange panics if high < low, or if n < 0.
func ErrRange(
	low, high, n int,
	f func(low, high int) error,
) error {
	var recur func(int, int, int) error
	recur = func(low, high, n int) (err error) {
		switch {
		case n == 1:
			return f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return f(low, high)
			}
			err0 := recur(low, mid, half)
			err1 := recur(mid, high, n-half)
			if err0 != nil {
				err = err0
			} else {
				err = err1
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeAnd receives a range, a batch count n, and a range predicate
// function f, divides the range into batches, and invokes the range
// predicate for each of these batches sequentially, covering the
// half-open interval from low to high, including low but excluding
// high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// RangeAnd returns by combining all return values with the &&
// operator.
//
// RangeAnd panics if high < low, or if n < 0.
func RangeAnd(
	low, high, n int,
	f func(low, high int) bool,
) bool {
	var recur func(int, int, int) bool
	recur = func(low, high, n int) (result bool) {
		switch {
		case n == 1:
			return f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return f(low, high)
			}
			b0 := recur(low, mid, half)
			b1 := recur(mid, high, n-half)
			return b0 && b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeOr receives a range, a batch count n, and a range predicate
// function f, divides the range into batches, and invokes the range
// predicate for each of these batches sequentially, covering the
// half-open interval from low to high, including low but excluding
// high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// RangeOr by combining all return values with the || operator.
//
// RangeOr panics if high < low, or if n < 0.
func RangeOr(
	low, high, n int,
	f func(low, high int) bool,
) bool {
	var recur func(int, int, int) bool
	recur = func(low, high, n int) (result bool) {
		switch {
		case n == 1:
			return f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return f(low, high)
			}
			b0 := recur(low, mid, half)
			b1 := recur(mid, high, n-half)
			return b0 || b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrRangeAnd receives a range, a batch count n, and a range
// predicate function f, divides the range into batches, and
// invokes the range predicate for each of these batches sequentially,
// covering the half-open interval from low to high, including low but
// excluding high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// ErrRangeAnd returns by combining all return values with the &&
// operator. ErrRangeAnd also returns the left-most error value that
// is different from nil as a second return value.
//
// ErrRangeAnd panics if high < low, or if n < 0.
func ErrRangeAnd(
	low, high, n int,
	f func(low, high int) (bool, error),
) (bool, error) {
	var recur func(int, int, int) (bool, error)
	recur = func(low, high, n int) (result bool, err error) {
		switch {
		case n == 1:
			return f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return f(low, high)
			}
			b0, err0 := recur(low, mid, half)
			b1, err1 := recur(mid, high, n-half)
			result = b0 && b1
			if err0 != nil {
				err = err0
			} else {
				err = err1
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrRangeOr receives a range, a batch count n, and a range
// predicate function f, divides the range into batches, and
// invokes the range predicate for each of these batches sequentially,
// covering the half-open interval from low to high, including low but
// excluding high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// ErrRangeOr returns by combining all return values with the ||
// operator. ErrRangeAnd also returns the left-most error value that
// is different from nil as a second return value.
//
// ErrRangeOr panics if high < low, or if n < 0.
func ErrRangeOr(
	low, high, n int,
	f func(low, high int) (bool, error),
) (bool, error) {
	var recur func(int, int, int) (bool, error)
	recur = func(low, high, n int) (result bool, err error) {
		switch {
		case n == 1:
			return f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return f(low, high)
			}
			b0, err0 := recur(low, mid, half)
			b1, err1 := recur(mid, high, n-half)
			result = b0 || b1
			if err0 != nil {
				err = err0
			} else {
				err = err1
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduce receives a range, a batch count n, a range reducer reduce,
// and a pair reducer pair, divides the range into batches, and invokes
// the range reducer for each of these batches sequentially, covering
// the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are
// then combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// RangeReduce panics if high < low, or if n < 0.
func RangeReduce(
	low, high, n int,
	reduce func(low, high int) interface{},
	pair func(x, y interface{}) interface{},
) interface{} {
	var recur func(int, int, int) interface{}
	recur = func(low, high, n int) (result interface{}) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			left := recur(low, mid, half)
			right := recur(mid, high, n-half)
			return pair(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrRangeReduce receives a range, a batch count n, a range reducer reduce,
// and a pair reducer pair, divides the range into batches, and
// invokes the range reducer for each of these batches sequentially,
// covering the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are
// then combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// ErrRangeReduce panics if high < low, or if n < 0.
func ErrRangeReduce(
	low, high, n int,
	reduce func(low, high int) (interface{}, error),
	pair func(x, y interface{}) (interface{}, error),
) (interface{}, error) {
	var recur func(int, int, int) (interface{}, error)
	recur = func(low, high, n int) (result interface{}, err error) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			left, err0 := recur(low, mid, half)
			right, err1 := recur(mid, high, n-half)
			if err0 != nil {
				err = err0
			} else if err1 != nil {
				err = err1
			} else {
				result, err = pair(left, right)
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// IntRangeReduce receives a range, a batch count n, a range reducer reduce,
// and a pair reducer pair, divides the range into batches, and
// invokes the range reducer for each of these batches sequentially,
// covering the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are
// then combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// IntRangeReduce panics if high < low, or if n < 0.
func IntRangeReduce(
	low, high, n int,
	reduce func(low, high int) int,
	pair func(x, y int) int,
) int {
	var recur func(int, int, int) int
	recur = func(low, high, n int) (result int) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			left := recur(low, mid, half)
			right := recur(mid, high, n-half)
			return pair(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrIntRangeReduce receives a range, a batch count n, a range reducer
// reduce, and pair reducer pair, divides the range into batches, and
// invokes the range reducer for each of these batches sequentially,
// covering the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are then
// combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// ErrIntRangeReduce panics if high < low, or if n < 0.
func ErrIntRangeReduce(
	low, high, n int,
	reduce func(low, high int) (int, error),
	pair func(x, y int) (int, error),
) (int, error) {
	var recur func(int, int, int) (int, error)
	recur = func(low, high, n int) (result int, err error) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			left, err0 := recur(low, mid, half)
			right, err1 := recur(mid, high, n-half)
			if err0 != nil {
				err = err0
			} else if err1 != nil {
				err = err1
			} else {
				result, err = pair(left, right)
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// Float64RangeReduce receives a range, a batch count n, a range reducer
// reduce, and a pair reducer pair, divides the range into batches, and
// invokes the range reducer for each of these batches sequentially,
// covering the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are then
// combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// Float64RangeReduce panics if high < low, or if n < 0.
func Float64RangeReduce(
	low, high, n int,
	reduce func(low, high int) float64,
	pair func(x, y float64) float64,
) float64 {
	var recur func(int, int, int) float64
	recur = func(low, high, n int) (result float64) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			left := recur(low, mid, half)
			right := recur(mid, high, n-half)
			return pair(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrFloat64RangeReduce receives a range, a batch count n, a range
// reducer reduce, and a pair reducer pair, divides the range into
// batches, and invokes the range reducer for each of these batches
// sequentially, covering the half-open interval from low to high,
// including low but excluding high. The results of the range reducer
// invocations are then combined by repeated invocations of the pair
// reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// ErrFloat64RangeReduce panics if high < low, or if n < 0.
func ErrFloat64RangeReduce(
	low, high, n int,
	reduce func(low, high int) (float64, error),
	pair func(x, y float64) (float64, error),
) (float64, error) {
	var recur func(int, int, int) (float64, error)
	recur = func(low, high, n int) (result float64, err error) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			left, err0 := recur(low, mid, half)
			right, err1 := recur(mid, high, n-half)
			if err0 != nil {
				err = err0
			} else if err1 != nil {
				err = err1
			} else {
				result, err = pair(left, right)
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// StringRangeReduce receives a range, a batch count n, a range reducer
// reduce, and a pair reducer pair, divides the range into batches, and
// invokes the range reducer for each of these batches sequentially,
// covering the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are then
// combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// StringRangeReduce panics if high < low, or if n < 0.
func StringRangeReduce(
	low, high, n int,
	reduce func(low, high int) string,
	pair func(x, y string) string,
) string {
	var recur func(int, int, int) string
	recur = func(low, high, n int) (result string) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			left := recur(low, mid, half)
			right := recur(mid, high, n-half)
			return pair(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrStringRangeReduce receives a range, a batch count n, a range
// reducer reduce, and a pair reducer pair, divides the range into
// batches, and invokes the range reducer for each of these batches
// sequentially, covering the half-open interval from low to high,
// including low but excluding high. The results of the range reducer
// invocations are then combined by repeated invocations of the pair
// reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// ErrStringRangeReduce panics if high < low, or if n < 0.
func ErrStringRangeReduce(
	low, high, n int,
	reduce func(low, high int) (string, error),
	pair func(x, y string) (string, error),
) (string, error) {
	var recur func(int, int, int) (string, error)
	recur = func(low, high, n int) (result string, err error) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			left, err0 := recur(low, mid, half)
			right, err1 := recur(mid, high, n-half)
			if err0 != nil {
				err = err0
			} else if err1 != nil {
				err = err1
			} else {
				result, err = pair(left, right)
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}
