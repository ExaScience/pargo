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
func Do(thunks ...func() error) (err error) {
	for _, thunk := range thunks {
		nerr := thunk()
		if err == nil {
			err = nerr
		}
	}
	return
}

// And receives zero or more predicate functions and executes
// them sequentially, combining all return values with the &&
// operator, with true as the default return value. And also
// returns the left-most error value that is different from nil as a
// second return value.
func And(predicates ...func() (bool, error)) (result bool, err error) {
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

// Or receives zero or more predicate functions and executes
// them sequentially, combining all return values with the ||
// operator, with false as the default return value.  Or also
// returns the left-most error value that is different from nil as a
// second return value.
func Or(predicates ...func() (bool, error)) (result bool, err error) {
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
// Range returns the left-most error value that is different from
// nil.
//
// Range panics if high < low, or if n < 0.
func Range(
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

// RangeAnd receives a range, a batch count n, and a range
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
// RangeAnd returns by combining all return values with the &&
// operator. RangeAnd also returns the left-most error value that
// is different from nil as a second return value.
//
// RangeAnd panics if high < low, or if n < 0.
func RangeAnd(
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

// RangeOr receives a range, a batch count n, and a range
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
// RangeOr returns by combining all return values with the ||
// operator. RangeAnd also returns the left-most error value that
// is different from nil as a second return value.
//
// RangeOr panics if high < low, or if n < 0.
func RangeOr(
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
// RangeReduce panics if high < low, or if n < 0.
func RangeReduce(
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

// IntRangeReduce receives a range, a batch count n, a range reducer
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
// IntRangeReduce panics if high < low, or if n < 0.
func IntRangeReduce(
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

// Float64RangeReduce receives a range, a batch count n, a range
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
// Float64RangeReduce panics if high < low, or if n < 0.
func Float64RangeReduce(
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

// StringRangeReduce receives a range, a batch count n, a range
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
// StringRangeReduce panics if high < low, or if n < 0.
func StringRangeReduce(
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
