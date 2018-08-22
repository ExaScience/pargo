// Package sequential provides sequential implementations of the functions
// provided by the parallel and speculative packages. This is useful for testing
// and debugging.
//
// It is not recommended to use the implementations of this package for any
// other purpose, because they are almost certainly too inefficient for regular
// sequential programs.
package sequential

import (
	"fmt"

	"github.com/exascience/pargo/internal"
)

// Reduce receives one or more functions, executes them sequentially, and
// combines their results sequentially.
//
// Partial results are combined with the join function.
func Reduce(
	join func(x, y interface{}) interface{},
	firstFunction func() interface{},
	moreFunctions ...func() interface{},
) interface{} {
	result := firstFunction()
	for _, f := range moreFunctions {
		result = join(result, f())
	}
	return result
}

// ReduceFloat64 receives one or more functions, executes them sequentially, and
// combines their results sequentially.
//
// Partial results are combined with the join function.
func ReduceFloat64(
	join func(x, y float64) float64,
	firstFunction func() float64,
	moreFunctions ...func() float64,
) float64 {
	result := firstFunction()
	for _, f := range moreFunctions {
		result = join(result, f())
	}
	return result
}

// ReduceFloat64Sum receives zero or more functions, executes them sequentially,
// and adds their results sequentially.
func ReduceFloat64Sum(functions ...func() float64) float64 {
	result := float64(0)
	for _, f := range functions {
		result += f()
	}
	return result
}

// ReduceFloat64Product receives zero or more functions, executes them
// sequentially, and multiplies their results sequentially.
func ReduceFloat64Product(functions ...func() float64) float64 {
	result := float64(1)
	for _, f := range functions {
		result *= f()
	}
	return result
}

// ReduceInt receives one or more functions, executes them sequentially, and
// combines their results sequentially.
//
// Partial results are combined with the join function.
func ReduceInt(
	join func(x, y int) int,
	firstFunction func() int,
	moreFunctions ...func() int,
) int {
	result := firstFunction()
	for _, f := range moreFunctions {
		result = join(result, f())
	}
	return result
}

// ReduceIntSum receives zero or more functions, executes them sequentially, and
// adds their results sequentially.
func ReduceIntSum(functions ...func() int) int {
	result := 0
	for _, f := range functions {
		result += f()
	}
	return result
}

// ReduceIntProduct receives zero or more functions, executes them sequentially,
// and multiplies their results sequentially.
func ReduceIntProduct(functions ...func() int) int {
	result := 1
	for _, f := range functions {
		result *= f()
	}
	return result
}

// ReduceString receives one or more functions, executes them in parallel, and
// combines their results in parallel.
//
// Partial results are combined with the join function.
func ReduceString(
	join func(x, y string) string,
	firstFunction func() string,
	moreFunctions ...func() string,
) string {
	result := firstFunction()
	for _, f := range moreFunctions {
		result = join(result, f())
	}
	return result
}

// ReduceStringSum receives zero or more functions, executes them in parallel,
// and concatenates their results in parallel.
func ReduceStringSum(functions ...func() string) string {
	result := ""
	for _, f := range functions {
		result += f()
	}
	return result
}

// Do receives zero or more thunks and executes them sequentially.
func Do(thunks ...func()) {
	for _, thunk := range thunks {
		thunk()
	}
}

// And receives zero or more predicate functions and executes them sequentially,
// combining all return values with the && operator, with true as the default
// return value.
func And(predicates ...func() bool) bool {
	result := true
	for _, predicate := range predicates {
		result = result && predicate()
	}
	return result
}

// Or receives zero or more predicate functions and executes them sequentially,
// combining all return values with the || operator, with false as the default
// return value.
func Or(predicates ...func() bool) bool {
	result := false
	for _, predicate := range predicates {
		result = result || predicate()
	}
	return result
}

// Range receives a range, a batch count n, and a range function f, divides the
// range into batches, and invokes the range function for each of these batches
// sequentially, covering the half-open interval from low to high, including low
// but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
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
			}
			recur(low, mid, half)
			recur(mid, high, n-half)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeAnd receives a range, a batch count n, and a range predicate function f,
// divides the range into batches, and invokes the range predicate for each of
// these batches sequentially, covering the half-open interval from low to high,
// including low but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// RangeAnd returns by combining all return values with the && operator.
//
// RangeAnd panics if high < low, or if n < 0.
func RangeAnd(
	low, high, n int,
	f func(low, high int) bool,
) bool {
	var recur func(int, int, int) bool
	recur = func(low, high, n int) bool {
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

// RangeOr receives a range, a batch count n, and a range predicate function f,
// divides the range into batches, and invokes the range predicate for each of
// these batches sequentially, covering the half-open interval from low to high,
// including low but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// RangeOr returns by combining all return values with the || operator.
//
// RangeOr panics if high < low, or if n < 0.
func RangeOr(
	low, high, n int,
	f func(low, high int) bool,
) bool {
	var recur func(int, int, int) bool
	recur = func(low, high, n int) bool {
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

// RangeReduce receives a range, a batch count, a range reduce function, and a
// join function, divides the range into batches, and invokes the range reducer
// for each of these batches sequentially, covering the half-open interval from
// low to high, including low but excluding high. The results of the range
// reducer invocations are then combined by repeated invocations of join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// RangeReduce panics if high < low, or if n < 0.
func RangeReduce(
	low, high, n int,
	reduce func(low, high int) interface{},
	join func(x, y interface{}) interface{},
) interface{} {
	var recur func(int, int, int) interface{}
	recur = func(low, high, n int) interface{} {
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
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceInt receives a range, a batch count n, a range reducer function,
// and a join function, divides the range into batches, and invokes the range
// reducer for each of these batches sequentially, covering the half-open
// interval from low to high, including low but excluding high. The results of
// the range reducer invocations are then combined by repeated invocations of
// join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// IntRangeReduce panics if high < low, or if n < 0.
func RangeReduceInt(
	low, high, n int,
	reduce func(low, high int) int,
	join func(x, y int) int,
) int {
	var recur func(int, int, int) int
	recur = func(low, high, n int) int {
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
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceIntSum receives a range, a batch count n, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches sequentially, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then added together.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// RangeReduceIntSum panics if high < low, or if n < 0.
func RangeReduceIntSum(
	low, high, n int,
	reduce func(low, high int) int,
) int {
	var recur func(int, int, int) int
	recur = func(low, high, n int) int {
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
			return left + right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceIntProduct receives a range, a batch count n, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches sequentially, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then multiplied with each other.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// RangeReduceIntProduct panics if high < low, or if n < 0.
func RangeReduceIntProduct(
	low, high, n int,
	reduce func(low, high int) int,
) int {
	var recur func(int, int, int) int
	recur = func(low, high, n int) int {
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
			return left * right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceFloat64 receives a range, a batch count n, a range reducer
// function, and a join function, divides the range into batches, and invokes
// the range reducer for each of these batches sequentially, covering the
// half-open interval from low to high, including low but excluding high. The
// results of the range reducer invocations are then combined by repeated
// invocations of join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// Float64RangeReduce panics if high < low, or if n < 0.
func RangeReduceFloat64(
	low, high, n int,
	reduce func(low, high int) float64,
	join func(x, y float64) float64,
) float64 {
	var recur func(int, int, int) float64
	recur = func(low, high, n int) float64 {
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
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceFloat64Sum receives a range, a batch count n, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches sequentially, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then added together.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// RangeReduceFloat64Sum panics if high < low, or if n < 0.
func RangeReduceFloat64Sum(
	low, high, n int,
	reduce func(low, high int) float64,
) float64 {
	var recur func(int, int, int) float64
	recur = func(low, high, n int) float64 {
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
			return left + right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceFloat64Product receives a range, a batch count n, and a range
// reducer function, divides the range into batches, and invokes the range
// reducer for each of these batches sequentially, covering the half-open
// interval from low to high, including low but excluding high. The results of
// the range reducer invocations are then multiplied with each other.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// RangeReduceFloat64Product panics if high < low, or if n < 0.
func RangeReduceFloat64Product(
	low, high, n int,
	reduce func(low, high int) float64,
) float64 {
	var recur func(int, int, int) float64
	recur = func(low, high, n int) float64 {
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
			return left * right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceString receives a range, a batch count n, a range reducer
// function, and a join function, divides the range into batches, and invokes
// the range reducer for each of these batches sequentially, covering the
// half-open interval from low to high, including low but excluding high. The
// results of the range reducer invocations are then combined by repeated
// invocations of join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// StringRangeReduce panics if high < low, or if n < 0.
func RangeReduceString(
	low, high, n int,
	reduce func(low, high int) string,
	join func(x, y string) string,
) string {
	var recur func(int, int, int) string
	recur = func(low, high, n int) string {
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
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceStringSum receives a range, a batch count n, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches sequentially, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then concatenated together.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// RangeReduceStringSum panics if high < low, or if n < 0.
func RangeReduceStringSum(
	low, high, n int,
	reduce func(low, high int) string,
) string {
	var recur func(int, int, int) string
	recur = func(low, high, n int) string {
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
			return left + right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}
