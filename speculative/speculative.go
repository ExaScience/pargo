// Package speculative provides functions for expressing parallel algorithms,
// similar to the functions in package parallel, except that the implementations
// here terminate early when they can.
//
// See https://github.com/ExaScience/pargo/wiki/TaskParallelism for a general
// overview.
package speculative

import (
	"fmt"
	"sync"

	"github.com/exascience/pargo/internal"
)

// Reduce receives one or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine. Reduce returns either when all
// functions have terminated with a second return value of false; or when one or
// more functions return a second return value of true. In the latter case, the
// first return value of the left-most function that returned true as a second
// return value becomes the final result, without waiting for the other
// functions to terminate.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and Reduce eventually panics with the left-most recovered panic
// value.
func Reduce(
	join func(x, y interface{}) (interface{}, bool),
	firstFunction func() (interface{}, bool),
	moreFunctions ...func() (interface{}, bool),
) (interface{}, bool) {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right interface{}
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = moreFunctions[0]()
		}()
		left, b0 = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = Reduce(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left, b0 = Reduce(join, firstFunction, moreFunctions[:half]...)
	}
	if b0 {
		return left, true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	if b1 {
		return right, true
	}
	return join(left, right)
}

// ReduceFloat64 receives one or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine. ReduceFloat64 returns either
// when all functions have terminated with a second return value of false; or
// when one or more functions return a second return value of true. In the
// latter case, the first return value of the left-most function that returned
// true as a second return value becomes the final result, without waiting for
// the other functions to terminate.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceFloat64 eventually panics with the left-most recovered
// panic value.
func ReduceFloat64(
	join func(x, y float64) (float64, bool),
	firstFunction func() (float64, bool),
	moreFunctions ...func() (float64, bool),
) (float64, bool) {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right float64
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = moreFunctions[0]()
		}()
		left, b0 = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = ReduceFloat64(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left, b0 = ReduceFloat64(join, firstFunction, moreFunctions[:half]...)
	}
	if b0 {
		return left, true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	if b1 {
		return right, true
	}
	return join(left, right)
}

// ReduceInt receives one or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine. ReduceInt returns either when
// all functions have terminated with a second return value of false; or when
// one or more functions return a second return value of true. In the latter
// case, the first return value of the left-most function that returned true as
// a second return value becomes the final result, without waiting for the other
// functions to terminate.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceInt eventually panics with the left-most recovered panic
// value.
func ReduceInt(
	join func(x, y int) (int, bool),
	firstFunction func() (int, bool),
	moreFunctions ...func() (int, bool),
) (int, bool) {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right int
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = moreFunctions[0]()
		}()
		left, b0 = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = ReduceInt(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left, b0 = ReduceInt(join, firstFunction, moreFunctions[:half]...)
	}
	if b0 {
		return left, true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	if b1 {
		return right, true
	}
	return join(left, right)
}

// ReduceString receives one or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine. ReduceString returns either
// when all functions have terminated with a second return value of false; or
// when one or more functions return a second return value of true. In the
// latter case, the first return value of the left-most function that returned
// true as a second return value becomes the final result, without waiting for
// the other functions to terminate.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceString eventually panics with the left-most recovered panic
// value.
func ReduceString(
	join func(x, y string) (string, bool),
	firstFunction func() (string, bool),
	moreFunctions ...func() (string, bool),
) (string, bool) {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right string
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = moreFunctions[0]()
		}()
		left, b0 = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = ReduceString(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left, b0 = ReduceString(join, firstFunction, moreFunctions[:half]...)
	}
	if b0 {
		return left, true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	if b1 {
		return right, true
	}
	return join(left, right)
}

// Do receives zero or more thunks and executes them in parallel.
//
// Each function is invoked in its own goroutine. Do returns either when all
// functions have terminated with a return value of false; or when one or more
// functions return true, without waiting for the other functions to terminate.
//
// If one or more thunks panic, the corresponding goroutines recover the panics,
// and Do may eventually panic with the left-most recovered panic value.
func Do(thunks ...func() bool) bool {
	switch len(thunks) {
	case 0:
		return false
	case 1:
		return thunks[0]()
	}
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(thunks) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = thunks[1]()
		}()
		b0 = thunks[0]()
	default:
		half := len(thunks) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = Do(thunks[half:]...)
		}()
		b0 = Do(thunks[:half]...)
	}
	if b0 {
		return true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b1
}

// And receives zero or more predicate functions and executes them in parallel.
//
// Each predicate is invoked in its own goroutine, and And returns true if all
// of them return true; or And returns false when at least one of them returns
// false, without waiting for the other predicates to terminate.
//
// If one or more predicates panic, the corresponding goroutines recover the
// panics, and And may eventually panic with the left-most recovered panic
// value.
func And(predicates ...func() bool) bool {
	switch len(predicates) {
	case 0:
		return true
	case 1:
		return predicates[0]()
	}
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(predicates) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = predicates[1]()
		}()
		b0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = And(predicates[half:]...)
		}()
		b0 = And(predicates[:half]...)
	}
	if !b0 {
		return false
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b1
}

// Or receives zero or more predicate functions and executes them in parallel.
//
// Each predicate is invoked in its own goroutine, and Or returns false if all
// of them return false; or Or returns true when at least one of them returns
// true, without waiting for the other predicates to terminate.
//
// If one or more predicates panic, the corresponding goroutines recover the
// panics, and Or may eventually panic with the left-most recovered panic value.
func Or(predicates ...func() bool) bool {
	switch len(predicates) {
	case 0:
		return false
	case 1:
		return predicates[0]()
	}
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(predicates) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = predicates[1]()
		}()
		b0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = Or(predicates[half:]...)
		}()
		b0 = Or(predicates[:half]...)
	}
	if b0 {
		return true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b1
}

// Range receives a range, a batch count n, and a range function f, divides the
// range into batches, and invokes the range function for each of these batches
// in parallel, covering the half-open interval from low to high, including low
// but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range function is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and Range returns either when all range functions have
// terminated with a return value of true; or when one or more range functions
// return true, without waiting for the other range functions to terminate.
//
// Range panics if high < low, or if n < 0.
//
// If one or more range functions panic, the corresponding goroutines recover
// the panics, and Range may eventually panic with the left-most recovered panic
// value. If both non-nil error values are returned and panics occur, then the
// left-most of these events take precedence.
func Range(
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
			var b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				b1 = recur(mid, high, n-half)
			}()
			if recur(low, mid, half) {
				return true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeAnd receives a range, a batch count n, and a range predicate function f,
// divides the range into batches, and invokes the range predicate for each of
// these batches in parallel, covering the half-open interval from low to high,
// including low but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range predicate is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeAnd returns true if all of them return true; or
// RangeAnd returns false when at least one of them returns false, without
// waiting for the other range predicates to terminate.
//
// RangeAnd panics if high < low, or if n < 0.
//
// If one or more range predicates panic, the corresponding goroutines recover
// the panics, and RangeAnd may eventually panic with the left-most recovered
// panic value.
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
			var b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				b1 = recur(mid, high, n-half)
			}()
			if !recur(low, mid, half) {
				return false
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeOr receives a range, a batch count n, and a range predicate function f,
// divides the range into batches, and invokes the range predicate for each of
// these batches in parallel, covering the half-open interval from low to high,
// including low but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range predicate is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeOr returns false if all of them return false; or
// RangeOr returns true when at least one of them returns true, without waiting
// for the other range predicates to terminate.
//
// RangeOr panics if high < low, or if n < 0.
//
// If one or more range predicates panic, the corresponding goroutines recover
// the panics, and RangeOr may eventually panic with the left-most recovered
// panic value.
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
			var b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				b1 = recur(mid, high, n-half)
			}()
			if recur(low, mid, half) {
				return true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduce receives a range, a batch count n, a range reducer function, and
// a join function, divides the range into batches, and invokes the range
// reducer for each of these batches in parallel, covering the half-open
// interval from low to high, including low but excluding high. The results of
// the range reducer invocations are then combined by repeated invocations of
// join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduce returns either when all range reducers and joins
// have terminated with a second return value of false; or when one or more
// range or join functions return a second return value of true. In the latter
// case, the first return value of the left-most function that returned true as
// a second return value becomes the final result, without waiting for the other
// range and pair reducers to terminate.
//
// RangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduce eventually panics with the left-most
// recovered panic value.
func RangeReduce(
	low, high, n int,
	reduce func(low, high int) (interface{}, bool),
	join func(x, y interface{}) (interface{}, bool),
) (interface{}, bool) {
	var recur func(int, int, int) (interface{}, bool)
	recur = func(low, high, n int) (interface{}, bool) {
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
			var left, right interface{}
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(mid, high, n-half)
			}()
			left, b0 = recur(low, mid, half)
			if b0 {
				return left, true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if b1 {
				return right, true
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceInt receives a range, a batch count n, a range reducer function,
// and a join function, divides the range into batches, and invokes the range
// reducer for each of these batches in parallel, covering the half-open
// interval from low to high, including low but excluding high. The results of
// the range reducer invocations are then combined by repeated invocations of
// join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceInt returns either when all range reducers and
// joins have terminated with a second return value of false; or when one or
// more range or join functions return a second return value of true. In the
// latter case, the first return value of the left-most function that returned
// true as a second return value becomes the final result, without waiting for
// the other range and pair reducers to terminate.
//
// RangeReduceInt panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceInt eventually panics with the left-most
// recovered panic value.
func RangeReduceInt(
	low, high, n int,
	reduce func(low, high int) (int, bool),
	join func(x, y int) (int, bool),
) (int, bool) {
	var recur func(int, int, int) (int, bool)
	recur = func(low, high, n int) (int, bool) {
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
			var left, right int
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(mid, high, n-half)
			}()
			left, b0 = recur(low, mid, half)
			if b0 {
				return left, true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if b1 {
				return right, true
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceFloat64 receives a range, a batch count n, a range reducer
// function, and a join function, divides the range into batches, and invokes
// the range reducer for each of these batches in parallel, covering the
// half-open interval from low to high, including low but excluding high. The
// results of the range reducer invocations are then combined by repeated
// invocations of join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceFloat64 returns either when all range reducers
// and joins have terminated with a second return value of false; or when one or
// more range or join functions return a second return value of true. In the
// latter case, the first return value of the left-most function that returned
// true as a second return value becomes the final result, without waiting for
// the other range and pair reducers to terminate.
//
// RangeReduceFloat64 panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceFloat64 eventually panics with the
// left-most recovered panic value.
func RangeReduceFloat64(
	low, high, n int,
	reduce func(low, high int) (float64, bool),
	join func(x, y float64) (float64, bool),
) (float64, bool) {
	var recur func(int, int, int) (float64, bool)
	recur = func(low, high, n int) (float64, bool) {
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
			var left, right float64
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(mid, high, n-half)
			}()
			left, b0 = recur(low, mid, half)
			if b0 {
				return left, true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if b1 {
				return right, true
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceString receives a range, a batch count n, a range reducer
// function, and a join function, divides the range into batches, and invokes
// the range reducer for each of these batches in parallel, covering the
// half-open interval from low to high, including low but excluding high. The
// results of the range reducer invocations are then combined by repeated
// invocations of join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceString returns either when all range reducers and
// joins have terminated with a second return value of false; or when one or
// more range or join functions return a second return value of true. In the
// latter case, the first return value of the left-most function that returned
// true as a second return value becomes the final result, without waiting for
// the other range and pair reducers to terminate.
//
// RangeReduceString panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceString eventually panics with the
// left-most recovered panic value.
func RangeReduceString(
	low, high, n int,
	reduce func(low, high int) (string, bool),
	join func(x, y string) (string, bool),
) (string, bool) {
	var recur func(int, int, int) (string, bool)
	recur = func(low, high, n int) (string, bool) {
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
			var left, right string
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(mid, high, n-half)
			}()
			left, b0 = recur(low, mid, half)
			if b0 {
				return left, true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if b1 {
				return right, true
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}
