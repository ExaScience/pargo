// Package parallel provides functions for expressing parallel algorithms.
//
// See https://github.com/ExaScience/pargo/wiki/TaskParallelism for a general
// overview.
package parallel

import (
	"fmt"
	"sync"

	"github.com/exascience/pargo/internal"
)

// Reduce receives one or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine, and Reduce returns only when
// all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and Reduce eventually panics with the left-most recovered panic
// value.
func Reduce(
	join func(x, y interface{}) interface{},
	firstFunction func() interface{},
	moreFunctions ...func() interface{},
) interface{} {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right interface{}
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = moreFunctions[0]()
		}()
		left = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = Reduce(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left = Reduce(join, firstFunction, moreFunctions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return join(left, right)
}

// ReduceFloat64 receives one or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine, and ReduceFloat64 returns only
// when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceFloat64 eventually panics with the left-most recovered
// panic value.
func ReduceFloat64(
	join func(x, y float64) float64,
	firstFunction func() float64,
	moreFunctions ...func() float64,
) float64 {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right float64
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = moreFunctions[0]()
		}()
		left = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceFloat64(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left = ReduceFloat64(join, firstFunction, moreFunctions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return join(left, right)
}

// ReduceFloat64Sum receives zero or more functions, executes them in parallel,
// and adds their results in parallel.
//
// Each function is invoked in its own goroutine, and ReduceFloat64Sum returns
// only when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceFloat64Sum eventually panics with the left-most recovered
// panic value.
func ReduceFloat64Sum(functions ...func() float64) float64 {
	switch len(functions) {
	case 0:
		return 0
	case 1:
		return functions[0]()
	}
	var left, right float64
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(functions) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = functions[1]()
		}()
		left = functions[0]()
	default:
		half := len(functions) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceFloat64Sum(functions[half:]...)
		}()
		left = ReduceFloat64Sum(functions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return left + right
}

// ReduceFloat64Product receives zero or more functions, executes them in
// parallel, and multiplies their results in parallel.
//
// Each function is invoked in its own goroutine, and ReduceFloat64Product
// returns only when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceFloat64Product eventually panics with the left-most
// recovered panic value.
func ReduceFloat64Product(functions ...func() float64) float64 {
	switch len(functions) {
	case 0:
		return 1
	case 1:
		return functions[0]()
	}
	var left, right float64
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(functions) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = functions[1]()
		}()
		left = functions[0]()
	default:
		half := len(functions) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceFloat64Product(functions[half:]...)
		}()
		left = ReduceFloat64Product(functions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return left * right
}

// ReduceInt receives zero or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine, and ReduceInt returns only
// when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceInt eventually panics with the left-most recovered panic
// value.
func ReduceInt(
	join func(x, y int) int,
	firstFunction func() int,
	moreFunctions ...func() int,
) int {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right int
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = moreFunctions[0]()
		}()
		left = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceInt(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left = ReduceInt(join, firstFunction, moreFunctions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return join(left, right)
}

// ReduceIntSum receives zero or more functions, executes them in parallel, and
// adds their results in parallel.
//
// Each function is invoked in its own goroutine, and ReduceIntSum returns only
// when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceIntSum eventually panics with the left-most recovered panic
// value.
func ReduceIntSum(functions ...func() int) int {
	switch len(functions) {
	case 0:
		return 0
	case 1:
		return functions[0]()
	}
	var left, right int
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(functions) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = functions[1]()
		}()
		left = functions[0]()
	default:
		half := len(functions) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceIntSum(functions[half:]...)
		}()
		left = ReduceIntSum(functions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return left + right
}

// ReduceIntProduct receives zero or more functions, executes them in parallel,
// and multiplies their results in parallel.
//
// Each function is invoked in its own goroutine, and ReduceIntProduct returns
// only when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceIntProduct eventually panics with the left-most recovered
// panic value.
func ReduceIntProduct(functions ...func() int) int {
	switch len(functions) {
	case 0:
		return 1
	case 1:
		return functions[0]()
	}
	var left, right int
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(functions) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = functions[1]()
		}()
		left = functions[0]()
	default:
		half := len(functions) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceIntProduct(functions[half:]...)
		}()
		left = ReduceIntProduct(functions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return left * right
}

// ReduceString receives zero or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine, and ReduceString returns only
// when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceString eventually panics with the left-most recovered panic
// value.
func ReduceString(
	join func(x, y string) string,
	firstFunction func() string,
	moreFunctions ...func() string,
) string {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right string
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = moreFunctions[0]()
		}()
		left = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceString(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left = ReduceString(join, firstFunction, moreFunctions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return join(left, right)
}

// ReduceStringSum receives zero or more functions, executes them in parallel,
// and concatenates their results in parallel.
//
// Each function is invoked in its own goroutine, and ReduceStringSum returns
// only when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceStringSum eventually panics with the left-most recovered
// panic value.
func ReduceStringSum(functions ...func() string) string {
	switch len(functions) {
	case 0:
		return ""
	case 1:
		return functions[0]()
	}
	var left, right string
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(functions) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = functions[1]()
		}()
		left = functions[0]()
	default:
		half := len(functions) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceStringSum(functions[half:]...)
		}()
		left = ReduceStringSum(functions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return left + right
}

// Do receives zero or more thunks and executes them in parallel.
//
// Each thunk is invoked in its own goroutine, and Do returns only when all
// thunks have terminated.
//
// If one or more thunks panic, the corresponding goroutines recover the panics,
// and Do eventually panics with the left-most recovered panic value.
func Do(thunks ...func()) {
	switch len(thunks) {
	case 0:
		return
	case 1:
		thunks[0]()
		return
	}
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
			thunks[1]()
		}()
		thunks[0]()
	default:
		half := len(thunks) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			Do(thunks[half:]...)
		}()
		Do(thunks[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
}

// And receives zero or more predicate functions and executes them in parallel.
//
// Each predicate is invoked in its own goroutine, and And returns only when all
// predicates have terminated, combining all return values with the && operator,
// with true as the default return value.
//
// If one or more predicates panic, the corresponding goroutines recover the
// panics, and And eventually panics with the left-most recovered panic value.
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
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b0 && b1
}

// Or receives zero or more predicate functions and executes them in parallel.
//
// Each predicate is invoked in its own goroutine, and Or returns only when all
// predicates have terminated, combining all return values with the || operator,
// with false as the default return value.
//
// If one or more predicates panic, the corresponding goroutines recover the
// panics, and Or eventually panics with the left-most recovered panic value.
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
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b0 || b1
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
// low <= high, and Range returns only when all range functions have terminated.
//
// Range panics if high < low, or if n < 0.
//
// If one or more range function invocations panic, the corresponding goroutines
// recover the panics, and Range eventually panics with the left-most recovered
// panic value.
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
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				recur(mid, high, n-half)
			}()
			recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	recur(low, high, internal.ComputeNofBatches(low, high, n))
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
// low <= high, and RangeAnd returns only when all range predicates have
// terminated, combining all return values with the && operator.
//
// RangeAnd panics if high < low, or if n < 0.
//
// If one or more range predicate invocations panic, the corresponding
// goroutines recover the panics, and RangeAnd eventually panics with the
// left-most recovered panic value.
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
			var b0, b1 bool
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
			b0 = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b0 && b1
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
// low <= high, and RangeOr returns only when all range predicates have
// terminated, combining all return values with the || operator.
//
// RangeOr panics if high < low, or if n < 0.
//
// If one or more range predicate invocations panic, the corresponding
// goroutines recover the panics, and RangeOr eventually panics with the
// left-most recovered panic value.
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
			var b0, b1 bool
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
			b0 = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b0 || b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduce receives a range, a batch count, a range reduce function, and a
// join function, divides the range into batches, and invokes the range reducer
// for each of these batches in parallel, covering the half-open interval from
// low to high, including low but excluding high. The results of the range
// reducer invocations are then combined by repeated invocations of join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduce returns only when all range reducers and pair
// reducers have terminated.
//
// RangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduce eventually panics with the left-most
// recovered panic value.
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
			var left, right interface{}
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
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
// low <= high, and RangeReduceInt returns only when all range reducers and pair
// reducers have terminated.
//
// RangeReduceInt panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceInt eventually panics with the left-most
// recovered panic value.
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
			var left, right int
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceIntSum receives a range, a batch count n, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches in parallel, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then added together.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceIntSum returns only when all range reducers and
// pair reducers have terminated.
//
// RangeReduceIntSum panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceIntSum eventually panics with the
// left-most recovered panic value.
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
			var left, right int
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return left + right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceIntProduct receives a range, a batch count n, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches in parallel, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then multiplied with each other.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceIntProduct returns only when all range reducers
// and pair reducers have terminated.
//
// RangeReduceIntProduct panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceIntProducet eventually panics with the
// left-most recovered panic value.
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
			var left, right int
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return left * right
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
// low <= high, and RangeReduceFloat64 returns only when all range reducers and
// pair reducers have terminated.
//
// RangeReduceFloat64 panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceFloat64 eventually panics with the
// left-most recovered panic value.
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
			var left, right float64
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceFloat64Sum receives a range, a batch count n, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches in parallel, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then added together.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceFloat64Sum returns only when all range reducers
// and pair reducers have terminated.
//
// RangeReduceFloat64Sum panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceFloat64Sum eventually panics with the
// left-most recovered panic value.
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
			var left, right float64
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return left + right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceFloat64Product receives a range, a batch count n, and a range
// reducer function, divides the range into batches, and invokes the range
// reducer for each of these batches in parallel, covering the half-open
// interval from low to high, including low but excluding high. The results of
// the range reducer invocations are then multiplied with each other.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceFloat64Product returns only when all range
// reducers and pair reducers have terminated.
//
// RangeReduceFloat64Product panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceFloat64Producet eventually panics with the
// left-most recovered panic value.
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
			var left, right float64
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return left * right
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
// low <= high, and RangeReduceString returns only when all range reducers and
// pair reducers have terminated.
//
// RangeReduceString panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceString eventually panics with the
// left-most recovered panic value.
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
			var left, right string
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceStringSum receives a range, a batch count n, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches in parallel, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then concatenated together.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.GOMAXPROCS(0)
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceStringSum returns only when all range reducers
// and pair reducers have terminated.
//
// RangeReduceStringSum panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceStringSum eventually panics with the
// left-most recovered panic value.
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
			var left, right string
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return left + right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}
