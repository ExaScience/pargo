// Package speculative provides functions for expressing parallel
// algorithms, similar to the functions in package parallel, except
// that the implementations here terminate early when they can.
//
// See https://github.com/ExaScience/pargo/wiki/TaskParallelism for a
// general overview.
package speculative

import (
	"fmt"
	"sync"

	"github.com/exascience/pargo/internal"
)

// Do receives zero or more thunks and executes them in parallel.
//
// Each thunk is invoked in its own goroutine, and Do returns
// either when all thunks have terminated; or when one or more thunks
// return an error value that is different from nil, returning the
// left-most of these error values, without waiting for the other
// thunks to terminate.
//
// If one or more thunks panic, the corresponding goroutines recover
// the panics, and Do may eventually panic with the left-most
// recovered panic value. If both non-nil error values are returned
// and panics occur, then the left-most of these events takes
// precedence.
func Do(thunks ...func() error) (err error) {
	switch len(thunks) {
	case 0:
		return nil
	case 1:
		return thunks[0]()
	}
	var err0, err1 error
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(thunks) {
	case 2:
		go func() {
			defer func() {
				p = recover()
				wg.Done()
			}()
			err1 = thunks[1]()
		}()
		err0 = thunks[0]()
	default:
		half := len(thunks) / 2
		go func() {
			defer func() {
				p = recover()
				wg.Done()
			}()
			err1 = Do(thunks[half:]...)
		}()
		err0 = Do(thunks[:half]...)
	}
	if err0 != nil {
		return err0
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return err1
}

// And receives zero or more predicate functions and executes
// them in parallel.
//
// Each predicate is invoked in its own goroutine, and And returns
// true if all of them return true; or And returns false when at
// least one of them returns false, without waiting for the other
// predicates to terminate.  And may also return the left-most
// error value that is different from nil as a second return value. If
// both false values and non-nil error values are returned, then the
// left-most of these return value pairs are returned.
//
// If one or more predicates panic, the corresponding goroutines
// recover the panics, and And may eventually panic with the
// left-most recovered panic value. If both panics occur on the one
// hand, and false or non-nil error values are returned on the other
// hand, then the left-most of these two kinds of events takes
// precedence.
func And(predicates ...func() (bool, error)) (result bool, err error) {
	switch len(predicates) {
	case 0:
		return true, nil
	case 1:
		return predicates[0]()
	}
	var b0, b1 bool
	var err0, err1 error
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(predicates) {
	case 2:
		go func() {
			defer func() {
				p = recover()
				wg.Done()
			}()
			b1, err1 = predicates[1]()
		}()
		b0, err0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				p = recover()
				wg.Done()
			}()
			b1, err1 = And(predicates[half:]...)
		}()
		b0, err0 = And(predicates[:half]...)
	}
	if !b0 || (err0 != nil) {
		return b0, err0
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	result = b0 && b1
	if err0 != nil {
		err = err0
	} else {
		err = err1
	}
	return
}

// Or receives zero or more predicate functions and executes
// them in parallel.
//
// Each predicate is invoked in its own goroutine, and Or returns
// false if all of them return false; or Or returns true when at
// least one of them returns true, without waiting for the other
// predicates to terminate.  Or may also return the left-most error
// value that is different from nil as a second return value. If both
// true values and non-nil error values are returned, then the
// left-most of these return value pairs are returned.
//
// If one or more predicates panic, the corresponding goroutines
// recover the panics, and Or may eventually panic with the
// left-most recovered panic value. If both panics occur on the one
// hand, and true or non-nil error values are returned on the other
// hand, then the left-most of these two kinds of events takes
// precedence.
func Or(predicates ...func() (bool, error)) (result bool, err error) {
	switch len(predicates) {
	case 0:
		return false, nil
	case 1:
		return predicates[0]()
	}
	var b0, b1 bool
	var err0, err1 error
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(predicates) {
	case 2:
		go func() {
			defer func() {
				p = recover()
				wg.Done()
			}()
			b1, err1 = predicates[1]()
		}()
		b0, err0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				p = recover()
				wg.Done()
			}()
			b1, err1 = Or(predicates[half:]...)
		}()
		b0, err0 = Or(predicates[:half]...)
	}
	if b0 || (err0 != nil) {
		return b0, err0
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	result = b0 || b1
	if err0 != nil {
		err = err0
	} else {
		err = err1
	}
	return
}

// Range receives a range, a batch count n, and a range function f,
// divides the range into batches, and invokes the range function for
// each of these batches in parallel, covering the half-open interval
// from low to high, including low but excluding high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range function is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and Range returns either when all range
// functions have terminated; or when one or more range functions
// return an error value that is different from nil, returning the
// left-most of these error values, without waiting for the other
// range functions to terminate.
//
// Range panics if high < low, or if n < 0.
//
// If one or more range functions panic, the corresponding goroutines
// recover the panics, and Range may eventually panic with the
// left-most recovered panic value. If both non-nil error values are
// returned and panics occur, then the left-most of these events take
// precedence.
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
			var err1 error
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				err1 = recur(mid, high, n-half)
			}()
			if err0 := recur(low, mid, half); err0 != nil {
				return err0
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return err1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeAnd receives a range, a batch count n, and a range
// predicate function f, divides the range into batches, and
// invokes the range predicate for each of these batches in parallel,
// covering the half-open interval from low to high, including low but
// excluding high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range predicate is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and RangeAnd returns true if all of them
// return true; or RangeAnd returns false when at least one of them
// returns false, without waiting for the other range predicates to
// terminate. RangeAnd may also return the left-most error value
// that is different from nil as a second return value. If both false
// values and non-nil error values are returned, then the left-most of
// these return value pairs are returned.
//
// RangeAnd panics if high < low, or if n < 0.
//
// If one or more range predicates panic, the corresponding goroutines
// recover the panics, and RangeAnd may eventually panic with the
// left-most recovered panic value. If both panics occur on the one
// hand, and false or non-nil error values are returned on the other
// hand, then the left-most of these two kinds of events takes
// precedence.
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
			var b0, b1 bool
			var err0, err1 error
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				b1, err1 = recur(mid, high, n-half)
			}()
			b0, err0 = recur(low, mid, half)
			if !b0 || (err0 != nil) {
				return b0, err0
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
// invokes the range predicate for each of these batches in parallel,
// covering the half-open interval from low to high, including low but
// excluding high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range predicate is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and RangeOr returns false if all of them
// return false; or RangeOr returns true when at least one of them
// returns true, without waiting for the other range predicates to
// terminate. RangeOr may also return the left-most error value
// that is different from nil as a second return value. If both true
// values and non-nil error values are returned, then the left-most of
// these return value pairs are returned.
//
// RangeOr panics if high < low, or if n < 0.
//
// If one or more range predicates panic, the corresponding goroutines
// recover the panics, and RangeOr may eventually panic with the
// left-most recovered panic value. If both panics occur on the one
// hand, and true or non-nil error values are returned on the other
// hand, then the left-most of these two kinds of events takes
// precedence.
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
			var b0, b1 bool
			var err0, err1 error
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				b1, err1 = recur(mid, high, n-half)
			}()
			b0, err0 = recur(low, mid, half)
			if b0 || (err0 != nil) {
				return b0, err0
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
// invokes the range reducer for each of these batches in parallel,
// covering the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are
// then combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range reducer is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and RangeReduce returns either when all
// range reducers and pair reducers have terminated; or when one or
// more range functions return an error value that is different from nil,
// returning the left-most of these error values, without waiting for
// the other range and pair reducers to terminate.
//
// RangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and RangeReduce eventually panics
// with the left-most recovered panic value.
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
			var left, right interface{}
			var err0, err1 error
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				right, err1 = recur(mid, high, n-half)
			}()
			left, err0 = recur(low, mid, half)
			if err0 != nil {
				err = err0
				return
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if err1 != nil {
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
// reduce, and a pair reducer pair, divides the range into batches, and
// invokes the range reducer for each of these batches in parallel,
// covering the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are then
// combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range reducer is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and IntRangeReduce returns either when all
// range reducers and pair reducers have terminated; or when one or more
// range functions return an error value that is different from nil,
// returning the left-most of these error values, without waiting for the
// other range and pair reducers to terminate.
//
// IntRangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and IntRangeReduce eventually
// panics with the left-most recovered panic value.
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
			var left, right int
			var err0, err1 error
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				right, err1 = recur(mid, high, n-half)
			}()
			left, err0 = recur(low, mid, half)
			if err0 != nil {
				err = err0
				return
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if err1 != nil {
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
// in parallel, covering the half-open interval from low to high,
// including low but excluding high. The results of the range reducer
// invocations are then combined by repeated invocations of the pair
// reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range reducer is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and Float64RangeReduce returns either
// when all range reducers and pair reducers have terminated; or when
// one or more range functions return an error value that is different
// from nil, returning the left-most of these error values, without
// waiting for the other range and pair reducers to terminate.
//
// Float64RangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and Float64RangeReduce eventually
// panics with the left-most recovered panic value.
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
			var left, right float64
			var err0, err1 error
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				right, err1 = recur(mid, high, n-half)
			}()
			left, err0 = recur(low, mid, half)
			if err0 != nil {
				err = err0
				return
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if err1 != nil {
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
// in parallel, covering the half-open interval from low to high,
// including low but excluding high. The results of the range reducer
// invocations are then combined by repeated invocations of the pair
// reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range reducer is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and StringRangeReduce returns either when
// all range reducers and pair reducers have terminated; or when one
// or more range functions return an error value that is different
// from nil, returning the left-most of these error values, without
// waiting for the other range and pair reducers to terminate.
//
// StringRangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and StringRangeReduce eventually
// panics with the left-most recovered panic value.
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
			var left, right string
			var err0, err1 error
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				right, err1 = recur(mid, high, n-half)
			}()
			left, err0 = recur(low, mid, half)
			if err0 != nil {
				err = err0
				return
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if err1 != nil {
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
