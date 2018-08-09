// Package parallel provides functions for expressing parallel
// algorithms.
//
// See https://github.com/ExaScience/pargo/wiki/TaskParallelism for a
// general overview.
package parallel

import (
	"fmt"
	"sync"

	"github.com/exascience/pargo/internal"
)

// Do receives zero or more thunks and executes them in parallel.
//
// Each thunks is invoked in its own goroutine, and Do returns only
// when all thunks have terminated.
//
// If one or more thunks panic, the corresponding goroutines recover
// the panics, and Do eventually panics with the left-most recovered
// panic value.
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
				p = recover()
				wg.Done()
			}()
			thunks[1]()
		}()
		thunks[0]()
	default:
		half := len(thunks) / 2
		go func() {
			defer func() {
				p = recover()
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

// ErrDo receives zero or more thunks and executes them in parallel.
//
// Each thunk is invoked in its own goroutine, and ErrDo returns only
// when all thunks have terminated, returning the left-most error
// value that is different from nil.
//
// If one or more thunks panic, the corresponding goroutines recover
// the panics, and ErrDo eventually panics with the left-most
// recovered panic value.
func ErrDo(thunks ...func() error) (err error) {
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
			err1 = ErrDo(thunks[half:]...)
		}()
		err0 = ErrDo(thunks[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	if err0 != nil {
		err = err0
	} else {
		err = err1
	}
	return
}

// And receives zero or more predicate functions and executes them in
// parallel.
//
// Each predicate is invoked in its own goroutine, and And returns
// only when all predicates have terminated, combining all return
// values with the && operator, with true as the default return value.
//
// If one or more predicates panic, the corresponding goroutines
// recover the panics, and And eventually panics with the left-most
// recovered panic value.
func And(predicates ...func() bool) (result bool) {
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
				p = recover()
				wg.Done()
			}()
			b1 = predicates[1]()
		}()
		b0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				p = recover()
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

// Or receives zero or more predicate functions and executes them in
// parallel.
//
// Each predicate is invoked in its own goroutine, and Or returns only
// when all predicates have terminated, combining all return values
// with the || operator, with false as the default return value.
//
// If one or more predicates panic, the corresponding goroutines
// recover the panics, and Or eventually panics with the left-most
// recovered panic value.
func Or(predicates ...func() bool) (result bool) {
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
				p = recover()
				wg.Done()
			}()
			b1 = predicates[1]()
		}()
		b0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				p = recover()
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

// ErrAnd receives zero or more predicate functions and executes
// them in parallel.
//
// Each predicate is invoked in its own goroutine, and ErrAnd returns
// only when all predicates have terminated, combining all return
// values with the && operator, with true as the default return value.
// ErrAnd also returns the left-most error value that is different
// from nil as a second return value.
//
// If one or more predicates panic, the corresponding goroutines
// recover the panics, and ErrAnd eventually panics with the left-most
// recovered panic value.
func ErrAnd(predicates ...func() (bool, error)) (result bool, err error) {
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
			b1, err1 = ErrAnd(predicates[half:]...)
		}()
		b0, err0 = ErrAnd(predicates[:half]...)
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

// ErrOr receives zero or more predicate functions and executes
// them in parallel.
//
// Each predicate is invoked in its own goroutine, and ErrOr returns
// only when all predicates have terminated, combining all return
// values with the || operator, with false as the default return
// value.  ErrOr also returns the left-most error value that is
// different from nil as a second return value.
//
// If one or more predicates panic, the corresponding goroutines
// recover the panics, and ErrOr eventually panics with the left-most
// recovered panic value.
func ErrOr(predicates ...func() (bool, error)) (result bool, err error) {
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
			b1, err1 = ErrOr(predicates[half:]...)
		}()
		b0, err0 = ErrOr(predicates[:half]...)
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
// with 0 <= low <= high, and Range returns only when all range functions
// have terminated.
//
// Range panics if high < low, or if n < 0.
//
// If one or more range function invocations panic, the corresponding
// goroutines recover the panics, and Range eventually panics with the
// left-most recovered panic value.
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
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						p = recover()
						wg.Done()
					}()
					recur(mid, high, n-half)
				}()
				recur(low, mid, half)
				wg.Wait()
				if p != nil {
					panic(p)
				}
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrRange receives a range, a batch count n, and a range function f,
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
// with 0 <= low <= high, and ErrRange returns only when all range
// functions have terminated, returning the left-most error value
// that is different from nil.
//
// ErrRange panics if high < low, or if n < 0.
//
// If one or more range function invocations panic, the corresponding
// goroutines recover the panics, and ErrRange eventually panics with
// the left-most recovered panic value.
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
			var err0, err1 error
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
			err0 = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
// predicate for each of these batches in parallel, covering the
// half-open interval from low to high, including low but excluding
// high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range predicate is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and RangeAnd returns only when all range
// predicates have terminated, combining all return values with
// the && operator.
//
// RangeAnd panics if high < low, or if n < 0.
//
// If one or more range predicate invocations panic, the corresponding
// goroutines recover the panics, and RangeAnd eventually panics with
// the left-most recovered panic value.
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
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
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

// RangeOr receives a range, a batch count n, and a range predicate
// function f, divides the range into batches, and invokes the range
// predicate for each of these batches in parallel, covering the
// half-open interval from low to high, including low but excluding
// high.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range predicate is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and RangeOr returns only when all range
// predicates have terminated, combining all return values with
// the || operator.
//
// RangeOr panics if high < low, or if n < 0.
//
// If one or more range predicate invocations panic, the corresponding
// goroutines recover the panics, and RangeOr eventually panics with
// the left-most recovered panic value.
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
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
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

// ErrRangeAnd receives a range, a batch count n, and a range
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
// with 0 <= low <= high, and ErrRangeAnd returns only when all range
// predicates have terminated, combining all return values with the &&
// operator. ErrRangeAnd also returns the left-most error value that
// is different from nil as a second return value.
//
// ErrRangeAnd panics if high < low, or if n < 0.
//
// If one or more range predicate invocations panic, the corresponding
// goroutines recover the panics, and ErrRangeAnd eventually panics
// with the left-most recovered panic value.
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

// ErrRangeOr receives a range, a batch count n, and a range
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
// with 0 <= low <= high, and ErrRangeOr returns only when all range
// predicates have terminated, combining all return values with the ||
// operator. ErrRangeAnd also returns the left-most error value that
// is different from nil as a second return value.
//
// ErrRangeOr panics if high < low, or if n < 0.
//
// If one or more range predicate invocations panic, the corresponding
// goroutines recover the panics, and ErrRangeOr eventually panics
// with the left-most recovered panic value.
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
// and a pair reducer pair, divides the range into batches, and invokes
// the range reducer for each of these batches in parallel, covering
// the half-open interval from low to high, including low but
// excluding high. The results of the range reducer invocations are
// then combined by repeated invocations of the pair reducer.
//
// The range is specified by a low and high integer, with low <=
// high. The batches are determined by dividing up the size of the
// range (high - low) by n. If n is 0, a reasonable default is used
// that takes runtime.GOMAXPROCS(0) into account.
//
// The range reducer is invoked for each batch in its own goroutine,
// with 0 <= low <= high, and RangeReduce returns only when all range
// reducers and pair reducers have terminated.
//
// RangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and RangeReduce eventually panics
// with the left-most recovered panic value.
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
			var left, right interface{}
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return pair(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrRangeReduce receives a range, a batch count, a range reducer reduce,
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
// with 0 <= low <= high, and ErrRangeReduce returns only when all range
// reducers and pair reducers have terminated. ErrRangeReduce also returns
// the left-most error value that is different from nil as a second return
// value.
//
// ErrRangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and ErrRangeReduce eventually panics
// with the left-most recovered panic value.
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
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
// with 0 <= low <= high, and RangeReduce returns only when all range
// reducers and pair reducers have terminated.
//
// IntRangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and IntRangeReduce eventually panics
// with the left-most recovered panic value.
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
			var left, right int
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return pair(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// ErrIntRangeReduce receives a range, a batch count n, a range reducer
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
// with 0 <= low <= high, and ErrIntRangeReduce returns only when all
// range reducers and pair reducers have terminated. ErrIntRangeReduce
// also returns the left-most error value that is different from nil as
// a second return value.
//
// ErrIntRangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and ErrIntRangeReduce eventually
// panics with the left-most recovered panic value.
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
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
// with 0 <= low <= high, and RangeReduce returns only when all range
// reducers and pair reducers have terminated.
//
// Float64RangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and Float64RangeReduce eventually
// panics with the left-most recovered panic value.
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
			var left, right float64
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
// with 0 <= low <= high, and ErrFloat64RangeReduce returns only when
// all range reducers and pair reducers have terminated.
// ErrFloat64RangeReduce also returns the left-most error value that
// is different from nil as a second return value.
//
// ErrFloat64RangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and ErrFloat64RangeReduce eventually
// panics with the left-most recovered panic value.
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
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
// with 0 <= low <= high, and RangeReduce returns only when all range
// reducers and pair reducers have terminated.
//
// StringRangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and StringRangeReduce eventually
// panics with the left-most recovered panic value.
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
			var left, right string
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = recover()
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
// with 0 <= low <= high, and ErrStringRangeReduce returns only when
// all range reducers and pair reducers have terminated.
// ErrStringRangeReduce also returns the left-most error value that
// is different from nil as a second return value.
//
// ErrStringRangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding
// goroutines recover the panics, and ErrStringRangeReduce eventually
// panics with the left-most recovered panic value.
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
			wg.Wait()
			if p != nil {
				panic(p)
			}
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
