/*
Package parallel provides functions for expressing parallel
algorithms.

Do, And, and Or are useful for executing two or more functions in
parallel, by invoking them each in their own goroutines. Do does this
for functions without return values, whereas And and Or do this for
predicates that return boolean values, and combine these return values
with the && or || operator.

ErrDo, ErrAnd, and ErrOr are like Do, And, and Or, except that the
functions they invoke in parallel can additionally also return error
values. If any function returns an error value different from nil, the
left-most of those non-nil error value is returned by ErrDo, ErrAnd,
or ErrOr (in addition to the primary return value in the case of
ErrAnd and ErrOr).

Range, RangeAnd, and RangeOr are similar do Do, And, and Or, except
they receive a range and a single range function each. The range is
specified by a low and high integer, with low <= high. Range,
RangeAnd, and RangeOr divide the range up into subranges and invoke
the range function for each of these subranges in parallel. RangeAnd
and RangeOr additionally combine the boolean return values of the
range predicates.

RangeReduce is similar to RangeAnd and RangeOr, except that the
partial results are of type interface{} instead of bool, and the
partial results are combined using a function that is explicitly
passed as a parameter.  IntRangeReduce, Float64RangeReduce, and
StringRangeReduce can be used in case it is known that the partial
results are of type int, float64, or string respectively.

ErrRange, ErrRangeAnd, ErrRangeOr, ErrRangeReduce, ErrIntRangeReduce,
ErrFloat64RangeReduce, and ErrStringRangeReduce are like Range,
RangeAnd, RangeOr, RangeReduce, IntRangeReduce, Float64RangeReduce,
and StringRangeReduce, except that the functions they invoke in
parallel can additionally also return error values, which are combined
in a similary way as by ErrDo, ErrAnd, and ErrOr.

Range, RangeAnd, RangeOr, RangeReduce and its variants that deal with
errors and/or are specialized for particular result types are useful
for example for expressing parallel algorithms over slices.

All of the functions described above also take care of dealing with
panics: If one or more of the functions invoked in parallel panics,
the panic is recovered in the corresponding goroutine, and the
goroutine that invoked one of the functions in the parallel package
panics with the left-most of the recovered panic values.

Both error value and panic handling are handled left-to-right to
ensure that the behavior is semantically similar to a corresponding
sequential left-to-right execution. (Parallelism is not concurrency,
so the functions here should only be used to speed up otherwise
sequential programs.)

There are also sequential implementations of these functions available
in the sequential package, which are useful for testing and debugging.

Some of the functions are also available as speculative variants in
the speculative package. The implementations in the parallel package
always wait for all invoked functions to terminate, whereas the
implementations in the speculative package terminate earlier if they
can.
*/
package parallel

import (
	"fmt"
	"sync"

	"github.com/exascience/pargo"
	"github.com/exascience/pargo/internal"
)

/*
Do receives zero or more Thunk functions and executes them in
parallel.

Each thunk is invoked in its own goroutine, and Do returns only when
all thunks have terminated.

If one or more thunks panic, the corresponding goroutines recover the
panics, and Do eventually panics with the left-most recovered panic
value.
*/
func Do(thunks ...pargo.Thunk) {
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
				wg.Done()
				p = recover()
			}()
			thunks[1]()
		}()
		thunks[0]()
	default:
		half := len(thunks) / 2
		go func() {
			defer func() {
				wg.Done()
				p = recover()
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

/*
ErrDo receives zero or more ErrThunk functions and executes them in
parallel.

Each thunk is invoked in its own goroutine, and ErrDo returns only
when all thunks have terminated, returning the left-most error value
that is different from nil.

If one or more thunks panic, the corresponding goroutines recover the
panics, and ErrDo eventually panics with the left-most recovered panic
value.
*/
func ErrDo(thunks ...pargo.ErrThunk) (err error) {
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
				wg.Done()
				p = recover()
			}()
			err1 = thunks[1]()
		}()
		err0 = thunks[0]()
	default:
		half := len(thunks) / 2
		go func() {
			defer func() {
				wg.Done()
				p = recover()
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

/*
And receives zero or more Predicate functions and executes them in
parallel.

Each predicate is invoked in its own goroutine, and And returns only
when all predicates have terminated, combining all return values with
the && operator, with true as the default return value.

If one or more predicates panic, the corresponding goroutines recover
the panics, and And eventually panics with the left-most recovered
panic value.
*/
func And(predicates ...pargo.Predicate) (result bool) {
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
				wg.Done()
				p = recover()
			}()
			b1 = predicates[1]()
		}()
		b0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				wg.Done()
				p = recover()
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

/*
Or receives zero or more Predicate functions and executes them in
parallel.

Each predicate is invoked in its own goroutine, and Or returns only
when all predicates have terminated, combining all return values with
the || operator, with false as the default return value.

If one or more predicates panic, the corresponding goroutines recover
the panics, and Or eventually panics with the left-most recovered
panic value.
*/
func Or(predicates ...pargo.Predicate) (result bool) {
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
				wg.Done()
				p = recover()
			}()
			b1 = predicates[1]()
		}()
		b0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				wg.Done()
				p = recover()
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

/*
ErrAnd receives zero or more ErrPredicate functions and executes them
in parallel.

Each predicate is invoked in its own goroutine, and ErrAnd returns
only when all predicates have terminated, combining all return values
with the && operator, with true as the default return value.  ErrAnd
also returns the left-most error value that is different from nil as a
second return value.

If one or more predicates panic, the corresponding goroutines recover
the panics, and ErrAnd eventually panics with the left-most recovered
panic value.
*/
func ErrAnd(predicates ...pargo.ErrPredicate) (result bool, err error) {
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
				wg.Done()
				p = recover()
			}()
			b1, err1 = predicates[1]()
		}()
		b0, err0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				wg.Done()
				p = recover()
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

/*
ErrOr receives zero or more ErrPredicate functions and executes them
in parallel.

Each predicate is invoked in its own goroutine, and ErrOr returns only
when all predicates have terminated, combining all return values with
the || operator, with false as the default return value.  ErrOr also
returns the left-most error value that is different from nil as a
second return value.

If one or more predicates panic, the corresponding goroutines recover
the panics, and ErrOr eventually panics with the left-most recovered
panic value.
*/
func ErrOr(predicates ...pargo.ErrPredicate) (result bool, err error) {
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
				wg.Done()
				p = recover()
			}()
			b1, err1 = predicates[1]()
		}()
		b0, err0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				wg.Done()
				p = recover()
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

/*
Range receives a range, a batch count, and a RangeFunc function,
divides the range into batches, and invokes the range function for
each of these batches in parallel.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range function is invoked for each batch in its own goroutine, and
Range returns only when all range functions have terminated.

Range panics if high < low, or if n < 0.

If one or more range function invocations panic, the corresponding
goroutines recover the panics, and Range eventually panics with the
left-most recovered panic value.
*/
func Range(low, high, n int, f pargo.RangeFunc) {
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
						wg.Done()
						p = recover()
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

/*
ErrRange receives a range, a batch count, and an ErrRangeFunc
function, divides the range into batches, and invokes the range
function for each of these batches in parallel.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range function is invoked for each batch in its own goroutine, and
ErrRange returns only when all range functions have terminated,
returning the left-most error value that is different from nil.

ErrRange panics if high < low, or if n < 0.

If one or more range function invocations panic, the corresponding
goroutines recover the panics, and ErrRange eventually panics with the
left-most recovered panic value.
*/
func ErrRange(low, high, n int, f pargo.ErrRangeFunc) error {
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
			} else {
				var err0, err1 error
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
RangeAnd receives a range, a batch count, and a RangePredicate
function, divides the range into batches, and invokes the range
predicate for each of these batches in parallel.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range predicate is invoked for each batch in its own goroutine,
and RangeAnd returns only when all range predicates have terminated,
combining all return values with the && operator.

RangeAnd panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and RangeAnd eventually panics with the
left-most recovered panic value.
*/
func RangeAnd(low, high, n int, f pargo.RangePredicate) bool {
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
			} else {
				var b0, b1 bool
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
					}()
					b1 = recur(mid, high, n-half)
				}()
				b0 = recur(low, mid, half)
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return b0 && b1
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
RangeOr receives a range, a batch count, and a RangePredicate
function, divides the range into batches, and invokes the range
predicate for each of these batches in parallel.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range predicate is invoked for each batch in its own goroutine,
and RangeOr returns only when all range predicates have terminated,
combining all return values with the || operator.

RangeOr panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and RangeOr eventually panics with the
left-most recovered panic value.
*/
func RangeOr(low, high, n int, f pargo.RangePredicate) bool {
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
			} else {
				var b0, b1 bool
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
					}()
					b1 = recur(mid, high, n-half)
				}()
				b0 = recur(low, mid, half)
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return b0 || b1
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
ErrRangeAnd receives a range, a batch count, and an ErrRangePredicate
function, divides the range into batches, and invokes the range
predicate for each of these batches in parallel.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range predicate is invoked for each batch in its own goroutine,
and ErrRangeAnd returns only when all range predicates have
terminated, combining all return values with the && operator.
ErrRangeAnd also returns the left-most error value that is different
from nil as a second return value.

ErrRangeAnd panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and ErrRangeAnd eventually panics with
the left-most recovered panic value.
*/
func ErrRangeAnd(low, high, n int, f pargo.ErrRangePredicate) (bool, error) {
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
			} else {
				var b0, b1 bool
				var err0, err1 error
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
ErrRangeOr receives a range, a batch count, and an ErrRangePredicate
function, divides the range into batches, and invokes the range
predicate for each of these batches in parallel.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range predicate is invoked for each batch in its own goroutine,
and ErrRangeOr returns only when all range predicates have terminated,
combining all return values with the || operator. ErrRangeAnd also
returns the left-most error value that is different from nil as a
second return value.

ErrRangeOr panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and ErrRangeOr eventually panics with
the left-most recovered panic value.
*/
func ErrRangeOr(low, high, n int, f pargo.ErrRangePredicate) (bool, error) {
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
			} else {
				var b0, b1 bool
				var err0, err1 error
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
RangeReduce receives a range, a batch count, a RangeReducer, and a
PairReducer function, divides the range into batches, and invokes the
range reducer for each of these batches in parallel. The results of
the range reducer invocations are then combined by repeated
invocations of the pair reducer.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range reducer is invoked for each batch in its own goroutine, and
RangeReduce returns only when all range reducers and pair reducers
have terminated.

RangeReduce panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and RangeReduce eventually panics with
the left-most recovered panic value.
*/
func RangeReduce(low, high, n int, reduce pargo.RangeReducer, pair pargo.PairReducer) interface{} {
	var recur func(int, int, int) interface{}
	recur = func(low, high, n int) (result interface{}) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= half {
				return reduce(low, high)
			} else {
				var left, right interface{}
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
					}()
					right = recur(mid, high, n-half)
				}()
				left = recur(low, mid, half)
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return pair(left, right)
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
ErrRangeReduce receives a range, a batch count, an ErrRangeReducer,
and an ErrPairReducer function, divides the range into batches, and
invokes the range reducer for each of these batches in parallel. The
results of the range reducer invocations are then combined by repeated
invocations of the pair reducer.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range reducer is invoked for each batch in its own goroutine, and
ErrRangeReduce returns only when all range reducers and pair reducers
have terminated. ErrRangeReduce also returns the left-most error value
that is different from nil as a second return value.

ErrRangeReduce panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and ErrRangeReduce eventually panics
with the left-most recovered panic value.
*/
func ErrRangeReduce(low, high, n int, reduce pargo.ErrRangeReducer, pair pargo.ErrPairReducer) (interface{}, error) {
	var recur func(int, int, int) (interface{}, error)
	recur = func(low, high, n int) (result interface{}, err error) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= half {
				return reduce(low, high)
			} else {
				var left, right interface{}
				var err0, err1 error
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
IntRangeReduce receives a range, a batch count, an IntRangeReducer,
and an IntPairReducer function, divides the range into batches, and
invokes the range reducer for each of these batches in parallel. The
results of the range reducer invocations are then combined by repeated
invocations of the pair reducer.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range reducer is invoked for each batch in its own goroutine, and
RangeReduce returns only when all range reducers and pair reducers
have terminated.

IntRangeReduce panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and IntRangeReduce eventually panics
with the left-most recovered panic value.
*/
func IntRangeReduce(low, high, n int, reduce pargo.IntRangeReducer, pair pargo.IntPairReducer) int {
	var recur func(int, int, int) int
	recur = func(low, high, n int) (result int) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= half {
				return reduce(low, high)
			} else {
				var left, right int
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
					}()
					right = recur(mid, high, n-half)
				}()
				left = recur(low, mid, half)
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return pair(left, right)
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
ErrIntRangeReduce receives a range, a batch count, an
ErrIntRangeReducer, and an ErrIntPairReducer function, divides the
range into batches, and invokes the range reducer for each of these
batches in parallel. The results of the range reducer invocations are
then combined by repeated invocations of the pair reducer.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range reducer is invoked for each batch in its own goroutine, and
ErrIntRangeReduce returns only when all range reducers and pair
reducers have terminated. ErrIntRangeReduce also returns the left-most
error value that is different from nil as a second return value.

ErrIntRangeReduce panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and ErrIntRangeReduce eventually panics
with the left-most recovered panic value.
*/
func ErrIntRangeReduce(low, high, n int, reduce pargo.ErrIntRangeReducer, pair pargo.ErrIntPairReducer) (int, error) {
	var recur func(int, int, int) (int, error)
	recur = func(low, high, n int) (result int, err error) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= half {
				return reduce(low, high)
			} else {
				var left, right int
				var err0, err1 error
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
Float64RangeReduce receives a range, a batch count, a
Float64RangeReducer, and a Float64PairReducer function, divides the
range into batches, and invokes the range reducer for each of these
batches in parallel. The results of the range reducer invocations are
then combined by repeated invocations of the pair reducer.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range reducer is invoked for each batch in its own goroutine, and
RangeReduce returns only when all range reducers and pair reducers
have terminated.

Float64RangeReduce panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and Float64RangeReduce eventually
panics with the left-most recovered panic value.
*/
func Float64RangeReduce(low, high, n int, reduce pargo.Float64RangeReducer, pair pargo.Float64PairReducer) float64 {
	var recur func(int, int, int) float64
	recur = func(low, high, n int) (result float64) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= half {
				return reduce(low, high)
			} else {
				var left, right float64
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
					}()
					right = recur(mid, high, n-half)
				}()
				left = recur(low, mid, half)
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return pair(left, right)
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
ErrFloat64RangeReduce receives a range, a batch count, an
ErrFloat64RangeReducer, and an ErrFloat64PairReducer function, divides
the range into batches, and invokes the range reducer for each of
these batches in parallel. The results of the range reducer
invocations are then combined by repeated invocations of the pair
reducer.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range reducer is invoked for each batch in its own goroutine, and
ErrFloat64RangeReduce returns only when all range reducers and pair
reducers have terminated. ErrFloat64RangeReduce also returns the
left-most error value that is different from nil as a second return
value.

ErrFloat64RangeReduce panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and ErrFloat64RangeReduce eventually
panics with the left-most recovered panic value.
*/
func ErrFloat64RangeReduce(low, high, n int, reduce pargo.ErrFloat64RangeReducer, pair pargo.ErrFloat64PairReducer) (float64, error) {
	var recur func(int, int, int) (float64, error)
	recur = func(low, high, n int) (result float64, err error) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= half {
				return reduce(low, high)
			} else {
				var left, right float64
				var err0, err1 error
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
StringRangeReduce receives a range, a batch count, a
StringRangeReducer, and a StringPairReducer function, divides the
range into batches, and invokes the range reducer for each of these
batches in parallel. The results of the range reducer invocations are
then combined by repeated invocations of the pair reducer.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range reducer is invoked for each batch in its own goroutine, and
RangeReduce returns only when all range reducers and pair reducers
have terminated.

StringRangeReduce panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and StringRangeReduce eventually panics
with the left-most recovered panic value.
*/
func StringRangeReduce(low, high, n int, reduce pargo.StringRangeReducer, pair pargo.StringPairReducer) string {
	var recur func(int, int, int) string
	recur = func(low, high, n int) (result string) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= half {
				return reduce(low, high)
			} else {
				var left, right string
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
					}()
					right = recur(mid, high, n-half)
				}()
				left = recur(low, mid, half)
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return pair(left, right)
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

/*
ErrStringRangeReduce receives a range, a batch count, an
ErrStringRangeReducer, and an ErrStringPairReducer function, divides
the range into batches, and invokes the range reducer for each of
these batches in parallel. The results of the range reducer
invocations are then combined by repeated invocations of the pair
reducer.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range reducer is invoked for each batch in its own goroutine, and
ErrStringRangeReduce returns only when all range reducers and pair
reducers have terminated. ErrStringRangeReduce also returns the
left-most error value that is different from nil as a second return
value.

ErrStringRangeReduce panics if high < low, or if n < 0.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and ErrStringRangeReduce eventually
panics with the left-most recovered panic value.
*/
func ErrStringRangeReduce(low, high, n int, reduce pargo.ErrStringRangeReducer, pair pargo.ErrStringPairReducer) (string, error) {
	var recur func(int, int, int) (string, error)
	recur = func(low, high, n int) (result string, err error) {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= half {
				return reduce(low, high)
			} else {
				var left, right string
				var err0, err1 error
				var p interface{}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						p = recover()
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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}
