/*
Package speculative provides functions for expressing parallel
algorithms, similar to the functions in package parallel, except that
the implementations here terminate early when they can.

And, Or, RangeAnd, and RangeOr terminate early if the final return
value is known early (if any of the predicates invoked in parallel
returns false for And, or true for Or).

ErrDo, ErrRange, ErrRangeReduce, ErrIntRangeReduce,
ErrFloat64RangeReduce, and ErrStringRangeReduce terminate early if any
of the functions invoked in parallel returns an error value different
from nil.

ErrAnd, ErrOr, ErrRangeAnd, and ErrRangeOr terminate early if the
final return value is known early, or if any predicate returns a
non-nil error value.

Additionally, all of these functions also handle panics, similar to
the functions in package parallel. However, panics may not propagate
to the invoking goroutine in case they terminate early because of a
known return value or a non-nil error value. See the documentations of
the functions for more precise details of the semantics.

None of the functions described above stop the execution of invoked
functions that may still be running in parallel in case of early
termination.  To ensure that compute resources are freed up in such
cases, user programs need to use some other safe form of communication
to gracefully stop their execution, for example the cancelation
feature of the context package of Go's standard library. (Any such
additional communication is likely to add additional performance
overhead, which is why this is not done by default.)
*/
package speculative

import (
	"fmt"
	"sync"

	"github.com/exascience/pargo"
)

/*
ErrDo receives zero or more ErrThunk functions and executes them in
parallel.

Each thunk is invoked in its own goroutine, and ErrDo returns either
when all thunks have terminated; or when one or more thunks return an
error value that is different from nil, returning the left-most of
these error values, without waiting for the other thunks to terminate.

If one or more thunks panic, the corresponding goroutines recover the
panics, and ErrDo may eventually panic with the left-most recovered
panic value. If both non-nil error values are returned and panics
occur, then the left-most of these events takes precedence.
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
	if err0 != nil {
		return err0
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return err1
}

/*
And receives zero or more Predicate functions and executes them in
parallel.

Each predicate is invoked in its own goroutine, and And returns true
if all of them return true; or And returns false when at least one of
them returns false, without waiting for the other predicates to
terminate.

If one or more predicates panic, the corresponding goroutines recover
the panics, and And may eventually panic with the left-most recovered
panic value. If both panics occur and false values are returned, then
the left-most of these events takes precedence.
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
	if !b0 {
		return false
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b1
}

/*
Or receives zero or more Predicate functions and executes them in
parallel.

Each predicate is invoked in its own goroutine, and Or returns false
if all of them return false; or Or returns true when at least one of
them returns true, without waiting for the other predicates to
terminate.

If one or more predicates panic, the corresponding goroutines recover
the panics, and Or may eventually panic with the left-most recovered
panic value. If both panics occur and true values are returned, then
the left-most of these events takes precedence.
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
	if b0 {
		return true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b1
}

/*
ErrAnd receives zero or more ErrPredicate functions and executes them
in parallel.

Each predicate is invoked in its own goroutine, and ErrAnd returns
true if all of them return true; or ErrAnd returns false when at least
one of them returns false, without waiting for the other predicates to
terminate.  ErrAnd may also return the left-most error value that is
different from nil as a second return value. If both false values and
non-nil error values are returned, then the left-most of these return
value pairs are returned.

If one or more predicates panic, the corresponding goroutines recover
the panics, and ErrAnd may eventually panic with the left-most
recovered panic value. If both panics occur on the one hand, and false
or non-nil error values are returned on the other hand, then the
left-most of these two kinds of events takes precedence.
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

/*
ErrOr receives zero or more ErrPredicate functions and executes them
in parallel.

Each predicate is invoked in its own goroutine, and ErrOr returns
false if all of them return false; or ErrOr returns true when at least
one of them returns true, without waiting for the other predicates to
terminate.  ErrOr may also return the left-most error value that is
different from nil as a second return value. If both true values and
non-nil error values are returned, then the left-most of these return
value pairs are returned.

If one or more predicates panic, the corresponding goroutines recover
the panics, and ErrOr may eventually panic with the left-most
recovered panic value. If both panics occur on the one hand, and true
or non-nil error values are returned on the other hand, then the
left-most of these two kinds of events takes precedence.
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

/*
ErrRange receives a range, a batch count, and an ErrRangeFunc
function, divides the range into batches, and invokes the range
function for each of these batches in parallel.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

The range function is invoked for each batch in its own goroutine, and
ErrRange returns either when all range functions have terminated; or
when one or more range functions return an error value that is
different from nil, returning the left-most of these error values,
without waiting for the other range functions to terminate.

ErrRange panics if high < low, or if n < 0.

If one or more range functions panic, the corresponding goroutines
recover the panics, and ErrRange may eventually panic with the
left-most recovered panic value. If both non-nil error values are
returned and panics occur, then the left-most of these events take
precedence.
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
				var err1 error
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
				if err0 := recur(low, mid, half); err0 != nil {
					return err0
				}
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return err1
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
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
and RangeAnd returns true if all of them return true; or RangeAnd
returns false when at least one of them returns false, without waiting
for the other range predicates to terminate.

RangeAnd panics if high < low, or if n < 0.

If one or more range predicates panic, the corresponding goroutines
recover the panics, and RangeAnd may eventually panic with the
left-most recovered panic value. If both panics occur and false values
are returned, then the left-most of these events takes precedence.
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
				var b1 bool
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
				if !recur(low, mid, half) {
					return false
				}
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return b1
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
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
and RangeOr returns false if all of them return false; or RangeOr
returns true when at least one of them returns true, without waiting
for the other range predicates to terminate.

RangeOr panics if high < low, or if n < 0.

If one or more range predicates panic, the corresponding goroutines
recover the panics, and RangeOr may eventually panic with the
left-most recovered panic value. If both panics occur and true values
are returned, then the left-most of these events takes precedence.
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
				var b1 bool
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
				if recur(low, mid, half) {
					return true
				}
				wg.Wait()
				if p != nil {
					panic(p)
				}
				return b1
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
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
and ErrRangeAnd returns true if all of them return true; or
ErrRangeAnd returns false when at least one of them returns false,
without waiting for the other range predicates to terminate.
ErrRangeAnd may also return the left-most error value that is
different from nil as a second return value. If both false values and
non-nil error values are returned, then the left-most of these return
value pairs are returned.

ErrRangeAnd panics if high < low, or if n < 0.

If one or more range predicates panic, the corresponding goroutines
recover the panics, and ErrRangeAnd may eventually panic with the
left-most recovered panic value. If both panics occur on the one hand,
and false or non-nil error values are returned on the other hand, then
the left-most of these two kinds of events takes precedence.
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
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
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
and ErrRangeOr returns false if all of them return false; or
ErrRangeOr returns true when at least one of them returns true,
without waiting for the other range predicates to terminate.
ErrRangeOr may also return the left-most error value that is different
from nil as a second return value. If both true values and non-nil
error values are returned, then the left-most of these return value
pairs are returned.

ErrRangeOr panics if high < low, or if n < 0.

If one or more range predicates panic, the corresponding goroutines
recover the panics, and ErrRangeOr may eventually panic with the
left-most recovered panic value. If both panics occur on the one hand,
and true or non-nil error values are returned on the other hand, then
the left-most of these two kinds of events takes precedence.
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
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
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
ErrRangeReduce returns either when all range reducers and pair
reducers have terminated; or when one or more range functions return
an error value that is different from nil, returning the left-most of
these error values, without waiting for the other range and pair
reducers to terminate.

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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
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
ErrIntRangeReduce returns either when all range reducers and pair
reducers have terminated; or when one or more range functions return
an error value that is different from nil, returning the left-most of
these error values, without waiting for the other range and pair
reducers to terminate.

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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
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
ErrFloat64RangeReduce returns either when all range reducers and pair
reducers have terminated; or when one or more range functions return
an error value that is different from nil, returning the left-most of
these error values, without waiting for the other range and pair
reducers to terminate.

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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
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
ErrStringRangeReduce returns either when all range reducers and pair
reducers have terminated; or when one or more range functions return
an error value that is different from nil, returning the left-most of
these error values, without waiting for the other range and pair
reducers to terminate.

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
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, pargo.ComputeNofBatches(low, high, n))
}
