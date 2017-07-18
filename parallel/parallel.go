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
specified by a low and high integer, with 0 <= low <= high. Range,
RangeAnd, and RangeOr divide the range up into subranges and invoke
the range function for each of these subranges in parallel. RangeAnd
and RangeOr additionally combine the boolean return values of the
range predicates.

ErrRange, ErrRangeAnd, and ErrRangeOr are like Range, RangeAnd, and
RangeOr, except that the functions they invoke in parallel can
additionally also return error values, which are combined in a
similary way as by ErrDo, ErrAnd, and ErrOr.

Range, ErrRange, RangeAnd, ErrRangeAnd, RangeOr, and ErrRangeOr are
useful for example for expressing parallel algorithms over slices.

All of these functions divide up the input ranges according to a
threshold parameter. See ComputeEffectiveThreshold in package pargo
for more details. Useful threshold parameter values are 1 to evenly
divide up the range across the avaliable logical CPUs (as determined
by runtime.GOMAXPROCS(0)); or 2 or higher to additionally divide that
number by the threshold parameter. Use 1 if you expect no load
imbalance, 2 if you expect some load imbalance, or 3 or more if you
expect even more load imbalance.

A threshold parameter value of 0 divides up the input range into
subranges of size 1 and yields the most fine-grained parallelism.
Fine-grained parallelism (with a threshold parameter of 0, or 2 or
higher) only pays off if the work per subrange is sufficiently large
to compensate for the scheduling overhead.

A threshold parameter value below zero can be used to specify the
subrange size directly, which becomes the absolute value of the
threshold parameter value.

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
	"sync"

	"github.com/exascience/pargo"
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
Range receives a range, a threshold, and a RangeFunc function, divides
the range into subranges, and invokes the range function for each of
these subranges in parallel.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.

The range function is invoked for each subrange in its own goroutine,
and Range returns only when all range functions have terminated.

If one or more range function invocations panic, the corresponding
goroutines recover the panics, and Range eventually panics with the
left-most recovered panic value.
*/
func Range(low, high, threshold int, f pargo.RangeFunc) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	var recur func(int, int)
	recur = func(low, high int) {
		if size := high - low; size <= threshold {
			f(low, high)
		} else {
			half := size / 2
			mid := low + half
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					p = recover()
				}()
				recur(mid, high)
			}()
			recur(low, mid)
			wg.Wait()
			if p != nil {
				panic(p)
			}
		}
	}
	recur(low, high)
}

/*
ErrRange receives a range, a threshold, and an ErrRangeFunc function,
divides the range into subranges, and invokes the range function for
each of these subranges in parallel.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.

The range function is invoked for each subrange in its own goroutine,
and ErrRange returns only when all range functions have terminated,
returning the left-most error value that is different from nil.

If one or more range function invocations panic, the corresponding
goroutines recover the panics, and ErrRange eventually panics with the
left-most recovered panic value.
*/
func ErrRange(low, high, threshold int, f pargo.ErrRangeFunc) error {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	var recur func(int, int) error
	recur = func(low, high int) (err error) {
		if size := high - low; size <= threshold {
			return f(low, high)
		} else {
			half := size / 2
			mid := low + half
			var err0, err1 error
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					p = recover()
				}()
				err1 = recur(mid, high)
			}()
			err0 = recur(low, mid)
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
	}
	return recur(low, high)
}

/*
RangeAnd receives a range, a threshold, and a RangePredicate function,
divides the range into subranges, and invokes the range predicate for
each of these subranges in parallel.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.

The range predicate is invoked for each subrange in its own goroutine,
and RangeAnd returns only when all range predicates have terminated,
combining all return values with the && operator.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and RangeAnd eventually panics with the
left-most recovered panic value.
*/
func RangeAnd(low, high, threshold int, f pargo.RangePredicate) bool {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	var recur func(int, int) bool
	recur = func(low, high int) (result bool) {
		if size := high - low; size <= threshold {
			return f(low, high)
		} else {
			half := size / 2
			mid := low + half
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					p = recover()
				}()
				b1 = recur(mid, high)
			}()
			b0 = recur(low, mid)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b0 && b1
		}
	}
	return recur(low, high)
}

/*
RangeOr receives a range, a threshold, and a RangePredicate function,
divides the range into subranges, and invokes the range predicate for
each of these subranges in parallel.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.

The range predicate is invoked for each subrange in its own goroutine,
and RangeOr returns only when all range predicates have terminated,
combining all return values with the || operator.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and RangeOr eventually panics with the
left-most recovered panic value.
*/
func RangeOr(low, high, threshold int, f pargo.RangePredicate) bool {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	var recur func(int, int) bool
	recur = func(low, high int) (result bool) {
		if size := high - low; size <= threshold {
			return f(low, high)
		} else {
			half := size / 2
			mid := low + half
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					p = recover()
				}()
				b1 = recur(mid, high)
			}()
			b0 = recur(low, mid)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b0 || b1
		}
	}
	return recur(low, high)
}

/*
ErrRangeAnd receives a range, a threshold, and an ErrRangePredicate
function, divides the range into subranges, and invokes the range
predicate for each of these subranges in parallel.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.

The range predicate is invoked for each subrange in its own goroutine,
and ErrRangeAnd returns only when all range predicates have
terminated, combining all return values with the && operator.
ErrRangeAnd also returns the left-most error value that is different
from nil as a second return value.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and ErrRangeAnd eventually panics with
the left-most recovered panic value.
*/
func ErrRangeAnd(low, high, threshold int, f pargo.ErrRangePredicate) (bool, error) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	var recur func(int, int) (bool, error)
	recur = func(low, high int) (result bool, err error) {
		if size := high - low; size <= threshold {
			return f(low, high)
		} else {
			half := size / 2
			mid := low + half
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
				b1, err1 = recur(mid, high)
			}()
			b0, err0 = recur(low, mid)
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
	}
	return recur(low, high)
}

/*
ErrRangeOr receives a range, a threshold, and an ErrRangePredicate
function, divides the range into subranges, and invokes the range
predicate for each of these subranges in parallel.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.

The range predicate is invoked for each subrange in its own goroutine,
and ErrRangeOr returns only when all range predicates have terminated,
combining all return values with the || operator.  ErrRangeOr also
returns the left-most error value that is different from nil as a
second return value.

If one or more range predicate invocations panic, the corresponding
goroutines recover the panics, and ErrRangeOr eventually panics with
the left-most recovered panic value.
*/
func ErrRangeOr(low, high, threshold int, f pargo.ErrRangePredicate) (bool, error) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	var recur func(int, int) (bool, error)
	recur = func(low, high int) (result bool, err error) {
		if size := high - low; size <= threshold {
			return f(low, high)
		} else {
			half := size / 2
			mid := low + half
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
				b1, err1 = recur(mid, high)
			}()
			b0, err0 = recur(low, mid)
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
	}
	return recur(low, high)
}
