/*
Package speculative provides functions for expressing parallel
algorithms, similar to the functions in package parallel, except that
the implementations here terminate early when they can.

And, Or, RangeAnd, and RangeOr terminate early if the final return
value is known early (if any of the predicates invoked in parallel
returns false for And, or true for Or).

ErrDo and ErrRange terminate early if any of the functions invoked in
parallel returns an error value different from nil.

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
ErrRange receives a range, a threshold, and an ErrRangeFunc function,
divides the range into subranges, and invokes the range function for
each of these subranges in parallel.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold. When in doubt,
use a threshold value of 4 here and tweak it later to fine-tune
performance.

The range function is invoked for each subrange in its own goroutine,
and ErrRange returns either when all range functions have terminated;
or when one or more range functions return an error value that is
different from nil, returning the left-most of these error values,
without waiting for the other range functions to terminate.

If one or more range functions panic, the corresponding goroutines
recover the panics, and ErrRange may eventually panic with the
left-most recovered panic value. If both non-nil error values are
returned and panics occur, then the left-most of these events take
precedence.
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
			var err1 error
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
			if err0 := recur(low, mid); err0 != nil {
				return err0
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return err1
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
range (high - low) in pargo.ComputeEffectiveThreshold. When in doubt,
use a threshold value of 4 here and tweak it later to fine-tune
performance.

The range predicate is invoked for each subrange in its own goroutine,
and RangeAnd returns true if all of them return true; or RangeAnd
returns false when at least one of them returns false, without waiting
for the other range predicates to terminate.

If one or more range predicates panic, the corresponding goroutines
recover the panics, and RangeAnd may eventually panic with the
left-most recovered panic value. If both panics occur and false values
are returned, then the left-most of these events takes precedence.
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
			var b1 bool
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
			if !recur(low, mid) {
				return false
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b1
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
range (high - low) in pargo.ComputeEffectiveThreshold. When in doubt,
use a threshold value of 4 here and tweak it later to fine-tune
performance.

The range predicate is invoked for each subrange in its own goroutine,
and RangeOr returns false if all of them return false; or RangeOr
returns true when at least one of them returns true, without waiting
for the other range predicates to terminate.

If one or more range predicates panic, the corresponding goroutines
recover the panics, and RangeOr may eventually panic with the
left-most recovered panic value. If both panics occur and true values
are returned, then the left-most of these events takes precedence.
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
			var b1 bool
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
			if recur(low, mid) {
				return true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b1
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
range (high - low) in pargo.ComputeEffectiveThreshold. When in doubt,
use a threshold value of 4 here and tweak it later to fine-tune
performance.

The range predicate is invoked for each subrange in its own goroutine,
and ErrRangeAnd returns true if all of them return true; or
ErrRangeAnd returns false when at least one of them returns false,
without waiting for the other range predicates to terminate.
ErrRangeAnd may also return the left-most error value that is
different from nil as a second return value. If both false values and
non-nil error values are returned, then the left-most of these return
value pairs are returned.

If one or more range predicates panic, the corresponding goroutines
recover the panics, and ErrRangeAnd may eventually panic with the
left-most recovered panic value. If both panics occur on the one hand,
and false or non-nil error values are returned on the other hand, then
the left-most of these two kinds of events takes precedence.
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
	}
	return recur(low, high)
}

/*
ErrRangeOr receives a range, a threshold, and an ErrRangePredicate
function, divides the range into subranges, and invokes the range
predicate for each of these subranges in parallel.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold. When in doubt,
use a threshold value of 4 here and tweak it later to fine-tune
performance.

The range predicate is invoked for each subrange in its own goroutine,
and ErrRangeOr returns false if all of them return false; or
ErrRangeOr returns true when at least one of them returns true,
without waiting for the other range predicates to terminate.
ErrRangeOr may also return the left-most error value that is different
from nil as a second return value. If both true values and non-nil
error values are returned, then the left-most of these return value
pairs are returned.

If one or more range predicates panic, the corresponding goroutines
recover the panics, and ErrRangeOr may eventually panic with the
left-most recovered panic value. If both panics occur on the one hand,
and true or non-nil error values are returned on the other hand, then
the left-most of these two kinds of events takes precedence.
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
	}
	return recur(low, high)
}
