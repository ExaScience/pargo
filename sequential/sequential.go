/*
Package sequential provides sequential implementations of the
functions provided by package parallel. This is useful for testing and
debugging.
*/
package sequential

import (
	"github.com/exascience/pargo"
)

/*
Do receives zero or more Thunk functions and executes them
sequentially.
*/
func Do(thunks ...pargo.Thunk) {
	for _, thunk := range thunks {
		thunk()
	}
}

/*
ErrDo receives zero or more ErrThunk functions and executes them
sequentially.
*/
func ErrDo(thunks ...pargo.ErrThunk) (err error) {
	for _, thunk := range thunks {
		nerr := thunk()
		if err == nil {
			err = nerr
		}
	}
	return
}

/*
And receives zero or more Predicate functions and executes them
sequentially, combining all return values with the && operator, with
true as the default return value.
*/
func And(predicates ...pargo.Predicate) (result bool) {
	result = true
	for _, predicate := range predicates {
		result = result && predicate()
	}
	return
}

/*
Or receives zero or more Predicate functions and executes them
sequentially, combining all return values with the || operator, with
false as the default return value.
*/
func Or(predicates ...pargo.Predicate) (result bool) {
	result = false
	for _, predicate := range predicates {
		result = result || predicate()
	}
	return
}

/*
ErrAnd receives zero or more ErrPredicate functions and executes them
sequentially, combining all return values with the && operator, with
true as the default return value. ErrAnd also returns the left-most
error value that is different from nil as a second return value.
*/
func ErrAnd(predicates ...pargo.ErrPredicate) (result bool, err error) {
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

/*
ErrOr receives zero or more ErrPredicate functions and executes them
sequentially, combining all return values with the || operator, with
false as the default return value.  ErrOr also returns the left-most
error value that is different from nil as a second return value.
*/
func ErrOr(predicates ...pargo.ErrPredicate) (result bool, err error) {
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

/*
Range receives a range, a threshold, and a RangeFunc function, divides
the range into subranges, and invokes the range function for each of
these subranges sequentially.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.
*/
func Range(low, high, threshold int, f pargo.RangeFunc) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	for start := low; start < high; start += threshold {
		end := start + threshold
		if end > high {
			end = high
		}
		f(start, end)
	}
}

/*
ErrRange receives a range, a threshold, and an ErrRangeFunc function,
divides the range into subranges, and invokes the range function for
each of these subranges sequentially, returning the left-most error
value that is different from nil.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.
*/
func ErrRange(low, high, threshold int, f pargo.ErrRangeFunc) (err error) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	for start := low; start < high; start += threshold {
		end := start + threshold
		if end > high {
			end = high
		}
		nerr := f(start, end)
		if err == nil {
			err = nerr
		}
	}
	return
}

/*
RangeAnd receives a range, a threshold, and a RangePredicate function,
divides the range into subranges, and invokes the range predicate for
each of these subranges sequentially, combining all return values with
the && operator.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.
*/
func RangeAnd(low, high, threshold int, f pargo.RangePredicate) (result bool) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	result = true
	for start := low; start < high; start += threshold {
		end := start + threshold
		if end > high {
			end = high
		}
		result = result && f(start, end)
	}
	return
}

/*
RangeOr receives a range, a threshold, and a RangePredicate function,
divides the range into subranges, and invokes the range predicate for
each of these subranges sequentially, combining all return values with
the || operator.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.
*/
func RangeOr(low, high, threshold int, f pargo.RangePredicate) (result bool) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	result = false
	for start := low; start < high; start += threshold {
		end := start + threshold
		if end > high {
			end = high
		}
		result = result || f(start, end)
	}
	return
}

/*
ErrRangeAnd receives a range, a threshold, and an ErrRangePredicate
function, divides the range into subranges, and invokes the range
predicate for each of these subranges sequentially, combining all
return values with the && operator.  ErrRangeAnd also returns the
left-most error value that is different from nil as a second return
value.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.
*/
func ErrRangeAnd(low, high, threshold int, f pargo.ErrRangePredicate) (result bool, err error) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	result = true
	for start := low; start < high; start += threshold {
		end := start + threshold
		if end > high {
			end = high
		}
		res, nerr := f(start, end)
		result = result && res
		if err == nil {
			err = nerr
		}
	}
	return
}

/*
ErrRangeOr receives a range, a threshold, and an ErrRangePredicate
function, divides the range into subranges, and invokes the range
predicate for each of these subranges in parallel, combining all
return values with the || operator.  ErrRangeOr also returns the
left-most error value that is different from nil as a second return
value.

The range is specified by a low and high integer, with 0 <= low <=
high. The subranges are determined by dividing up the size of the
range (high - low) in pargo.ComputeEffectiveThreshold.
*/
func ErrRangeOr(low, high, threshold int, f pargo.ErrRangePredicate) (result bool, err error) {
	threshold = pargo.ComputeEffectiveThreshold(low, high, threshold)
	result = false
	for start := low; start < high; start += threshold {
		end := start + threshold
		if end > high {
			end = high
		}
		res, nerr := f(start, end)
		result = result || res
		if err == nil {
			err = nerr
		}
	}
	return
}
