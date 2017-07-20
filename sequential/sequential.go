/*
Package sequential provides sequential implementations of the
functions provided by package parallel. This is useful for testing and
debugging.
*/
package sequential

import (
	"fmt"

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
Range receives a range, a batch count, and a RangeFunc function,
divides the range into batches, and invokes the range function for
each of these batches sequentially.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

Range panics if high < low, or if n < 0.
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
				recur(low, mid, half)
				recur(mid, high, n-half)
			}
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	recur(low, high, pargo.ComputeNofBatches(low, high, n))
}

/*
ErrRange receives a range, a batch count, and an ErrRangeFunc
function, divides the range into batches, and invokes the range
function for each of these batches sequentially.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

ErrRange returns the left-most error value that is different from nil.

ErrRange panics if high < low, or if n < 0.
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
				err0 := recur(low, mid, half)
				err1 := recur(mid, high, n-half)
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
RangeAnd receives a range, a batch count, and a RangePredicate
function, divides the range into batches, and invokes the range
predicate for each of these batches sequentially.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

RangeAnd returns by combining all return values with the && operator.

RangeAnd panics if high < low, or if n < 0.
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
				b0 := recur(low, mid, half)
				b1 := recur(mid, high, n-half)
				return b0 && b1
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
predicate for each of these batches sequentially.

The range is specified by a low and high integer, with low <=
high. The batches are determined by dividing up the size of the range
(high - low) by n. If n is 0, a reasonable default is used that takes
runtime.GOMAXPROCS(0) into account.

RangeOr by combining all return values with the || operator.

RangeOr panics if high < low, or if n < 0.
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
				b0 := recur(low, mid, half)
				b1 := recur(mid, high, n-half)
				return b0 || b1
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

ErrRangeAnd returns by combining all return values with the &&
operator. ErrRangeAnd also returns the left-most error value that is
different from nil as a second return value.

ErrRangeAnd panics if high < low, or if n < 0.
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
				b0, err0 := recur(low, mid, half)
				b1, err1 := recur(mid, high, n-half)
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

ErrRangeOr returns by combining all return values with the ||
operator. ErrRangeAnd also returns the left-most error value that is
different from nil as a second return value.

ErrRangeOr panics if high < low, or if n < 0.
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
				b0, err0 := recur(low, mid, half)
				b1, err1 := recur(mid, high, n-half)
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
