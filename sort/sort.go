/*
Package sort provides implementations of parallel sorting algorithms.
*/
package sort

import (
	"sort"
	"sync/atomic"

	"github.com/exascience/pargo/speculative"
)

/*
A type, typically a collection, that can be sequentially sorted. This
is needed as a base case for the parallel sorting algorithms in this
package. It is recommended to implement this interface by using the
functions in the sort package of Go's standard library.
*/
type SequentialSorter interface {
	// Sort the range that starts at index i and ends at index j. If the
	// collection that is represented by this interface is a slice, then
	// the slice expression collection[i:j] returns the correct slice to
	// be sorted.
	SequentialSort(i, j int)
}

const serialCutoff = 10

/*
IsSorted determines in parallel whether data is already sorted. It
attempts to terminate early when the return value is false.
*/
func IsSorted(data sort.Interface) bool {
	size := data.Len()
	if size < qsortGrainSize {
		return sort.IsSorted(data)
	}
	for i := 1; i < serialCutoff; i++ {
		if data.Less(i, i-1) {
			return false
		}
	}
	var done int32
	defer atomic.StoreInt32(&done, 1)
	var pTest func(int, int) bool
	pTest = func(index, size int) bool {
		if size < qsortGrainSize {
			for i := index; i < index+size; i++ {
				if ((i % 1024) == 0) && (atomic.LoadInt32(&done) != 0) {
					return false
				}
				if data.Less(i, i-1) {
					return false
				}
			}
			return true
		}
		half := size / 2
		return speculative.And(
			func() bool { return pTest(index, half) },
			func() bool { return pTest(index+half, size-half) },
		)
	}
	return pTest(serialCutoff, size-serialCutoff)
}
