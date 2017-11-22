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
SequentialSorter is a type, typically a collection, that can be
sequentially sorted. This is needed as a base case for the parallel
sorting algorithms in this package. It is recommended to implement
this interface by using the functions in the sort package of Go's
standard library.
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

/*
IntSlice attaches the methods of sort.Interface, SequentialSorter,
Sorter, and StableSorter to []int, sorting in increasing order.
*/
type IntSlice []int

// SequentialSort implements the method of of the SequentialSorter interface.
func (s IntSlice) SequentialSort(i, j int) {
	sort.Stable(sort.IntSlice(s[i:j]))
}

func (s IntSlice) Len() int {
	return len(s)
}

func (s IntSlice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s IntSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// NewTemp implements the method of the StableSorter interface.
func (s IntSlice) NewTemp() StableSorter {
	return IntSlice(make([]int, len(s)))
}

// Assign implements the method of the StableSorter interface.
func (s IntSlice) Assign(source StableSorter) func(i, j, len int) {
	dst, src := s, source.(IntSlice)
	return func(i, j, len int) {
		copy(dst[i:i+len], src[j:j+len])
	}
}

/*
IntsAreSorted determines in parallel whether a slice of ints is
already sorted in increasing order. It attempts to terminate early
when the return value is false.
*/
func IntsAreSorted(a []int) bool {
	return IsSorted(IntSlice(a))
}

/*
Float64Slice attaches the methods of sort.Interface, SequentialSorter,
Sorter, and StableSorter to []float64, sorting in increasing order.
*/
type Float64Slice []float64

// SequentialSort implements the method of the SequentialSorter interface.
func (s Float64Slice) SequentialSort(i, j int) {
	sort.Stable(sort.Float64Slice(s[i:j]))
}

func (s Float64Slice) Len() int {
	return len(s)
}

func (s Float64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Float64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// NewTemp implements the method of the StableSorter interface.
func (s Float64Slice) NewTemp() StableSorter {
	return Float64Slice(make([]float64, len(s)))
}

// Assign implements the method of the StableSorter interface.
func (s Float64Slice) Assign(source StableSorter) func(i, j, len int) {
	dst, src := s, source.(Float64Slice)
	return func(i, j, len int) {
		copy(dst[i:i+len], src[j:j+len])
	}
}

/*
Float64sAreSorted determines in parallel whether a slice of float64s
is already sorted in increasing order. It attempts to terminate early
when the return value is false.
*/
func Float64sAreSorted(a []float64) bool {
	return IsSorted(Float64Slice(a))
}

/*
StringSlice attaches the methods of sort.Interface, SequentialSorter,
Sorter, and StableSorter to []string, sorting in increasing order.
*/
type StringSlice []string

// SequentialSort implements the method of the SequentialSorter interface.
func (s StringSlice) SequentialSort(i, j int) {
	sort.Stable(sort.StringSlice(s[i:j]))
}

func (s StringSlice) Len() int {
	return len(s)
}

func (s StringSlice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s StringSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// NewTemp implements the method of the StableSorter interface.
func (s StringSlice) NewTemp() StableSorter {
	return StringSlice(make([]string, len(s)))
}

// Assign implements the method of the StableSorter interface.
func (s StringSlice) Assign(source StableSorter) func(i, j, len int) {
	dst, src := s, source.(StringSlice)
	return func(i, j, len int) {
		copy(dst[i:i+len], src[j:j+len])
	}
}

/*
StringsAreSorted determines in parallel whether a slice of strings is
already sorted in increasing order. It attempts to terminate early
when the return value is false.
*/
func StringsAreSorted(a []string) bool {
	return IsSorted(StringSlice(a))
}
