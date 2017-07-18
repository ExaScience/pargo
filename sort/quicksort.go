package sort

import (
	"sort"

	"github.com/exascience/pargo/parallel"
)

const qsortGrainSize = 0x500

/*
A type, typically a collection, that satisfies sort.Sorter can be
sorted by Sort in this package. The methods require that (ranges of)
elements of the collection can be enumerated by integer indices.
*/
type Sorter interface {
	SequentialSorter
	sort.Interface
}

func medianOfThree(data sort.Interface, l, m, r int) int {
	if data.Less(l, m) {
		if data.Less(m, r) {
			return m
		} else if data.Less(l, r) {
			return r
		}
	} else if data.Less(r, m) {
		return m
	} else if data.Less(r, l) {
		return r
	}
	return l
}

func pseudoMedianOfNine(data sort.Interface, index, size int) int {
	offset := size / 8
	return medianOfThree(data,
		medianOfThree(data, index, index+offset, index+offset*2),
		medianOfThree(data, index+offset*3, index+offset*4, index+offset*5),
		medianOfThree(data, index+offset*6, index+offset*7, index+size-1),
	)
}

/*
Sort uses a parallel quicksort implementation.

It is good for small core counts and small collection sizes.
*/
func Sort(data Sorter) {
	size := data.Len()
	sSort := data.SequentialSort
	if size < qsortGrainSize {
		sSort(0, size)
		return
	}
	var pSort func(int, int)
	pSort = func(index, size int) {
		if size < qsortGrainSize {
			sSort(index, index+size)
		} else {
			m := pseudoMedianOfNine(data, index, size)
			if m > index {
				data.Swap(index, m)
			}
			i, j := index, index+size
		outer:
			for {
				for {
					j--
					if !data.Less(index, j) {
						break
					}
				}
				for {
					if i == j {
						break outer
					}
					i++
					if !data.Less(i, index) {
						break
					}
				}
				if i == j {
					break outer
				}
				data.Swap(i, j)
			}
			data.Swap(j, index)
			i = j + 1
			parallel.Do(
				func() { pSort(index, j-index) },
				func() { pSort(i, index+size-i) },
			)
		}
	}
	if !IsSorted(data) {
		pSort(0, size)
	}
}
