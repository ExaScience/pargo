package sort

import (
	"sync"

	"github.com/exascience/pargo/parallel"
)

const msortGrainSize = 0x3000

// StableSorter is a type, typically a collection, that can be sorted
// by StableSort in this package. The methods require that ranges of
// elements of the collection can be enumerated by integer indices.
type StableSorter interface {
	SequentialSorter

	// NewTemp creates a new collection that can hold as many elements
	// as the original collection. This is temporary memory needed by
	// StableSort, but not needed anymore afterwards. The temporary
	// collection does not need to be initialized.
	NewTemp() StableSorter

	// Len is the number of elements in the collection.
	Len() int

	// Less reports whether the element with index i should sort
	// before the element with index j.
	Less(i, j int) bool

	// Assign returns a function that assigns ranges from source to the
	// receiver collection. The element with index i is the first
	// element in the receiver to assign to, and the element with index
	// j is the first element in the source collection to assign from,
	// with len determining the number of elements to assign. The effect
	// should be the same as receiver[i:i+len] = source[j:j+len].
	Assign(source StableSorter) func(i, j, len int)
}

type sorter struct {
	less   func(i, j int) bool
	assign func(i, j, len int)
}

func binarySearchEq(x int, T *sorter, p, r int) int {
	low, high := p, r+1
	if low > high {
		return low
	}
	for low < high {
		mid := (low + high) / 2
		if !T.less(mid, x) {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return high
}

func binarySearchNeq(x int, T *sorter, p, r int) int {
	low, high := p, r+1
	if low > high {
		return low
	}
	for low < high {
		mid := (low + high) / 2
		if T.less(x, mid) {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return high
}

func sMerge(T *sorter, p1, r1, p2, r2 int, A *sorter, p3 int) {
	for {
		if p2 > r2 {
			A.assign(p3, p1, r1+1-p1)
			return
		}

		q1 := p1
		for (p1 <= r1) && !T.less(p2, p1) {
			p1++
		}
		n1 := p1 - q1
		A.assign(p3, q1, n1)
		p3 += n1

		if p1 > r1 {
			A.assign(p3, p2, r2+1-p2)
			return
		}

		q2 := p2
		for (p2 <= r2) && T.less(p2, p1) {
			p2++
		}
		n2 := p2 - q2
		A.assign(p3, q2, n2)
		p3 += n2
	}
}

func pMerge(T *sorter, p1, r1, p2, r2 int, A *sorter, p3 int) {
	n1 := r1 - p1 + 1
	n2 := r2 - p2 + 1
	if (n1 + n2) < msortGrainSize {
		sMerge(T, p1, r1, p2, r2, A, p3)
		return
	}
	if n1 > n2 {
		if n1 == 0 {
			return
		}
		q1 := (p1 + r1) / 2
		q2 := binarySearchEq(q1, T, p2, r2)
		q3 := p3 + (q1 - p1) + (q2 - p2)
		A.assign(q3, q1, 1)
		parallel.Do(
			func() { pMerge(T, p1, q1-1, p2, q2-1, A, p3) },
			func() { pMerge(T, q1+1, r1, q2, r2, A, q3+1) },
		)
	} else {
		if n2 == 0 {
			return
		}
		q2 := (p2 + r2) / 2
		q1 := binarySearchNeq(q2, T, p1, r1)
		q3 := p3 + (q1 - p1) + (q2 - p2)
		A.assign(q3, q2, 1)
		parallel.Do(
			func() { pMerge(T, p1, q1-1, p2, q2-1, A, p3) },
			func() { pMerge(T, q1, r1, q2+1, r2, A, q3+1) },
		)
	}
}

// StableSort uses a parallel implementation of merge sort, also known
// as cilksort.
//
// StableSort is only stable if data's SequentialSort method is
// stable.
//
// StableSort is good for large core counts and large collection
// sizes, but needs a shallow copy of the data collection as
// additional temporary memory.
func StableSort(data StableSorter) {
	// See https://en.wikipedia.org/wiki/Introduction_to_Algorithms and
	// https://www.clear.rice.edu/comp422/lecture-notes/ for details on the algorithm.
	size := data.Len()
	sSort := data.SequentialSort
	if size < msortGrainSize {
		sSort(0, size)
		return
	}
	var T, A *sorter
	var temp sync.WaitGroup
	temp.Add(1)
	go func() {
		defer temp.Done()
		a := data.NewTemp()
		T = &sorter{data.Less, data.Assign(a)}
		A = &sorter{a.Less, a.Assign(data)}
	}()
	var pSort func(int, int)
	pSort = func(index, size int) {
		if size < msortGrainSize {
			sSort(index, index+size)
		} else {
			q1 := size / 4
			q2 := q1 + q1
			q3 := q2 + q1
			parallel.Do(
				func() { pSort(index, q1) },
				func() { pSort(index+q1, q1) },
				func() { pSort(index+q2, q1) },
				func() { pSort(index+q3, size-q3) },
			)
			temp.Wait()
			parallel.Do(
				func() { pMerge(T, index, index+q1-1, index+q1, index+q2-1, A, index) },
				func() { pMerge(T, index+q2, index+q3-1, index+q3, index+size-1, A, index+q2) },
			)
			pMerge(A, index, index+q2-1, index+q2, index+size-1, T, index)
		}
	}
	pSort(0, size)
}
