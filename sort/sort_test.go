package sort

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

type (
	By func(i, j int) bool

	IntSliceSorter struct {
		slice []int
		by    By
	}
)

func (s IntSliceSorter) NewTemp() StableSorter {
	return IntSliceSorter{make([]int, len(s.slice)), s.by}
}

func (s IntSliceSorter) Len() int {
	return len(s.slice)
}

func (s IntSliceSorter) Less(i, j int) bool {
	return s.by(s.slice[i], s.slice[j])
}

func (s IntSliceSorter) Swap(i, j int) {
	s.slice[i], s.slice[j] = s.slice[j], s.slice[i]
}

func (s IntSliceSorter) Assign(t StableSorter) func(i, j, len int) {
	dst, src := s.slice, t.(IntSliceSorter).slice
	return func(i, j, len int) {
		for k := 0; k < len; k++ {
			dst[i+k] = src[j+k]
		}
	}
}

func (s IntSliceSorter) SequentialSort(i, j int) {
	slice, by := s.slice[i:j], s.by
	sort.Slice(slice, func(i, j int) bool {
		return by(slice[i], slice[j])
	})
}

func (by By) SequentialSort(slice []int) {
	sort.Sort(IntSliceSorter{slice, by})
}

func (by By) ParallelStableSort(slice []int) {
	StableSort(IntSliceSorter{slice, by})
}

func (by By) ParallelSort(slice []int) {
	Sort(IntSliceSorter{slice, by})
}

func makeRandomSlice(size, limit int) []int {
	result := make([]int, size)
	for i := 0; i < size; i++ {
		result[i] = rand.Intn(limit)
	}
	return result
}

func TestSort(t *testing.T) {
	orgSlice := makeRandomSlice(100*0x6000, 100*100*0x6000)
	s1 := make([]int, len(orgSlice))
	s2 := make([]int, len(orgSlice))
	s3 := make([]int, len(orgSlice))
	copy(s1, orgSlice)
	copy(s2, orgSlice)
	copy(s3, orgSlice)

	By(func(i, j int) bool { return i < j }).SequentialSort(s1)

	t.Run("ParallelStableSort", func(t *testing.T) {
		By(func(i, j int) bool { return i < j }).ParallelStableSort(s2)
		if !reflect.DeepEqual(s1, s2) {
			t.Errorf("Parallel stable sort incorrect.")
		}
	})

	t.Run("ParallelSort", func(t *testing.T) {
		By(func(i, j int) bool { return i < j }).ParallelSort(s3)
		if !reflect.DeepEqual(s1, s3) {
			t.Errorf("Parallel sort incorrect.")
		}
	})
}

func BenchmarkSort(b *testing.B) {
	orgSlice := makeRandomSlice(100*0x6000, 100*100*0x6000)
	s1 := make([]int, len(orgSlice))
	s2 := make([]int, len(orgSlice))
	s3 := make([]int, len(orgSlice))

	b.Run("SequentialSort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			copy(s1, orgSlice)
			b.StartTimer()
			By(func(i, j int) bool { return i < j }).SequentialSort(s1)
		}
	})

	b.Run("ParallelStableSort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			copy(s2, orgSlice)
			b.StartTimer()
			By(func(i, j int) bool { return i < j }).ParallelStableSort(s2)
		}
	})

	b.Run("ParallelSort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			copy(s3, orgSlice)
			b.StartTimer()
			By(func(i, j int) bool { return i < j }).ParallelSort(s3)
		}
	})
}
