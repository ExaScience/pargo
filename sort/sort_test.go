package sort

import (
	"bytes"
	"math/rand"
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

func (by By) IsSorted(slice []int) bool {
	return sort.IsSorted(IntSliceSorter{slice, by})
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
	copy(s1, orgSlice)
	copy(s2, orgSlice)

	t.Run("ParallelStableSort", func(t *testing.T) {
		By(func(i, j int) bool { return i < j }).ParallelStableSort(s1)
		if !By(func(i, j int) bool { return i < j }).IsSorted(s1) {
			t.Errorf("parallel stable sort incorrect")
		}
	})

	t.Run("ParallelSort", func(t *testing.T) {
		By(func(i, j int) bool { return i < j }).ParallelSort(s2)
		if !By(func(i, j int) bool { return i < j }).IsSorted(s2) {
			t.Errorf("parallel sort incorrect")
		}
	})
}

func TestIntSort(t *testing.T) {
	orgSlice := makeRandomSlice(100*0x6000, 100*100*0x6000)
	s1 := make([]int, len(orgSlice))
	s2 := make([]int, len(orgSlice))
	copy(s1, orgSlice)
	copy(s2, orgSlice)

	t.Run("ParallelStableSort IntSlice", func(t *testing.T) {
		StableSort(IntSlice(s1))
		if !sort.IntsAreSorted(s1) {
			t.Errorf("parallel stable sort on IntSlice incorrect")
		}
		if !IntsAreSorted(s1) {
			t.Errorf("parallel IntsAreSorted incorrect")
		}
	})

	t.Run("ParallelSort IntSlice", func(t *testing.T) {
		Sort(IntSlice(s2))
		if !sort.IntsAreSorted(s2) {
			t.Errorf("parallel sort on IntSlice incorrect")
		}
		if !IntsAreSorted(s2) {
			t.Errorf("parallel IntsAreSorted incorrect")
		}
	})
}

func makeRandomFloat64Slice(size int) []float64 {
	result := make([]float64, size)
	for i := 0; i < size; i++ {
		result[i] = rand.NormFloat64()
	}
	return result
}

func TestFloat64Sort(t *testing.T) {
	orgSlice := makeRandomFloat64Slice(100 * 0x6000)
	s1 := make([]float64, len(orgSlice))
	s2 := make([]float64, len(orgSlice))
	copy(s1, orgSlice)
	copy(s2, orgSlice)

	t.Run("ParallelStableSort Float64Slice", func(t *testing.T) {
		StableSort(Float64Slice(s1))
		if !sort.Float64sAreSorted(s1) {
			t.Errorf("parallel stable sort on Float64Slice incorrect")
		}
		if !Float64sAreSorted(s1) {
			t.Errorf("parallel Float64sAreSorted incorrect")
		}
	})

	t.Run("ParallelSort Float64Slice", func(t *testing.T) {
		Sort(Float64Slice(s2))
		if !sort.Float64sAreSorted(s2) {
			t.Errorf("parallel sort on Float64Slice incorrect")
		}
		if !Float64sAreSorted(s2) {
			t.Errorf("parallel Float64sAreSorted incorrect")
		}
	})
}

func makeRandomStringSlice(size, lenlimit int, limit int32) []string {
	result := make([]string, size)
	for i := 0; i < size; i++ {
		var buf bytes.Buffer
		len := rand.Intn(lenlimit)
		for j := 0; j < len; j++ {
			buf.WriteRune(rand.Int31n(limit))
		}
		result[i] = buf.String()
	}
	return result
}

func TestStringSort(t *testing.T) {
	orgSlice := makeRandomStringSlice(100*0x6000, 256, 16384)
	s1 := make([]string, len(orgSlice))
	s2 := make([]string, len(orgSlice))
	copy(s1, orgSlice)
	copy(s2, orgSlice)

	t.Run("ParallelStableSort StringSlice", func(t *testing.T) {
		StableSort(StringSlice(s1))
		if !sort.StringsAreSorted(s1) {
			t.Errorf("parallel stable sort on StringSlice incorrect")
		}
		if !StringsAreSorted(s1) {
			t.Errorf("parallel StringsAreSorted incorrect")
		}
	})

	t.Run("ParallelSort StringSlice", func(t *testing.T) {
		Sort(StringSlice(s2))
		if !sort.StringsAreSorted(s2) {
			t.Errorf("parallel sort on StringSlice incorrect")
		}
		if !StringsAreSorted(s2) {
			t.Errorf("parallel StringsAreSorted incorrect")
		}
	})
}

type (
	box struct {
		primary, secondary int
	}

	boxSlice []box
)

func makeRandomBoxSlice(size int) boxSlice {
	result := make([]box, size)
	half := ((size - 1) / 2) + 1
	for i := 0; i < size; i++ {
		result[i].primary = rand.Intn(half)
		result[i].secondary = i + 1
	}
	return result
}

func (s boxSlice) NewTemp() StableSorter {
	return boxSlice(make([]box, len(s)))
}

func (s boxSlice) Len() int {
	return len(s)
}

func (s boxSlice) Less(i, j int) bool {
	return s[i].primary < s[j].primary
}

func (s boxSlice) Assign(source StableSorter) func(i, j, len int) {
	dst, src := s, source.(boxSlice)
	return func(i, j, len int) {
		for k := 0; k < len; k++ {
			dst[i+k] = src[j+k]
		}
	}
}

func (s boxSlice) SequentialSort(i, j int) {
	slice := s[i:j]
	sort.SliceStable(slice, func(i, j int) bool {
		return slice[i].primary < slice[j].primary
	})
}

func checkStable(b boxSlice) bool {
	m := make(map[int]int)
	for _, el := range b {
		if m[el.primary] < el.secondary {
			m[el.primary] = el.secondary
		} else {
			return false
		}
	}
	return true
}

func TestStableSort(t *testing.T) {
	orgSlice := makeRandomBoxSlice(100 * 0x6000)
	s1 := make(boxSlice, len(orgSlice))
	copy(s1, orgSlice)

	t.Run("ParallelStableSort boxSlice", func(t *testing.T) {
		StableSort(s1)
		if !sort.SliceIsSorted(s1, func(i, j int) bool {
			return s1[i].primary < s1[j].primary
		}) {
			t.Errorf("parallel stable sort on boxSlice incorrect")
		}
	})

	t.Run("CheckStable ParallelStableSort boxSlice", func(t *testing.T) {
		if !checkStable(s1) {
			t.Errorf("parallel stable sort on boxSlice not stable")
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
