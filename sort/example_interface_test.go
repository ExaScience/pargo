// Copyright 2011 The Go Authors. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

// Adapted by Pascal Costanza for the Pargo package.

package sort_test

import (
	"fmt"
	stdsort "sort"

	sort "github.com/exascience/pargo/sort"
)

type Person struct {
	Name string
	Age  int
}

func (p Person) String() string {
	return fmt.Sprintf("%s: %d", p.Name, p.Age)
}

// ByAge implements sort.SequentialSorter, sort.Sorter, and sort.StableSorter
// for []Person based on the Age field.
type ByAge []Person

func (a ByAge) SequentialSort(i, j int) {
	stdsort.SliceStable(a, func(i, j int) bool {
		return a[i].Age < a[j].Age
	})
}

func (a ByAge) Len() int           { return len(a) }
func (a ByAge) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByAge) Less(i, j int) bool { return a[i].Age < a[j].Age }

func (a ByAge) NewTemp() sort.StableSorter { return make(ByAge, len(a)) }

func (this ByAge) Assign(that sort.StableSorter) func(i, j, len int) {
	dst, src := this, that.(ByAge)
	return func(i, j, len int) {
		for k := 0; k < len; k++ {
			dst[i+k] = src[j+k]
		}
	}
}

func Example() {
	people := []Person{
		{"Bob", 31},
		{"John", 42},
		{"Michael", 17},
		{"Jenny", 26},
	}

	fmt.Println(people)
	sort.Sort(ByAge(people))
	fmt.Println(people)

	people = []Person{
		{"Bob", 31},
		{"John", 42},
		{"Michael", 17},
		{"Jenny", 26},
	}

	fmt.Println(people)
	sort.StableSort(ByAge(people))
	fmt.Println(people)

	// Output:
	// [Bob: 31 John: 42 Michael: 17 Jenny: 26]
	// [Michael: 17 Jenny: 26 Bob: 31 John: 42]
	// [Bob: 31 John: 42 Michael: 17 Jenny: 26]
	// [Michael: 17 Jenny: 26 Bob: 31 John: 42]
}
