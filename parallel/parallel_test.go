package parallel_test

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/exascience/pargo/parallel"
)

func ExampleDo() {
	var fib func(int) (int, error)

	fib = func(n int) (result int, err error) {
		if n < 0 {
			err = errors.New("invalid argument")
		} else if n < 2 {
			result = n
		} else {
			var n1, n2 int
			n1, err = fib(n - 1)
			if err != nil {
				return
			}
			n2, err = fib(n - 2)
			result = n1 + n2
		}
		return
	}

	type intErr struct {
		n   int
		err error
	}

	var parallelFib func(int) intErr

	parallelFib = func(n int) (result intErr) {
		if n < 0 {
			result.err = errors.New("invalid argument")
		} else if n < 20 {
			result.n, result.err = fib(n)
		} else {
			var n1, n2 intErr
			parallel.Do(
				func() { n1 = parallelFib(n - 1) },
				func() { n2 = parallelFib(n - 2) },
			)
			result.n = n1.n + n2.n
			if n1.err != nil {
				result.err = n1.err
			} else {
				result.err = n2.err
			}
		}
		return
	}

	if result := parallelFib(-1); result.err != nil {
		fmt.Println(result.err)
	} else {
		fmt.Println(result.n)
	}

	// Output:
	// invalid argument
}

func ExampleRangeReduceIntSum() {
	numDivisors := func(n int) int {
		return parallel.RangeReduceIntSum(
			1, n+1, runtime.GOMAXPROCS(0),
			func(low, high int) int {
				var sum int
				for i := low; i < high; i++ {
					if (n % i) == 0 {
						sum++
					}
				}
				return sum
			},
		)
	}

	fmt.Println(numDivisors(12))

	// Output:
	// 6
}

func numDivisors(n int) int {
	return parallel.RangeReduceIntSum(
		1, n+1, runtime.GOMAXPROCS(0),
		func(low, high int) int {
			var sum int
			for i := low; i < high; i++ {
				if (n % i) == 0 {
					sum++
				}
			}
			return sum
		},
	)
}

func ExampleRangeReduce() {
	findPrimes := func(n int) []int {
		result := parallel.RangeReduce(
			2, n, 4*runtime.GOMAXPROCS(0),
			func(low, high int) interface{} {
				var slice []int
				for i := low; i < high; i++ {
					if numDivisors(i) == 2 { // see RangeReduceInt example
						slice = append(slice, i)
					}
				}
				return slice
			},
			func(x, y interface{}) interface{} {
				return append(x.([]int), y.([]int)...)
			},
		)
		return result.([]int)
	}

	fmt.Println(findPrimes(20))

	// Output:
	// [2 3 5 7 11 13 17 19]
}

func ExampleRangeReduceFloat64Sum() {
	sumFloat64s := func(f []float64) float64 {
		result := parallel.RangeReduceFloat64Sum(
			0, len(f), runtime.GOMAXPROCS(0),
			func(low, high int) float64 {
				var sum float64
				for i := low; i < high; i++ {
					sum += f[i]
				}
				return sum
			},
		)
		return result
	}

	fmt.Println(sumFloat64s([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))

	// Output:
	// 55
}
