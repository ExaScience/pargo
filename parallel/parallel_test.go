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

	var parallelFib func(int) (int, error)

	parallelFib = func(n int) (result int, err error) {
		if n < 0 {
			err = errors.New("invalid argument")
		} else if n < 20 {
			result, err = fib(n)
		} else {
			var n1, n2 int
			err = parallel.Do(
				func() error {
					n, err := parallelFib(n - 1)
					n1 = n
					return err
				},
				func() error {
					n, err := parallelFib(n - 2)
					n2 = n
					return err
				},
			)
			result = n1 + n2
		}
		return
	}

	if result, err := parallelFib(-1); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(result)
	}

	// Output:
	// invalid argument
}

func ExampleIntRangeReduce() {
	numDivisors := func(n int) int {
		result, _ := parallel.IntRangeReduce(
			1, n+1, runtime.GOMAXPROCS(0),
			func(low, high int) (int, error) {
				var sum int
				for i := low; i < high; i++ {
					if (n % i) == 0 {
						sum++
					}
				}
				return sum, nil
			},
			func(x, y int) (int, error) { return x + y, nil },
		)
		return result
	}

	fmt.Println(numDivisors(12))

	// Output:
	// 6
}

func numDivisors(n int) int {
	result, _ := parallel.IntRangeReduce(
		1, n+1, runtime.GOMAXPROCS(0),
		func(low, high int) (int, error) {
			var sum int
			for i := low; i < high; i++ {
				if (n % i) == 0 {
					sum++
				}
			}
			return sum, nil
		},
		func(x, y int) (int, error) { return x + y, nil },
	)
	return result
}

func ExampleRangeReduce() {
	findPrimes := func(n int) []int {
		result, _ := parallel.RangeReduce(
			2, n, 4*runtime.GOMAXPROCS(0),
			func(low, high int) (interface{}, error) {
				var slice []int
				for i := low; i < high; i++ {
					if numDivisors(i) == 2 { // see IntRangeReduce example
						slice = append(slice, i)
					}
				}
				return slice, nil
			},
			func(x, y interface{}) (interface{}, error) {
				return append(x.([]int), y.([]int)...), nil
			},
		)
		return result.([]int)
	}

	fmt.Println(findPrimes(20))

	// Output:
	// [2 3 5 7 11 13 17 19]
}

func ExampleFloat64RangeReduce() {
	sumFloat64s := func(f []float64) float64 {
		result, _ := parallel.Float64RangeReduce(
			0, len(f), runtime.GOMAXPROCS(0),
			func(low, high int) (float64, error) {
				var sum float64
				for i := low; i < high; i++ {
					sum += f[i]
				}
				return sum, nil
			},
			func(x, y float64) (float64, error) { return x + y, nil },
		)
		return result
	}

	fmt.Println(sumFloat64s([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))

	// Output:
	// 55
}
