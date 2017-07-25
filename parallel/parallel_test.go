package parallel

import "errors"

func ExampleDo() {
	var fib func(int) int

	fib = func(n int) int {
		if n < 2 {
			return n
		} else {
			return fib(n-1) + fib(n-2)
		}
	}

	var parallelFib func(int) int

	parallelFib = func(n int) int {
		if n < 20 {
			return fib(n)
		} else {
			var n1, n2 int
			parallel.Do(
				func() { n1 = parallelFib(n - 1) },
				func() { n2 = parallelFib(n - 2) },
			)
			return n1 + n2
		}
	}

	result := parallelFib(30)
}

func ExampleErrDo() {
	var fib func(int) (int, error)

	fib = func(n int) (result int, err error) {
		if n < 0 {
			err = errors.New("Invalid argument.")
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
			err = errors.New("Invalid argument.")
		} else if n < 20 {
			result, err = fib(n)
		} else {
			var n1, n2 int
			err = parallel.ErrDo(
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

	result, err := parallelFib(-1)
}
