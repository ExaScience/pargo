package internal

import (
	"fmt"
	"runtime"
)

func ComputeNofBatches(low, high, n int) (batches int) {
	switch size := high - low; {
	case size > 0:
		switch {
		case n == 0:
			batches = 2 * runtime.GOMAXPROCS(0)
		case n > 0:
			batches = n
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
		if batches > size {
			batches = size
		}
	case size == 0:
		batches = 1
	default:
		panic(fmt.Sprintf("invalid range: %v:%v", low, high))
	}
	return
}
