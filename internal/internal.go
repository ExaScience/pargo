package internal

import (
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
)

// ComputeNofBatches divides the size of the range (high - low) by n. If n is 0,
// a default is used that takes runtime.GOMAXPROCS(0) into account.
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

type runtimeError struct{ error }

func (runtimeError) RuntimeError() {}

// WrapPanic adds stack trace information to a recovered panic.
func WrapPanic(p interface{}) interface{} {
	if p != nil {
		s := fmt.Sprintf("%v\n%s\nrethrown at", p, debug.Stack())
		if _, isError := p.(error); isError {
			r := errors.New(s)
			if _, isRuntimeError := p.(runtime.Error); isRuntimeError {
				return runtimeError{r}
			}
			return r
		}
		return s
	}
	return nil
}
