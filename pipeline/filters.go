package pipeline

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
)

/*
NewNode creates a node of the given kind, with the given filters.

It is often more convenient to use one of the Ord, Seq, or Par
methods.
*/
func NewNode(kind NodeKind, filters ...Filter) Node {
	switch kind {
	case Ordered, Sequential:
		return &seqnode{kind: kind, filters: filters}
	case Parallel:
		return &parnode{filters: filters}
	default:
		panic("Invalid NodeKind in pipeline.NewNode.")
	}
}

/*
Receive creates a Filter that returns the given receiver and a nil
finalizer.
*/
func Receive(receive Receiver) Filter {
	return func(_ *Pipeline, _ NodeKind, _ *int) (receiver Receiver, _ Finalizer) {
		receiver = receive
		return
	}
}

/*
Finalize creates a filter that returns a nil receiver and the given
finalizer.
*/
func Finalize(finalize Finalizer) Filter {
	return func(_ *Pipeline, _ NodeKind, _ *int) (_ Receiver, finalizer Finalizer) {
		finalizer = finalize
		return
	}
}

/*
ReceiveAndFinalize creates a filter that returns the given filter and
receiver.
*/
func ReceiveAndFinalize(receive Receiver, finalize Finalizer) Filter {
	return func(_ *Pipeline, _ NodeKind, _ *int) (receiver Receiver, finalizer Finalizer) {
		receiver = receive
		finalizer = finalize
		return
	}
}

/*
A Predicate is a function that is passed a data batch and returns a
boolean value.

In most cases, it will cast the data parameter to a specific slice
type and check a predicate on each element of the slice.
*/
type Predicate func(data interface{}) bool

/*
Every creates a filter that sets the result pointer to true if the
given predicate returns true for every data batch. If cancelWhenKnown
is true, this filter cancels the pipeline as soon as the predicate
returns false on a data batch.
*/
func Every(result *bool, cancelWhenKnown bool, predicate Predicate) Filter {
	*result = true
	return func(pipeline *Pipeline, kind NodeKind, _ *int) (receiver Receiver, finalizer Finalizer) {
		switch kind {
		case Parallel:
			res := int32(1)
			receiver = func(_ int, data interface{}) interface{} {
				if !predicate(data) {
					atomic.StoreInt32(&res, 0)
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
			finalizer = func() {
				if res == 0 {
					*result = false
				}
			}
		default:
			receiver = func(_ int, data interface{}) interface{} {
				if !predicate(data) {
					*result = false
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
		}
		return
	}
}

/*
NotEvery creates a filter that sets the result pointer to true if the
given predicate returns false for at least one of the data batches it
is passed. If cancelWhenKnown is true, this filter cancels the
pipeline as soon as the predicate returns false on a data batch.
*/
func NotEvery(result *bool, cancelWhenKnown bool, predicate Predicate) Filter {
	*result = false
	return func(pipeline *Pipeline, kind NodeKind, _ *int) (receiver Receiver, finalizer Finalizer) {
		switch kind {
		case Parallel:
			res := int32(0)
			receiver = func(_ int, data interface{}) interface{} {
				if !predicate(data) {
					atomic.StoreInt32(&res, 1)
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
			finalizer = func() {
				if res == 1 {
					*result = true
				}
			}
		default:
			receiver = func(_ int, data interface{}) interface{} {
				if !predicate(data) {
					*result = true
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
		}
		return
	}
}

/*
Some creates a filter that sets the result pointer to true if the
given predicate returns true for at least one of the data batches it
is passed. If cancelWhenKnown is true, this filter cancels the
pipeline as soon as the predicate returns true on a data batch.
*/
func Some(result *bool, cancelWhenKnown bool, predicate Predicate) Filter {
	*result = false
	return func(pipeline *Pipeline, kind NodeKind, _ *int) (receiver Receiver, finalizer Finalizer) {
		switch kind {
		case Parallel:
			res := int32(0)
			receiver = func(_ int, data interface{}) interface{} {
				if predicate(data) {
					atomic.StoreInt32(&res, 1)
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
			finalizer = func() {
				if res == 1 {
					*result = true
				}
			}
		default:
			receiver = func(_ int, data interface{}) interface{} {
				if predicate(data) {
					*result = true
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
		}
		return
	}
}

/*
NotAny creates a filter that sets the result pointer to true if the
given predicate returns false for every data batch. If cancelWhenKnown
is true, this filter cancels the pipeline as soon as the predicate
returns true on a data batch.
*/
func NotAny(result *bool, cancelWhenKnown bool, predicate Predicate) Filter {
	*result = true
	return func(pipeline *Pipeline, kind NodeKind, _ *int) (receiver Receiver, finalizer Finalizer) {
		switch kind {
		case Parallel:
			res := int32(1)
			receiver = func(_ int, data interface{}) interface{} {
				if predicate(data) {
					atomic.StoreInt32(&res, 0)
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
			finalizer = func() {
				if res == 0 {
					*result = false
				}
			}
		default:
			receiver = func(_ int, data interface{}) interface{} {
				if predicate(data) {
					*result = false
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
		}
		return
	}
}

/*
Slice creates a filter that appends all the data batches it sees to
the result. The result must represent a settable slice, for example by
using the address operator & on a given slice.
*/
func Slice(result interface{}) Filter {
	res := reflect.ValueOf(result).Elem()
	return func(pipeline *Pipeline, kind NodeKind, _ *int) (receiver Receiver, finalizer Finalizer) {
		if (res.Kind() != reflect.Slice) && !res.CanSet() {
			pipeline.Err(errors.New("result is not a settable slice in Pipeline.ToSlice"))
			return
		}
		switch kind {
		case Parallel:
			var m sync.Mutex
			receiver = func(_ int, data interface{}) interface{} {
				if data != nil {
					d := reflect.ValueOf(data)
					m.Lock()
					defer m.Unlock()
					res.Set(reflect.AppendSlice(res, d))
				}
				return data
			}
		default:
			receiver = func(_ int, data interface{}) interface{} {
				if data != nil {
					res.Set(reflect.AppendSlice(res, reflect.ValueOf(data)))
				}
				return data
			}
		}
		return
	}
}

/*
Count creates a filter that sets the result pointer to the total size
of all data batches it sees.
*/
func Count(result *int) Filter {
	return func(pipeline *Pipeline, kind NodeKind, size *int) (receiver Receiver, finalizer Finalizer) {
		switch {
		case *size >= 0:
			*result = *size
		case kind == Parallel:
			var res = int64(0)
			receiver = func(_ int, data interface{}) interface{} {
				if data != nil {
					d := reflect.ValueOf(data)
					atomic.AddInt64(&res, int64(d.Len()))
				}
				return data
			}
			finalizer = func() {
				*result = int(res)
			}
		default:
			receiver = func(_ int, data interface{}) interface{} {
				if data != nil {
					d := reflect.ValueOf(data)
					*result += d.Len()
				}
				return data
			}
		}
		return
	}
}

/*
Limit creates an ordered node with a filter that caps the total size
of all data batches it passes to the next filter in the pipeline to
the given limit. If cancelWhenKnown is true, this filter cancels the
pipeline as soon as the limit is reached. If limit is negative, all
data is passed through unmodified.
*/
func Limit(limit int, cancelWhenReached bool) Node {
	return Ord(func(pipeline *Pipeline, _ NodeKind, size *int) (receiver Receiver, _ Finalizer) {
		switch {
		case limit < 0: // unlimited
		case limit == 0:
			*size = 0
			if cancelWhenReached {
				pipeline.Cancel()
			}
			receiver = func(_ int, _ interface{}) interface{} { return nil }
		case (*size < 0) || (*size > limit):
			if *size > 0 {
				*size = limit
			}
			seen := 0
			receiver = func(_ int, data interface{}) (result interface{}) {
				if seen >= limit {
					return
				}
				d := reflect.ValueOf(data)
				l := d.Len()
				if (seen + l) > limit {
					result = d.Slice(0, limit-seen).Interface()
					seen = limit
				} else {
					result = data
					seen += l
				}
				if cancelWhenReached && (seen == limit) {
					pipeline.Cancel()
				}
				return
			}
		}
		return
	})
}

/*
Skip creates an ordered node with a filter that skips the first n
elements from the data batches it passes to the next filter in the
pipeline. If n is negative, no data is passed through, and the error
value of the pipeline is set to a non-nil value.
*/
func Skip(n int) Node {
	return Ord(func(pipeline *Pipeline, _ NodeKind, size *int) (receiver Receiver, _ Finalizer) {
		switch {
		case n < 0: // skip everything
			*size = 0
			pipeline.Err(errors.New("skip filter with unknown size"))
			receiver = func(_ int, _ interface{}) interface{} { return nil }
		case n == 0: // nothing to skip
		case (*size < 0) || (*size > n):
			if *size > 0 {
				*size = n
			}
			seen := 0
			receiver = func(_ int, data interface{}) (result interface{}) {
				if seen >= n {
					result = data
					return
				}
				d := reflect.ValueOf(data)
				l := d.Len()
				if (seen + l) > n {
					result = d.Slice(n-seen, l).Interface()
					seen = n
				} else {
					seen += l
				}
				return
			}
		case *size <= n:
			*size = 0
			receiver = func(_ int, _ interface{}) interface{} { return nil }
		}
		return
	})
}
