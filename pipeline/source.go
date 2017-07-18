package pipeline

import (
	"bufio"
	"context"
	"io"
	"reflect"
)

/*
A Source represents an object that can generate data batches for
pipelines.
*/
type Source interface {
	// Err returns an error value or nil
	Err() error

	// Prepare receives a pipeline context and informs the pipeline what
	// the total expected size of all data batches is. The return value
	// is -1 if the total size is unknown or difficult to determine.
	Prepare(ctx context.Context) (size int)

	// Fetch gets a data batch of the requested size from the source.
	// It returns the size of the data batch that it was actually able
	// to fetch. It returns 0 if there is no more data to be fetched
	// from the source; the pipeline will then make no further attempts
	// to fetch more elements.
	Fetch(size int) (fetched int)

	// Data returns the last fetched data batch.
	Data() interface{}
}

type sliceSource struct {
	value       reflect.Value
	index, size int
	data        interface{}
}

func newSliceSource(value reflect.Value) *sliceSource {
	return &sliceSource{value: value, size: value.Len()}
}

func (src *sliceSource) Err() error {
	return nil
}

func (src *sliceSource) Prepare(_ context.Context) int {
	return src.size
}

func (src *sliceSource) Fetch(n int) (fetched int) {
	switch {
	case src.index >= src.size:
		src.data = nil
	case (src.index + n) > src.size:
		src.data = src.value.Slice(src.index, src.size).Interface()
		fetched = src.size - src.index
		src.index = src.size
	default:
		src.data = src.value.Slice(src.index, src.index+n).Interface()
		src.index += n
		fetched = n
	}
	return
}

func (src *sliceSource) Data() interface{} {
	return src.data
}

type chanSource struct {
	cases []reflect.SelectCase
	zero  reflect.Value
	data  interface{}
}

func newChanSource(value reflect.Value) *chanSource {
	zeroElem := value.Type().Elem()
	return &chanSource{
		cases: []reflect.SelectCase{{reflect.SelectRecv, value, reflect.Zero(zeroElem)}},
		zero:  reflect.Zero(reflect.SliceOf(zeroElem)),
	}
}

func (src *chanSource) Err() error {
	return nil
}

var doneZero struct{}

func (src *chanSource) Prepare(ctx context.Context) (size int) {
	src.cases = append(src.cases, reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(ctx.Done()), reflect.ValueOf(doneZero)})
	return -1
}

func (src *chanSource) Fetch(n int) (fetched int) {
	data := src.zero
	for fetched = 0; fetched < n; fetched++ {
		if chosen, element, ok := reflect.Select(src.cases); (chosen == 0) && ok {
			data = reflect.Append(data, element)
		} else {
			break
		}
	}
	src.data = data.Interface()
	return
}

func (src *chanSource) Data() interface{} {
	return src.data
}

func reflectSource(source interface{}) Source {
	switch value := reflect.ValueOf(source); value.Kind() {
	case reflect.Array, reflect.Slice, reflect.String:
		return newSliceSource(value)
	case reflect.Chan:
		return newChanSource(value)
	default:
		panic("A default pipeline source is not of kind Array, Slice, String, or Chan.")
	}
}

/*
Scanner is a wrapper around bufio.Scanner so it can act as a data
source for pipelines.
*/
type Scanner struct {
	*bufio.Scanner
	data interface{}
}

/*
NewScanner returns a new Scanner to read from r. The split function
defaults to bufio.ScanLines.
*/
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{Scanner: bufio.NewScanner(r)}
}

// Implements the Prepare method of the Source interface.
func (src *Scanner) Prepare(_ context.Context) (size int) {
	return -1
}

// Implements the Fetch method of the Source interface.
func (src *Scanner) Fetch(n int) (fetched int) {
	var data []string
	for fetched = 0; fetched < n; fetched++ {
		if src.Scan() {
			data = append(data, src.Text())
		} else {
			break
		}
	}
	src.data = data
	return
}

// Implements the Data method of the Source interface.
func (src *Scanner) Data() interface{} {
	return src.data
}
