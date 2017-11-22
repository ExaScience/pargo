package pipeline

// A NodeKind reperesents the different kinds of nodes.
type NodeKind int

const (
	// Ordered nodes receive batches in encounter order.
	Ordered NodeKind = iota

	// Sequential nodes receive batches in arbitrary sequential order.
	Sequential

	// Parallel nodes receives batches in parallel.
	Parallel
)

/*
A Filter is a function that returns a Receiver and a Finalizer to be
added to a node. It receives a pipeline, the kind of node it will be
added to, and the expected total data size that the receiver will be
asked to process.

The dataSize parameter is either positive, in which case it indicates
the expected total size of all batches that will eventually be passed
to this filter's receiver, or it is negative, in which case the
expected size is either unknown or too difficult to determine. The
dataSize parameter is a pointer whose contents can be modified by the
filter, for example if this filter increases or decreases the total
size for subsequent filters, or if this filter can change dataSize
from an unknown to a known value, or vice versa, must change it from a
known to an unknown value.

Either the receiver or the finalizer or both can be nil, in which case
they will not be added to the current node.
*/
type Filter func(pipeline *Pipeline, kind NodeKind, dataSize *int) (Receiver, Finalizer)

// A Receiver is called for every data batch, and returns a
// potentially modified data batch. The seqNo parameter indicates the
// order in which the data batch was encountered at the current
// pipeline's data source.
type Receiver func(seqNo int, data interface{}) (filteredData interface{})

// A Finalizer is called once after the corresponding receiver has
// been called for all data batches in the current pipeline.
type Finalizer func()

/*
ComposeFilters takes a number of filters, calls them with the given
pipeline, kind, and dataSize parameters in order, and appends the
returned receivers and finalizers (except for nil values) to the
result slices.

ComposeFilters is used in Node implementations. User programs
typically do not call ComposeFilters.
*/
func ComposeFilters(pipeline *Pipeline, kind NodeKind, dataSize *int, filters []Filter) (receivers []Receiver, finalizers []Finalizer) {
	for _, filter := range filters {
		receiver, finalizer := filter(pipeline, kind, dataSize)
		if receiver != nil {
			receivers = append(receivers, receiver)
		}
		if finalizer != nil {
			finalizers = append(finalizers, finalizer)
		}
	}
	return
}

func feed(p *Pipeline, receivers []Receiver, index int, seqNo int, data interface{}) {
	for _, receive := range receivers {
		data = receive(seqNo, data)
	}
	p.FeedForward(index, seqNo, data)
}
