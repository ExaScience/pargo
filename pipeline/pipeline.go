/*
Package pipeline provides means to construct and execute parallel
pipelines.

A Pipeline feeds batches of data through several functions that can be
specified to be executed in encounter order, in arbitrary sequential
order, or in parallel.  Ordered, sequential, or parallel stages can
arbitrarily alternate.

A Pipeline consists of a Source object, and several Node objects.

Source objects that are supported by this implementation are arrays,
slices, strings, channels, and bufio.Scanner objects, but other kinds
of Source objects can be added by user programs.

Node objects can be specified to receive batches from the input source
either sequentially in encounter order, which is always the same order
in which they were originally encountered at the source; sequentially,
but in arbitrary order; or in parallel. Ordered nodes always receive
batches in encounter order even if they are preceded by arbitrary
sequential, or even parallel nodes.

Node objects consist of filters, which are pairs of receiver and
finalizer functions. Each batch is passed to each receiver function,
which can transform and modify the batch for the next receiver
function in the pipeline. Each finalizer function is called once when
all batches have been passed through all receiver functions.

Pipelines do not have an explicit representation for sinks. Instead,
filters can use side effects to generate results.

Pipelines also support cancelation by way of the context package of
Go's standard library.
*/
package pipeline

import (
	"context"
	"runtime"
	"sync"
)

type (
	/*
	  A Node object represents a sequence of filters which are together
	  executed either in encounter order, in arbitrary sequential order,
	  or in parallel.

	  The methods of this interface are typically not called by user
	  programs, but rather implemented by specific node types and called
	  by pipelines. Ordered, sequential, and parallel nodes are also
	  implemented in this package, so that user programs are typically not
	  concerned with Node methods at all.
	*/
	Node interface {

		// TryMerge tries to merge node with the current node by appending
		// its filters to the filters of the current node, which succeeds
		// if both nodes are either sequential or parallel. The return
		// value merged indicates whether merging succeeded.
		TryMerge(node Node) (merged bool)

		// Begin informs this node that the pipeline is going to start to
		// feed batches of data to this node. The pipeline, the index of
		// this node among all the nodes in the pipeline, and the expected
		// total size of all batches combined are passed as parameters.
		//
		// The dataSize parameter is either positive, in which case it
		// indicates the expected total size of all batches that will
		// eventually be passed to this node's Feed method, or it is
		// negative, in which case the expected size is either unknown or
		// too difficult to determine. The dataSize parameter is a pointer
		// whose contents can be modified by Begin, for example if this
		// node increases or decreases the total size for subsequent
		// nodes, or if this node can change dataSize from an unknown to a
		// known value, or vice versa, must change it from a known to an
		// unknown value.
		//
		// A node may decide that, based on the given information, it will
		// actually not need to see any of the batches that are normally
		// going to be passed to it. In that case, it can return false as
		// a result, and its Feed and End method will not be called
		// anymore.  Otherwise, it should return true by default.
		Begin(p *Pipeline, index int, dataSize *int) (keep bool)

		// Feed is called for each batch of data. The pipeline, the index
		// of this node among all the nodes in the pipeline (which may be
		// different from the index number seen by Begin), the sequence
		// number of the batch (according to the encounter order), and the
		// actual batch of data are passed as parameters.
		//
		// The data parameter contains the batch of data, which is usually
		// a slice of a particular type. After the data has been processed
		// by all filters of this node, the node must call p.FeedForward
		// with exactly the same index and sequence numbers, but a
		// potentially modified batch of data. FeedForward must be called
		// even when the data batch is or becomes empty, to ensure that
		// all sequence numbers are seen by subsequent nodes.
		Feed(p *Pipeline, index int, seqNo int, data interface{})

		// End is called after all batches have been passed to Feed. This
		// allows the node to release resources and call the finalizers of
		// its filters.
		End()
	}

	/*
	  A Pipeline is a parallel pipeline that can feed batches of data
	  fetched from a source through several nodes that are ordered,
	  sequential, or parallel.

	  The zero Pipeline is valid and empty.

	  A Pipeline must not be copied after first use.
	*/
	Pipeline struct {
		mutex      sync.RWMutex
		err        error
		ctx        context.Context
		cancel     context.CancelFunc
		source     Source
		nodes      []Node
		nofBatches int
	}
)

/*
Err sets or gets an error value for this pipeline.

If err is nil, Err returns the current error value for this pipeline.

If err is not nil, Err attempts to set a new error value for this
pipeline, unless it already has a non-nil error value. If the attempt
is successful, err is returned and Err also cancels the pipeline. If
the attempt is not successful, the current error value for this
pipeline is returned instead.

Err is safe to be invoked from different goroutines, for example from
the different goroutines executing filters of parallel nodes in this
pipeline.
*/
func (p *Pipeline) Err(err error) error {
	if err == nil {
		p.mutex.RLock()
		err := p.err
		p.mutex.RUnlock()
		return err
	}
	p.mutex.Lock()
	if p.err == nil {
		p.err = err
		p.mutex.Unlock()
		p.cancel()
	} else {
		err = p.err
		p.mutex.Unlock()
	}
	return err
}

// Context returns this pipeline's context.
func (p *Pipeline) Context() context.Context {
	return p.ctx
}

// Cancel calls the cancel function of this pipeline's context.
func (p *Pipeline) Cancel() {
	p.cancel()
}

/*
Source sets the data source for this pipeline.

If source does not implement the Source interface, the pipeline uses
reflection to create a proper source for arrays, slices, strings, or
channels.

It is safe to call Source multiple times before Run or RunWithContext
is called, in which case only the last call to Source is effective.
*/
func (p *Pipeline) Source(source interface{}) {
	switch src := source.(type) {
	case Source:
		p.source = src
	default:
		p.source = reflectSource(source)
	}
}

// Add appends nodes to the end of this pipeline.
func (p *Pipeline) Add(nodes ...Node) {
	for _, node := range nodes {
		if l := len(p.nodes); (l == 0) || !p.nodes[l-1].TryMerge(node) {
			p.nodes = append(p.nodes, node)
		}
	}
}

/*
NofBatches sets or gets the number of batches that are created from
the data source for this pipeline, if the expected total size for this
pipeline's data source is known or can be determined easily.

NofBatches can be called safely by user programs before Run or
RunWithContext is called.

If user programs do not call NofBatches, or call them with a value <
1, then the pipeline will choose a reasonable default value that takes
runtime.GOMAXPROCS(0) into account.

If the expected total size for this pipeline's data source is unknown,
or is difficult to determine, then the pipeline will start with a
reasonably small batch size and increase the batch size for every
subsequent batch, to accomodate data sources of different total sizes.
*/
func (p *Pipeline) NofBatches(n int) (nofBatches int) {
	if n < 1 {
		nofBatches = p.nofBatches
		if nofBatches < 1 {
			nofBatches = 2 * runtime.GOMAXPROCS(0)
			p.nofBatches = nofBatches
		}
	} else {
		nofBatches = n
		p.nofBatches = n
	}
	return
}

const (
	batchInc     = 1024
	maxBatchSize = 0x2000000
)

func nextBatchSize(batchSize int) (result int) {
	result = batchSize + batchInc
	if result > maxBatchSize {
		result = maxBatchSize
	}
	return
}

/*
RunWithContext initiates pipeline execution.

It expects a context and a cancel function as parameters, for example
from context.WithCancel(context.Background()). It does not ensure that
the cancel function is called at least once, so this must be ensured
by the function calling RunWithContext.

RunWithContext should only be called after a data source has been set
using the Source method, and one or more Node objects have been added
to the pipeline using the Add method. NofBatches can be called before
RunWithContext to deal with load imbalance, but this is not necessary
since RunWithContext chooses a reasonable default value.

RunWithContext prepares the data source, tells each node that batches
are going to be sent to them by calling Begin, and then fetches
batches from the data source and sends them to the nodes. Once the
data source is depleted, the nodes are informed that the end of the
data source has been reached.
*/
func (p *Pipeline) RunWithContext(ctx context.Context, cancel context.CancelFunc) {
	if p.err != nil {
		return
	}
	p.ctx, p.cancel = ctx, cancel
	dataSize := p.source.Prepare(p.ctx)
	filteredSize := dataSize
	for index := 0; index < len(p.nodes); {
		if p.nodes[index].Begin(p, index, &filteredSize) {
			index++
		} else {
			p.nodes = append(p.nodes[:index], p.nodes[index+1:]...)
		}
	}
	if p.err != nil {
		return
	}
	if len(p.nodes) > 0 {
		for index := 0; index < len(p.nodes)-1; {
			if p.nodes[index].TryMerge(p.nodes[index+1]) {
				p.nodes = append(p.nodes[:index+1], p.nodes[index+2:]...)
			} else {
				index++
			}
		}
		if dataSize < 0 {
			for seqNo, batchSize := 0, batchInc; p.source.Fetch(batchSize) > 0; seqNo, batchSize = seqNo+1, nextBatchSize(batchSize) {
				p.nodes[0].Feed(p, 0, seqNo, p.source.Data())
				if p.Err(p.source.Err()) != nil {
					return
				}
			}
		} else {
			batchSize := ((dataSize - 1) / p.NofBatches(0)) + 1
			if batchSize == 0 {
				batchSize = 1
			}
			for seqNo := 0; p.source.Fetch(batchSize) > 0; seqNo++ {
				p.nodes[0].Feed(p, 0, seqNo, p.source.Data())
				if p.Err(p.source.Err()) != nil {
					return
				}
			}
		}
	}
	for _, node := range p.nodes {
		node.End()
	}
}

/*
Run initiates pipeline execution by calling
RunWithContext(context.WithCancel(context.Background())), and ensures
that the cancel function is called at least once when the pipeline is
done.

Run should only be called after a data source has been set using the
Source method, and one or more Node objects have been added to the
pipeline using the Add method. NofBatches can be called before Run to
deal with load imbalance, but this is not necessary since Run chooses
a reasonable default value.

Run prepares the data source, tells each node that batches are going
to be sent to them by calling Begin, and then fetches batches from the
data source and sends them to the nodes. Once the data source is
depleted, the nodes are informed that the end of the data source has
been reached.
*/
func (p *Pipeline) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.RunWithContext(ctx, cancel)
}

/*
FeedForward must be called in the Feed method of a node to forward a
potentially modified data batch to the next node in the current
pipeline.

FeedForward is used in Node implementations. User programs typically
do not call FeedForward.

FeedForward must be called with the pipeline received as a parameter
by Feed, and must pass the same index and seqNo received by Feed. The
data parameter can be either a modified or an unmodified data
batch. FeedForward must always be called, even if the data batch is
unmodified, and even if the data batch is or becomes empty.
*/
func (p *Pipeline) FeedForward(index int, seqNo int, data interface{}) {
	if index++; index < len(p.nodes) {
		p.nodes[index].Feed(p, index, seqNo, data)
	}
}
