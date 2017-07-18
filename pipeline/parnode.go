package pipeline

import (
	"sync"
)

type parnode struct {
	waitGroup  sync.WaitGroup
	filters    []Filter
	receivers  []Receiver
	finalizers []Finalizer
}

// Par creates a parallel node with the given filters.
func Par(filters ...Filter) Node {
	return &parnode{filters: filters}
}

// Implements the TryMerge method of the Node interface.
func (node *parnode) TryMerge(next Node) bool {
	if nxt, merge := next.(*parnode); merge {
		node.filters = append(node.filters, nxt.filters...)
		node.receivers = append(node.receivers, nxt.receivers...)
		node.finalizers = append(node.finalizers, nxt.finalizers...)
		return true
	}
	return false
}

// Implements the Begin method of the Node interface.
func (node *parnode) Begin(p *Pipeline, _ int, dataSize *int) (keep bool) {
	node.receivers, node.finalizers = ComposeFilters(p, Parallel, dataSize, node.filters)
	node.filters = nil
	keep = (len(node.receivers) > 0) || (len(node.finalizers) > 0)
	return
}

// Implements the Feed method of the Node interface.
func (node *parnode) Feed(p *Pipeline, index int, seqNo int, data interface{}) {
	node.waitGroup.Add(1)
	go func() {
		defer node.waitGroup.Done()
		select {
		case <-p.ctx.Done():
			return
		default:
			feed(p, node.receivers, index, seqNo, data)
		}
	}()
}

// Implements the End method of the Node interface.
func (node *parnode) End() {
	node.waitGroup.Wait()
	for _, finalize := range node.finalizers {
		finalize()
	}
	node.receivers = nil
	node.finalizers = nil
}
