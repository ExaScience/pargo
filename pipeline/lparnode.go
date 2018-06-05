package pipeline

import (
	"reflect"
	"runtime"
	"sync"
)

type lparnode struct {
	limit      int
	channels   []chan dataBatch
	cases      []reflect.SelectCase
	waitGroup  sync.WaitGroup
	filters    []Filter
	receivers  []Receiver
	finalizers []Finalizer
}

// LimitedPar creates a parallel node with the given filters.
// The node uses at most limit goroutines at the same time.
// If limit is 0, a reasonable default is used instead.
// Even if limit is 0, the node is still limited.
// For unlimited nodes, use Par instead.
func LimitedPar(limit int, filters ...Filter) Node {
	if limit <= 0 {
		limit = runtime.GOMAXPROCS(0)
	}
	if limit == 1 {
		return &seqnode{kind: Sequential, filters: filters}
	}
	return &lparnode{limit: limit, filters: filters}
}

// Implements the TryMerge method of the Node interface.
func (node *lparnode) TryMerge(next Node) bool {
	if nxt, merge := next.(*lparnode); merge && (nxt.limit == node.limit) {
		node.filters = append(node.filters, nxt.filters...)
		node.receivers = append(node.receivers, nxt.receivers...)
		node.finalizers = append(node.finalizers, nxt.finalizers...)
		return true
	}
	return false
}

// Implements the Begin method of the Node interface.
func (node *lparnode) Begin(p *Pipeline, index int, dataSize *int) (keep bool) {
	node.receivers, node.finalizers = ComposeFilters(p, Parallel, dataSize, node.filters)
	node.filters = nil
	if keep = (len(node.receivers) > 0) || (len(node.finalizers) > 0); keep {
		node.cases = []reflect.SelectCase{{Dir: reflect.SelectRecv}}
		node.waitGroup.Add(node.limit)
		for i := 0; i < node.limit; i++ {
			channel := make(chan dataBatch)
			node.channels = append(node.channels, channel)
			node.cases = append(node.cases, reflect.SelectCase{
				Dir:  reflect.SelectSend,
				Chan: reflect.ValueOf(channel),
			})
			go func() {
				defer node.waitGroup.Done()
				for {
					select {
					case <-p.ctx.Done():
						return
					case batch, ok := <-channel:
						if !ok {
							return
						}
						feed(p, node.receivers, index, batch.seqNo, batch.data)
					}
				}
			}()
		}
	}
	return
}

// Implements the Feed method of the Node interface.
func (node *lparnode) Feed(p *Pipeline, _ int, seqNo int, data interface{}) {
	node.cases[0].Chan = reflect.ValueOf(p.ctx.Done())
	send := reflect.ValueOf(dataBatch{seqNo: seqNo, data: data})
	for i := 1; i < len(node.cases); i++ {
		node.cases[i].Send = send
	}
	reflect.Select(node.cases)
}

// Implements the End method of the Node interface.
func (node *lparnode) End() {
	for _, channel := range node.channels {
		close(channel)
	}
	node.waitGroup.Wait()
	for _, finalize := range node.finalizers {
		finalize()
	}
	node.receivers = nil
	node.finalizers = nil
}
