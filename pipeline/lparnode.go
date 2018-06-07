package pipeline

import (
	"runtime"
	"sync"
)

type lparnode struct {
	limit      int
	ordered    bool
	cond       *sync.Cond
	channel    chan dataBatch
	waitGroup  sync.WaitGroup
	run        int
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

func (node *lparnode) makeOrdered() {
	node.ordered = true
	node.cond = sync.NewCond(&sync.Mutex{})
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
		node.channel = make(chan dataBatch)
		node.waitGroup.Add(node.limit)
		for i := 0; i < node.limit; i++ {
			go func() {
				defer node.waitGroup.Done()
				for {
					select {
					case <-p.ctx.Done():
						return
					case batch, ok := <-node.channel:
						if !ok {
							return
						}
						if node.ordered {
							node.cond.L.Lock()
							if batch.seqNo != node.run {
								panic("Invalid receive order in an ordered limited parallel pipeline node.")
							}
							node.run++
							node.cond.L.Unlock()
							node.cond.Broadcast()
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
	if node.ordered {
		node.cond.L.Lock()
		defer node.cond.L.Unlock()
		for {
			if node.run == seqNo {
				select {
				case <-p.ctx.Done():
					return
				case node.channel <- dataBatch{seqNo, data}:
					return
				}
			}
			node.cond.Wait()
		}
	}
	select {
	case <-p.ctx.Done():
		return
	case node.channel <- dataBatch{seqNo, data}:
		return
	}
}

// Implements the End method of the Node interface.
func (node *lparnode) End() {
	close(node.channel)
	node.waitGroup.Wait()
	for _, finalize := range node.finalizers {
		finalize()
	}
	node.receivers = nil
	node.finalizers = nil
}
