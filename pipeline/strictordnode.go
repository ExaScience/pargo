package pipeline

import (
	"sync"
)

type strictordnode struct {
	cond       *sync.Cond
	channel    chan dataBatch
	waitGroup  sync.WaitGroup
	run        int
	filters    []Filter
	receivers  []Receiver
	finalizers []Finalizer
}

// StrictOrd creates an ordered node with the given filters.
func StrictOrd(filters ...Filter) Node {
	return &strictordnode{filters: filters}
}

// Implements the TryMerge method of the Node interface.
func (node *strictordnode) TryMerge(next Node) bool {
	switch nxt := next.(type) {
	case *seqnode:
		node.filters = append(node.filters, nxt.filters...)
		node.receivers = append(node.receivers, nxt.receivers...)
		node.finalizers = append(node.finalizers, nxt.finalizers...)
		return true
	case *strictordnode:
		node.filters = append(node.filters, nxt.filters...)
		node.receivers = append(node.receivers, nxt.receivers...)
		node.finalizers = append(node.finalizers, nxt.finalizers...)
		return true
	default:
		return false
	}
}

//Implements the Begin method of the Node interface.
func (node *strictordnode) Begin(p *Pipeline, index int, dataSize *int) (keep bool) {
	node.receivers, node.finalizers = ComposeFilters(p, Ordered, dataSize, node.filters)
	node.filters = nil
	if keep = (len(node.receivers) > 0) || (len(node.finalizers) > 0); keep {
		node.cond = sync.NewCond(&sync.Mutex{})
		node.channel = make(chan dataBatch)
		node.waitGroup.Add(1)
		go func() {
			defer node.waitGroup.Done()
			for {
				select {
				case <-p.ctx.Done():
					node.cond.Broadcast()
					return
				case batch, ok := <-node.channel:
					if !ok {
						return
					}
					node.cond.L.Lock()
					if batch.seqNo != node.run {
						panic("Invalid receive order in a strictly ordered pipeline node.")
					}
					node.run++
					node.cond.L.Unlock()
					node.cond.Broadcast()
					feed(p, node.receivers, index, batch.seqNo, batch.data)
				}
			}
		}()
	}
	return
}

// Implements the Feed method of the Node interface.
func (node *strictordnode) Feed(p *Pipeline, _ int, seqNo int, data interface{}) {
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
		select {
		case <-p.ctx.Done():
			return
		default:
			node.cond.Wait()
		}
	}
}

// Implements the End method of the Node interface.
func (node *strictordnode) End() {
	close(node.channel)
	node.waitGroup.Wait()
	for _, finalize := range node.finalizers {
		finalize()
	}
	node.receivers = nil
	node.finalizers = nil
}
