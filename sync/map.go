/*
Package sync provides synchronization primitives similar to the sync
package of Go's standard library, however here with a focus on
parallel performance rather than concurrency. So far, this package
only provides support for a parallel map that can be used to some
extent as a drop-in replacement for the concurrent map of the standard
library. For other synchronization primitivies, such as condition
variables, mutual exclusion locks, object pools, or atomic memory
primitives, please use the standard library.
*/
package sync

import (
	"context"
	"runtime"
	"sync"

	"github.com/exascience/pargo/parallel"
	"github.com/exascience/pargo/speculative"
)

/*
A Hasher represents an object that has a hash value, which is needed
by Map.

If Go would allow access to the predefined hash functions for Go
types, this interface would not be needed.
*/
type Hasher interface {
	Hash() uint64
}

/*
A Split is a partial map that belongs to a larger Map, which can be
individually locked. Its enclosed map can then be individually
accessed without blocking accesses to other splits.
*/
type Split struct {
	sync.RWMutex
	Map map[interface{}]interface{}
}

/*
A Map is a parallel map that consists of several split maps that can
be individually locked and accessed.

The zero Map is not valid.
*/
type Map struct {
	splits []Split
}

/*
NewMap returns a map with size splits.

If size is <= 0, runtime.GOMAXPROCS(0) is used instead.
*/
func NewMap(size int) *Map {
	if size <= 0 {
		size = runtime.GOMAXPROCS(0)
	}
	splits := make([]Split, size)
	for i := range splits {
		splits[i].Map = make(map[interface{}]interface{})
	}
	return &Map{splits}
}

/*
Split retrieves the split for a particular key.

The split must be locked/unlocked properly by user programs to safely
access its contents. In many cases, it is easier to use one of the
high-level methods, like Load, LoadOrStore, LoadOrCompute, Delete,
DeleteOrStore, DeleteOrCompute, and Modify, which implicitly take care
of proper locking.
*/
func (m *Map) Split(key Hasher) *Split {
	splits := m.splits
	return &splits[key.Hash()%uint64(len(splits))]
}

// Delete deletes the value for a key.
func (m *Map) Delete(key Hasher) {
	split := m.Split(key)
	split.Lock()
	delete(split.Map, key)
	split.Unlock()
}

/*
Load returns the value stored in the map for a key, or nil if no value
is present. The ok result indicates whether value was found in the
map.
*/
func (m *Map) Load(key Hasher) (value interface{}, ok bool) {
	split := m.Split(key)
	split.RLock()
	value, ok = split.Map[key]
	split.RUnlock()
	return
}

/*
LoadOrStore returns the existing value for the key if
present. Otherwise, it stores and returns the given value. The loaded
result is true if the value was loaded, false if stored.
*/
func (m *Map) LoadOrStore(key Hasher, value interface{}) (actual interface{}, loaded bool) {
	split := m.Split(key)
	split.RLock()
	actual, loaded = split.Map[key]
	split.RUnlock()
	if loaded {
		return
	}
	split.Lock()
	if actual, loaded = split.Map[key]; !loaded {
		actual = value
		split.Map[key] = value
	}
	split.Unlock()
	return
}

/*
LoadOrCompute returns the existing value for the key if
present. Otherwise, it calls computer, and then stores and returns the
computed value. The loaded result is true if the value was loaded,
false if stored.

The computer function is invoked either zero times or once. While
computer is executing no locks related to this map are being held.

The computed value may not be stored and returned, since a parallel
thread may have successfully stored a value for the key in the
meantime. In that case, the value stored by the parallel thread is
returned instead.
*/
func (m *Map) LoadOrCompute(key Hasher, computer func() interface{}) (actual interface{}, loaded bool) {
	split := m.Split(key)
	split.RLock()
	actual, loaded = split.Map[key]
	split.RUnlock()
	if loaded {
		return
	}
	value := computer()
	split.Lock()
	if actual, loaded = split.Map[key]; !loaded {
		actual = value
		split.Map[key] = actual
	}
	split.Unlock()
	return
}

/*
DeleteOrStore deletes and returns the existing value for the key if
present. Otherwise, it stores and returns the given value. The deleted
result is true if the value was deleted, false if stored.
*/
func (m *Map) DeleteOrStore(key Hasher, value interface{}) (actual interface{}, deleted bool) {
	split := m.Split(key)
	split.Lock()
	if actual, deleted = split.Map[key]; deleted {
		delete(split.Map, key)
	} else {
		actual = value
		split.Map[key] = value
	}
	split.Unlock()
	return
}

/*
DeleteOrCompute deletes and returns the existing value for the key if
present. Otherwise, it calls computer, and then stores and returns the
computed value. The deleted result is true if the value was deleted,
false if stored.

The computer function is invoked either zero times or once. While
computer is executing, a lock is being held on a portion of the map,
so the function should be brief.
*/
func (m *Map) DeleteOrCompute(key Hasher, computer func() interface{}) (actual interface{}, deleted bool) {
	split := m.Split(key)
	split.Lock()
	if actual, deleted = split.Map[key]; deleted {
		delete(split.Map, key)
	} else {
		actual = computer()
		split.Map[key] = actual
	}
	split.Unlock()
	return
}

/*
Modify looks up a value for the key if present and passes it to the
modifier. The ok parameter indicates whether value was found in the
map. The replacement returned by the modifier is then stored as a
value for key in the map if storeNotDelete is true, otherwise the
value is deleted from the map. Modify returns the same results as
modifier.

The modifier is invoked exactly once. While modifier is executing, a
lock is being held on a portion of the map, so the function should be
brief.

This is the most general modification function for parallel
maps. Other functions that modify the map are potentially more
efficient, so it is better to be more specific if possible.
*/
func (m *Map) Modify(key Hasher, modifier func(value interface{}, ok bool) (replacement interface{}, storeNotDelete bool)) (replacement interface{}, storeNotDelete bool) {
	split := m.Split(key)
	split.Lock()
	value, ok := split.Map[key]
	if replacement, storeNotDelete = modifier(value, ok); storeNotDelete {
		split.Map[key] = replacement
	} else {
		delete(split.Map, key)
	}
	split.Unlock()
	return
}

func (split *Split) splitRange(f func(key, value interface{}) bool) bool {
	split.Lock()
	defer split.Unlock()
	for key, value := range split.Map {
		if !f(key, value) {
			return false
		}
	}
	return true
}

/*
Range calls f sequentially for each key and value present in the
map. If f returns false, Range stops the iteration.

Range does not necessarily correspond to any consistent snapshot of
the Map's contents: no key will be visited more than once, but if the
value for any key is stored or deleted concurrently, Range may reflect
any mapping for that key from any point during the Range call.
*/
func (m *Map) Range(f func(key, value interface{}) bool) {
	for i := range m.splits {
		if !m.splits[i].splitRange(f) {
			return
		}
	}
}

/*
ParallelRange calls f in parallel for each key and value present in
the map. If f returns false, ParallelRange stops the iteration.

ParallelRange does not necessarily correspond to any consistent
snapshot of the Map's contents: no key will be visited more than once,
but if the value for any key is stored or deleted concurrently,
ParallelRange may reflect any mapping for that key from any point
during the Range call.
*/
func (m *Map) ParallelRange(f func(key, value interface{}) bool) {
	splits := m.splits
	parallel.RangeAnd(0, len(splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if !splits[i].splitRange(f) {
				return false
			}
		}
		return true
	})
}

func (split *Split) splitRangeWithContext(ctx context.Context, f func(key, value interface{}) bool) bool {
	split.Lock()
	defer split.Unlock()
	for key, value := range split.Map {
		select {
		case <-ctx.Done():
			return true
		default:
			if !f(key, value) {
				return false
			}
		}
	}
	return true
}

/*
SpeculativeRange calls f in parallel for each key and value present in
the map. If f returns false, SpeculativeRange stops the iteration, and
makes an attempt to terminate the goroutines early which were started
by this call to SpeculativeRange.

SpeculativeRange is useful as an alternative to ParallelRange in cases
where ParallelRange tends to use computational resources for too long
when false is a common and/or early return value for f.  On the other
hand, SpeculativeRange adds overhead, so for cases where false is an
uncommon and/or late return value for f, it may be more efficient to
use ParallelRange.

SpeculativeRange does not necessarily correspond to any consistent
snapshot of the Map's contents: no key will be visited more than once,
but if the value for any key is stored or deleted concurrently,
SpeculativeRange may reflect any mapping for that key from any point
during the Range call.
*/
func (m *Map) SpeculativeRange(f func(key, value interface{}) bool) {
	splits := m.splits
	ctx, cancel := context.WithCancel(context.Background())
	speculative.RangeAnd(0, len(splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if !splits[i].splitRangeWithContext(ctx, f) {
				cancel()
				return false
			}
		}
		return true
	})
}
