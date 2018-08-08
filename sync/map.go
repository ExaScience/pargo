// Package sync provides synchronization primitives similar to the
// sync package of Go's standard library, however here with a focus on
// parallel performance rather than concurrency. So far, this package
// only provides support for a parallel map that can be used to some
// extent as a drop-in replacement for the concurrent map of the
// standard library. For other synchronization primitivies, such as
// condition variables, mutual exclusion locks, object pools, or
// atomic memory primitives, please use the standard library.
package sync

import (
	"context"
	"runtime"
	"sync"

	"github.com/exascience/pargo"
	"github.com/exascience/pargo/parallel"
	"github.com/exascience/pargo/speculative"
)

// A Hasher represents an object that has a hash value, which is
// needed by Map.
//
// If Go would allow access to the predefined hash functions for Go
// types, this interface would not be needed.
type Hasher interface {
	Hash() uint64
}

// A Split is a partial map that belongs to a larger Map, which can be
// individually locked. Its enclosed map can then be individually
// accessed without blocking accesses to other splits.
type Split struct {
	sync.RWMutex
	Map map[interface{}]interface{}
}

// A Map is a parallel map that consists of several split maps that
// can be individually locked and accessed.
//
// The zero Map is not valid.
type Map struct {
	splits []Split
}

// NewMap returns a map with size splits.
//
// If size is <= 0, runtime.GOMAXPROCS(0) is used instead.
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

// Split retrieves the split for a particular key.
//
// The split must be locked/unlocked properly by user programs to
// safely access its contents. In many cases, it is easier to use one
// of the high-level methods, like Load, LoadOrStore, LoadOrCompute,
// Delete, DeleteOrStore, DeleteOrCompute, and Modify, which
// implicitly take care of proper locking.
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

// Load returns the value stored in the map for a key, or nil if no
// value is present. The ok result indicates whether value was found
// in the map.
func (m *Map) Load(key Hasher) (value interface{}, ok bool) {
	split := m.Split(key)
	split.RLock()
	value, ok = split.Map[key]
	split.RUnlock()
	return
}

// LoadOrStore returns the existing value for the key if
// present. Otherwise, it stores and returns the given value. The
// loaded result is true if the value was loaded, false if stored.
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

// LoadOrCompute returns the existing value for the key if
// present. Otherwise, it calls computer, and then stores and returns
// the computed value. The loaded result is true if the value was
// loaded, false if stored.
//
// The computer function is invoked either zero times or once. While
// computer is executing no locks related to this map are being held.
//
// The computed value may not be stored and returned, since a parallel
// thread may have successfully stored a value for the key in the
// meantime. In that case, the value stored by the parallel thread is
// returned instead.
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

// DeleteOrStore deletes and returns the existing value for the key if
// present. Otherwise, it stores and returns the given value. The
// deleted result is true if the value was deleted, false if stored.
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

// DeleteOrCompute deletes and returns the existing value for the key
// if present. Otherwise, it calls computer, and then stores and
// returns the computed value. The deleted result is true if the value
// was deleted, false if stored.
//
// The computer function is invoked either zero times or once. While
// computer is executing, a lock is being held on a portion of the
// map, so the function should be brief.
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

// Modify looks up a value for the key if present and passes it to the
// modifier. The ok parameter indicates whether value was found in the
// map. The replacement returned by the modifier is then stored as a
// value for key in the map if storeNotDelete is true, otherwise the
// value is deleted from the map. Modify returns the same results as
// modifier.
//
// The modifier is invoked exactly once. While modifier is executing,
// a lock is being held on a portion of the map, so the function
// should be brief.
//
// This is the most general modification function for parallel
// maps. Other functions that modify the map are potentially more
// efficient, so it is better to be more specific if possible.
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

// Range calls f sequentially for each key and value present in the
// map. If f returns false, Range stops the iteration.
//
// While iterating through a split of m, Range holds the corresponding
// lock.
//
// Range does not necessarily correspond to any consistent snapshot of
// the Map's contents: no key will be visited more than once, but if
// the value for any key is stored or deleted concurrently, Range may
// reflect any mapping for that key from any point during the Range
// call.
func (m *Map) Range(f func(key, value interface{}) bool) {
	for i := range m.splits {
		if !m.splits[i].splitRange(f) {
			return
		}
	}
}

// ParallelRange calls f in parallel for each key and value present in
// the map. If f returns false, ParallelRange stops the iteration.
//
// While iterating through a split of m, ParallelRange holds the
// corresponding lock.
//
// ParallelRange does not necessarily correspond to any consistent
// snapshot of the Map's contents: no key will be visited more than
// once, but if the value for any key is stored or deleted
// concurrently, ParallelRange may reflect any mapping for that key
// from any point during the Range call.
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

// SpeculativeRange calls f in parallel for each key and value present
// in the map. If f returns false, SpeculativeRange stops the
// iteration, and makes an attempt to terminate the goroutines early
// which were started by this call to SpeculativeRange.
//
// While iterating through a split of m, SpeculativeRange holds the
// corresponding lock.
//
// SpeculativeRange is useful as an alternative to ParallelRange in
// cases where ParallelRange tends to use computational resources for
// too long when false is a common and/or early return value for f.
// On the other hand, SpeculativeRange adds overhead, so for cases
// where false is an uncommon and/or late return value for f, it may
// be more efficient to use ParallelRange.
//
// SpeculativeRange does not necessarily correspond to any consistent
// snapshot of the Map's contents: no key will be visited more than
// once, but if the value for any key is stored or deleted
// concurrently, SpeculativeRange may reflect any mapping for that key
// from any point during the Range call.
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

func (split *Split) splitPredicate(predicate func(map[interface{}]interface{}) bool) bool {
	split.Lock()
	defer split.Unlock()
	return predicate(split.Map)
}

// And calls predicate for every split of m sequentially. If any predicate
// invocation returns false, And immediately terminates and also returns false.
// Otherwise, And returns true.
//
// While predicate is executed on a split of m,
// And holds the corresponding lock.
//
// And does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, And may
// reflect any mapping for that key from any point during the And
// call.
func (m *Map) And(predicate func(map[interface{}]interface{}) bool) bool {
	for i := range m.splits {
		if !m.splits[i].splitPredicate(predicate) {
			return false
		}
	}
	return true
}

// ParallelAnd calls predicate for every split of m in parallel. The results
// of the predicate invocations are then combined with the && operator.
//
// ParallelAnd returns only when all goroutines it spawns have terminated.
//
// While predicate is executed on a split of m,
// ParallelAnd holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and ParallelAnd eventually panics with the left-most
// recovered panic value.
//
// ParallelAnd does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelAnd may
// reflect any mapping for that key from any point during the ParallelAnd
// call.
func (m *Map) ParallelAnd(predicate func(map[interface{}]interface{}) bool) bool {
	return parallel.RangeAnd(0, len(m.splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if !m.splits[i].splitPredicate(predicate) {
				return false
			}
		}
		return true
	})
}

// SpeculativeAnd calls predicate for every split of m in parallel.
// SpeculativeAnd returns true if all predicate invocations return true;
// or SpeculativeAnd return false when at least one of them returns false,
// without waiting for the other predicates to terminate.
//
// While predicate is executed on a split of m,
// SpeculativeAnd holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeAnd eventually panics with the left-most
// recovered panic value.
//
// SpeculativeAnd does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeAnd may
// reflect any mapping for that key from any point during the SpeculativeAnd
// call.
func (m *Map) SpeculativeAnd(predicate func(map[interface{}]interface{}) bool) bool {
	return speculative.RangeAnd(0, len(m.splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if !m.splits[i].splitPredicate(predicate) {
				return false
			}
		}
		return true
	})
}

// Or calls predicate for every split of m sequentially. If any predicate
// invocation returns true, Or immediately terminates and also returns true.
// Otherwise, Or returns false.
//
// While predicate is executed on a split of m,
// Or holds the corresponding lock.
//
// Or does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, Or may
// reflect any mapping for that key from any point during the Or
// call.
func (m *Map) Or(predicate func(map[interface{}]interface{}) bool) bool {
	for i := range m.splits {
		if m.splits[i].splitPredicate(predicate) {
			return true
		}
	}
	return false
}

// ParallelOr calls predicate for every split of m in parallel. The results
// of the predicate invocations are then combined with the || operator.
//
// ParallelOr returns only when all goroutines it spawns have terminated.
//
// While predicate is executed on a split of m,
// ParallelOr holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and ParallelOr eventually panics with the left-most
// recovered panic value.
//
// ParallelOr does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelOr may
// reflect any mapping for that key from any point during the ParallelOr
// call.
func (m *Map) ParallelOr(predicate func(map[interface{}]interface{}) bool) bool {
	return parallel.RangeOr(0, len(m.splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if m.splits[i].splitPredicate(predicate) {
				return true
			}
		}
		return false
	})
}

// SpeculativeOr calls predicate for every split of m in parallel.
// SpeculativeOr returns false if all predicate invocations return false;
// or SpeculativeOr return true when at least one of them returns true,
// without waiting for the other predicates to terminate.
//
// While predicate is executed on a split of m,
// SpeculativeOr holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeOr eventually panics with the left-most
// recovered panic value.
//
// SpeculativeOr does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeOr may
// reflect any mapping for that key from any point during the SpeculativeOr
// call.
func (m *Map) SpeculativeOr(predicate func(map[interface{}]interface{}) bool) bool {
	return speculative.RangeOr(0, len(m.splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if m.splits[i].splitPredicate(predicate) {
				return true
			}
		}
		return false
	})
}

func (split *Split) splitErrPredicate(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	split.Lock()
	defer split.Unlock()
	return predicate(split.Map)
}

// ErrAnd calls predicate for every split of m sequentially. If any predicate
// invocation returns false, ErrAnd immediately terminates and also returns false.
// Otherwise, ErrAnd returns true.
//
// If predicate also returns a non-nil error value, ErrAnd also immediately terminates
// and returns that error value.
//
// While predicate is executed on a split of m,
// ErrAnd holds the corresponding lock.
//
// ErrAnd does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ErrAnd may
// reflect any mapping for that key from any point during the ErrAnd
// call.
func (m *Map) ErrAnd(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	for i := range m.splits {
		if ok, err := m.splits[i].splitErrPredicate(predicate); err != nil {
			return ok, err
		} else if !ok {
			return false, nil
		}
	}
	return true, nil
}

// ParallelErrAnd calls predicate for every split of m in parallel. The results
// of the predicate invocations are then combined with the && operator.
//
// ParallelErrAnd returns only when all goroutines it spawns have terminated.
//
// If any predicate invocation also returns a non-nil error value,
// ParallelErrAnd returns the left-most of those error values.
//
// While predicate is executed on a split of m,
// ParallelErrAnd holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and ParallelAnd eventually panics with the left-most
// recovered panic value.
//
// ParallelErrAnd does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelErrAnd may
// reflect any mapping for that key from any point during the ParallelErrAnd
// call.
func (m *Map) ParallelErrAnd(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	return parallel.ErrRangeAnd(0, len(m.splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if ok, err := m.splits[i].splitErrPredicate(predicate); err != nil {
				return ok, err
			} else if !ok {
				return false, nil
			}
		}
		return true, nil
	})
}

// SpeculativeErrAnd calls predicate for every split of m in parallel.
// SpeculativeErrAnd returns true if all predicate invocations return true;
// or SpeculativeErrAnd return false when at least one of them returns false,
// without waiting for the other predicates to terminate.
//
// SpeculativeErrAnd may also return the left-most error value that is different
// from nil as a second return value. If both false values and non-nil error
// values are returned, then the left-most of these return value pairs are
// returned.
//
// While predicate is executed on a split of m,
// SpeculativeErrAnd holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeErrAnd eventually panics with the left-most
// recovered panic value.
//
// SpeculativeErrAnd does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeErrAnd may
// reflect any mapping for that key from any point during the SpeculativeErrAnd
// call.
func (m *Map) SpeculativeErrAnd(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	return speculative.ErrRangeAnd(0, len(m.splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if ok, err := m.splits[i].splitErrPredicate(predicate); err != nil {
				return ok, err
			} else if !ok {
				return false, nil
			}
		}
		return true, nil
	})
}

// ErrOr calls predicate for every split of m sequentially. If any predicate
// invocation returns true, ErrOr immediately terminates and also returns true.
// Otherwise, ErrOr returns false.
//
// If predicate also returns a non-nil error value, ErrOr also immediately terminates
// and returns that error value.
//
// While predicate is executed on a split of m,
// ErrOr holds the corresponding lock.
//
// ErrOr does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ErrOr may
// reflect any mapping for that key from any point during the ErrOr
// call.
func (m *Map) ErrOr(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	for i := range m.splits {
		if ok, err := m.splits[i].splitErrPredicate(predicate); err != nil {
			return ok, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}

// ParallelErrOr calls predicate for every split of m in parallel. The results
// of the predicate invocations are then combined with the || operator.
//
// ParallelErrOr returns only when all goroutines it spawns have terminated.
//
// If any predicate invocation also returns a non-nil error value,
// ParallelErrOr returns the left-most of those error values.
//
// While predicate is executed on a split of m,
// ParallelErrOr holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and ParallelAnd eventually panics with the left-most
// recovered panic value.
//
// ParallelErrOr does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelErrOr may
// reflect any mapping for that key from any point during the ParallelErrOr
// call.
func (m *Map) ParallelErrOr(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	return parallel.ErrRangeOr(0, len(m.splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if ok, err := m.splits[i].splitErrPredicate(predicate); err != nil {
				return ok, err
			} else if ok {
				return true, nil
			}
		}
		return false, nil
	})
}

// SpeculativeErrOr calls predicate for every split of m in parallel.
// SpeculativeErrOr returns false if all predicate invocations return false;
// or SpeculativeErrOr return true when at least one of them returns true,
// without waiting for the other predicates to terminate.
//
// SpeculativeErrOr may also return the left-most error value that is different
// from nil as a second return value. If both true values and non-nil error
// values are returned, then the left-most of these return value pairs are
// returned.
//
// While predicate is executed on a split of m,
// SpeculativeErrOr holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeErrOr eventually panics with the left-most
// recovered panic value.
//
// SpeculativeErrOr does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeErrOr may
// reflect any mapping for that key from any point during the SpeculativeErrOr
// call.
func (m *Map) SpeculativeErrOr(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	return speculative.ErrRangeOr(0, len(m.splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if ok, err := m.splits[i].splitErrPredicate(predicate); err != nil {
				return ok, err
			} else if ok {
				return true, nil
			}
		}
		return false, nil
	})
}

func (split *Split) splitReduce(reduce func(map[interface{}]interface{}) interface{}) interface{} {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// Reduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// While reduce is executed on a split of m,
// Reduce holds the corresponding lock.
//
// Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, Reduce may
// reflect any mapping for that key from any point during the Reduce
// call.
func (m *Map) Reduce(reduce func(map[interface{}]interface{}) interface{}, pair pargo.PairReducer) interface{} {
	if len(m.splits) == 0 {
		return nil
	}
	result := m.splits[0].splitReduce(reduce)
	for i := 1; i < len(m.splits); i++ {
		r := m.splits[i].splitReduce(reduce)
		result = pair(result, r)
	}
	return result
}

// ParallelReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelReduce returns only when all goroutines it spawns have terminated.
//
// While reduce is executed on a split of m,
// ParallelReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and ParallelReduce eventually panics with the left-most
// recovered panic value.
//
// ParallelReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelReduce may
// reflect any mapping for that key from any point during the ParallelReduce
// call.
func (m *Map) ParallelReduce(reduce func(map[interface{}]interface{}) interface{}, pair pargo.PairReducer) interface{} {
	return parallel.RangeReduce(0, len(m.splits), 0, func(low, high int) interface{} {
		if low >= high {
			return nil
		}
		result := m.splits[low].splitReduce(reduce)
		for i := low + 1; i < high; i++ {
			r := m.splits[i].splitReduce(reduce)
			result = pair(result, r)
		}
		return result
	}, pair)
}

func (split *Split) splitIntReduce(reduce func(map[interface{}]interface{}) int) int {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// IntReduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// While reduce is executed on a split of m,
// IntReduce holds the corresponding lock.
//
// IntReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, IntReduce may
// reflect any mapping for that key from any point during the IntReduce
// call.
func (m *Map) IntReduce(reduce func(map[interface{}]interface{}) int, pair pargo.IntPairReducer) int {
	if len(m.splits) == 0 {
		return 0
	}
	result := m.splits[0].splitIntReduce(reduce)
	for i := 1; i < len(m.splits); i++ {
		r := m.splits[i].splitIntReduce(reduce)
		result = pair(result, r)
	}
	return result
}

// ParallelIntReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelIntReduce returns only when all goroutines it spawns have terminated.
//
// While reduce is executed on a split of m,
// ParallelIntReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and ParallelIntReduce eventually panics with the left-most
// recovered panic value.
//
// ParallelIntReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelIntReduce may
// reflect any mapping for that key from any point during the ParallelIntReduce
// call.
func (m *Map) ParallelIntReduce(reduce func(map[interface{}]interface{}) int, pair pargo.IntPairReducer) int {
	return parallel.IntRangeReduce(0, len(m.splits), 0, func(low, high int) int {
		if low >= high {
			return 0
		}
		result := m.splits[low].splitIntReduce(reduce)
		for i := low + 1; i < high; i++ {
			r := m.splits[i].splitIntReduce(reduce)
			result = pair(result, r)
		}
		return result
	}, pair)
}

func (split *Split) splitFloat64Reduce(reduce func(map[interface{}]interface{}) float64) float64 {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// Float64Reduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// While reduce is executed on a split of m,
// Float64Reduce holds the corresponding lock.
//
// Float64Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, Float64Reduce may
// reflect any mapping for that key from any point during the Float64Reduce
// call.
func (m *Map) Float64Reduce(reduce func(map[interface{}]interface{}) float64, pair pargo.Float64PairReducer) float64 {
	if len(m.splits) == 0 {
		return 0
	}
	result := m.splits[0].splitFloat64Reduce(reduce)
	for i := 1; i < len(m.splits); i++ {
		r := m.splits[i].splitFloat64Reduce(reduce)
		result = pair(result, r)
	}
	return result
}

// ParallelFloat64Reduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelFloat64Reduce returns only when all goroutines it spawns have terminated.
//
// While reduce is executed on a split of m,
// ParallelFloat64Reduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and ParallelFloat64Reduce eventually panics with the left-most
// recovered panic value.
//
// ParallelFloat64Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelFloat64Reduce may
// reflect any mapping for that key from any point during the ParallelFloat64Reduce
// call.
func (m *Map) ParallelFloat64Reduce(reduce func(map[interface{}]interface{}) float64, pair pargo.Float64PairReducer) float64 {
	return parallel.Float64RangeReduce(0, len(m.splits), 0, func(low, high int) float64 {
		if low >= high {
			return 0
		}
		result := m.splits[low].splitFloat64Reduce(reduce)
		for i := low + 1; i < high; i++ {
			r := m.splits[i].splitFloat64Reduce(reduce)
			result = pair(result, r)
		}
		return result
	}, pair)
}

func (split *Split) splitStringReduce(reduce func(map[interface{}]interface{}) string) string {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// StringReduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// While reduce is executed on a split of m,
// StringReduce holds the corresponding lock.
//
// StringReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, StringReduce may
// reflect any mapping for that key from any point during the StringReduce
// call.
func (m *Map) StringReduce(reduce func(map[interface{}]interface{}) string, pair pargo.StringPairReducer) string {
	if len(m.splits) == 0 {
		return ""
	}
	result := m.splits[0].splitStringReduce(reduce)
	for i := 1; i < len(m.splits); i++ {
		r := m.splits[i].splitStringReduce(reduce)
		result = pair(result, r)
	}
	return result
}

// ParallelStringReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelStringReduce returns only when all goroutines it spawns have terminated.
//
// While reduce is executed on a split of m,
// ParallelStringReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and ParallelStringReduce eventually panics with the left-most
// recovered panic value.
//
// ParallelStringReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelStringReduce may
// reflect any mapping for that key from any point during the ParallelStringReduce
// call.
func (m *Map) ParallelStringReduce(reduce func(map[interface{}]interface{}) string, pair pargo.StringPairReducer) string {
	return parallel.StringRangeReduce(0, len(m.splits), 0, func(low, high int) string {
		if low >= high {
			return ""
		}
		result := m.splits[low].splitStringReduce(reduce)
		for i := low + 1; i < high; i++ {
			r := m.splits[i].splitStringReduce(reduce)
			result = pair(result, r)
		}
		return result
	}, pair)
}

func (split *Split) splitErrReduce(reduce func(map[interface{}]interface{}) (interface{}, error)) (interface{}, error) {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// ErrReduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// If either reduce or pair also return a non-nil error value,
// ErrReduce immediately returns nil and that error value.
//
// While reduce is executed on a split of m,
// ErrReduce holds the corresponding lock.
//
// ErrReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ErrReduce may
// reflect any mapping for that key from any point during the ErrReduce
// call.
func (m *Map) ErrReduce(reduce func(map[interface{}]interface{}) (interface{}, error), pair pargo.ErrPairReducer) (interface{}, error) {
	if len(m.splits) == 0 {
		return nil, nil
	}
	result, err := m.splits[0].splitErrReduce(reduce)
	if err == nil {
		return nil, err
	}
	for i := 1; i < len(m.splits); i++ {
		if r, err := m.splits[i].splitErrReduce(reduce); err != nil {
			return nil, err
		} else if result, err = pair(result, r); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ParallelErrReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelErrReduce returns only when all goroutines it spawns have terminated.
//
// If either reduce or pair also return a non-nil error value,
// ParallelErrReduce returns nil and the left-most of those error values.
//
// While reduce is executed on a split of m,
// ParallelErrReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and ParallelErrReduce eventually panics with the left-most
// recovered panic value.
//
// ParallelErrReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelErrReduce may
// reflect any mapping for that key from any point during the ParallelErrReduce
// call.
func (m *Map) ParallelErrReduce(reduce func(map[interface{}]interface{}) (interface{}, error), pair pargo.ErrPairReducer) (interface{}, error) {
	return parallel.ErrRangeReduce(0, len(m.splits), 0, func(low, high int) (interface{}, error) {
		if low >= high {
			return nil, nil
		}
		result, err := m.splits[low].splitErrReduce(reduce)
		if err == nil {
			return nil, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitErrReduce(reduce); err != nil {
				return nil, err
			} else if result, err = pair(result, r); err != nil {
				return nil, err
			}
		}
		return result, nil
	}, pair)
}

// SpeculativeErrReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// SpeculativeErrReduce returns either when all goroutines it spawns have
// terminated, or when one or more reduce or pair functions return
// a non-nil error value. In the latter case, SpeculativeErrReduce returns
// nil and the left-most of these error values, without waiting for the
// other goroutines to terminate.
//
// While reduce is executed on a split of m,
// SpeculativeErrReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeErrReduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeErrReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeErrReduce may
// reflect any mapping for that key from any point during the SpeculativeErrReduce
// call.
func (m *Map) SpeculativeErrReduce(reduce func(map[interface{}]interface{}) (interface{}, error), pair pargo.ErrPairReducer) (interface{}, error) {
	return speculative.ErrRangeReduce(0, len(m.splits), 0, func(low, high int) (interface{}, error) {
		if low >= high {
			return nil, nil
		}
		result, err := m.splits[low].splitErrReduce(reduce)
		if err == nil {
			return nil, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitErrReduce(reduce); err != nil {
				return nil, err
			} else if result, err = pair(result, r); err != nil {
				return nil, err
			}
		}
		return result, nil
	}, pair)
}

func (split *Split) splitErrIntReduce(reduce func(map[interface{}]interface{}) (int, error)) (int, error) {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// ErrIntReduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// If either reduce or pair also return a non-nil error value,
// ErrIntReduce immediately returns 0 and that error value.
//
// While reduce is executed on a split of m,
// ErrIntReduce holds the corresponding lock.
//
// ErrIntReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ErrIntReduce may
// reflect any mapping for that key from any point during the ErrIntReduce
// call.
func (m *Map) ErrIntReduce(reduce func(map[interface{}]interface{}) (int, error), pair pargo.ErrIntPairReducer) (int, error) {
	if len(m.splits) == 0 {
		return 0, nil
	}
	result, err := m.splits[0].splitErrIntReduce(reduce)
	if err == nil {
		return 0, err
	}
	for i := 1; i < len(m.splits); i++ {
		if r, err := m.splits[i].splitErrIntReduce(reduce); err != nil {
			return 0, err
		} else if result, err = pair(result, r); err != nil {
			return 0, err
		}
	}
	return result, nil
}

// ParallelErrIntReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelErrIntReduce returns only when all goroutines it spawns have terminated.
//
// If either reduce or pair also return a non-nil error value,
// ParallelErrIntReduce returns 0 and the left-most of those error values.
//
// While reduce is executed on a split of m,
// ParallelErrIntReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and ParallelErrIntReduce eventually panics with the left-most
// recovered panic value.
//
// ParallelErrIntReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelErrIntReduce may
// reflect any mapping for that key from any point during the ParallelErrIntReduce
// call.
func (m *Map) ParallelErrIntReduce(reduce func(map[interface{}]interface{}) (int, error), pair pargo.ErrIntPairReducer) (int, error) {
	return parallel.ErrIntRangeReduce(0, len(m.splits), 0, func(low, high int) (int, error) {
		if low >= high {
			return 0, nil
		}
		result, err := m.splits[low].splitErrIntReduce(reduce)
		if err == nil {
			return 0, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitErrIntReduce(reduce); err != nil {
				return 0, err
			} else if result, err = pair(result, r); err != nil {
				return 0, err
			}
		}
		return result, nil
	}, pair)
}

// SpeculativeErrIntReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// SpeculativeErrIntReduce returns either when all goroutines it spawns have
// terminated, or when one or more reduce or pair functions return
// a non-nil error value. In the latter case, SpeculativeErrIntReduce returns
// 0 and the left-most of these error values, without waiting for the
// other goroutines to terminate.
//
// While reduce is executed on a split of m,
// SpeculativeErrIntReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeErrIntReduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeErrIntReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeErrIntReduce may
// reflect any mapping for that key from any point during the SpeculativeErrIntReduce
// call.
func (m *Map) SpeculativeErrIntReduce(reduce func(map[interface{}]interface{}) (int, error), pair pargo.ErrIntPairReducer) (int, error) {
	return speculative.ErrIntRangeReduce(0, len(m.splits), 0, func(low, high int) (int, error) {
		if low >= high {
			return 0, nil
		}
		result, err := m.splits[low].splitErrIntReduce(reduce)
		if err == nil {
			return 0, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitErrIntReduce(reduce); err != nil {
				return 0, err
			} else if result, err = pair(result, r); err != nil {
				return 0, err
			}
		}
		return result, nil
	}, pair)
}

func (split *Split) splitErrFloat64Reduce(reduce func(map[interface{}]interface{}) (float64, error)) (float64, error) {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// ErrFloat64Reduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// If either reduce or pair also return a non-nil error value,
// ErrFloat64Reduce immediately returns 0 and that error value.
//
// While reduce is executed on a split of m,
// ErrFloat64Reduce holds the corresponding lock.
//
// ErrFloat64Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ErrFloat64Reduce may
// reflect any mapping for that key from any point during the ErrFloat64Reduce
// call.
func (m *Map) ErrFloat64Reduce(reduce func(map[interface{}]interface{}) (float64, error), pair pargo.ErrFloat64PairReducer) (float64, error) {
	if len(m.splits) == 0 {
		return 0, nil
	}
	result, err := m.splits[0].splitErrFloat64Reduce(reduce)
	if err == nil {
		return 0, err
	}
	for i := 1; i < len(m.splits); i++ {
		if r, err := m.splits[i].splitErrFloat64Reduce(reduce); err != nil {
			return 0, err
		} else if result, err = pair(result, r); err != nil {
			return 0, err
		}
	}
	return result, nil
}

// ParallelErrFloat64Reduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelErrFloat64Reduce returns only when all goroutines it spawns have terminated.
//
// If either reduce or pair also return a non-nil error value,
// ParallelErrFloat64Reduce returns 0 and the left-most of those error values.
//
// While reduce is executed on a split of m,
// ParallelErrFloat64Reduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and ParallelErrFloat64Reduce eventually panics with the left-most
// recovered panic value.
//
// ParallelErrFloat64Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelErrFloat64Reduce may
// reflect any mapping for that key from any point during the ParallelErrFloat64Reduce
// call.
func (m *Map) ParallelErrFloat64Reduce(reduce func(map[interface{}]interface{}) (float64, error), pair pargo.ErrFloat64PairReducer) (float64, error) {
	return parallel.ErrFloat64RangeReduce(0, len(m.splits), 0, func(low, high int) (float64, error) {
		if low >= high {
			return 0, nil
		}
		result, err := m.splits[low].splitErrFloat64Reduce(reduce)
		if err == nil {
			return 0, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitErrFloat64Reduce(reduce); err != nil {
				return 0, err
			} else if result, err = pair(result, r); err != nil {
				return 0, err
			}
		}
		return result, nil
	}, pair)
}

// SpeculativeErrFloat64Reduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// SpeculativeErrFloat64Reduce returns either when all goroutines it spawns have
// terminated, or when one or more reduce or pair functions return
// a non-nil error value. In the latter case, SpeculativeErrFloat64Reduce returns
// 0 and the left-most of these error values, without waiting for the
// other goroutines to terminate.
//
// While reduce is executed on a split of m,
// SpeculativeErrFloat64Reduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeErrFloat64Reduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeErrFloat64Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeErrFloat64Reduce may
// reflect any mapping for that key from any point during the SpeculativeErrFloat64Reduce
// call.
func (m *Map) SpeculativeErrFloat64Reduce(reduce func(map[interface{}]interface{}) (float64, error), pair pargo.ErrFloat64PairReducer) (float64, error) {
	return speculative.ErrFloat64RangeReduce(0, len(m.splits), 0, func(low, high int) (float64, error) {
		if low >= high {
			return 0, nil
		}
		result, err := m.splits[low].splitErrFloat64Reduce(reduce)
		if err == nil {
			return 0, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitErrFloat64Reduce(reduce); err != nil {
				return 0, err
			} else if result, err = pair(result, r); err != nil {
				return 0, err
			}
		}
		return result, nil
	}, pair)
}

func (split *Split) splitErrStringReduce(reduce func(map[interface{}]interface{}) (string, error)) (string, error) {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// ErrStringReduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// If either reduce or pair also return a non-nil error value,
// ErrStringReduce immediately returns 0 and that error value.
//
// While reduce is executed on a split of m,
// ErrStringReduce holds the corresponding lock.
//
// ErrStringReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ErrStringReduce may
// reflect any mapping for that key from any point during the ErrStringReduce
// call.
func (m *Map) ErrStringReduce(reduce func(map[interface{}]interface{}) (string, error), pair pargo.ErrStringPairReducer) (string, error) {
	if len(m.splits) == 0 {
		return "", nil
	}
	result, err := m.splits[0].splitErrStringReduce(reduce)
	if err == nil {
		return "", err
	}
	for i := 1; i < len(m.splits); i++ {
		if r, err := m.splits[i].splitErrStringReduce(reduce); err != nil {
			return "", err
		} else if result, err = pair(result, r); err != nil {
			return "", err
		}
	}
	return result, nil
}

// ParallelErrStringReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelErrStringReduce returns only when all goroutines it spawns have terminated.
//
// If either reduce or pair also return a non-nil error value,
// ParallelErrStringReduce returns 0 and the left-most of those error values.
//
// While reduce is executed on a split of m,
// ParallelErrStringReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and ParallelErrStringReduce eventually panics with the left-most
// recovered panic value.
//
// ParallelErrStringReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelErrStringReduce may
// reflect any mapping for that key from any point during the ParallelErrStringReduce
// call.
func (m *Map) ParallelErrStringReduce(reduce func(map[interface{}]interface{}) (string, error), pair pargo.ErrStringPairReducer) (string, error) {
	return parallel.ErrStringRangeReduce(0, len(m.splits), 0, func(low, high int) (string, error) {
		if low >= high {
			return "", nil
		}
		result, err := m.splits[low].splitErrStringReduce(reduce)
		if err == nil {
			return "", err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitErrStringReduce(reduce); err != nil {
				return "", err
			} else if result, err = pair(result, r); err != nil {
				return "", err
			}
		}
		return result, nil
	}, pair)
}

// SpeculativeErrStringReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// SpeculativeErrStringReduce returns either when all goroutines it spawns have
// terminated, or when one or more reduce or pair functions return
// a non-nil error value. In the latter case, SpeculativeErrStringReduce returns
// 0 and the left-most of these error values, without waiting for the
// other goroutines to terminate.
//
// While reduce is executed on a split of m,
// SpeculativeErrStringReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeErrStringReduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeErrStringReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeErrStringReduce may
// reflect any mapping for that key from any point during the SpeculativeErrStringReduce
// call.
func (m *Map) SpeculativeErrStringReduce(reduce func(map[interface{}]interface{}) (string, error), pair pargo.ErrStringPairReducer) (string, error) {
	return speculative.ErrStringRangeReduce(0, len(m.splits), 0, func(low, high int) (string, error) {
		if low >= high {
			return "", nil
		}
		result, err := m.splits[low].splitErrStringReduce(reduce)
		if err == nil {
			return "", err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitErrStringReduce(reduce); err != nil {
				return "", err
			} else if result, err = pair(result, r); err != nil {
				return "", err
			}
		}
		return result, nil
	}, pair)
}
