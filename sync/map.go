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
func (m *Map) Modify(
	key Hasher,
	modifier func(value interface{}, ok bool) (replacement interface{}, storeNotDelete bool),
) (replacement interface{}, storeNotDelete bool) {
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
	parallel.RangeAnd(0, len(splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if !splits[i].splitRange(f) {
				return false, nil
			}
		}
		return true, nil
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
	speculative.RangeAnd(0, len(splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if !splits[i].splitRangeWithContext(ctx, f) {
				cancel()
				return false, nil
			}
		}
		return true, nil
	})
}

func (split *Split) splitPredicate(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	split.Lock()
	defer split.Unlock()
	return predicate(split.Map)
}

// And calls predicate for every split of m sequentially. If any predicate
// invocation returns false, And immediately terminates and also returns false.
// Otherwise, And returns true.
//
// If predicate also returns a non-nil error value, And also immediately terminates
// and returns that error value.
//
// While predicate is executed on a split of m,
// And holds the corresponding lock.
//
// And does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, And may
// reflect any mapping for that key from any point during the And
// call.
func (m *Map) And(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	for i := range m.splits {
		if ok, err := m.splits[i].splitPredicate(predicate); err != nil {
			return ok, err
		} else if !ok {
			return false, nil
		}
	}
	return true, nil
}

// ParallelAnd calls predicate for every split of m in parallel. The results
// of the predicate invocations are then combined with the && operator.
//
// ParallelAnd returns only when all goroutines it spawns have terminated.
//
// If any predicate invocation also returns a non-nil error value,
// ParallelAnd returns the left-most of those error values.
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
func (m *Map) ParallelAnd(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	return parallel.RangeAnd(0, len(m.splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if ok, err := m.splits[i].splitPredicate(predicate); err != nil {
				return ok, err
			} else if !ok {
				return false, nil
			}
		}
		return true, nil
	})
}

// SpeculativeAnd calls predicate for every split of m in parallel.
// SpeculativeAnd returns true if all predicate invocations return true;
// or SpeculativeAnd return false when at least one of them returns false,
// without waiting for the other predicates to terminate.
//
// SpeculativeAnd may also return the left-most error value that is different
// from nil as a second return value. If both false values and non-nil error
// values are returned, then the left-most of these return value pairs are
// returned.
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
func (m *Map) SpeculativeAnd(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	return speculative.RangeAnd(0, len(m.splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if ok, err := m.splits[i].splitPredicate(predicate); err != nil {
				return ok, err
			} else if !ok {
				return false, nil
			}
		}
		return true, nil
	})
}

// Or calls predicate for every split of m sequentially. If any predicate
// invocation returns true, Or immediately terminates and also returns true.
// Otherwise, Or returns false.
//
// If predicate also returns a non-nil error value, Or also immediately terminates
// and returns that error value.
//
// While predicate is executed on a split of m,
// Or holds the corresponding lock.
//
// Or does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, Or may
// reflect any mapping for that key from any point during the Or
// call.
func (m *Map) Or(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	for i := range m.splits {
		if ok, err := m.splits[i].splitPredicate(predicate); err != nil {
			return ok, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}

// ParallelOr calls predicate for every split of m in parallel. The results
// of the predicate invocations are then combined with the || operator.
//
// ParallelOr returns only when all goroutines it spawns have terminated.
//
// If any predicate invocation also returns a non-nil error value,
// ParallelOr returns the left-most of those error values.
//
// While predicate is executed on a split of m,
// ParallelOr holds the corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and ParallelAnd eventually panics with the left-most
// recovered panic value.
//
// ParallelOr does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, ParallelOr may
// reflect any mapping for that key from any point during the ParallelOr
// call.
func (m *Map) ParallelOr(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	return parallel.RangeOr(0, len(m.splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if ok, err := m.splits[i].splitPredicate(predicate); err != nil {
				return ok, err
			} else if ok {
				return true, nil
			}
		}
		return false, nil
	})
}

// SpeculativeOr calls predicate for every split of m in parallel.
// SpeculativeOr returns false if all predicate invocations return false;
// or SpeculativeOr return true when at least one of them returns true,
// without waiting for the other predicates to terminate.
//
// SpeculativeOr may also return the left-most error value that is different
// from nil as a second return value. If both true values and non-nil error
// values are returned, then the left-most of these return value pairs are
// returned.
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
func (m *Map) SpeculativeOr(predicate func(map[interface{}]interface{}) (bool, error)) (bool, error) {
	return speculative.RangeOr(0, len(m.splits), 0, func(low, high int) (bool, error) {
		for i := low; i < high; i++ {
			if ok, err := m.splits[i].splitPredicate(predicate); err != nil {
				return ok, err
			} else if ok {
				return true, nil
			}
		}
		return false, nil
	})
}

func (split *Split) splitReduce(reduce func(map[interface{}]interface{}) (interface{}, error)) (interface{}, error) {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// Reduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// If either reduce or pair also return a non-nil error value,
// Reduce immediately returns nil and that error value.
//
// While reduce is executed on a split of m,
// Reduce holds the corresponding lock.
//
// Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, Reduce may
// reflect any mapping for that key from any point during the Reduce
// call.
func (m *Map) Reduce(
	reduce func(map[interface{}]interface{}) (interface{}, error),
	pair func(x, y interface{}) (interface{}, error),
) (interface{}, error) {
	if len(m.splits) == 0 {
		return nil, nil
	}
	result, err := m.splits[0].splitReduce(reduce)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(m.splits); i++ {
		if r, err := m.splits[i].splitReduce(reduce); err != nil {
			return nil, err
		} else if result, err = pair(result, r); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ParallelReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelReduce returns only when all goroutines it spawns have terminated.
//
// If either reduce or pair also return a non-nil error value,
// ParallelReduce returns nil and the left-most of those error values.
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
func (m *Map) ParallelReduce(
	reduce func(map[interface{}]interface{}) (interface{}, error),
	pair func(x, y interface{}) (interface{}, error),
) (interface{}, error) {
	return parallel.RangeReduce(0, len(m.splits), 0, func(low, high int) (interface{}, error) {
		if low >= high {
			return nil, nil
		}
		result, err := m.splits[low].splitReduce(reduce)
		if err != nil {
			return nil, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitReduce(reduce); err != nil {
				return nil, err
			} else if result, err = pair(result, r); err != nil {
				return nil, err
			}
		}
		return result, nil
	}, pair)
}

// SpeculativeReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// SpeculativeReduce returns either when all goroutines it spawns have
// terminated, or when one or more reduce or pair functions return
// a non-nil error value. In the latter case, SpeculativeReduce returns
// nil and the left-most of these error values, without waiting for the
// other goroutines to terminate.
//
// While reduce is executed on a split of m,
// SpeculativeReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeReduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeReduce may
// reflect any mapping for that key from any point during the SpeculativeReduce
// call.
func (m *Map) SpeculativeReduce(
	reduce func(map[interface{}]interface{}) (interface{}, error),
	pair func(x, y interface{}) (interface{}, error),
) (interface{}, error) {
	return speculative.RangeReduce(0, len(m.splits), 0, func(low, high int) (interface{}, error) {
		if low >= high {
			return nil, nil
		}
		result, err := m.splits[low].splitReduce(reduce)
		if err != nil {
			return nil, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitReduce(reduce); err != nil {
				return nil, err
			} else if result, err = pair(result, r); err != nil {
				return nil, err
			}
		}
		return result, nil
	}, pair)
}

func (split *Split) splitIntReduce(reduce func(map[interface{}]interface{}) (int, error)) (int, error) {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// IntReduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// If either reduce or pair also return a non-nil error value,
// IntReduce immediately returns 0 and that error value.
//
// While reduce is executed on a split of m,
// IntReduce holds the corresponding lock.
//
// IntReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, IntReduce may
// reflect any mapping for that key from any point during the IntReduce
// call.
func (m *Map) IntReduce(
	reduce func(map[interface{}]interface{}) (int, error),
	pair func(x, y int) (int, error),
) (int, error) {
	if len(m.splits) == 0 {
		return 0, nil
	}
	result, err := m.splits[0].splitIntReduce(reduce)
	if err != nil {
		return 0, err
	}
	for i := 1; i < len(m.splits); i++ {
		if r, err := m.splits[i].splitIntReduce(reduce); err != nil {
			return 0, err
		} else if result, err = pair(result, r); err != nil {
			return 0, err
		}
	}
	return result, nil
}

// ParallelIntReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelIntReduce returns only when all goroutines it spawns have terminated.
//
// If either reduce or pair also return a non-nil error value,
// ParallelIntReduce returns 0 and the left-most of those error values.
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
func (m *Map) ParallelIntReduce(
	reduce func(map[interface{}]interface{}) (int, error),
	pair func(x, y int) (int, error),
) (int, error) {
	return parallel.IntRangeReduce(0, len(m.splits), 0, func(low, high int) (int, error) {
		if low >= high {
			return 0, nil
		}
		result, err := m.splits[low].splitIntReduce(reduce)
		if err != nil {
			return 0, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitIntReduce(reduce); err != nil {
				return 0, err
			} else if result, err = pair(result, r); err != nil {
				return 0, err
			}
		}
		return result, nil
	}, pair)
}

// SpeculativeIntReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// SpeculativeIntReduce returns either when all goroutines it spawns have
// terminated, or when one or more reduce or pair functions return
// a non-nil error value. In the latter case, SpeculativeIntReduce returns
// 0 and the left-most of these error values, without waiting for the
// other goroutines to terminate.
//
// While reduce is executed on a split of m,
// SpeculativeIntReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeIntReduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeIntReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeIntReduce may
// reflect any mapping for that key from any point during the SpeculativeIntReduce
// call.
func (m *Map) SpeculativeIntReduce(
	reduce func(map[interface{}]interface{}) (int, error),
	pair func(x, y int) (int, error),
) (int, error) {
	return speculative.IntRangeReduce(0, len(m.splits), 0, func(low, high int) (int, error) {
		if low >= high {
			return 0, nil
		}
		result, err := m.splits[low].splitIntReduce(reduce)
		if err != nil {
			return 0, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitIntReduce(reduce); err != nil {
				return 0, err
			} else if result, err = pair(result, r); err != nil {
				return 0, err
			}
		}
		return result, nil
	}, pair)
}

func (split *Split) splitFloat64Reduce(reduce func(map[interface{}]interface{}) (float64, error)) (float64, error) {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// Float64Reduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// If either reduce or pair also return a non-nil error value,
// Float64Reduce immediately returns 0 and that error value.
//
// While reduce is executed on a split of m,
// Float64Reduce holds the corresponding lock.
//
// Float64Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, Float64Reduce may
// reflect any mapping for that key from any point during the Float64Reduce
// call.
func (m *Map) Float64Reduce(
	reduce func(map[interface{}]interface{}) (float64, error),
	pair func(x, y float64) (float64, error),
) (float64, error) {
	if len(m.splits) == 0 {
		return 0, nil
	}
	result, err := m.splits[0].splitFloat64Reduce(reduce)
	if err != nil {
		return 0, err
	}
	for i := 1; i < len(m.splits); i++ {
		if r, err := m.splits[i].splitFloat64Reduce(reduce); err != nil {
			return 0, err
		} else if result, err = pair(result, r); err != nil {
			return 0, err
		}
	}
	return result, nil
}

// ParallelFloat64Reduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelFloat64Reduce returns only when all goroutines it spawns have terminated.
//
// If either reduce or pair also return a non-nil error value,
// ParallelFloat64Reduce returns 0 and the left-most of those error values.
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
func (m *Map) ParallelFloat64Reduce(
	reduce func(map[interface{}]interface{}) (float64, error),
	pair func(x, y float64) (float64, error),
) (float64, error) {
	return parallel.Float64RangeReduce(0, len(m.splits), 0, func(low, high int) (float64, error) {
		if low >= high {
			return 0, nil
		}
		result, err := m.splits[low].splitFloat64Reduce(reduce)
		if err != nil {
			return 0, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitFloat64Reduce(reduce); err != nil {
				return 0, err
			} else if result, err = pair(result, r); err != nil {
				return 0, err
			}
		}
		return result, nil
	}, pair)
}

// SpeculativeFloat64Reduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// SpeculativeFloat64Reduce returns either when all goroutines it spawns have
// terminated, or when one or more reduce or pair functions return
// a non-nil error value. In the latter case, SpeculativeFloat64Reduce returns
// 0 and the left-most of these error values, without waiting for the
// other goroutines to terminate.
//
// While reduce is executed on a split of m,
// SpeculativeFloat64Reduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeFloat64Reduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeFloat64Reduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeFloat64Reduce may
// reflect any mapping for that key from any point during the SpeculativeFloat64Reduce
// call.
func (m *Map) SpeculativeFloat64Reduce(
	reduce func(map[interface{}]interface{}) (float64, error),
	pair func(x, y float64) (float64, error),
) (float64, error) {
	return speculative.Float64RangeReduce(0, len(m.splits), 0, func(low, high int) (float64, error) {
		if low >= high {
			return 0, nil
		}
		result, err := m.splits[low].splitFloat64Reduce(reduce)
		if err != nil {
			return 0, err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitFloat64Reduce(reduce); err != nil {
				return 0, err
			} else if result, err = pair(result, r); err != nil {
				return 0, err
			}
		}
		return result, nil
	}, pair)
}

func (split *Split) splitStringReduce(reduce func(map[interface{}]interface{}) (string, error)) (string, error) {
	split.Lock()
	defer split.Unlock()
	return reduce(split.Map)
}

// StringReduce calls reduce for every split of m sequentially. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// If either reduce or pair also return a non-nil error value,
// StringReduce immediately returns 0 and that error value.
//
// While reduce is executed on a split of m,
// StringReduce holds the corresponding lock.
//
// StringReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, StringReduce may
// reflect any mapping for that key from any point during the StringReduce
// call.
func (m *Map) StringReduce(
	reduce func(map[interface{}]interface{}) (string, error),
	pair func(x, y string) (string, error),
) (string, error) {
	if len(m.splits) == 0 {
		return "", nil
	}
	result, err := m.splits[0].splitStringReduce(reduce)
	if err != nil {
		return "", err
	}
	for i := 1; i < len(m.splits); i++ {
		if r, err := m.splits[i].splitStringReduce(reduce); err != nil {
			return "", err
		} else if result, err = pair(result, r); err != nil {
			return "", err
		}
	}
	return result, nil
}

// ParallelStringReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// ParallelStringReduce returns only when all goroutines it spawns have terminated.
//
// If either reduce or pair also return a non-nil error value,
// ParallelStringReduce returns 0 and the left-most of those error values.
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
func (m *Map) ParallelStringReduce(
	reduce func(map[interface{}]interface{}) (string, error),
	pair func(x, y string) (string, error),
) (string, error) {
	return parallel.StringRangeReduce(0, len(m.splits), 0, func(low, high int) (string, error) {
		if low >= high {
			return "", nil
		}
		result, err := m.splits[low].splitStringReduce(reduce)
		if err != nil {
			return "", err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitStringReduce(reduce); err != nil {
				return "", err
			} else if result, err = pair(result, r); err != nil {
				return "", err
			}
		}
		return result, nil
	}, pair)
}

// SpeculativeStringReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations
// of the pair reducer.
//
// SpeculativeStringReduce returns either when all goroutines it spawns have
// terminated, or when one or more reduce or pair functions return
// a non-nil error value. In the latter case, SpeculativeStringReduce returns
// 0 and the left-most of these error values, without waiting for the
// other goroutines to terminate.
//
// While reduce is executed on a split of m,
// SpeculativeStringReduce holds the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeStringReduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeStringReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if
// the value for any key is stored or deleted concurrently, SpeculativeStringReduce may
// reflect any mapping for that key from any point during the SpeculativeStringReduce
// call.
func (m *Map) SpeculativeStringReduce(
	reduce func(map[interface{}]interface{}) (string, error),
	pair func(x, y string) (string, error),
) (string, error) {
	return speculative.StringRangeReduce(0, len(m.splits), 0, func(low, high int) (string, error) {
		if low >= high {
			return "", nil
		}
		result, err := m.splits[low].splitStringReduce(reduce)
		if err != nil {
			return "", err
		}
		for i := low + 1; i < high; i++ {
			if r, err := m.splits[i].splitStringReduce(reduce); err != nil {
				return "", err
			} else if result, err = pair(result, r); err != nil {
				return "", err
			}
		}
		return result, nil
	}, pair)
}
