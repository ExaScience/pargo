// Package sync provides synchronization primitives similar to the sync package
// of Go's standard library, however here with a focus on parallel performance
// rather than concurrency. So far, this package only provides support for a
// parallel map that can be used to some extent as a drop-in replacement for the
// concurrent map of the standard library. For other synchronization
// primitivies, such as condition variables, mutual exclusion locks, object
// pools, or atomic memory primitives, please use the standard library.
package sync

import (
	"runtime"
	"sync"

	"github.com/exascience/pargo/internal"

	"github.com/exascience/pargo/parallel"
	"github.com/exascience/pargo/speculative"
)

// A Hasher represents an object that has a hash value, which is needed by Map.
//
// If Go would allow access to the predefined hash functions for Go types, this
// interface would not be needed.
type Hasher interface {
	Hash() uint64
}

// A Split is a partial map that belongs to a larger Map, which can be
// individually locked. Its enclosed map can then be individually accessed
// without blocking accesses to other splits.
type Split struct {
	sync.RWMutex
	Map map[interface{}]interface{}
}

// A Map is a parallel map that consists of several split maps that can be
// individually locked and accessed.
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
// The split must be locked/unlocked properly by user programs to safely access
// its contents. In many cases, it is easier to use one of the high-level
// methods, like Load, LoadOrStore, LoadOrCompute, Delete, DeleteOrStore,
// DeleteOrCompute, and Modify, which implicitly take care of proper locking.
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

// Load returns the value stored in the map for a key, or nil if no value is
// present. The ok result indicates whether value was found in the map.
func (m *Map) Load(key Hasher) (value interface{}, ok bool) {
	split := m.Split(key)
	split.RLock()
	value, ok = split.Map[key]
	split.RUnlock()
	return
}

// LoadOrStore returns the existing value for the key if present. Otherwise, it
// stores and returns the given value. The loaded result is true if the value
// was loaded, false if stored.
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

// LoadOrCompute returns the existing value for the key if present. Otherwise,
// it calls computer, and then stores and returns the computed value. The loaded
// result is true if the value was loaded, false if stored.
//
// The computer function is invoked either zero times or once. While computer is
// executing no locks related to this map are being held.
//
// The computed value may not be stored and returned, since a parallel thread
// may have successfully stored a value for the key in the meantime. In that
// case, the value stored by the parallel thread is returned instead.
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

// DeleteOrStore deletes and returns the existing value for the key if present.
// Otherwise, it stores and returns the given value. The deleted result is true
// if the value was deleted, false if stored.
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

// DeleteOrCompute deletes and returns the existing value for the key if
// present. Otherwise, it calls computer, and then stores and returns the
// computed value. The deleted result is true if the value was deleted, false if
// stored.
//
// The computer function is invoked either zero times or once. While computer is
// executing, a lock is being held on a portion of the map, so the function
// should be brief.
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

// Modify looks up a value for the key if present and passes it to the modifier.
// The ok parameter indicates whether value was found in the map. The
// replacement returned by the modifier is then stored as a value for key in the
// map if storeNotDelete is true, otherwise the value is deleted from the map.
// Modify returns the same results as modifier.
//
// The modifier is invoked exactly once. While modifier is executing, a lock is
// being held on a portion of the map, so the function should be brief.
//
// This is the most general modification function for parallel maps. Other
// functions that modify the map are potentially more efficient, so it is better
// to be more specific if possible.
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

func (split *Split) srange(f func(key, value interface{}) bool) bool {
	split.Lock()
	defer split.Unlock()
	for key, value := range split.Map {
		if !f(key, value) {
			return false
		}
	}
	return true
}

// Range calls f sequentially for each key and value present in the map. If f
// returns false, Range stops the iteration.
//
// While iterating through a split of m, Range holds the corresponding lock.
//
// Range does not necessarily correspond to any consistent snapshot of the Map's
// contents: no key will be visited more than once, but if the value for any key
// is stored or deleted concurrently, Range may reflect any mapping for that key
// from any point during the Range call.
func (m *Map) Range(f func(key, value interface{}) bool) {
	splits := m.splits
	for i := 0; i < len(splits); i++ {
		if !splits[i].srange(f) {
			return
		}
	}
}

func (split *Split) parallelRange(f func(key, value interface{})) {
	split.Lock()
	defer split.Unlock()
	for key, value := range split.Map {
		f(key, value)
	}
}

// ParallelRange calls f in parallel for each key and value present in the map.
//
// While iterating through a split of m, ParallelRange holds the corresponding
// lock.
//
// ParallelRange does not necessarily correspond to any consistent snapshot of
// the Map's contents: no key will be visited more than once, but if the value
// for any key is stored or deleted concurrently, ParallelRange may reflect any
// mapping for that key from any point during the Range call.
func (m *Map) ParallelRange(f func(key, value interface{})) {
	splits := m.splits
	parallel.Range(0, len(splits), 0, func(low, high int) {
		for i := low; i < high; i++ {
			splits[i].parallelRange(f)
		}
	})
}

// SpeculativeRange calls f in parallel for each key and value present in the
// map. If f returns false, SpeculativeRange stops the iteration, and returns
// without waiting for the other goroutines that it invoked to terminate.
//
// While iterating through a split of m, SpeculativeRange holds the
// corresponding lock.
//
// SpeculativeRange is useful as an alternative to ParallelRange in cases where
// ParallelRange tends to use computational resources for too long when false is
// a common and/or early return value for f. On the other hand, SpeculativeRange
// adds overhead, so for cases where false is an uncommon and/or late return
// value for f, it may be more efficient to use ParallelRange.
//
// SpeculativeRange does not necessarily correspond to any consistent snapshot
// of the Map's contents: no key will be visited more than once, but if the
// value for any key is stored or deleted concurrently, SpeculativeRange may
// reflect any mapping for that key from any point during the Range call.
func (m *Map) SpeculativeRange(f func(key, value interface{}) bool) {
	splits := m.splits
	speculative.Range(0, len(splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if !splits[i].srange(f) {
				return false
			}
		}
		return true
	})
}

func (split *Split) predicate(p func(map[interface{}]interface{}) bool) bool {
	split.Lock()
	defer split.Unlock()
	return p(split.Map)
}

// And calls predicate for every split of m sequentially. If any predicate
// invocation returns false, And immediately terminates and also returns false.
// Otherwise, And returns true.
//
// While predicate is executed on a split of m, And holds the corresponding
// lock.
//
// And does not necessarily correspond to any consistent snapshot of the Map's
// contents: no split will be visited more than once, but if the value for any
// key is stored or deleted concurrently, And may reflect any mapping for that
// key from any point during the And call.
func (m *Map) And(predicate func(map[interface{}]interface{}) bool) bool {
	splits := m.splits
	for i := 0; i < len(splits); i++ {
		if ok := splits[i].predicate(predicate); !ok {
			return false
		}
	}
	return true
}

// ParallelAnd calls predicate for every split of m in parallel. The results of
// the predicate invocations are then combined with the && operator.
//
// ParallelAnd returns only when all goroutines it spawns have terminated.
//
// While predicate is executed on a split of m, ParallelAnd holds the
// corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and ParallelAnd eventually panics with the left-most
// recovered panic value.
//
// ParallelAnd does not necessarily correspond to any consistent snapshot of the
// Map's contents: no split will be visited more than once, but if the value for
// any key is stored or deleted concurrently, ParallelAnd may reflect any
// mapping for that key from any point during the ParallelAnd call.
func (m *Map) ParallelAnd(predicate func(map[interface{}]interface{}) bool) bool {
	splits := m.splits
	return parallel.RangeAnd(0, len(splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if ok := splits[i].predicate(predicate); !ok {
				return false
			}
		}
		return true
	})
}

// SpeculativeAnd calls predicate for every split of m in parallel.
// SpeculativeAnd returns true if all predicate invocations return true; or
// SpeculativeAnd return false when at least one of them returns false, without
// waiting for the other predicates to terminate.
//
// While predicate is executed on a split of m, SpeculativeAnd holds the
// corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeAnd eventually panics with the left-most
// recovered panic value.
//
// SpeculativeAnd does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if the value
// for any key is stored or deleted concurrently, SpeculativeAnd may reflect any
// mapping for that key from any point during the SpeculativeAnd call.
func (m *Map) SpeculativeAnd(predicate func(map[interface{}]interface{}) bool) bool {
	splits := m.splits
	return speculative.RangeAnd(0, len(splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if ok := splits[i].predicate(predicate); !ok {
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
// While predicate is executed on a split of m, Or holds the corresponding lock.
//
// Or does not necessarily correspond to any consistent snapshot of the Map's
// contents: no split will be visited more than once, but if the value for any
// key is stored or deleted concurrently, Or may reflect any mapping for that
// key from any point during the Or call.
func (m *Map) Or(predicate func(map[interface{}]interface{}) bool) bool {
	splits := m.splits
	for i := 0; i < len(splits); i++ {
		if ok := splits[i].predicate(predicate); ok {
			return true
		}
	}
	return false
}

// ParallelOr calls predicate for every split of m in parallel. The results of
// the predicate invocations are then combined with the || operator.
//
// ParallelOr returns only when all goroutines it spawns have terminated.
//
// While predicate is executed on a split of m, ParallelOr holds the
// corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and ParallelAnd eventually panics with the left-most
// recovered panic value.
//
// ParallelOr does not necessarily correspond to any consistent snapshot of the
// Map's contents: no split will be visited more than once, but if the value for
// any key is stored or deleted concurrently, ParallelOr may reflect any mapping
// for that key from any point during the ParallelOr call.
func (m *Map) ParallelOr(predicate func(map[interface{}]interface{}) bool) bool {
	splits := m.splits
	return parallel.RangeOr(0, len(splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if ok := splits[i].predicate(predicate); ok {
				return true
			}
		}
		return false
	})
}

// SpeculativeOr calls predicate for every split of m in parallel. SpeculativeOr
// returns false if all predicate invocations return false; or SpeculativeOr
// return true when at least one of them returns true, without waiting for the
// other predicates to terminate.
//
// While predicate is executed on a split of m, SpeculativeOr holds the
// corresponding lock.
//
// If one or more predicate invocations panic, the corresponding goroutines
// recover the panics, and SpeculativeOr eventually panics with the left-most
// recovered panic value.
//
// SpeculativeOr does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if the value
// for any key is stored or deleted concurrently, SpeculativeOr may reflect any
// mapping for that key from any point during the SpeculativeOr call.
func (m *Map) SpeculativeOr(predicate func(map[interface{}]interface{}) bool) bool {
	splits := m.splits
	return speculative.RangeOr(0, len(splits), 0, func(low, high int) bool {
		for i := low; i < high; i++ {
			if ok := splits[i].predicate(predicate); ok {
				return true
			}
		}
		return false
	})
}

func (split *Split) reduce(r func(map[interface{}]interface{}) interface{}) interface{} {
	split.Lock()
	defer split.Unlock()
	return r(split.Map)
}

// Reduce calls reduce for every split of m sequentially. The results of the
// reduce invocations are then combined by repeated invocations of the join
// function.
//
// While reduce is executed on a split of m, Reduce holds the corresponding
// lock.
//
// Reduce does not necessarily correspond to any consistent snapshot of the
// Map's contents: no split will be visited more than once, but if the value for
// any key is stored or deleted concurrently, Reduce may reflect any mapping for
// that key from any point during the Reduce call.
func (m *Map) Reduce(
	reduce func(map[interface{}]interface{}) interface{},
	join func(x, y interface{}) interface{},
) interface{} {
	splits := m.splits
	// NewMap ensures that len(m.splits) > 0
	result := splits[0].reduce(reduce)
	for i := 1; i < len(splits); i++ {
		result = join(result, splits[i].reduce(reduce))
	}
	return result
}

func (split *Split) reduceFloat64(r func(map[interface{}]interface{}) float64) float64 {
	split.Lock()
	defer split.Unlock()
	return r(split.Map)
}

// ReduceFloat64 calls reduce for every split of m sequentially. The results of
// the reduce invocations are then combined by repeated invocations of the join
// function.
//
// While reduce is executed on a split of m, ReduceFloat64 holds the
// corresponding lock.
//
// ReduceFloat64 does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if the value
// for any key is stored or deleted concurrently, ReduceFloat64 may reflect any
// mapping for that key from any point during the ReduceFloat64 call.
func (m *Map) ReduceFloat64(
	reduce func(map[interface{}]interface{}) float64,
	join func(x, y float64) float64,
) float64 {
	splits := m.splits
	// NewMap ensures that len(m.splits) > 0
	result := splits[0].reduceFloat64(reduce)
	for i := 1; i < len(splits); i++ {
		result = join(result, splits[i].reduceFloat64(reduce))
	}
	return result
}

// ReduceFloat64Sum calls reduce for every split of m sequentially. The results
// of the reduce invocations are then added together.
//
// While reduce is executed on a split of m, ReduceFloat64Sum holds the
// corresponding lock.
//
// ReduceFloat64Sum does not necessarily correspond to any consistent snapshot
// of the Map's contents: no split will be visited more than once, but if the
// value for any key is stored or deleted concurrently, ReduceFloat64Sum may
// reflect any mapping for that key from any point during the ReduceFloat64Sum
// call.
func (m *Map) ReduceFloat64Sum(reduce func(map[interface{}]interface{}) float64) float64 {
	result := float64(0)
	splits := m.splits
	for i := 0; i < len(splits); i++ {
		result += splits[i].reduceFloat64(reduce)
	}
	return result
}

// ReduceFloat64Product calls reduce for every split of m sequentially. The
// results of the reduce invocations are then multiplied with each other.
//
// While reduce is executed on a split of m, ReduceFloat64Product holds the
// corresponding lock.
//
// ReduceFloat64Product does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// ReduceFloat64Product may reflect any mapping for that key from any point
// during the ReduceFloat64Product call.
func (m *Map) ReduceFloat64Product(reduce func(map[interface{}]interface{}) float64) float64 {
	result := float64(1)
	splits := m.splits
	for i := 0; i < len(splits); i++ {
		result *= splits[i].reduceFloat64(reduce)
	}
	return result
}

func (split *Split) reduceInt(r func(map[interface{}]interface{}) int) int {
	split.Lock()
	defer split.Unlock()
	return r(split.Map)
}

// ReduceInt calls reduce for every split of m sequentially. The results of the
// reduce invocations are then combined by repeated invocations of the join
// function.
//
// While reduce is executed on a split of m, ReduceInt holds the corresponding
// lock.
//
// ReduceInt does not necessarily correspond to any consistent snapshot of the
// Map's contents: no split will be visited more than once, but if the value for
// any key is stored or deleted concurrently, ReduceInt may reflect any mapping
// for that key from any point during the ReduceInt call.
func (m *Map) ReduceInt(
	reduce func(map[interface{}]interface{}) int,
	join func(x, y int) int,
) int {
	splits := m.splits
	// NewMap ensures that len(splits) > 0
	result := splits[0].reduceInt(reduce)
	for i := 1; i < len(splits); i++ {
		result = join(result, splits[i].reduceInt(reduce))
	}
	return result
}

// ReduceIntSum calls reduce for every split of m sequentially. The results of
// the reduce invocations are then added together.
//
// While reduce is executed on a split of m, ReduceIntSum holds the
// corresponding lock.
//
// ReduceIntSum does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if the value
// for any key is stored or deleted concurrently, ReduceIntSum may reflect any
// mapping for that key from any point during the ReduceIntSum call.
func (m *Map) ReduceIntSum(reduce func(map[interface{}]interface{}) int) int {
	result := 0
	splits := m.splits
	for i := 0; i < len(splits); i++ {
		result += splits[i].reduceInt(reduce)
	}
	return result
}

// ReduceIntProduct calls reduce for every split of m sequentially. The results
// of the reduce invocations are then multiplied with each other.
//
// While reduce is executed on a split of m, ReduceIntProduct holds the
// corresponding lock.
//
// ReduceIntProduct does not necessarily correspond to any consistent snapshot
// of the Map's contents: no split will be visited more than once, but if the
// value for any key is stored or deleted concurrently, ReduceIntProduct may
// reflect any mapping for that key from any point during the ReduceIntProduct
// call.
func (m *Map) ReduceIntProduct(reduce func(map[interface{}]interface{}) int) int {
	result := 1
	splits := m.splits
	for i := 0; i < len(splits); i++ {
		result *= splits[i].reduceInt(reduce)
	}
	return result
}

func (split *Split) reduceString(r func(map[interface{}]interface{}) string) string {
	split.Lock()
	defer split.Unlock()
	return r(split.Map)
}

// ReduceString calls reduce for every split of m sequentially. The results of
// the reduce invocations are then combined by repeated invocations of the join
// function.
//
// While reduce is executed on a split of m, ReduceString holds the
// corresponding lock.
//
// ReduceString does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if the value
// for any key is stored or deleted concurrently, ReduceString may reflect any
// mapping for that key from any point during the ReduceString call.
func (m *Map) ReduceString(
	reduce func(map[interface{}]interface{}) string,
	join func(x, y string) string,
) string {
	splits := m.splits
	// NewMap ensures that len(splits) > 0
	result := splits[0].reduceString(reduce)
	for i := 1; i < len(splits); i++ {
		result = join(result, splits[i].reduceString(reduce))
	}
	return result
}

// ReduceStringSum calls reduce for every split of m sequentially. The results
// of the reduce invocations are then concatenated with each other.
//
// While reduce is executed on a split of m, ReduceStringSum holds the
// corresponding lock.
//
// ReduceStringSum does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if the value
// for any key is stored or deleted concurrently, ReduceStringSum may reflect
// any mapping for that key from any point during the ReduceStringSum call.
func (m *Map) ReduceStringSum(reduce func(map[interface{}]interface{}) string) string {
	result := ""
	splits := m.splits
	for i := 0; i < len(splits); i++ {
		result += splits[i].reduceString(reduce)
	}
	return result
}

// ParallelReduce calls reduce for every split of m in parallel. The results of
// the reduce invocations are then combined by repeated invocations of the join
// function.
//
// ParallelReduce returns only when all goroutines it spawns have terminated.
//
// While reduce is executed on a split of m, ParallelReduce holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduce eventually panics with the left-most recovered
// panic value.
//
// ParallelReduce does not necessarily correspond to any consistent snapshot of
// the Map's contents: no split will be visited more than once, but if the value
// for any key is stored or deleted concurrently, ParallelReduce may reflect any
// mapping for that key from any point during the ParallelReduce call.
func (m *Map) ParallelReduce(
	reduce func(map[interface{}]interface{}) interface{},
	join func(x, y interface{}) interface{},
) interface{} {
	var recur func(splits []Split) interface{}
	recur = func(splits []Split) interface{} {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduce(reduce)
		}
		var left, right interface{}
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduce(reduce)
			}()
			left = splits[0].reduce(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return join(left, right)
	}
	return recur(m.splits)
}

// ParallelReduceFloat64 calls reduce for every split of m in parallel. The
// results of the reduce invocations are then combined by repeated invocations
// of the join function.
//
// ParallelReduceFloat64 returns only when all goroutines it spawns have
// terminated.
//
// While reduce is executed on a split of m, ParallelReduceFloat64 holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduceFloat64 eventually panics with the left-most
// recovered panic value.
//
// ParallelReduceFloat64 does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// ParallelReduceFloat64 may reflect any mapping for that key from any point
// during the ParallelReduceFloat64 call.
func (m *Map) ParallelReduceFloat64(
	reduce func(map[interface{}]interface{}) float64,
	join func(x, y float64) float64,
) float64 {
	var recur func(splits []Split) float64
	recur = func(splits []Split) float64 {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduceFloat64(reduce)
		}
		var left, right float64
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduceFloat64(reduce)
			}()
			left = splits[0].reduceFloat64(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return join(left, right)
	}
	return recur(m.splits)
}

// ParallelReduceFloat64Sum calls reduce for every split of m in parallel. The
// results of the reduce invocations are then added together.
//
// ParallelReduceFloat64Sum returns only when all goroutines it spawns have
// terminated.
//
// While reduce is executed on a split of m, ParallelReduceFloat64Sum holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduceFloat64Sum eventually panics with the left-most
// recovered panic value.
//
// ParallelReduceFloat64Sum does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// ParallelReduceFloat64Sum may reflect any mapping for that key from any point
// during the ParallelReduceFloat64Sum call.
func (m *Map) ParallelReduceFloat64Sum(reduce func(map[interface{}]interface{}) float64) float64 {
	var recur func(splits []Split) float64
	recur = func(splits []Split) float64 {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduceFloat64(reduce)
		}
		var left, right float64
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduceFloat64(reduce)
			}()
			left = splits[0].reduceFloat64(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return left + right
	}
	return recur(m.splits)
}

// ParallelReduceFloat64Product calls reduce for every split of m in parallel.
// The results of the reduce invocations are then multiplied with each other.
//
// ParallelReduceFloat64Product returns only when all goroutines it spawns have
// terminated.
//
// While reduce is executed on a split of m, ParallelReduceFloat64Product holds
// the corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduceFloat64Product eventually panics with the
// left-most recovered panic value.
//
// ParallelReduceFloat64Product does not necessarily correspond to any
// consistent snapshot of the Map's contents: no split will be visited more than
// once, but if the value for any key is stored or deleted concurrently,
// ParallelReduceFloat64Product may reflect any mapping for that key from any
// point during the ParallelReduceFloat64Product call.
func (m *Map) ParallelReduceFloat64Product(reduce func(map[interface{}]interface{}) float64) float64 {
	var recur func(splits []Split) float64
	recur = func(splits []Split) float64 {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduceFloat64(reduce)
		}
		var left, right float64
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduceFloat64(reduce)
			}()
			left = splits[0].reduceFloat64(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return left * right
	}
	return recur(m.splits)
}

// ParallelReduceInt calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations of the
// join function.
//
// While reduce is executed on a split of m, ParallelReduceInt holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduceInt eventually panics with the left-most
// recovered panic value.
//
// ParallelReduceInt does not necessarily correspond to any consistent snapshot
// of the Map's contents: no split will be visited more than once, but if the
// value for any key is stored or deleted concurrently, ParallelReduceInt may
// reflect any mapping for that key from any point during the ParallelReduceInt
// call.
func (m *Map) ParallelReduceInt(
	reduce func(map[interface{}]interface{}) int,
	join func(x, y int) int,
) int {
	var recur func(splits []Split) int
	recur = func(splits []Split) int {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduceInt(reduce)
		}
		var left, right int
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduceInt(reduce)
			}()
			left = splits[0].reduceInt(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return join(left, right)
	}
	return recur(m.splits)
}

// ParallelReduceIntSum calls reduce for every split of m in parallel. The
// results of the reduce invocations are then added together.
//
// ParallelReduceIntSum returns only when all goroutines it spawns have
// terminated.
//
// While reduce is executed on a split of m, ParallelReduceIntSum holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduceIntSum eventually panics with the left-most
// recovered panic value.
//
// ParallelReduceIntSum does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// ParallelReduceIntSum may reflect any mapping for that key from any point
// during the ParallelReduceIntSum call.
func (m *Map) ParallelReduceIntSum(reduce func(map[interface{}]interface{}) int) int {
	var recur func(splits []Split) int
	recur = func(splits []Split) int {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduceInt(reduce)
		}
		var left, right int
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduceInt(reduce)
			}()
			left = splits[0].reduceInt(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return left + right
	}
	return recur(m.splits)
}

// ParallelReduceIntProduct calls reduce for every split of m in parallel. The
// results of the reduce invocations are then multiplied with each other.
//
// ParallelReduceIntProduct returns only when all goroutines it spawns have
// terminated.
//
// While reduce is executed on a split of m, ParallelReduceIntProduct holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduceIntProduct eventually panics with the left-most
// recovered panic value.
//
// ParallelReduceIntProduct does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// ParallelReduceIntProduct may reflect any mapping for that key from any point
// during the ParallelReduceIntProduct call.
func (m *Map) ParallelReduceIntProduct(reduce func(map[interface{}]interface{}) int) int {
	var recur func(splits []Split) int
	recur = func(splits []Split) int {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduceInt(reduce)
		}
		var left, right int
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduceInt(reduce)
			}()
			left = splits[0].reduceInt(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return left * right
	}
	return recur(m.splits)
}

// ParallelReduceString calls reduce for every split of m in parallel. The
// results of the reduce invocations are then combined by repeated invocations
// of the join function.
//
// ParallelReduceString returns only when all goroutines it spawns have
// terminated.
//
// While reduce is executed on a split of m, ParallelReduceString holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduceString eventually panics with the left-most
// recovered panic value.
//
// ParallelReduceString does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// ParallelReduceString may reflect any mapping for that key from any point
// during the ParallelReduceString call.
func (m *Map) ParallelReduceString(
	reduce func(map[interface{}]interface{}) string,
	join func(x, y string) string,
) string {
	var recur func(splits []Split) string
	recur = func(splits []Split) string {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduceString(reduce)
		}
		var left, right string
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduceString(reduce)
			}()
			left = splits[0].reduceString(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return join(left, right)
	}
	return recur(m.splits)
}

// ParallelReduceStringSum calls reduce for every split of m in parallel. The
// results of the reduce invocations are then concatenated together.
//
// ParallelReduceStringSum returns only when all goroutines it spawns have
// terminated.
//
// While reduce is executed on a split of m, ParallelReduceStringSum holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and ParallelReduceStringSum eventually panics with the left-most
// recovered panic value.
//
// ParallelReduceStringSum does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// ParallelReduceStringSum may reflect any mapping for that key from any point
// during the ParallelReduceStringSum call.
func (m *Map) ParallelReduceStringSum(reduce func(map[interface{}]interface{}) string) string {
	var recur func(splits []Split) string
	recur = func(splits []Split) string {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].reduceString(reduce)
		}
		var left, right string
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = splits[1].reduceString(reduce)
			}()
			left = splits[0].reduceString(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(splits[half:])
			}()
			left = recur(splits[:half])
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		return left + right
	}
	return recur(m.splits)
}

func (split *Split) speculativeReduce(r func(map[interface{}]interface{}) (interface{}, bool)) (interface{}, bool) {
	split.Lock()
	defer split.Unlock()
	return r(split.Map)
}

// SpeculativeReduce calls reduce for every split of m in parallel. The results
// of the reduce invocations are then combined by repeated invocations of the
// join function.
//
// SpeculativeReduce returns either when all goroutines it spawns have
// terminated with a second return value of false; or when one or more reduce or
// join functions return a second return value of true. In the latter case, the
// first return value of the left-most function that returned true as a second
// return value becomes the final result, without waiting for the other
// functions to terminate.
//
// While reduce is executed on a split of m, SpeculativeReduce holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and SpeculativeReduce eventually panics with the left-most
// recovered panic value.
//
// SpeculativeReduce does not necessarily correspond to any consistent snapshot
// of the Map's contents: no split will be visited more than once, but if the
// value for any key is stored or deleted concurrently, SpeculativeReduce may
// reflect any mapping for that key from any point during the SpeculativeReduce
// call.
func (m *Map) SpeculativeReduce(
	reduce func(map[interface{}]interface{}) (interface{}, bool),
	join func(x, y interface{}) (interface{}, bool),
) (interface{}, bool) {
	var recur func(splits []Split) (interface{}, bool)
	recur = func(splits []Split) (interface{}, bool) {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].speculativeReduce(reduce)
		}
		var left, right interface{}
		var b0, b1 bool
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = splits[1].speculativeReduce(reduce)
			}()
			left, b0 = splits[0].speculativeReduce(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(splits[half:])
			}()
			left, b0 = recur(splits[:half])
		}
		if b0 {
			return left, true
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		if b1 {
			return right, true
		}
		return join(left, right)
	}
	return recur(m.splits)
}

func (split *Split) speculativeReduceFloat64(r func(map[interface{}]interface{}) (float64, bool)) (float64, bool) {
	split.Lock()
	defer split.Unlock()
	return r(split.Map)
}

// SpeculativeReduceFloat64 calls reduce for every split of m in parallel. The
// results of the reduce invocations are then combined by repeated invocations
// of the join function.
//
// SpeculativeReduceFloat64 returns either when all goroutines it spawns have
// terminated with a second return value of false; or when one or more reduce or
// join functions return a second return value of true. In the latter case, the
// first return value of the left-most function that returned true as a second
// return value becomes the final result, without waiting for the other
// functions to terminate.
//
// While reduce is executed on a split of m, SpeculativeReduceFloat64 holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and SpeculativeReduceFloat64 eventually panics with the left-most
// recovered panic value.
//
// SpeculativeReduceFloat64 does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// SpeculativeReduceFloat64 may reflect any mapping for that key from any point
// during the SpeculativeReduceFloat64 call.
func (m *Map) SpeculativeReduceFloat64(
	reduce func(map[interface{}]interface{}) (float64, bool),
	join func(x, y float64) (float64, bool),
) (float64, bool) {
	var recur func(splits []Split) (float64, bool)
	recur = func(splits []Split) (float64, bool) {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].speculativeReduceFloat64(reduce)
		}
		var left, right float64
		var b0, b1 bool
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = splits[1].speculativeReduceFloat64(reduce)
			}()
			left, b0 = splits[0].speculativeReduceFloat64(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(splits[half:])
			}()
			left, b0 = recur(splits[:half])
		}
		if b0 {
			return left, true
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		if b1 {
			return right, true
		}
		return join(left, right)
	}
	return recur(m.splits)
}

func (split *Split) speculativeReduceInt(r func(map[interface{}]interface{}) (int, bool)) (int, bool) {
	split.Lock()
	defer split.Unlock()
	return r(split.Map)
}

// SpeculativeReduceInt calls reduce for every split of m in parallel. The
// results of the reduce invocations are then combined by repeated invocations
// of the join function.
//
// SpeculativeReduceInt returns either when all goroutines it spawns have
// terminated with a second return value of false; or when one or more reduce or
// join functions return a second return value of true. In the latter case, the
// first return value of the left-most function that returned true as a second
// return value becomes the final result, without waiting for the other
// functions to terminate.
//
// While reduce is executed on a split of m, SpeculativeReduceInt holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and SpeculativeReduceInt eventually panics with the left-most
// recovered panic value.
//
// SpeculativeReduceInt does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// SpeculativeReduceInt may reflect any mapping for that key from any point
// during the SpeculativeReduceInt call.
func (m *Map) SpeculativeReduceInt(
	reduce func(map[interface{}]interface{}) (int, bool),
	join func(x, y int) (int, bool),
) (int, bool) {
	var recur func(splits []Split) (int, bool)
	recur = func(splits []Split) (int, bool) {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].speculativeReduceInt(reduce)
		}
		var left, right int
		var b0, b1 bool
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = splits[1].speculativeReduceInt(reduce)
			}()
			left, b0 = splits[0].speculativeReduceInt(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(splits[half:])
			}()
			left, b0 = recur(splits[:half])
		}
		if b0 {
			return left, true
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		if b1 {
			return right, true
		}
		return join(left, right)
	}
	return recur(m.splits)
}

func (split *Split) speculativeReduceString(r func(map[interface{}]interface{}) (string, bool)) (string, bool) {
	split.Lock()
	defer split.Unlock()
	return r(split.Map)
}

// SpeculativeReduceString calls reduce for every split of m in parallel. The
// results of the reduce invocations are then combined by repeated invocations
// of the join function.
//
// SpeculativeReduceString returns either when all goroutines it spawns have
// terminated with a second return value of false; or when one or more reduce or
// join functions return a second return value of true. In the latter case, the
// first return value of the left-most function that returned true as a second
// return value becomes the final result, without waiting for the other
// functions to terminate.
//
// While reduce is executed on a split of m, SpeculativeReduceString holds the
// corresponding lock.
//
// If one or more reduce invocations panic, the corresponding goroutines recover
// the panics, and SpeculativeReduceString eventually panics with the left-most
// recovered panic value.
//
// SpeculativeReduceString does not necessarily correspond to any consistent
// snapshot of the Map's contents: no split will be visited more than once, but
// if the value for any key is stored or deleted concurrently,
// SpeculativeReduceString may reflect any mapping for that key from any point
// during the SpeculativeReduceString call.
func (m *Map) SpeculativeReduceString(
	reduce func(map[interface{}]interface{}) (string, bool),
	join func(x, y string) (string, bool),
) (string, bool) {
	var recur func(splits []Split) (string, bool)
	recur = func(splits []Split) (string, bool) {
		if len(splits) < 2 {
			// NewMap and case 2 below ensure that len(splits) > 0
			return splits[0].speculativeReduceString(reduce)
		}
		var left, right string
		var b0, b1 bool
		var p interface{}
		var wg sync.WaitGroup
		wg.Add(1)
		switch len(m.splits) {
		case 2:
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = splits[1].speculativeReduceString(reduce)
			}()
			left, b0 = splits[0].speculativeReduceString(reduce)
		default:
			half := len(m.splits) / 2
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(splits[half:])
			}()
			left, b0 = recur(splits[:half])
		}
		if b0 {
			return left, true
		}
		wg.Wait()
		if p != nil {
			panic(p)
		}
		if b1 {
			return right, true
		}
		return join(left, right)
	}
	return recur(m.splits)
}
