package sqlite

import (
	"iter"
	"sync"
)

// ThreadSafeMap provides a thread-safe map implementation using generics
type ThreadSafeMap[K comparable, V any] struct {
	m sync.Map
}

// NewThreadSafeMap creates a new thread-safe map
func NewThreadSafeMap[K comparable, V any]() *ThreadSafeMap[K, V] {
	return &ThreadSafeMap[K, V]{}
}

// Store sets the value for a key
func (tm *ThreadSafeMap[K, V]) Store(key K, value V) {
	tm.m.Store(key, value)
}

// Load returns the value stored in the map for a key, or zero value if no value is present
func (tm *ThreadSafeMap[K, V]) Load(key K) (value V, ok bool) {
	v, ok := tm.m.Load(key)
	if !ok {
		return value, false
	}
	return v.(V), true
}

// LoadOrStore returns the existing value for the key if present
// Otherwise, it stores and returns the given value
func (tm *ThreadSafeMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, loaded := tm.m.LoadOrStore(key, value)
	return v.(V), loaded
}

// LoadAndDelete deletes the value for a key, returning the previous value if any
func (tm *ThreadSafeMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, loaded := tm.m.LoadAndDelete(key)
	if !loaded {
		return value, false
	}
	return v.(V), true
}

// Delete deletes the value for a key
func (tm *ThreadSafeMap[K, V]) Delete(key K) {
	tm.m.Delete(key)
}

// Swap swaps the value for a key and returns the previous value if any
func (tm *ThreadSafeMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	v, loaded := tm.m.Swap(key, value)
	if !loaded {
		return previous, false
	}
	return v.(V), true
}

// CompareAndSwap swaps the old and new values for key if the value stored in the map is equal to old
func (tm *ThreadSafeMap[K, V]) CompareAndSwap(key K, old, new V) bool {
	return tm.m.CompareAndSwap(key, old, new)
}

// CompareAndDelete deletes the entry for key if its value is equal to old
func (tm *ThreadSafeMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	return tm.m.CompareAndDelete(key, old)
}

// Iter returns an iterator over key-value pairs in the map
func (tm *ThreadSafeMap[K, V]) Iter() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		tm.m.Range(func(k, v any) bool {
			return yield(k.(K), v.(V))
		})
	}
}

// Keys returns an iterator over keys in the map
func (tm *ThreadSafeMap[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		tm.m.Range(func(k, v any) bool {
			return yield(k.(K))
		})
	}
}

// Values returns an iterator over values in the map
func (tm *ThreadSafeMap[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		tm.m.Range(func(k, v any) bool {
			return yield(v.(V))
		})
	}
}

// Len returns the number of elements in the map
// Note: This is an O(n) operation as it needs to iterate through all elements
func (tm *ThreadSafeMap[K, V]) Len() int {
	count := 0
	tm.m.Range(func(k, v any) bool {
		count++
		return true
	})
	return count
}

// Clear removes all entries from the map
func (tm *ThreadSafeMap[K, V]) Clear() {
	tm.m.Clear()
}
