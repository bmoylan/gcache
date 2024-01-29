package gcache

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	TYPE_SIMPLE = "simple"
	TYPE_LRU    = "lru"
	TYPE_LFU    = "lfu"
	TYPE_ARC    = "arc"
)

var KeyNotFoundError = errors.New("Key not found.")

type Cache[K comparable, V any] interface {
	// Set inserts or updates the specified key-value pair.
	Set(key K, value V) error
	// SetWithExpire inserts or updates the specified key-value pair with an expiration time.
	SetWithExpire(key K, value V, expiration time.Duration) error
	// Get returns the value for the specified key if it is present in the cache.
	// If the key is not present in the cache and the cache has LoaderFunc,
	// invoke the `LoaderFunc` function and inserts the key-value pair in the cache.
	// If the key is not present in the cache and the cache does not have a LoaderFunc,
	// return KeyNotFoundError.
	Get(key K) (V, error)
	// GetIFPresent returns the value for the specified key if it is present in the cache.
	// Return KeyNotFoundError if the key is not present.
	GetIFPresent(key K) (V, error)
	// GetALL returns a map containing all key-value pairs in the cache.
	GetALL(checkExpired bool) map[K]V
	// get is a private method.
	// If onLoad is true (get is called after loading a miss), a cache hit is not recorded.
	// It expects to be run under lock.
	get(key K, onLoad bool) (V, error)
	// Remove removes the specified key from the cache if the key is present.
	// Returns true if the key was present and the key has been deleted.
	Remove(key K) bool
	// Purge removes all key-value pairs from the cache.
	Purge()
	// Keys returns a slice containing all keys in the cache.
	Keys(checkExpired bool) []K
	// Len returns the number of items in the cache.
	Len(checkExpired bool) int
	// Has returns true if the key exists in the cache.
	Has(key K) bool

	statsAccessor
}

type baseCache[K comparable, V any, S any] struct {
	clock            Clock
	size             int
	loaderExpireFunc LoaderExpireFunc[K, V]
	evictedFunc      EvictedFunc[K, V]
	purgeVisitorFunc PurgeVisitorFunc[K, V]
	addedFunc        AddedFunc[K, V]
	deserializeFunc  DeserializeFunc[K, V, S]
	serializeFunc    SerializeFunc[K, V, S]
	expiration       *time.Duration
	mu               sync.RWMutex
	loadGroup        Group[K, V]
	*stats
}

type (
	LoaderFunc[K comparable, V any]             func(K) (V, error)
	LoaderExpireFunc[K comparable, V any]       func(K) (V, *time.Duration, error)
	EvictedFunc[K comparable, V any]            func(K, V)
	PurgeVisitorFunc[K comparable, V any]       func(K, V)
	AddedFunc[K comparable, V any]              func(K, V)
	DeserializeFunc[K comparable, V any, S any] func(K, S) (V, error)
	SerializeFunc[K comparable, V any, S any]   func(K, V) (S, error)
)

// CacheBuilder is used to build a cache.
// The K type represents the cache key.
// The V type represents the cache value.
// The S type represents the serialized value. If the serialized type is the same as the value type, use CacheBuilder[K, V, V].
type CacheBuilder[K comparable, V any, S any] struct {
	clock            Clock
	tp               string
	size             int
	loaderExpireFunc LoaderExpireFunc[K, V]
	evictedFunc      EvictedFunc[K, V]
	purgeVisitorFunc PurgeVisitorFunc[K, V]
	addedFunc        AddedFunc[K, V]
	expiration       *time.Duration
	deserializeFunc  DeserializeFunc[K, V, S]
	serializeFunc    SerializeFunc[K, V, S]
}

func New[K comparable, V any](size int) *CacheBuilder[K, V, V] {
	return &CacheBuilder[K, V, V]{
		clock:           NewRealClock(),
		tp:              TYPE_SIMPLE,
		size:            size,
		deserializeFunc: func(_ K, v V) (V, error) { return v, nil },
		serializeFunc:   func(_ K, v V) (V, error) { return v, nil },
	}
}

// NewWithSerializedType creates a new cache with serialized type different than the type used in the Cache API.
// The builder's SerializeFunc and DeserializeFunc must be set; otherwise, all operations will error.
func NewWithSerializedType[K comparable, V any, S any](size int) *CacheBuilder[K, V, S] {
	return &CacheBuilder[K, V, S]{
		clock: NewRealClock(),
		tp:    TYPE_SIMPLE,
		size:  size,
	}
}

func (cb *CacheBuilder[K, V, S]) Clock(clock Clock) *CacheBuilder[K, V, S] {
	cb.clock = clock
	return cb
}

// Set a loader function.
// loaderFunc: create a new value with this function if cached value is expired.
func (cb *CacheBuilder[K, V, S]) LoaderFunc(loaderFunc LoaderFunc[K, V]) *CacheBuilder[K, V, S] {
	cb.loaderExpireFunc = func(k K) (V, *time.Duration, error) {
		v, err := loaderFunc(k)
		return v, nil, err
	}
	return cb
}

// Set a loader function with expiration.
// loaderExpireFunc: create a new value with this function if cached value is expired.
// If nil returned instead of time.Duration from loaderExpireFunc than value will never expire.
func (cb *CacheBuilder[K, V, S]) LoaderExpireFunc(loaderExpireFunc LoaderExpireFunc[K, V]) *CacheBuilder[K, V, S] {
	cb.loaderExpireFunc = loaderExpireFunc
	return cb
}

func (cb *CacheBuilder[K, V, S]) EvictType(tp string) *CacheBuilder[K, V, S] {
	cb.tp = tp
	return cb
}

func (cb *CacheBuilder[K, V, S]) Simple() *CacheBuilder[K, V, S] {
	return cb.EvictType(TYPE_SIMPLE)
}

func (cb *CacheBuilder[K, V, S]) LRU() *CacheBuilder[K, V, S] {
	return cb.EvictType(TYPE_LRU)
}

func (cb *CacheBuilder[K, V, S]) LFU() *CacheBuilder[K, V, S] {
	return cb.EvictType(TYPE_LFU)
}

func (cb *CacheBuilder[K, V, S]) ARC() *CacheBuilder[K, V, S] {
	return cb.EvictType(TYPE_ARC)
}

func (cb *CacheBuilder[K, V, S]) EvictedFunc(evictedFunc EvictedFunc[K, V]) *CacheBuilder[K, V, S] {
	cb.evictedFunc = evictedFunc
	return cb
}

func (cb *CacheBuilder[K, V, S]) PurgeVisitorFunc(purgeVisitorFunc PurgeVisitorFunc[K, V]) *CacheBuilder[K, V, S] {
	cb.purgeVisitorFunc = purgeVisitorFunc
	return cb
}

func (cb *CacheBuilder[K, V, S]) AddedFunc(addedFunc AddedFunc[K, V]) *CacheBuilder[K, V, S] {
	cb.addedFunc = addedFunc
	return cb
}

func (cb *CacheBuilder[K, V, S]) DeserializeFunc(deserializeFunc DeserializeFunc[K, V, S]) *CacheBuilder[K, V, S] {
	cb.deserializeFunc = deserializeFunc
	return cb
}

func (cb *CacheBuilder[K, V, S]) SerializeFunc(serializeFunc SerializeFunc[K, V, S]) *CacheBuilder[K, V, S] {
	cb.serializeFunc = serializeFunc
	return cb
}

func (cb *CacheBuilder[K, V, S]) Expiration(expiration time.Duration) *CacheBuilder[K, V, S] {
	cb.expiration = &expiration
	return cb
}

func (cb *CacheBuilder[K, V, S]) Build() Cache[K, V] {
	if cb.size <= 0 && cb.tp != TYPE_SIMPLE {
		panic("gcache: Cache size <= 0")
	}

	return cb.build()
}

func (cb *CacheBuilder[K, V, S]) build() Cache[K, V] {
	switch cb.tp {
	case TYPE_SIMPLE:
		return newSimpleCache[K, V](cb)
	case TYPE_LRU:
		return newLRUCache[K, V](cb)
	case TYPE_LFU:
		return newLFUCache[K, V](cb)
	case TYPE_ARC:
		return newARC[K, V](cb)
	default:
		panic("gcache: Unknown type " + cb.tp)
	}
}

func (cb *CacheBuilder[K, V, S]) newBaseCache() baseCache[K, V, S] {
	return baseCache[K, V, S]{
		clock:            cb.clock,
		size:             cb.size,
		loaderExpireFunc: cb.loaderExpireFunc,
		evictedFunc:      cb.evictedFunc,
		purgeVisitorFunc: cb.purgeVisitorFunc,
		addedFunc:        cb.addedFunc,
		deserializeFunc:  cb.deserializeFunc,
		serializeFunc:    cb.serializeFunc,
		expiration:       cb.expiration,
		stats:            new(stats),
	}
}

// load a new value using by specified key.
func (c *baseCache[K, V, S]) load(key K, cb func(V, *time.Duration, error) (V, error), isWait bool) (V, error) {
	if c.loaderExpireFunc == nil {
		return zero[V](), KeyNotFoundError
	}
	v, _, err := c.loadGroup.Do(key, func() (v V, e error) {
		defer func() {
			if r := recover(); r != nil {
				e = fmt.Errorf("loader panics: %v", r)
			}
		}()
		return cb(c.loaderExpireFunc(key))
	}, isWait)
	return v, err
}

func (c *baseCache[K, V, S]) serializeValue(k K, v V) (_ S, err error) {
	if c.serializeFunc == nil {
		return zero[S](), fmt.Errorf("gcache: serializeFunc is nil")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("serializer panics: %v", r)
		}
	}()
	return c.serializeFunc(k, v)
}

func (c *baseCache[K, V, S]) deserializeValue(k K, s S) (_ V, err error) {
	if c.deserializeFunc == nil {
		return zero[V](), fmt.Errorf("gcache: deserializeFunc is nil")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("deserializer panics: %v", r)
		}
	}()
	return c.deserializeFunc(k, s)
}

func (c *baseCache[K, V, S]) evictValue(key K, serialized S) (err error) {
	if c.evictedFunc != nil {
		value, err := c.deserializeValue(key, serialized)
		if err != nil {
			return err
		}
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("evicter panics: %v", r)
			}
		}()
		c.evictedFunc(key, value)
	}
	return nil
}

func (c *baseCache[K, V, S]) purgeValue(key K, serialized S) error {
	if c.purgeVisitorFunc != nil {
		value, err := c.deserializeValue(key, serialized)
		if err != nil {
			return err
		}
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("purger panics: %v", r)
			}
		}()
		c.purgeVisitorFunc(key, value)
	}
	return nil
}

func (c *baseCache[K, V, S]) addValue(key K, value V) (err error) {
	if c.addedFunc != nil {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("adder panics: %v", r)
			}
		}()
		c.addedFunc(key, value)
	}
	return nil
}

func (c *baseCache[K, V, S]) newExpiration() *time.Time {
	if c.expiration == nil {
		return nil
	}
	t := c.clock.Now().Add(*c.expiration)
	return &t
}

func zero[T any]() (t T) { return }
