package gcache

import (
	"time"
)

// SimpleCache has no clear priority for evict cache. It depends on key-value map order.
type SimpleCache[K comparable, V any, S any] struct {
	baseCache[K, V, S]
	items map[K]*simpleItem[S]
}

func newSimpleCache[K comparable, V any, S any](cb *CacheBuilder[K, V, S]) *SimpleCache[K, V, S] {
	c := &SimpleCache[K, V, S]{baseCache: cb.newBaseCache()}
	c.init()
	c.loadGroup.cache = c
	return c
}

func (c *SimpleCache[K, V, S]) init() {
	if c.size <= 0 {
		c.items = make(map[K]*simpleItem[S])
	} else {
		c.items = make(map[K]*simpleItem[S], c.size)
	}
}

// Set a new key-value pair
func (c *SimpleCache[K, V, S]) Set(key K, value V) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

// Set a new key-value pair with an expiration time
func (c *SimpleCache[K, V, S]) SetWithExpire(key K, value V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.set(key, value)
	if err != nil {
		return err
	}

	t := c.clock.Now().Add(expiration)
	item.(*simpleItem[V]).expiration = &t
	return nil
}

func (c *SimpleCache[K, V, S]) set(key K, value V) (any, error) {
	serializedValue, err := c.serializeValue(key, value)
	if err != nil {
		return nil, err
	}

	// Check for existing item
	item, ok := c.items[key]
	if !ok {
		// Verify size not exceeded
		if (len(c.items) >= c.size) && c.size > 0 {
			c.evict(1)
		}
		item = &simpleItem[S]{
			clock: c.clock,
		}
		c.items[key] = item
	}
	item.value = serializedValue
	item.expiration = c.newExpiration()

	if err := c.addValue(key, value); err != nil {
		return nil, err
	}

	return item, nil
}

// Get a value from cache pool using key if it exists.
// If it does not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *SimpleCache[K, V, S]) Get(key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

// GetIFPresent gets a value from cache pool using key if it exists.
// If it does not exists key, returns KeyNotFoundError.
// And send a request which refresh value for specified key if cache object has LoaderFunc.
func (c *SimpleCache[K, V, S]) GetIFPresent(key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, nil
}

func (c *SimpleCache[K, V, S]) get(key K, onLoad bool) (V, error) {
	v, err := c.getValue(key, onLoad)
	if err != nil {
		return zero[V](), err
	}
	return c.deserializeValue(key, v)
}

func (c *SimpleCache[K, V, S]) getValue(key K, onLoad bool) (S, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(nil) {
			v := item.value
			c.mu.Unlock()
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.remove(key)
	}
	c.mu.Unlock()
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return zero[S](), KeyNotFoundError
}

func (c *SimpleCache[K, V, S]) getWithLoader(key K, isWait bool) (V, error) {
	if c.loaderExpireFunc == nil {
		return zero[V](), KeyNotFoundError
	}
	return c.load(key, func(v V, expiration *time.Duration, e error) (V, error) {
		if e != nil {
			return zero[V](), e
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		item, err := c.set(key, v)
		if err != nil {
			return zero[V](), err
		}
		if expiration != nil {
			t := c.clock.Now().Add(*expiration)
			item.(*simpleItem[V]).expiration = &t
		}
		return v, nil
	}, isWait)
}

func (c *SimpleCache[K, V, S]) evict(count int) {
	now := c.clock.Now()
	current := 0
	for key, item := range c.items {
		if current >= count {
			return
		}
		if item.expiration == nil || now.After(*item.expiration) {
			defer c.remove(key)
			current++
		}
	}
}

// Has checks if key exists in cache
func (c *SimpleCache[K, V, S]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	return c.has(key, &now)
}

func (c *SimpleCache[K, V, S]) has(key K, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now)
}

// Remove removes the provided key from the cache.
func (c *SimpleCache[K, V, S]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *SimpleCache[K, V, S]) remove(key K) bool {
	item, ok := c.items[key]
	if ok {
		delete(c.items, key)

		if err := c.evictValue(key, item.value); err != nil {
			// TODO: log error
			_ = err
		}
		return true
	}
	return false
}

// Returns a slice of the keys in the cache.
func (c *SimpleCache[K, V, S]) keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, len(c.items))
	var i = 0
	for k := range c.items {
		keys[i] = k
		i++
	}
	return keys
}

// GetALL returns all key-value pairs in the cache.
func (c *SimpleCache[K, V, S]) GetALL(checkExpired bool) map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[K]V, len(c.items))
	now := time.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			value, err := c.deserializeValue(k, item.value)
			if err != nil {
				// TODO: log error
				continue
			}
			items[k] = value
		}
	}
	return items
}

// Keys returns a slice of the keys in the cache.
func (c *SimpleCache[K, V, S]) Keys(checkExpired bool) []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.items))
	now := time.Now()
	for k := range c.items {
		if !checkExpired || c.has(k, &now) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *SimpleCache[K, V, S]) Len(checkExpired bool) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !checkExpired {
		return len(c.items)
	}
	var length int
	now := time.Now()
	for k := range c.items {
		if c.has(k, &now) {
			length++
		}
	}
	return length
}

// Completely clear the cache
func (c *SimpleCache[K, V, S]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		err := c.purgeValue(key, item.value)
		_ = err // TODO: log error
	}
	c.init()
}

type simpleItem[V any] struct {
	clock      Clock
	value      V
	expiration *time.Time
}

// IsExpired returns boolean value whether this item is expired or not.
func (si *simpleItem[V]) IsExpired(now *time.Time) bool {
	if si.expiration == nil {
		return false
	}
	if now == nil {
		t := si.clock.Now()
		now = &t
	}
	return si.expiration.Before(*now)
}
