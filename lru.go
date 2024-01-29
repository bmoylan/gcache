package gcache

import (
	"container/list"
	"time"
)

// Discards the least recently used items first.
type LRUCache[K comparable, V any, S any] struct {
	baseCache[K, V, S]
	items     map[K]*list.Element
	evictList *list.List
}

func newLRUCache[K comparable, V any, S any](cb *CacheBuilder[K, V, S]) *LRUCache[K, V, S] {
	c := &LRUCache[K, V, S]{baseCache: cb.newBaseCache()}
	c.init()
	c.loadGroup.cache = c
	return c
}

func (c *LRUCache[K, V, S]) init() {
	c.evictList = list.New()
	c.items = make(map[K]*list.Element, c.size+1)
}

func (c *LRUCache[K, V, S]) set(key K, value V) (*lruItem[K, S], error) {
	serializedValue, err := c.serializeValue(key, value)
	if err != nil {
		return nil, err
	}

	// Check for existing item
	var item *lruItem[K, S]
	if it, ok := c.items[key]; ok {
		c.evictList.MoveToFront(it)
		item = it.Value.(*lruItem[K, S])
	} else {
		// Verify size not exceeded
		if c.evictList.Len() >= c.size {
			c.evict(1)
		}
		item = &lruItem[K, S]{
			clock: c.clock,
			key:   key,
		}
		c.items[key] = c.evictList.PushFront(item)
	}
	item.value = serializedValue
	item.expiration = c.newExpiration()

	if err := c.addValue(key, value); err != nil {
		return nil, err
	}

	return item, nil
}

// set a new key-value pair
func (c *LRUCache[K, V, S]) Set(key K, value V) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

// Set a new key-value pair with an expiration time
func (c *LRUCache[K, V, S]) SetWithExpire(key K, value V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.set(key, value)
	if err != nil {
		return err
	}

	t := c.clock.Now().Add(expiration)
	item.expiration = &t
	return nil
}

// Get a value from cache pool using key if it exists.
// If it does not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *LRUCache[K, V, S]) Get(key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

// GetIFPresent gets a value from cache pool using key if it exists.
// If it does not exists key, returns KeyNotFoundError.
// And send a request which refresh value for specified key if cache object has LoaderFunc.
func (c *LRUCache[K, V, S]) GetIFPresent(key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, err
}

func (c *LRUCache[K, V, S]) get(key K, onLoad bool) (V, error) {
	v, err := c.getValue(key, onLoad)
	if err != nil {
		return zero[V](), err
	}
	return c.deserializeValue(key, v)
}

func (c *LRUCache[K, V, S]) getValue(key K, onLoad bool) (S, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		it := item.Value.(*lruItem[K, S])
		if !it.IsExpired(nil) {
			c.evictList.MoveToFront(item)
			v := it.value
			c.mu.Unlock()
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.removeElement(item)
	}
	c.mu.Unlock()
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return zero[S](), KeyNotFoundError
}

func (c *LRUCache[K, V, S]) getWithLoader(key K, isWait bool) (V, error) {
	if c.loaderExpireFunc == nil {
		return zero[V](), KeyNotFoundError
	}
	return c.load(key, func(v V, expiration *time.Duration) (V, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		item, err := c.set(key, v)
		if err != nil {
			return zero[V](), err
		}
		if expiration != nil {
			t := c.clock.Now().Add(*expiration)
			item.expiration = &t
		}
		return v, nil
	}, isWait)
}

// evict removes the oldest item from the cache.
func (c *LRUCache[K, V, S]) evict(count int) {
	for i := 0; i < count; i++ {
		ent := c.evictList.Back()
		if ent == nil {
			return
		} else {
			c.removeElement(ent)
		}
	}
}

// Has checks if key exists in cache
func (c *LRUCache[K, V, S]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	return c.has(key, &now)
}

func (c *LRUCache[K, V, S]) has(key K, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.Value.(*lruItem[K, S]).IsExpired(now)
}

// Remove removes the provided key from the cache.
func (c *LRUCache[K, V, S]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *LRUCache[K, V, S]) remove(key K) bool {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

func (c *LRUCache[K, V, S]) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*lruItem[K, S])
	delete(c.items, entry.key)
	if err := c.evictValue(entry.key, entry.value); err != nil {
		// TODO: log error
		_ = err
	}
}

func (c *LRUCache[K, V, S]) keys() []K {
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
func (c *LRUCache[K, V, S]) GetALL(checkExpired bool) map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[K]V, len(c.items))
	now := time.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			value, err := c.deserializeValue(k, item.Value.(*lruItem[K, S]).value)
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
func (c *LRUCache[K, V, S]) Keys(checkExpired bool) []K {
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
func (c *LRUCache[K, V, S]) Len(checkExpired bool) int {
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
func (c *LRUCache[K, V, S]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		it := item.Value.(*lruItem[K, S])
		v := it.value
		err := c.purgeValue(key, v)
		_ = err // TODO: log error
	}

	c.init()
}

type lruItem[K comparable, S any] struct {
	clock      Clock
	key        K
	value      S
	expiration *time.Time
}

// IsExpired returns boolean value whether this item is expired or not.
func (it *lruItem[K, S]) IsExpired(now *time.Time) bool {
	if it.expiration == nil {
		return false
	}
	if now == nil {
		t := it.clock.Now()
		now = &t
	}
	return it.expiration.Before(*now)
}
