package gcache

import (
	"container/list"
	"time"
)

// Discards the least frequently used items first.
type LFUCache[K comparable, V any, S any] struct {
	baseCache[K, V, S]
	items    map[K]*lfuItem[K, S]
	freqList *list.List // list for freqEntry
}

var _ Cache[string, any] = (*LFUCache[string, any, any])(nil)

type lfuItem[K comparable, S any] struct {
	clock       Clock
	key         K
	value       S
	freqElement *list.Element
	expiration  *time.Time
}

type freqEntry[K comparable, S any] struct {
	freq  uint
	items map[*lfuItem[K, S]]struct{}
}

func newLFUCache[K comparable, V any, S any](cb *CacheBuilder[K, V, S]) *LFUCache[K, V, S] {
	c := &LFUCache[K, V, S]{baseCache: cb.newBaseCache()}
	c.init()
	c.loadGroup.cache = c
	return c
}

func (c *LFUCache[K, V, S]) init() {
	c.freqList = list.New()
	c.items = make(map[K]*lfuItem[K, S], c.size)
	c.freqList.PushFront(&freqEntry[K, S]{
		freq:  0,
		items: make(map[*lfuItem[K, S]]struct{}),
	})
}

// Set a new key-value pair
func (c *LFUCache[K, V, S]) Set(key K, value V) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

// Set a new key-value pair with an expiration time
func (c *LFUCache[K, V, S]) SetWithExpire(key K, value V, expiration time.Duration) error {
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

func (c *LFUCache[K, V, S]) set(key K, value V) (*lfuItem[K, S], error) {
	serializedValue, err := c.serializeValue(key, value)
	if err != nil {
		return nil, err
	}

	// Check for existing item
	item, ok := c.items[key]
	if ok {
		item.value = serializedValue
	} else {
		// Verify size not exceeded
		if len(c.items) >= c.size {
			c.evict(1)
		}
		item = &lfuItem[K, S]{
			clock:       c.clock,
			key:         key,
			value:       serializedValue,
			freqElement: nil,
		}
		el := c.freqList.Front()
		fe := el.Value.(*freqEntry[K, S])
		fe.items[item] = struct{}{}

		item.freqElement = el
		c.items[key] = item
	}

	if c.expiration != nil {
		t := c.clock.Now().Add(*c.expiration)
		item.expiration = &t
	}

	if err := c.addValue(key, value); err != nil {
		return nil, err
	}

	return item, nil
}

// Get a value from cache pool using key if it exists.
// If it does not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *LFUCache[K, V, S]) Get(key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

// GetIFPresent gets a value from cache pool using key if it exists.
// If it does not exists key, returns KeyNotFoundError.
// And send a request which refresh value for specified key if cache object has LoaderFunc.
func (c *LFUCache[K, V, S]) GetIFPresent(key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, err
}

func (c *LFUCache[K, V, S]) get(key K, onLoad bool) (V, error) {
	v, err := c.getValue(key, onLoad)
	if err != nil {
		return zero[V](), err
	}
	return c.deserializeValue(key, v)
}

func (c *LFUCache[K, V, S]) getValue(key K, onLoad bool) (S, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(nil) {
			c.increment(item)
			v := item.value
			c.mu.Unlock()
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.removeItem(item)
	}
	c.mu.Unlock()
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return zero[S](), KeyNotFoundError
}

func (c *LFUCache[K, V, S]) getWithLoader(key K, isWait bool) (V, error) {
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

func (c *LFUCache[K, V, S]) increment(item *lfuItem[K, S]) {
	currentFreqElement := item.freqElement
	currentFreqEntry := currentFreqElement.Value.(*freqEntry[K, S])
	nextFreq := currentFreqEntry.freq + 1
	delete(currentFreqEntry.items, item)

	// a boolean whether reuse the empty current entry
	removable := isRemovableFreqEntry(currentFreqEntry)

	// insert item into a valid entry
	nextFreqElement := currentFreqElement.Next()
	switch {
	case nextFreqElement == nil || nextFreqElement.Value.(*freqEntry[K, S]).freq > nextFreq:
		if removable {
			currentFreqEntry.freq = nextFreq
			nextFreqElement = currentFreqElement
		} else {
			nextFreqElement = c.freqList.InsertAfter(&freqEntry[K, S]{
				freq:  nextFreq,
				items: make(map[*lfuItem[K, S]]struct{}),
			}, currentFreqElement)
		}
	case nextFreqElement.Value.(*freqEntry[K, S]).freq == nextFreq:
		if removable {
			c.freqList.Remove(currentFreqElement)
		}
	default:
		panic("unreachable")
	}
	nextFreqElement.Value.(*freqEntry[K, S]).items[item] = struct{}{}
	item.freqElement = nextFreqElement
}

// evict removes the least frequence item from the cache.
func (c *LFUCache[K, V, S]) evict(count int) {
	entry := c.freqList.Front()
	for i := 0; i < count; {
		if entry == nil {
			return
		} else {
			for item := range entry.Value.(*freqEntry[K, S]).items {
				if i >= count {
					return
				}
				c.removeItem(item)
				i++
			}
			entry = entry.Next()
		}
	}
}

// Has checks if key exists in cache
func (c *LFUCache[K, V, S]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	return c.has(key, &now)
}

func (c *LFUCache[K, V, S]) has(key K, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now)
}

// Remove removes the provided key from the cache.
func (c *LFUCache[K, V, S]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *LFUCache[K, V, S]) remove(key K) bool {
	if item, ok := c.items[key]; ok {
		c.removeItem(item)
		return true
	}
	return false
}

// removeElement is used to remove a given list element from the cache
func (c *LFUCache[K, V, S]) removeItem(item *lfuItem[K, S]) {
	entry := item.freqElement.Value.(*freqEntry[K, S])
	delete(c.items, item.key)
	delete(entry.items, item)
	if isRemovableFreqEntry(entry) {
		c.freqList.Remove(item.freqElement)
	}

	if err := c.evictValue(item.key, item.value); err != nil {
		// TODO: log error
		_ = err
	}
}

func (c *LFUCache[K, V, S]) keys() []K {
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
func (c *LFUCache[K, V, S]) GetALL(checkExpired bool) map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[K]V, len(c.items))
	now := time.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			value, err := c.deserializeValue(item.key, item.value)
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
func (c *LFUCache[K, V, S]) Keys(checkExpired bool) []K {
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
func (c *LFUCache[K, V, S]) Len(checkExpired bool) int {
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
func (c *LFUCache[K, V, S]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		err := c.purgeValue(key, item.value)
		_ = err // TODO: log error
	}

	c.init()
}

// IsExpired returns boolean value whether this item is expired or not.
func (it *lfuItem[K, S]) IsExpired(now *time.Time) bool {
	if it.expiration == nil {
		return false
	}
	if now == nil {
		t := it.clock.Now()
		now = &t
	}
	return it.expiration.Before(*now)
}

func isRemovableFreqEntry[K comparable, S any](entry *freqEntry[K, S]) bool {
	return entry.freq != 0 && len(entry.items) == 0
}
