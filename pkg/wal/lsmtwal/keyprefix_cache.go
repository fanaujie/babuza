package lsmtwal

import (
	"container/list"
	"github.com/fanaujie/babuza/ibabuza"
	"sync"
)

// keyPrefixCache is an LRU cache for keyPrefix objects
type keyPrefixCache struct {
	mu       sync.Mutex
	capacity int
	cache    map[ibabuza.RaftGroupID]*list.Element
	lru      *list.List
}

// keyPrefixCacheEntry represents an entry in the keyPrefixCache
type keyPrefixCacheEntry struct {
	groupID   ibabuza.RaftGroupID
	keyPrefix *keyPrefix
}

// newKeyPrefixCache creates a new keyPrefixCache with the specified capacity
func newKeyPrefixCache(capacity int) *keyPrefixCache {
	return &keyPrefixCache{
		capacity: capacity,
		cache:    make(map[ibabuza.RaftGroupID]*list.Element),
		lru:      list.New(),
	}
}

// get returns the keyPrefix for the specified groupID, creating a new one if it doesn't exist
func (c *keyPrefixCache) get(groupID ibabuza.RaftGroupID) *keyPrefix {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the keyPrefix is already in the cache
	if elem, ok := c.cache[groupID]; ok {
		// Move the element to the front of the list (mark as recently used)
		c.lru.MoveToFront(elem)
		// Return the keyPrefix
		return elem.Value.(*keyPrefixCacheEntry).keyPrefix
	}

	// Create a new keyPrefix
	kp := newKeyPrefix(groupID)

	// Add it to the cache
	entry := &keyPrefixCacheEntry{
		groupID:   groupID,
		keyPrefix: kp,
	}
	elem := c.lru.PushFront(entry)
	c.cache[groupID] = elem

	// If the cache is over capacity, remove the least recently used item
	if c.lru.Len() > c.capacity {
		oldest := c.lru.Back()
		if oldest != nil {
			c.lru.Remove(oldest)
			delete(c.cache, oldest.Value.(*keyPrefixCacheEntry).groupID)
		}
	}
	return kp
}
