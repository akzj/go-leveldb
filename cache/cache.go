package cache

import (
	"sync"

	"github.com/akzj/go-leveldb/util"
)

// Handle is an opaque handle to an entry stored in the cache.
type Handle struct {
	value    interface{}
	refCount int
	deleted  bool
	mu       sync.RWMutex
}

// Cache is an interface that maps keys to values.
// It has internal synchronization and may be safely accessed concurrently.
type Cache interface {
	// Insert inserts a mapping from key->value into the cache.
	// Returns a handle that must be released when no longer needed.
	Insert(key util.Slice, value interface{}, charge int) Handle

	// Lookup returns a handle for the key, or nil if not found.
	Lookup(key util.Slice) Handle

	// Release releases a handle.
	Release(h Handle)

	// Erase removes the entry for key from the cache.
	Erase(key util.Slice)

	// NewId returns a new unique id.
	NewId() uint64

	// Prune removes all entries that are not actively in use.
	Prune()

	// TotalCharge returns the total charge of all entries.
	TotalCharge() int
}

// LRUCache is a least-recently-used cache implementation.
type LRUCache struct {
	mu         sync.Mutex
	capacity   int
	totalCharge int
	usage      int
	entries    map[string]*lruEntry
	lruHead    *lruEntry
	lruTail    *lruEntry
	nextId     uint64
}

type lruEntry struct {
	key         string
	value       interface{}
	charge      int
	hash        uint64
	inCache     bool
	refCount    int
	handle      Handle
	prev, next  *lruEntry
}

const cacheEntrySize = 48 // approximate overhead per entry

// NewLRUCache creates a new LRU cache with the given capacity.
func NewLRUCache(capacity int) *LRUCache {
	c := &LRUCache{
		capacity: capacity,
		entries:  make(map[string]*lruEntry),
	}
	c.lruHead = &lruEntry{}
	c.lruTail = &lruEntry{}
	c.lruHead.next = c.lruTail
	c.lruTail.prev = c.lruHead
	return c
}

// NewLRUCacheInterface creates a cache with the given capacity and returns it as Cache interface.
func NewLRUCacheInterface(capacity int) Cache {
	return NewLRUCache(capacity)
}

// Insert implements Cache.
func (c *LRUCache) Insert(key util.Slice, value interface{}, charge int) Handle {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyStr := string(key.Data())
	id := c.nextId
	c.nextId++

	// Remove existing entry if present
	if entry, ok := c.entries[keyStr]; ok {
		c.removeEntry(entry)
		c.totalCharge -= entry.charge
	}

	// Evict entries if necessary
	for c.usage+charge > c.capacity && c.lruHead.next != c.lruTail {
		c.removeLruEntry(c.lruHead.next)
	}

	entry := &lruEntry{
		key:        keyStr,
		value:      value,
		charge:     charge,
		hash:       id,
		inCache:    true,
		refCount:   1,
	}
	entry.handle.value = entry
	c.entries[keyStr] = entry
	c.addToLru(entry)
	c.totalCharge += charge
	c.usage += charge + cacheEntrySize

	return Handle{value: entry, refCount: 1}
}

// Lookup implements Cache.
func (c *LRUCache) Lookup(key util.Slice) Handle {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyStr := string(key.Data())
	entry, ok := c.entries[keyStr]
	if !ok {
		return Handle{}
	}
	entry.refCount++
	c.moveToLruFront(entry)
	return Handle{value: entry, refCount: entry.refCount}
}

// Release implements Cache.
func (c *LRUCache) Release(h Handle) {
	if h.value == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := h.value.(*lruEntry)
	entry.refCount--
	if entry.refCount <= 0 && !entry.inCache {
		delete(c.entries, entry.key)
	}
}

// Erase implements Cache.
func (c *LRUCache) Erase(key util.Slice) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyStr := string(key.Data())
	entry, ok := c.entries[keyStr]
	if ok {
		c.removeEntry(entry)
		c.totalCharge -= entry.charge
		c.usage -= entry.charge + cacheEntrySize
		entry.inCache = false
		if entry.refCount <= 0 {
			delete(c.entries, entry.key)
		}
	}
}

// NewId implements Cache.
func (c *LRUCache) NewId() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextId
	c.nextId++
	return id
}

// Prune implements Cache.
func (c *LRUCache) Prune() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.lruHead.next != c.lruTail {
		c.removeLruEntry(c.lruHead.next)
	}
}

// TotalCharge implements Cache.
func (c *LRUCache) TotalCharge() int {
	return c.totalCharge
}

func (c *LRUCache) addToLru(entry *lruEntry) {
	entry.next = c.lruHead.next
	entry.prev = c.lruHead
	c.lruHead.next.prev = entry
	c.lruHead.next = entry
}

func (c *LRUCache) removeLruEntry(entry *lruEntry) {
	entry.prev.next = entry.next
	entry.next.prev = entry.prev
}

func (c *LRUCache) removeEntry(entry *lruEntry) {
	if entry.inCache {
		c.removeLruEntry(entry)
		entry.inCache = false
	}
}

func (c *LRUCache) moveToLruFront(entry *lruEntry) {
	if entry.inCache {
		c.removeLruEntry(entry)
		c.addToLru(entry)
	}
}

// Value returns the value for a handle.
func (h *Handle) Value() interface{} {
	if h.value == nil {
		return nil
	}
	return h.value.(*lruEntry).value
}
