package memtable

import (
	"bytes"
	"errors"
	"sync"

	"github.com/akzj/go-leveldb/internal"
)

// ErrNotFound is returned when a key is not found in the MemTable.
var ErrNotFound = errors.New("leveldb: not found")

// MemTable is an in-memory data structure that stores key-value pairs.
// It wraps a SkipList and provides additional functionality for Get operations.
type MemTable struct {
	sl   *SkipList
	mu   sync.RWMutex
	size int
}

// NewMemTable creates a new MemTable with a random seed based on the current time.
func NewMemTable() *MemTable {
	return &MemTable{
		sl: NewSkipList(1),
	}
}

// Put inserts a key-value pair into the MemTable.
func (mt *MemTable) Put(ikey internal.InternalKey, value []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.sl.Put(ikey, value)
	// Track size: userKey + InternalKeyOverhead (seq+type) + value
	mt.size += len(ikey.UserKey()) + internal.InternalKeyOverhead + len(value)
}

// Get retrieves the value for the given user key.
// It returns the value with the highest sequence number for that user key.
// Returns ErrNotFound if the key doesn't exist or is deleted.
func (mt *MemTable) Get(userKey []byte) ([]byte, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	it := mt.sl.NewIterator()

	// Iterate from first element to find matching userKey.
	// Due to Compare ordering (user_key asc, seq desc), entries with same
	// userKey are grouped together with highest sequence first.
	// First matching entry is the latest version.
	if !it.First() {
		return nil, ErrNotFound
	}

	// Find first entry with matching userKey
	for it.Valid() && !bytes.Equal(it.Key().UserKey(), userKey) {
		it.Next()
	}

	// No entry found with this userKey
	if !it.Valid() {
		return nil, ErrNotFound
	}

	// First match has highest sequence (desc order)
	if it.Key().Type() == internal.TypeDelete {
		return nil, ErrNotFound
	}

	return it.Value(), nil
}

// NewIterator returns an iterator over the MemTable.
func (mt *MemTable) NewIterator() *SkipListIterator {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.sl.NewIterator()
}

// ApproximateSize returns the approximate size of the MemTable in bytes.
func (mt *MemTable) ApproximateSize() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// Size returns the number of entries in the MemTable.
func (mt *MemTable) Size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.sl.Size()
}