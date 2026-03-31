// Package memtable provides in-memory storage using a SkipList data structure.
package memtable

import (
	"bytes"
	"math/rand"

	"github.com/akzj/go-leveldb/internal"
)

// MaxHeight is the maximum height of the skip list.
// With p=0.25, the expected height is approximately log_{1/0.25}(n) ≈ log_4(n).
const MaxHeight = 12

// SkipList is a concurrent skiplist implementation for storing InternalKey-value pairs.
type SkipList struct {
	head    *node
	height  int
	size    int
	rng     *rand.Rand
}

// node represents a single element in the skip list.
type node struct {
	key   internal.InternalKey
	value []byte
	next  [MaxHeight]*node
}

// NewSkipList creates a new SkipList with the given random seed.
func NewSkipList(seed int64) *SkipList {
	sl := &SkipList{
		height: 1,
		rng:     rand.New(rand.NewSource(seed)),
	}
	sl.head = &node{}
	return sl
}

// randomHeight generates a random height for a new node.
// Height is between 1 and MaxHeight (inclusive).
// Uses geometric distribution with p=0.25.
func (sl *SkipList) randomHeight() int {
	h := 1
	for h < MaxHeight && sl.rng.Float64() < 0.25 {
		h++
	}
	return h
}

// Put inserts a key-value pair into the skip list.
// The key is always inserted; version handling is done by the caller
// (using sequence numbers in InternalKey).
func (sl *SkipList) Put(key internal.InternalKey, value []byte) {
	// Compute random height for new node
	h := sl.randomHeight()

	// Expand head height if needed
	if h > sl.height {
		sl.height = h
	}

	// Find predecessors at each level
	prevs := make([]*node, sl.height)
	cur := sl.head
	for i := sl.height - 1; i >= 0; i-- {
		for cur.next[i] != nil && internal.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		prevs[i] = cur
	}

	// Create new node
	nd := &node{
		key:   key,
		value: value,
	}

	// Insert at each level up to height h
	for i := 0; i < h; i++ {
		nd.next[i] = prevs[i].next[i]
		prevs[i].next[i] = nd
	}

	sl.size++
}

// Size returns the number of elements in the skip list.
func (sl *SkipList) Size() int {
	return sl.size
}

// SkipListIterator provides forward/backward iteration over a SkipList.
type SkipListIterator struct {
	list *SkipList
	cur  *node
}

// NewIterator creates a new iterator for the skip list.
func (sl *SkipList) NewIterator() *SkipListIterator {
	return &SkipListIterator{list: sl}
}

// Valid returns true if the iterator is positioned at a valid node.
func (it *SkipListIterator) Valid() bool {
	return it.cur != nil
}

// Key returns the key at the current position.
func (it *SkipListIterator) Key() internal.InternalKey {
	return it.cur.key
}

// Value returns the value at the current position.
func (it *SkipListIterator) Value() []byte {
	return it.cur.value
}

// First moves the iterator to the first element.
func (it *SkipListIterator) First() bool {
	if it.list.size == 0 {
		it.cur = nil
		return false
	}
	it.cur = it.list.head.next[0]
	return true
}

// Last moves the iterator to the last element.
func (it *SkipListIterator) Last() bool {
	if it.list.size == 0 {
		it.cur = nil
		return false
	}
	cur := it.list.head
	for cur.next[0] != nil {
		cur = cur.next[0]
	}
	it.cur = cur
	return true
}

// Next moves the iterator to the next element.
func (it *SkipListIterator) Next() bool {
	if it.cur == nil {
		return false
	}
	it.cur = it.cur.next[0]
	return it.cur != nil
}

// Prev moves the iterator to the previous element.
func (it *SkipListIterator) Prev() bool {
	if it.cur == nil || it.cur == it.list.head {
		return false
	}
	// Find predecessor by traversing from head
	cur := it.list.head
	for cur.next[0] != nil && cur.next[0] != it.cur {
		cur = cur.next[0]
	}
	// Move to predecessor (may be head)
	it.cur = cur
	// If predecessor is head, we've reached the beginning - invalidate
	if cur == it.list.head {
		it.cur = nil // Valid() checks cur != nil, so this makes Valid()=false
		return false
	}
	return true
}

// Seek moves the iterator to the first node with key >= target.
// Comparison is done using internal.Compare (user_key asc, sequence desc).
func (it *SkipListIterator) Seek(target internal.InternalKey) bool {
	cur := it.list.head
	for i := it.list.height - 1; i >= 0; i-- {
		for cur.next[i] != nil && internal.Compare(cur.next[i].key, target) < 0 {
			cur = cur.next[i]
		}
	}
	if cur.next[0] != nil {
		it.cur = cur.next[0]
		return true
	}
	it.cur = nil
	return false
}

// SeekForPrev moves the iterator to the last node with key <= target.
func (it *SkipListIterator) SeekForPrev(target internal.InternalKey) bool {
	cur := it.list.head
	for i := it.list.height - 1; i >= 0; i-- {
		for cur.next[i] != nil && internal.Compare(cur.next[i].key, target) <= 0 {
			cur = cur.next[i]
		}
	}
	it.cur = cur
	return cur != it.list.head
}

// userKeyLess compares two internal keys by user key only.
// Returns true if a.UserKey() < b.UserKey().
func userKeyLess(a, b internal.InternalKey) bool {
	return bytes.Compare(a.UserKey(), b.UserKey()) < 0
}

// userKeyEqual compares two internal keys by user key only.
// Returns true if a.UserKey() == b.UserKey().
func userKeyEqual(a, b internal.InternalKey) bool {
	return bytes.Equal(a.UserKey(), b.UserKey())
}