package memdb

import (
	"math/rand"

	"github.com/akzj/go-leveldb/util"
)

const kMaxHeight = 12
const kBranchingFactor = 4

type node struct {
	key    []byte
	value  []byte
	height int
	next   []*node // next[i] points to the next node at level i
}

// SkipList is a lock-free skip list implementation.
type SkipList struct {
	comparator util.Comparator
	arena      *util.Arena
	head       *node
	maxHeight  int
	nodeCount  int
}

// NewSkipList creates a new skip list with the given comparator and arena.
func NewSkipList(comparator util.Comparator, arena *util.Arena) *SkipList {
	head := &node{
		key:    nil,
		value:  nil,
		height: kMaxHeight,
		next:   make([]*node, kMaxHeight),
	}
	return &SkipList{
		comparator: comparator,
		arena:      arena,
		head:       head,
		maxHeight:  1,
	}
}

// randomHeight generates a random height for a new node.
func (s *SkipList) randomHeight() int {
	height := 1
	for height < kMaxHeight && rand.Int()%kBranchingFactor == 0 {
		height++
	}
	return height
}

// KeyIsAfterNode returns true if key is strictly greater than node's key.
func (s *SkipList) KeyIsAfterNode(key []byte, n *node) bool {
	if n == nil {
		return true
	}
	return s.comparator.Compare(util.MakeSlice(key), util.MakeSlice(n.key)) > 0
}

// findPrevNodes finds the predecessors for inserting key.
func (s *SkipList) findPrevNodes(key []byte) []*node {
	prev := make([]*node, kMaxHeight)
	x := s.head

	// Iterate through all levels, including levels beyond maxHeight
	// This ensures prev[i] is never nil for any i < height of new node
	for level := kMaxHeight - 1; level >= 0; level-- {
		if level < s.maxHeight {
			next := x.next[level]
			for next != nil && s.KeyIsAfterNode(key, next) {
				x = next
				next = x.next[level]
			}
		}
		prev[level] = x
	}

	return prev
}

// Find finds a node with the given key. Returns nil if not found.
func (s *SkipList) Find(key []byte) *node {
	x := s.head
	for level := s.maxHeight - 1; level >= 0; level-- {
		next := x.next[level]
		for next != nil && s.KeyIsAfterNode(key, next) {
			x = next
			next = x.next[level]
		}
	}
	x = x.next[0]
	if x != nil && s.comparator.Compare(util.MakeSlice(key), util.MakeSlice(x.key)) == 0 {
		return x
	}
	return nil
}

// Insert inserts a key-value pair into the skip list.
func (s *SkipList) Insert(key, value []byte) {
	prev := s.findPrevNodes(key)

	// Check for existing key
	x := prev[0].next[0]
	if x != nil && s.comparator.Compare(util.MakeSlice(key), util.MakeSlice(x.key)) == 0 {
		// Update existing value
		newValue := make([]byte, len(value))
		copy(newValue, value)
		x.value = newValue
		return
	}

	// Create new node
	height := s.randomHeight()
	newNode := &node{
		key:    key,
		value:  value,
		height: height,
		next:   make([]*node, height),
	}

	// Link into skiplist
	for i := 0; i < height; i++ {
		newNode.next[i] = prev[i].next[i]
		prev[i].next[i] = newNode
	}

	s.nodeCount++

	if height > s.maxHeight {
		s.maxHeight = height
	}
}

// NewIterator creates a new iterator over the skip list.
func (s *SkipList) NewIterator() Iterator {
	return &skipListIterator{
		list: s,
		node: s.head.next[0],
	}
}

// ApproximateMemoryUsage returns the approximate memory usage of the skip list.
func (s *SkipList) ApproximateMemoryUsage() int {
	return int(s.arena.MemoryUsage())
}

type skipListIterator struct {
	list *SkipList
	node *node
}

func (i *skipListIterator) Valid() bool {
	return i.node != nil
}

func (i *skipListIterator) SeekToFirst() {
	i.node = i.list.head.next[0]
}

func (i *skipListIterator) SeekToLast() {
	x := i.list.head
	for x.next[i.list.maxHeight-1] != nil {
		x = x.next[i.list.maxHeight-1]
	}
	if x != i.list.head {
		i.node = x
	} else {
		i.node = nil
	}
}

func (i *skipListIterator) Seek(target util.Slice) {
	x := i.list.head
	for level := i.list.maxHeight - 1; level >= 0; level-- {
		next := x.next[level]
		for next != nil && i.list.KeyIsAfterNode(target.Data(), next) {
			x = next
			next = x.next[level]
		}
	}
	i.node = x.next[0]
}

func (i *skipListIterator) Next() {
	if i.node != nil {
		i.node = i.node.next[0]
	}
}

func (i *skipListIterator) Prev() {
	if i.node != i.list.head {
		x := i.list.head
		for x.next[0] != i.node && x.next[0] != nil {
			x = x.next[0]
		}
		if x != i.list.head {
			i.node = x
		} else {
			i.node = nil
		}
	}
}

func (i *skipListIterator) Key() util.Slice {
	if i.node != nil {
		return util.MakeSlice(i.node.key)
	}
	return util.MakeSlice(nil)
}

func (i *skipListIterator) Value() util.Slice {
	if i.node != nil {
		return util.MakeSlice(i.node.value)
	}
	return util.MakeSlice(nil)
}

func (i *skipListIterator) Status() *util.Status {
	return util.NewStatusOK()
}

func (i *skipListIterator) Release() {
	// No-op for skiplist iterator
}
