package db

import (
	"container/heap"

	"github.com/akzj/go-leveldb/util"
)

// MergingIterator combines multiple sorted iterators into one.
// Used by DB to merge memtable + imm + SSTable iterators.
// Invariant: All input iterators must be sorted in the same order.
type MergingIterator struct {
	icmp     *InternalKeyComparator
	children []Iterator
	valid   bool
	status_ *util.Status
	heap    iterHeap
	ikey    util.Slice
}

// iterHeap implements heap.Interface for iterItem.
type iterHeap []*iterItem

func (h iterHeap) Len() int { return len(h) }

func (h iterHeap) Less(i, j int) bool {
	// Compare by key (higher sequence first for same user key)
	return h[i].ikey.Compare(h[j].ikey) < 0
}

func (h iterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iterHeap) Push(x interface{}) {
	*h = append(*h, x.(*iterItem))
}

func (h *iterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// iterItem is an item in the heap.
type iterItem struct {
	ikey util.Slice
	iter Iterator
	idx  int
}

// NewMergingIterator creates a new merging iterator.
func NewMergingIterator(icmp *InternalKeyComparator, children []Iterator) Iterator {
	m := &MergingIterator{
		icmp:     icmp,
		children: children,
		status_:  util.NewStatusOK(),
		heap:     make(iterHeap, 0, len(children)),
	}

	// Build initial heap
	for i, child := range children {
		if child != nil && child.Valid() {
			m.heap = append(m.heap, &iterItem{
				ikey: child.Key(),
				iter: child,
				idx:  i,
			})
		}
	}
	heap.Init(&m.heap)

	if len(m.heap) > 0 {
		m.ikey = m.heap[0].ikey
		m.valid = true
	}

	return m
}

// Valid implements Iterator.
func (m *MergingIterator) Valid() bool {
	return m.valid
}

// SeekToFirst implements Iterator.
func (m *MergingIterator) SeekToFirst() {
	for _, child := range m.children {
		if child != nil {
			child.SeekToFirst()
		}
	}
	m.rebuildHeap()
	m.updateCurrent()
}

// SeekToLast implements Iterator.
func (m *MergingIterator) SeekToLast() {
	for _, child := range m.children {
		if child != nil {
			child.SeekToLast()
		}
	}
	m.rebuildHeap()
	m.updateCurrent()
}

// Seek implements Iterator.
func (m *MergingIterator) Seek(target util.Slice) {
	for _, child := range m.children {
		if child != nil {
			child.Seek(target)
		}
	}
	m.rebuildHeap()
	m.updateCurrent()
}

// Next implements Iterator.
func (m *MergingIterator) Next() {
	if !m.valid || len(m.heap) == 0 {
		return
	}

	// Advance current iterator
	m.heap[0].iter.Next()

	// Rebuild heap
	m.rebuildHeap()
	m.updateCurrent()
}

// Prev implements Iterator.
func (m *MergingIterator) Prev() {
	if !m.valid || len(m.heap) == 0 {
		return
	}

	// Go back on current iterator
	m.heap[0].iter.Prev()

	// Rebuild heap
	m.rebuildHeap()
	m.updateCurrent()
}

// Key implements Iterator.
func (m *MergingIterator) Key() util.Slice {
	if !m.valid || len(m.heap) == 0 {
		return util.MakeSlice(nil)
	}
	return m.heap[0].ikey
}

// Value implements Iterator.
func (m *MergingIterator) Value() util.Slice {
	if !m.valid || len(m.heap) == 0 {
		return util.MakeSlice(nil)
	}
	return m.heap[0].iter.Value()
}

// Status implements Iterator.
func (m *MergingIterator) Status() *util.Status {
	if !m.status_.OK() {
		return m.status_
	}
	for _, iter := range m.children {
		if iter != nil && !iter.Status().OK() {
			return iter.Status()
		}
	}
	return util.NewStatusOK()
}

// Release implements Iterator.
func (m *MergingIterator) Release() {
	for _, iter := range m.children {
		if iter != nil {
			iter.Release()
		}
	}
	m.children = nil
	m.valid = false
}

// rebuildHeap rebuilds the min-heap from valid iterators.
func (m *MergingIterator) rebuildHeap() {
	m.heap = m.heap[:0]
	for _, child := range m.children {
		if child != nil && child.Valid() {
			m.heap = append(m.heap, &iterItem{
				ikey: child.Key(),
				iter: child,
			})
		}
	}
	heap.Init(&m.heap)
}

// updateCurrent advances to the next minimum key.
func (m *MergingIterator) updateCurrent() {
	if len(m.heap) == 0 {
		m.valid = false
		return
	}

	minItem := m.heap[0]
	m.ikey = minItem.ikey
	m.valid = true
}
