package db

import (
	"github.com/akzj/go-leveldb/util"
)

// Iterator is an interface for iterating over key-value pairs.
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.
type Iterator interface {
	// Valid returns true iff the iterator is positioned at a key/value pair.
	Valid() bool

	// SeekToFirst positions the iterator at the first key in the source.
	// The iterator is Valid() after this call iff the source is not empty.
	SeekToFirst()

	// SeekToLast positions the iterator at the last key in the source.
	// The iterator is Valid() after this call iff the source is not empty.
	SeekToLast()

	// Seek positions at the first key in the source that is at or past target.
	// The iterator is Valid() after this call iff the source contains
	// an entry that comes at or past target.
	Seek(target util.Slice)

	// Next moves to the next entry in the source.
	// After this call, Valid() is true iff the iterator was not positioned
	// at the last entry in the source.
	// REQUIRES: Valid()
	Next()

	// Prev moves to the previous entry in the source.
	// After this call, Valid() is true iff the iterator was not positioned
	// at the first entry in source.
	// REQUIRES: Valid()
	Prev()

	// Key returns the key for the current entry.
	// The underlying storage for the returned slice is valid only until
	// the next modification of the iterator.
	// REQUIRES: Valid()
	Key() util.Slice

	// Value returns the value for the current entry.
	// The underlying storage for the returned slice is valid only until
	// the next modification of the iterator.
	// REQUIRES: Valid()
	Value() util.Slice

	// Status returns the current status of the iterator.
	// If an error has occurred, return it. Else return OK.
	Status() *util.Status

	// Release releases the iterator resources.
	Release()
}

// CleanupFunction is a function called when an iterator is destroyed.
type CleanupFunction func(arg1, arg2 interface{})

// CleanupNode represents a cleanup function to be called on iterator release.
type CleanupNode struct {
	Function CleanupFunction
	Arg1     interface{}
	Arg2     interface{}
	Next     *CleanupNode
}

// IteratorImpl is a basic implementation of Iterator for embedded use.
type IteratorImpl struct {
	valid    bool
	current  struct {
		key   util.Slice
		value util.Slice
	}
	status *util.Status
}

// NewIteratorImpl creates a new iterator implementation.
func NewIteratorImpl() *IteratorImpl {
	return &IteratorImpl{}
}

// Valid implements Iterator.
func (i *IteratorImpl) Valid() bool {
	return i.valid
}

// SeekToFirst implements Iterator.
func (i *IteratorImpl) SeekToFirst() {
	i.valid = false
}

// SeekToLast implements Iterator.
func (i *IteratorImpl) SeekToLast() {
	i.valid = false
}

// Seek implements Iterator.
func (i *IteratorImpl) Seek(target util.Slice) {
	i.valid = false
}

// Next implements Iterator.
func (i *IteratorImpl) Next() {
	i.valid = false
}

// Prev implements Iterator.
func (i *IteratorImpl) Prev() {
	i.valid = false
}

// Key implements Iterator.
func (i *IteratorImpl) Key() util.Slice {
	return i.current.key
}

// Value implements Iterator.
func (i *IteratorImpl) Value() util.Slice {
	return i.current.value
}

// Status implements Iterator.
func (i *IteratorImpl) Status() *util.Status {
	if i.status == nil {
		return util.NewStatusOK()
	}
	return i.status
}

// Release implements Iterator.
func (i *IteratorImpl) Release() {
	i.valid = false
}

// EmptyIterator returns an iterator that yields nothing.
type EmptyIterator struct {
	IteratorImpl
	status *util.Status
}

// NewEmptyIterator creates an empty iterator with OK status.
func NewEmptyIterator() Iterator {
	return &EmptyIterator{status: util.NewStatusOK()}
}

// NewErrorIterator creates an empty iterator with error status.
func NewErrorIterator(err *util.Status) Iterator {
	return &EmptyIterator{status: err}
}
