package table

import (
	"github.com/akzj/go-leveldb/util"
)

// Iterator is an interface for iterating over key-value pairs in a table.
// This mirrors db.Iterator but is defined in table package to avoid import cycles.
type Iterator interface {
	// Valid returns true iff the iterator is positioned at a key/value pair.
	Valid() bool

	// SeekToFirst positions the iterator at the first key in the source.
	SeekToFirst()

	// SeekToLast positions the iterator at the last key in the source.
	SeekToLast()

	// Seek positions at the first key in the source that is at or past target.
	Seek(target util.Slice)

	// Next moves to the next entry in the source.
	Next()

	// Prev moves to the previous entry in the source.
	Prev()

	// Key returns the key for the current entry.
	Key() util.Slice

	// Value returns the value for the current entry.
	Value() util.Slice

	// Status returns the current status of the iterator.
	Status() *util.Status

	// Release releases the iterator resources.
	Release()
}

// NewEmptyIterator returns an iterator that yields nothing.
func NewEmptyIterator() Iterator {
	return &emptyIterator{}
}

// emptyIterator is an iterator that yields nothing.
type emptyIterator struct{}

func (e *emptyIterator) Valid() bool         { return false }
func (e *emptyIterator) SeekToFirst()       {}
func (e *emptyIterator) SeekToLast()        {}
func (e *emptyIterator) Seek(target util.Slice) {}
func (e *emptyIterator) Next()             {}
func (e *emptyIterator) Prev()             {}
func (e *emptyIterator) Key() util.Slice   { return util.MakeSlice(nil) }
func (e *emptyIterator) Value() util.Slice { return util.MakeSlice(nil) }
func (e *emptyIterator) Status() *util.Status { return util.NewStatusOK() }
func (e *emptyIterator) Release()          {}

// NewErrorIterator returns an iterator that returns an error.
func NewErrorIterator(err *util.Status) Iterator {
	return &errorIterator{err: err}
}

// errorIterator returns errors on all operations.
type errorIterator struct {
	err *util.Status
}

func (e *errorIterator) Valid() bool         { return false }
func (e *errorIterator) SeekToFirst()       {}
func (e *errorIterator) SeekToLast()        {}
func (e *errorIterator) Seek(target util.Slice) {}
func (e *errorIterator) Next()             {}
func (e *errorIterator) Prev()             {}
func (e *errorIterator) Key() util.Slice   { return util.MakeSlice(nil) }
func (e *errorIterator) Value() util.Slice { return util.MakeSlice(nil) }
func (e *errorIterator) Status() *util.Status { return e.err }
func (e *errorIterator) Release()          {}
