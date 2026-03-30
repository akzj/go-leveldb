package table

import (
	"github.com/akzj/go-leveldb/util"
)

// BlockFunction creates a data block iterator from an index value.
type BlockFunction func(arg interface{}, indexValue util.Slice) Iterator

// TwoLevelIterator provides two-level iteration over SSTable data.
// Level 1: Index block (binary searchable)
// Level 2: Data blocks (linear iteration within block)
type TwoLevelIterator struct {
	blockFunction    BlockFunction
	arg              interface{}
	indexIter       Iterator
	dataIter        Iterator
	status_         *util.Status
	dataBlockHandle util.Slice
}

// NewTwoLevelIterator creates a new two-level iterator.
func NewTwoLevelIterator(indexIter Iterator, blockFunction BlockFunction, arg interface{}) Iterator {
	return &TwoLevelIterator{
		blockFunction:    blockFunction,
		arg:              arg,
		indexIter:        indexIter,
		dataIter:         nil,
		status_:          util.NewStatusOK(),
	}
}

// Valid implements Iterator.
func (t *TwoLevelIterator) Valid() bool {
	return t.dataIter != nil && t.dataIter.Valid()
}

// SeekToFirst implements Iterator.
func (t *TwoLevelIterator) SeekToFirst() {
	t.indexIter.SeekToFirst()
	t.initDataBlock()
	if t.dataIter != nil {
		t.dataIter.SeekToFirst()
	}
	t.skipEmptyDataBlocksForward()
}

// SeekToLast implements Iterator.
func (t *TwoLevelIterator) SeekToLast() {
	t.indexIter.SeekToLast()
	t.initDataBlock()
	if t.dataIter != nil {
		t.dataIter.SeekToLast()
	}
	t.skipEmptyDataBlocksBackward()
}

// Seek implements Iterator.
func (t *TwoLevelIterator) Seek(target util.Slice) {
	t.indexIter.Seek(target)
	t.initDataBlock()
	if t.dataIter != nil {
		t.dataIter.Seek(target)
	}
	t.skipEmptyDataBlocksForward()
}

// Next implements Iterator.
func (t *TwoLevelIterator) Next() {
	if t.dataIter == nil || !t.dataIter.Valid() {
		return
	}
	t.dataIter.Next()
	t.skipEmptyDataBlocksForward()
}

// Prev implements Iterator.
func (t *TwoLevelIterator) Prev() {
	if t.dataIter == nil || !t.dataIter.Valid() {
		return
	}
	t.dataIter.Prev()
	t.skipEmptyDataBlocksBackward()
}

// Key implements Iterator.
func (t *TwoLevelIterator) Key() util.Slice {
	if t.dataIter == nil {
		return util.MakeSlice(nil)
	}
	return t.dataIter.Key()
}

// Value implements Iterator.
func (t *TwoLevelIterator) Value() util.Slice {
	if t.dataIter == nil {
		return util.MakeSlice(nil)
	}
	return t.dataIter.Value()
}

// Status implements Iterator.
func (t *TwoLevelIterator) Status() *util.Status {
	if !t.status_.OK() {
		return t.status_
	}
	if !t.indexIter.Status().OK() {
		return t.indexIter.Status()
	}
	if t.dataIter != nil && !t.dataIter.Status().OK() {
		return t.dataIter.Status()
	}
	return util.NewStatusOK()
}

// Release implements Iterator.
func (t *TwoLevelIterator) Release() {
	if t.dataIter != nil {
		t.dataIter.Release()
		t.dataIter = nil
	}
	if t.indexIter != nil {
		t.indexIter.Release()
		t.indexIter = nil
	}
}

func (t *TwoLevelIterator) skipEmptyDataBlocksForward() {
	for t.dataIter == nil || !t.dataIter.Valid() {
		if !t.indexIter.Valid() {
			t.setDataIterator(nil)
			return
		}
		t.indexIter.Next()
		t.initDataBlock()
		if t.dataIter != nil {
			t.dataIter.SeekToFirst()
		}
	}
}

func (t *TwoLevelIterator) skipEmptyDataBlocksBackward() {
	for t.dataIter == nil || !t.dataIter.Valid() {
		if !t.indexIter.Valid() {
			t.setDataIterator(nil)
			return
		}
		t.indexIter.Prev()
		t.initDataBlock()
		if t.dataIter != nil {
			t.dataIter.SeekToLast()
		}
	}
}

func (t *TwoLevelIterator) initDataBlock() {
	if !t.indexIter.Valid() {
		t.setDataIterator(nil)
		return
	}

	handle := t.indexIter.Value()

	// Reuse current block if same
	if t.dataIter != nil && t.dataBlockHandle.Size() == handle.Size() &&
		bytesEqual(t.dataBlockHandle.Data(), handle.Data()) {
		return
	}

	t.dataIter = t.blockFunction(t.arg, handle)
	t.dataBlockHandle = handle

	if t.dataIter != nil && !t.dataIter.Status().OK() {
		t.status_ = t.dataIter.Status()
	}
}

func (t *TwoLevelIterator) setDataIterator(iter Iterator) {
	if t.dataIter != nil {
		if !t.dataIter.Status().OK() {
			t.status_ = t.dataIter.Status()
		}
	}
	t.dataIter = iter
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
