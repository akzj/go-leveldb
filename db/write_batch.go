package db

import (
	"github.com/akzj/go-leveldb/util"
)

// WriteBatchOpCode represents the type of operation in a write batch.
type WriteBatchOpCode uint8

const (
	WriteBatchPut    WriteBatchOpCode = 1
	WriteBatchDelete WriteBatchOpCode = 2
)

// WriteBatchOp represents a single operation in a write batch.
type WriteBatchOp struct {
	Code   WriteBatchOpCode
	Key    []byte
	Value  []byte
}

// Handler interface for iterating over write batch contents.
type WriteBatchHandler interface {
	// Put handles a put operation.
	Put(key, value util.Slice)
	// Delete handles a delete operation.
	Delete(key util.Slice)
}

// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// Binary format (C++ compatible):
//   Sequence: WriteBatch::rep_
//   Format: [count:varint32][record...][header...]
//   Each record: [kPutCode][key_len:varint32][key][value_len:varint32][value]
//                [kDeleteCode][key_len:varint32][key]
type WriteBatch struct {
	ops []WriteBatchOp
}

// NewWriteBatch creates a new empty write batch.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{}
}

// Put stores the mapping key->value in the database.
func (b *WriteBatch) Put(key, value util.Slice) {
	b.ops = append(b.ops, WriteBatchOp{
		Code:  WriteBatchPut,
		Key:   key.Data(),
		Value: value.Data(),
	})
}

// Delete removes the database entry for key if it exists.
func (b *WriteBatch) Delete(key util.Slice) {
	b.ops = append(b.ops, WriteBatchOp{
		Code: WriteBatchDelete,
		Key:  key.Data(),
	})
}

// Clear removes all operations from the batch.
func (b *WriteBatch) Clear() {
	b.ops = b.ops[:0]
}

// ApproximateSize returns the approximate size of the batch in bytes.
// This matches C++ LevelDB's WriteBatch::ApproximateSize().
func (b *WriteBatch) ApproximateSize() int {
	// Each op: 1 byte code + varint key_len + key + (optional) varint value_len + value
	size := 4 // 4 bytes for count (varint32 overhead)
	for _, op := range b.ops {
		size += 1 // opcode
		size += varintLength(uint64(len(op.Key)))
		size += len(op.Key)
		if op.Code == WriteBatchPut {
			size += varintLength(uint64(len(op.Value)))
			size += len(op.Value)
		}
	}
	return size
}

// Iterate calls the handler for each operation in the batch.
func (b *WriteBatch) Iterate(handler WriteBatchHandler) *util.Status {
	for _, op := range b.ops {
		switch op.Code {
		case WriteBatchPut:
			handler.Put(util.MakeSlice(op.Key), util.MakeSlice(op.Value))
		case WriteBatchDelete:
			handler.Delete(util.MakeSlice(op.Key))
		}
	}
	return util.NewStatusOK()
}

// Ops returns the operations in the batch.
func (b *WriteBatch) Ops() []WriteBatchOp {
	return b.ops
}

// SetOps sets the operations in the batch.
func (b *WriteBatch) SetOps(ops []WriteBatchOp) {
	b.ops = ops
}

// Append appends operations from another batch.
func (b *WriteBatch) Append(other *WriteBatch) {
	b.ops = append(b.ops, other.ops...)
}

// varintLength returns the length of the varint encoding of v.
func varintLength(v uint64) int {
	if v < 1<<7 {
		return 1
	}
	if v < 1<<14 {
		return 2
	}
	if v < 1<<21 {
		return 3
	}
	if v < 1<<28 {
		return 4
	}
	return 5
}
