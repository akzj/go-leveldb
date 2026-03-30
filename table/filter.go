package table

import (
	"github.com/akzj/go-leveldb/util"
)

// FilterType constants for filter blocks.
const (
	// KFullFilter indicates a full filter block (no metaindex).
	KFullFilter = 1
	// KBlockBasedFilter indicates a block-based filter (deprecated).
	KBlockBasedFilter = 2
)

// FilterPolicy interface for creating and checking filters.
type FilterPolicy interface {
	Name() string
	CreateFilter(keys []util.Slice) []byte
	KeyMayMatch(key, filter util.Slice) bool
}

// FilterBlockBuilder builds filter blocks.
type FilterBlockBuilder struct {
	policy FilterPolicy
	keys   []util.Slice
}

// NewFilterBlockBuilder creates a new filter block builder.
func NewFilterBlockBuilder(policy FilterPolicy) *FilterBlockBuilder {
	return &FilterBlockBuilder{
		policy: policy,
		keys:   make([]util.Slice, 0, 64),
	}
}

// AddKey adds a key to the filter.
func (b *FilterBlockBuilder) AddKey(key util.Slice) {
	b.keys = append(b.keys, key)
}

// Finish returns the encoded filter data.
func (b *FilterBlockBuilder) Finish() []byte {
	if len(b.keys) == 0 {
		return nil
	}
	return b.policy.CreateFilter(b.keys)
}

// FilterBlockReader reads filter blocks from a table.
// Why separate from FilterBlockBuilder? Reader may read from cached blocks.
type FilterBlockReader struct {
	data   []byte
	policy FilterPolicy
}

// NewFilterBlockReader creates a new filter block reader.
func NewFilterBlockReader(data []byte, policy FilterPolicy) *FilterBlockReader {
	return &FilterBlockReader{
		data:   data,
		policy: policy,
	}
}

// KeyMayMatch returns true if the key may be present in the data.
func (r *FilterBlockReader) KeyMayMatch(key util.Slice) bool {
	if r.data == nil || r.policy == nil {
		return true // Conservative: assume may match
	}
	return r.policy.KeyMayMatch(key, util.MakeSlice(r.data))
}
