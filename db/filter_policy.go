package db

import (
	"github.com/akzj/go-leveldb/util"
)

// FilterPolicy is an interface for creating and checking filters.
// Filters are used to reduce disk reads during DB::Get() calls.
type FilterPolicy interface {
	// Name returns the name of the filter policy.
	// If the filter encoding changes incompatibly, the name must change.
	Name() string

	// CreateFilter creates a filter from a set of keys.
	// Appends the filter to dst.
	// Does not modify the initial contents of dst.
	CreateFilter(keys []util.Slice, dst []byte) []byte

	// KeyMayMatch returns true if the key may be in the filter.
	// May return true or false if the key is not in the filter,
	// but should return false with high probability.
	KeyMayMatch(key, filter util.Slice) bool
}

// BloomFilterPolicy implements a bloom filter.
type BloomFilterPolicy struct {
	bitsPerKey int
}

// NewBloomFilterPolicy creates a new bloom filter policy.
// A good value for bitsPerKey is 10, which yields ~1% false positive rate.
func NewBloomFilterPolicy(bitsPerKey int) *BloomFilterPolicy {
	return &BloomFilterPolicy{bitsPerKey: bitsPerKey}
}

// Name implements FilterPolicy.
func (p *BloomFilterPolicy) Name() string {
	return "leveldb.BuiltinBloomFilter2"
}

// CreateFilter implements FilterPolicy.
func (p *BloomFilterPolicy) CreateFilter(keys []util.Slice, dst []byte) []byte {
	// TODO: implement bloom filter creation
	return dst
}

// KeyMayMatch implements FilterPolicy.
func (p *BloomFilterPolicy) KeyMayMatch(key, filter util.Slice) bool {
	// TODO: implement bloom filter lookup
	return true
}
