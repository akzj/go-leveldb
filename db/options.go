package db

import (
	"github.com/akzj/go-leveldb/cache"
	"github.com/akzj/go-leveldb/table"
	"github.com/akzj/go-leveldb/util"
)

// Options controls DB behavior.
// Invariant: Comparator must never be nil (defaults to BytewiseComparator).
type Options struct {
	// Comparator used to define the order of keys in the table.
	// Default: util.DefaultBytewiseComparator()
	Comparator util.Comparator

	// If true, the database will be created if it is missing.
	CreateIfMissing bool

	// If true, an error is raised if the database already exists.
	ErrorIfExists bool

	// If true, the implementation will do aggressive checking.
	ParanoidChecks bool

	// Env for file system operations.
	Env Env

	// Logger for informational messages.
	InfoLog Logger

	// Amount of data to build up in memory before converting to a sorted
	// on-disk file. Default: 4MB
	WriteBufferSize int

	// Maximum number of open files. Default: 1000
	MaxOpenFiles int

	// Cache for blocks.
	BlockCache *LRUCache

	// Approximate size of user data packed per block. Default: 4KB
	BlockSize int

	// Number of keys between restart points for delta encoding of keys.
	// Default: 16
	BlockRestartInterval int

	// Maximum file size before switching to a new one. Default: 2MB
	MaxFileSize int

	// Compression algorithm. Default: SnappyCompression
	Compression table.CompressionType

	// Compression level for zstd. Default: 1
	ZstdCompressionLevel int

	// If true, reuse existing MANIFEST and log files on open.
	ReuseLogs bool

	// Filter policy for bloom filter.
	FilterPolicy FilterPolicy
}

// NewOptions returns default options.
func NewOptions() *Options {
	return &Options{
		Comparator:           util.DefaultBytewiseComparator(),
		CreateIfMissing:      false,
		ErrorIfExists:        false,
		ParanoidChecks:       false,
		WriteBufferSize:      4 * 1024 * 1024,
		MaxOpenFiles:         1000,
		BlockSize:            4 * 1024,
		BlockRestartInterval: 16,
		MaxFileSize:          2 * 1024 * 1024,
		Compression:          table.KSnappyCompression,
		ZstdCompressionLevel: 1,
		ReuseLogs:            false,
	}
}

// SetCreateIfMissing sets the CreateIfMissing option.
// If true, the database will be created if it is missing.
// Required by acceptance test syntax: opts.SetCreateIfMissing(true)
func (o *Options) SetCreateIfMissing(v bool) *Options {
	o.CreateIfMissing = v
	return o
}

// ReadOptions controls read operations.
type ReadOptions struct {
	// If true, all data read will be verified against checksums.
	VerifyChecksums bool

	// Should the data read be cached? Default: true.
	FillCache bool

	// If non-nil, read as of the supplied snapshot.
	Snapshot Snapshot
}

// NewReadOptions returns default read options.
func NewReadOptions() *ReadOptions {
	return &ReadOptions{
		VerifyChecksums: false,
		FillCache:       true,
		Snapshot:        nil,
	}
}

// WriteOptions controls write operations.
type WriteOptions struct {
	// If true, the write will be synced to disk before returning.
	Sync bool
}

// NewWriteOptions returns default write options.
func NewWriteOptions() *WriteOptions {
	return &WriteOptions{
		Sync: false,
	}
}

// Range represents a range of keys.
type Range struct {
	Start util.Slice
	Limit util.Slice
}

// NewRange creates a new range.
func NewRange(start, limit util.Slice) Range {
	return Range{Start: start, Limit: limit}
}

// LRUCache type alias for Options compatibility.
type LRUCache = cache.LRUCache
