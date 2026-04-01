package db

import (
	"github.com/akzj/go-leveldb/util"
)

// DB is a persistent ordered map from keys to values.
// DB is safe for concurrent access from multiple threads without
// any external synchronization.
type DB interface {
	// Put sets the database entry for key to value.
	// Returns OK on success, non-OK on error.
	// Note: consider setting options.Sync = true.
	Put(options *WriteOptions, key, value util.Slice) *util.Status

	// Delete removes the database entry for key.
	// Returns OK on success, non-OK on error.
	// Note: consider setting options.Sync = true.
	Delete(options *WriteOptions, key util.Slice) *util.Status

	// Write applies the specified updates to the database atomically.
	// Returns OK on success, non-OK on failure.
	// Note: consider setting options.Sync = true.
	Write(options *WriteOptions, batch *WriteBatch) *util.Status

	// Get retrieves the value for key.
	// If the key doesn't exist, returns a status with IsNotFound() = true.
	// May return other errors on failure.
	Get(options *ReadOptions, key util.Slice) ([]byte, *util.Status)

	// NewIterator returns an iterator over the database contents.
	// The result is initially invalid; caller must call Seek before using it.
	// Caller should delete the iterator when it is no longer needed.
	// The returned iterator should be deleted before this db is deleted.
	NewIterator(options *ReadOptions) Iterator

	// GetSnapshot returns a handle to the current DB state.
	// Iterators created with this handle will observe a stable snapshot.
	// Caller must call ReleaseSnapshot when the snapshot is no longer needed.
	GetSnapshot() Snapshot

	// ReleaseSnapshot releases a previously acquired snapshot.
	// Caller must not use the snapshot after this call.
	ReleaseSnapshot(snapshot Snapshot)

	// GetProperty returns a property value.
	// Valid property names:
	//   "leveldb.num-files-at-level<N>" - number of files at level N
	//   "leveldb.stats" - multi-line string describing internal statistics
	//   "leveldb.sstables" - multi-line string describing sstables
	//   "leveldb.approximate-memory-usage" - approximate memory usage in bytes
	GetProperty(property util.Slice) (string, bool)

	// GetApproximateSizes returns approximate file system space used by
	// keys in the given ranges.
	GetApproximateSizes(ranges []Range) []uint64

	// CompactRange compacts the underlying storage for the key range.
	// begin==nil is treated as before all keys.
	// end==nil is treated as after all keys.
	CompactRange(begin, end util.Slice)

	// Close closes the database.
	Close() *util.Status
}

// DestroyDB destroys the contents of the specified database.
func DestroyDB(name string, options *Options) *util.Status {
	panic("TODO: implement DestroyDB")
}

// RepairDB attempts to recover as much as possible from a corrupted database.
func RepairDB(name string, options *Options) (DB, *util.Status) {
	panic("TODO: implement RepairDB")
}
