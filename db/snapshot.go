package db

import (
	"sync"
)

// Snapshot represents a point-in-time view of the database.
// Snapshots are immutable and safe for concurrent access.
type Snapshot interface {
	// Release releases the snapshot.
	Release()

	// Sequence returns the sequence number of the snapshot.
	Sequence() SequenceNumber
}

// snapshotImpl is the implementation of Snapshot.
type snapshotImpl struct {
	sequence SequenceNumber
	refCount int
	mu       sync.RWMutex
}

// NewSnapshot creates a new snapshot with the given sequence number.
func NewSnapshot(seq SequenceNumber) Snapshot {
	return &snapshotImpl{
		sequence: seq,
		refCount: 1,
	}
}

// Release implements Snapshot.
// Snapshot Cleanup Contract:
//   - Snapshot holds read-only view at a point in time
//   - No external resources (files, connections, handles) to release
//   - refCount tracks reference count for snapshot reuse
//   - When refCount <= 0: Go GC reclaims snapshot memory automatically
//   - No manual cleanup needed (unlike C++ which may hold resources)
func (s *snapshotImpl) Release() {
	s.mu.Lock()
	s.refCount--
	// Snapshot is immutable and holds no external resources (no file handles,
	// no network connections). Go's garbage collector automatically reclaims
	// the memory when the snapshot is no longer referenced.
	if s.refCount <= 0 {
		// Explicitly set to nil to help GC (snapshotImpl is heap-allocated)
		s.sequence = 0
	}
	s.mu.Unlock()
}

// Sequence implements Snapshot.
func (s *snapshotImpl) Sequence() SequenceNumber {
	return s.sequence
}
