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
func (s *snapshotImpl) Release() {
	s.mu.Lock()
	s.refCount--
	if s.refCount <= 0 {
		// TODO: cleanup
	}
	s.mu.Unlock()
}

// Sequence implements Snapshot.
func (s *snapshotImpl) Sequence() SequenceNumber {
	return s.sequence
}
