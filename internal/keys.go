package internal

import (
	"bytes"
	"encoding/binary"
)

// ValueType represents the type of an internal key.
type ValueType byte

const (
	TypePut    ValueType = 1
	TypeDelete ValueType = 2
)

// Constants for key sizes.
const (
	SequenceSize         = 8
	TypeSize             = 1
	InternalKeyOverhead  = SequenceSize + TypeSize
)

// InternalKey represents a key-value pair's metadata in the internal format.
// The format is: user_key (variable length) + sequence number (8 bytes big-endian) + value type (1 byte)
type InternalKey struct {
	data []byte
}

// MakeInternalKey creates a new InternalKey with the given user key, sequence number, and value type.
func MakeInternalKey(userKey []byte, seq uint64, vt ValueType) InternalKey {
	// Allocate buffer: userKey + sequence (8 bytes) + type (1 byte)
	buf := make([]byte, len(userKey)+SequenceSize+TypeSize)
	
	// Copy user key
	copy(buf, userKey)
	
	// Write sequence number as big-endian uint64
	binary.BigEndian.PutUint64(buf[len(userKey):len(userKey)+SequenceSize], seq)
	
	// Write type
	buf[len(userKey)+SequenceSize] = byte(vt)
	
	return InternalKey{data: buf}
}

// MakeInternalKeyFromBytes creates an InternalKey from raw bytes.
// The bytes must be in the format: user_key + sequence (8 bytes BE) + type (1 byte).
func MakeInternalKeyFromBytes(data []byte) InternalKey {
	return InternalKey{data: bytes.Clone(data)}
}

// UserKey returns the user key portion of the internal key.
func (k InternalKey) UserKey() []byte {
	if len(k.data) <= InternalKeyOverhead {
		return []byte{}
	}
	return k.data[:len(k.data)-InternalKeyOverhead]
}

// Sequence returns the sequence number of the internal key.
func (k InternalKey) Sequence() uint64 {
	if len(k.data) < SequenceSize+TypeSize {
		return 0
	}
	offset := len(k.data) - InternalKeyOverhead
	return binary.BigEndian.Uint64(k.data[offset : offset+SequenceSize])
}

// Type returns the value type of the internal key.
func (k InternalKey) Type() ValueType {
	if len(k.data) < TypeSize {
		return 0
	}
	return ValueType(k.data[len(k.data)-TypeSize])
}

// Data returns the raw bytes of the internal key.
func (k InternalKey) Data() []byte {
	return k.data
}

// Compare compares two internal keys.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// Comparison order: user_key ascending (lexicographical), sequence descending (newer first).
func Compare(a, b InternalKey) int {
	// Compare user keys first (ascending)
	userKeyA := a.UserKey()
	userKeyB := b.UserKey()
	
	if cmp := bytes.Compare(userKeyA, userKeyB); cmp != 0 {
		if cmp < 0 {
			return -1
		}
		return 1
	}
	
	// User keys are equal, compare sequences (descending - higher sequence first)
	seqA := a.Sequence()
	seqB := b.Sequence()
	
	if seqA < seqB {
		return 1  // Higher sequence comes first
	}
	if seqA > seqB {
		return -1
	}
	
	// Sequences equal, compare types
	if a.Type() < b.Type() {
		return -1
	}
	if a.Type() > b.Type() {
		return 1
	}
	
	return 0
}