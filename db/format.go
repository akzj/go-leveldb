package db

import (
	"github.com/akzj/go-leveldb/util"
)

// ValueType represents the type of value encoded in an internal key.
// DO NOT CHANGE THESE VALUES: they are embedded in the on-disk data structures.
type ValueType uint8

const (
	KTypeDeletion ValueType = 0x0
	KTypeValue    ValueType = 0x1
)

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
const kValueTypeForSeek = KTypeValue

// SequenceNumber is a 56-bit sequence number.
// The top 8 bits are reserved for the ValueType.
type SequenceNumber uint64

// kMaxSequenceNumber is the maximum valid sequence number.
const kMaxSequenceNumber SequenceNumber = (1<<56) - 1

// ParsedInternalKey represents a parsed internal key.
type ParsedInternalKey struct {
	UserKey  util.Slice
	Sequence SequenceNumber
	Type     ValueType
}

// InternalKeyEncodingLength returns the length of the encoding of key.
// Format: [user_key][sequence_number|type_tag]
// The last 8 bytes contain: [7 bytes sequence][1 byte type]
func InternalKeyEncodingLength(key *ParsedInternalKey) int {
	return key.UserKey.Size() + 8
}

// AppendInternalKey appends the serialization of key to result.
// Format: [user_key][8 bytes: sequence (7 bytes, big-endian) + type (1 byte)]
func AppendInternalKey(result []byte, key *ParsedInternalKey) []byte {
	result = append(result, key.UserKey.Data()...)
	// Encode sequence (56 bits) and type (8 bits) into 8 bytes
	// Sequence is in the upper 56 bits of the 64-bit value
	seq := uint64(key.Sequence) << 8
	result = append(result, byte(seq>>56)) // byte 0: MSB
	result = append(result, byte(seq>>48))
	result = append(result, byte(seq>>40))
	result = append(result, byte(seq>>32))
	result = append(result, byte(seq>>24))
	result = append(result, byte(seq>>16))
	result = append(result, byte(seq>>8))
	result = append(result, byte(seq)|byte(key.Type)) // byte 7: LSB + type
	return result
}

// ParseInternalKey parses an internal key from internalKey.
// Returns false if the key is malformed (too short).
// Format: [user_key][8 bytes sequence+type]
func ParseInternalKey(internalKey util.Slice, result *ParsedInternalKey) bool {
	n := internalKey.Size()
	if n < 8 {
		return false
	}
	// Decode the last 8 bytes
	offset := n - 8
	num := util.DecodeFixed64(internalKey.Data()[offset:])
	c := uint8(num & 0xff)
	result.Sequence = SequenceNumber(num >> 8)
	result.Type = ValueType(c)
	result.UserKey = util.MakeSlice(internalKey.Data()[:offset])
	return c <= uint8(KTypeValue)
}

// ExtractUserKey extracts the user key from an internal key.
// The internal key must be at least 8 bytes.
func ExtractUserKey(internalKey util.Slice) util.Slice {
	return util.MakeSlice(internalKey.Data()[:internalKey.Size()-8])
}

// InternalKey represents an internal key with its encoded form.
type InternalKey struct {
	rep []byte // Encoded form: user_key + sequence + type
}

// NewInternalKey creates a new internal key from user key, sequence, and type.
func NewInternalKey(userKey util.Slice, seq SequenceNumber, t ValueType) InternalKey {
	var key ParsedInternalKey
	key.UserKey = userKey
	key.Sequence = seq
	key.Type = t
	var result []byte
	result = AppendInternalKey(result, &key)
	return InternalKey{rep: result}
}

// Encode returns the encoded form of the internal key.
func (k *InternalKey) Encode() util.Slice {
	return util.MakeSlice(k.rep)
}

// DecodeFrom decodes an internal key from s.
func (k *InternalKey) DecodeFrom(s util.Slice) bool {
	if s.Size() < 8 {
		return false
	}
	k.rep = make([]byte, s.Size())
	copy(k.rep, s.Data())
	return true
}

// UserKey returns the user key portion.
func (k *InternalKey) UserKey() util.Slice {
	return ExtractUserKey(k.Encode())
}

// Clear clears the internal key.
func (k *InternalKey) Clear() {
	k.rep = nil
}

// Compare compares two internal keys using the provided comparator.
// Returns -1, 0, or +1.
// Keys are compared by user key first (using userComparator), then by
// decreasing sequence number.
func (k *InternalKey) Compare(other *InternalKey, userComparator util.Comparator) int {
	a := k.Encode()
	b := other.Encode()
	return CompareInternalKeys(a, b, userComparator)
}

// CompareInternalKeys compares two encoded internal keys.
func CompareInternalKeys(a, b util.Slice, userComparator util.Comparator) int {
	// First compare user keys
	userA := ExtractUserKey(a)
	userB := ExtractUserKey(b)
	cmp := userComparator.Compare(userA, userB)
	if cmp != 0 {
		return cmp
	}
	// Same user key: compare sequence numbers (higher sequence first)
	n := a.Size()
	seqA := util.DecodeFixed64(a.Data()[n-8:])
	seqB := util.DecodeFixed64(b.Data()[n-8:])
	if seqA > seqB {
		return -1
	}
	if seqA < seqB {
		return 1
	}
	return 0
}

// InternalKeyComparator wraps a user comparator for internal keys.
type InternalKeyComparator struct {
	user util.Comparator
}

// NewInternalKeyComparator creates a new internal key comparator.
func NewInternalKeyComparator(user util.Comparator) *InternalKeyComparator {
	return &InternalKeyComparator{user: user}
}

// Name returns the comparator name.
func (c *InternalKeyComparator) Name() string {
	return "leveldb.InternalKeyComparator"
}

// Compare compares two encoded internal keys.
func (c *InternalKeyComparator) Compare(a, b util.Slice) int {
	return CompareInternalKeys(a, b, c.user)
}

// UserComparator returns the underlying user comparator.
func (c *InternalKeyComparator) UserComparator() util.Comparator {
	return c.user
}

// FindShortestSeparator finds a separator between start and limit.
// Delegates to user comparator for the user key portion.
func (c *InternalKeyComparator) FindShortestSeparator(start, limit util.Slice) {
	// Extract user keys for separator finding
	startUser := ExtractUserKey(start)
	limitUser := ExtractUserKey(limit)
	c.user.FindShortestSeparator(startUser, limitUser)
}

// FindShortestSuccessor finds a short successor for key.
// Delegates to user comparator for the user key portion.
func (c *InternalKeyComparator) FindShortestSuccessor(key util.Slice) {
	keyUser := ExtractUserKey(key)
	c.user.FindShortestSuccessor(keyUser)
}
