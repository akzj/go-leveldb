package util

import (
	"bytes"
)

// Comparator interface for comparing byte slices.
// This is the user-facing comparator interface.
type Comparator interface {
	// Compare compares two byte slices.
	// Returns -1, 0, or +1.
	Compare(a, b Slice) int

	// Name returns the comparator name.
	// Names starting with "leveldb." are reserved.
	Name() string

	// FindShortestSeparator modifies start to be a short separator between start and limit.
	FindShortestSeparator(start, limit Slice)

	// FindShortestSuccessor modifies key to be a short successor.
	FindShortestSuccessor(key Slice)
}

// DefaultComparator implements Comparator using lexicographic byte comparison.
type DefaultComparator struct{}

// Name implements Comparator.
func (c *DefaultComparator) Name() string {
	return "leveldb.BytewiseComparator"
}

// Compare implements Comparator.
func (c *DefaultComparator) Compare(a, b Slice) int {
	return bytes.Compare(a.Data(), b.Data())
}

// FindShortestSeparator implements Comparator.
func (c *DefaultComparator) FindShortestSeparator(start, limit Slice) {
	// Find common prefix
	s := start.Data()
	l := limit.Data()
	minLen := len(s)
	if len(l) < minLen {
		minLen = len(l)
	}
	common := 0
	for common < minLen && s[common] == l[common] {
		common++
	}
	if common >= minLen {
		return
	}
	prev := s[common]
	if prev < 0xff && prev+1 < l[common] {
		start = Slice{data: append(s[:common], prev+1)}
	}
}

// FindShortestSuccessor implements Comparator.
func (c *DefaultComparator) FindShortestSuccessor(key Slice) {
	k := key.Data()
	for i := 0; i < len(k); i++ {
		if k[i] != 0xff {
			k[i]++
			key = Slice{data: k[:i+1]}
			return
		}
	}
}

// DefaultBytewiseComparator returns the default bytewise comparator.
func DefaultBytewiseComparator() Comparator {
	return &DefaultComparator{}
}
