// Package util provides core data types and utilities for LevelDB.
// Binary format compatible with C++ LevelDB v1.23.
package util

import (
	"bytes"
	"errors"
)

// Slice is a simple structure containing a pointer into some external
// storage and a size. The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.
type Slice struct {
	data []byte
}

// MakeSlice creates a slice that refers to d[0:n-1].
// Why not *[]byte? Caller may pass subslice; we need independent length tracking.
func MakeSlice(data []byte) Slice {
	return Slice{data: data}
}

// MakeSliceFromString creates a slice from string content.
func MakeSliceFromString(s string) Slice {
	return Slice{data: []byte(s)}
}

// MakeSliceFromStr creates a slice that refers to s[0:len(s)].
// Invariant: caller must ensure underlying string memory is valid.
func MakeSliceFromStr(s string) Slice {
	return Slice{data: []byte(s)}
}

// Data returns a pointer to the beginning of the referenced data.
func (s Slice) Data() []byte {
	return s.data
}

// DataString returns data as string (copies).
func (s Slice) DataString() string {
	return string(s.data)
}

// Size returns the length (in bytes) of the referenced data.
func (s Slice) Size() int {
	return len(s.data)
}

// Empty returns true iff the length of the referenced data is zero.
func (s Slice) Empty() bool {
	return len(s.data) == 0
}

// At returns the ith byte in the referenced data.
// Panics if n >= size().
func (s Slice) At(n int) byte {
	if n >= len(s.data) {
		panic("index out of range")
	}
	return s.data[n]
}

// Clear changes this slice to refer to an empty array.
func (s *Slice) Clear() {
	s.data = nil
}

// RemovePrefix drops the first n bytes from this slice.
// Panics if n > size().
func (s *Slice) RemovePrefix(n int) {
	if n > len(s.data) {
		panic("prefix larger than slice")
	}
	s.data = s.data[n:]
}

// ToString returns a string that contains the copy of the referenced data.
func (s Slice) ToString() string {
	return string(s.data)
}

// Compare returns -1, 0, or +1 depending on whether s < b, s == b, or s > b.
func (s Slice) Compare(b Slice) int {
	return bytes.Compare(s.data, b.data)
}

// StartsWith returns true iff x is a prefix of *this.
func (s Slice) StartsWith(x Slice) bool {
	if len(s.data) < len(x.data) {
		return false
	}
	return bytes.HasPrefix(s.data, x.data)
}

// Equal returns true iff x and y have the same content.
func (s Slice) Equal(x Slice) bool {
	return bytes.Equal(s.data, x.data)
}

// NotEqual returns true iff s and x have different content.
func (s Slice) NotEqual(x Slice) bool {
	return !bytes.Equal(s.data, x.data)
}

// String implements fmt.Stringer.
func (s Slice) String() string {
	return s.ToString()
}

// SplitUnsafe splits at position i without bounds checking.
// Returns (prefix, suffix) where prefix=d[0:i], suffix=d[i:].
// Invariant: caller must ensure 0 <= i <= len(data).
// Why not return error? Performance - this is hot path.
func (s Slice) SplitUnsafe(i int) (Slice, Slice) {
	return Slice{data: s.data[:i]}, Slice{data: s.data[i:]}
}

// Subslice returns a new slice referring to s[offset:offset+size].
// Panics if offset+size > len(data).
func (s Slice) Subslice(offset, size int) Slice {
	if offset+size > len(s.data) {
		panic("subslice out of bounds")
	}
	return Slice{data: s.data[offset : offset+size]}
}

// CopyTo copies len(s.data) bytes to dst.
// Returns number of bytes copied.
func (s Slice) CopyTo(dst []byte) int {
	return copy(dst, s.data)
}

// Errors for parsing operations
var (
	ErrSliceTooShort = errors.New("slice too short for operation")
)
