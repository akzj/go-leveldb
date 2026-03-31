// Package internal provides low-level encoding utilities and internal key types for leveldb.
package internal

import (
	"encoding/binary"
)

// PutUvarint encodes x into buf and returns the number of bytes written.
// If the buffer is too small, PutUvarint will panic.
// This wraps encoding/binary.PutUvarint.
func PutUvarint(buf []byte, x uint64) int {
	return binary.PutUvarint(buf, x)
}

// Uvarint decodes a varint from buf and returns its value and the number
// of bytes read. If Uvarint returns n == 0, the read was a no-op.
// This wraps encoding/binary.Uvarint.
func Uvarint(buf []byte) (uint64, int) {
	return binary.Uvarint(buf)
}

// AppendUvarint appends the varint encoding of x to dst and returns the
// extended slice.
func AppendUvarint(dst []byte, x uint64) []byte {
	// Calculate required size (max 10 bytes for uint64)
	n := binary.PutUvarint(make([]byte, 10), x)
	// Grow dst if needed
	newDst := append(dst, make([]byte, n)...)
	binary.PutUvarint(newDst[len(dst):], x)
	return newDst
}