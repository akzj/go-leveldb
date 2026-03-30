package util

import (
	"hash/crc32"
)

// CRC32C constants and functions.
// This package provides CRC32C (Castagnoli) checksum, used by LevelDB
// for block verification. The polynomial used is 0x82F63B78 (Castagnoli).

// kMaskDelta is added to CRC to avoid embedded CRC issues.
// See C++ LevelDB's crc32c::Mask.
const kMaskDelta uint32 = 0xa282ead8

// Extend updates a running CRC with data[0:n-1] using the Castagnoli polynomial.
// This is equivalent to crc32c.Value(crc32c.Extend(crc, data, n)).
func Extend(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, crc32.MakeTable(crc32.Castagnoli), data)
}

// Value returns the CRC32C of data[0:n-1].
func Value(data []byte) uint32 {
	return crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), data)
}

// Mask returns a masked representation of crc.
//
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs. Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
func Mask(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + kMaskDelta
}

// Unmask returns the crc whose masked representation is maskedCrc.
func Unmask(maskedCrc uint32) uint32 {
	rot := maskedCrc - kMaskDelta
	return ((rot >> 17) | (rot << 15))
}

// FastCRC32 uses hardware acceleration when available.
// This is a wrapper that provides the same interface as the C++ version.
type FastCRC32 struct{}

func NewFastCRC32() *FastCRC32 {
	return &FastCRC32{}
}

// Update computes CRC32C using the Castagnoli polynomial.
func (c *FastCRC32) Update(crc uint32, data []byte) uint32 {
	return Extend(crc, data)
}
