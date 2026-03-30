package util

// Binary encoding/decoding utilities.
// All formats are endian-neutral and match C++ LevelDB exactly.

// EncodeFixed32 encodes v into dst[0:4] in little-endian order.
// Requires dst to have at least 4 bytes.
func EncodeFixed32(dst []byte, v uint32) {
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
	dst[2] = byte(v >> 16)
	dst[3] = byte(v >> 24)
}

// EncodeFixed64 encodes v into dst[0:8] in little-endian order.
// Requires dst to have at least 8 bytes.
func EncodeFixed64(dst []byte, v uint64) {
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
	dst[2] = byte(v >> 16)
	dst[3] = byte(v >> 24)
	dst[4] = byte(v >> 32)
	dst[5] = byte(v >> 40)
	dst[6] = byte(v >> 48)
	dst[7] = byte(v >> 56)
}

// DecodeFixed32 decodes 4 bytes from src[0:4] as little-endian uint32.
// Requires src to have at least 4 bytes.
func DecodeFixed32(src []byte) uint32 {
	return uint32(src[0]) |
		uint32(src[1])<<8 |
		uint32(src[2])<<16 |
		uint32(src[3])<<24
}

// DecodeFixed64 decodes 8 bytes from src[0:8] as little-endian uint64.
// Requires src to have at least 8 bytes.
func DecodeFixed64(src []byte) uint64 {
	return uint64(src[0]) |
		uint64(src[1])<<8 |
		uint64(src[2])<<16 |
		uint64(src[3])<<24 |
		uint64(src[4])<<32 |
		uint64(src[5])<<40 |
		uint64(src[6])<<48 |
		uint64(src[7])<<56
}

// MaxVarint32Length is the maximum length of a varint32 encoding.
const MaxVarint32Length = 5

// MaxVarint64Length is the maximum length of a varint64 encoding.
const MaxVarint64Length = 10

// EncodeVarint32 encodes v into dst and returns the bytes written.
// Requires dst to have at least MaxVarint32Length bytes.
func EncodeVarint32(dst []byte, v uint32) int {
	// Hand-optimized inline version of varint encoding.
	if v < 1<<7 {
		dst[0] = byte(v)
		return 1
	}
	if v < 1<<14 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte(v >> 7)
		return 2
	}
	if v < 1<<21 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte(v >> 14)
		return 3
	}
	if v < 1<<28 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte((v >> 14) | 0x80)
		dst[3] = byte(v >> 21)
		return 4
	}
	dst[0] = byte(v | 0x80)
	dst[1] = byte((v >> 7) | 0x80)
	dst[2] = byte((v >> 14) | 0x80)
	dst[3] = byte((v >> 21) | 0x80)
	dst[4] = byte(v >> 28)
	return 5
}

// EncodeVarint64 encodes v into dst and returns the bytes written.
// Requires dst to have at least MaxVarint64Length bytes.
func EncodeVarint64(dst []byte, v uint64) int {
	if v < 1<<7 {
		dst[0] = byte(v)
		return 1
	}
	if v < 1<<14 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte(v >> 7)
		return 2
	}
	if v < 1<<21 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte(v >> 14)
		return 3
	}
	if v < 1<<28 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte((v >> 14) | 0x80)
		dst[3] = byte(v >> 21)
		return 4
	}
	if v < 1<<35 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte((v >> 14) | 0x80)
		dst[3] = byte((v >> 21) | 0x80)
		dst[4] = byte(v >> 28)
		return 5
	}
	if v < 1<<42 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte((v >> 14) | 0x80)
		dst[3] = byte((v >> 21) | 0x80)
		dst[4] = byte((v >> 28) | 0x80)
		dst[5] = byte(v >> 35)
		return 6
	}
	if v < 1<<49 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte((v >> 14) | 0x80)
		dst[3] = byte((v >> 21) | 0x80)
		dst[4] = byte((v >> 28) | 0x80)
		dst[5] = byte((v >> 35) | 0x80)
		dst[6] = byte(v >> 42)
		return 7
	}
	if v < 1<<56 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte((v >> 14) | 0x80)
		dst[3] = byte((v >> 21) | 0x80)
		dst[4] = byte((v >> 28) | 0x80)
		dst[5] = byte((v >> 35) | 0x80)
		dst[6] = byte((v >> 42) | 0x80)
		dst[7] = byte(v >> 49)
		return 8
	}
	if v < 1<<63 {
		dst[0] = byte(v | 0x80)
		dst[1] = byte((v >> 7) | 0x80)
		dst[2] = byte((v >> 14) | 0x80)
		dst[3] = byte((v >> 21) | 0x80)
		dst[4] = byte((v >> 28) | 0x80)
		dst[5] = byte((v >> 35) | 0x80)
		dst[6] = byte((v >> 42) | 0x80)
		dst[7] = byte((v >> 49) | 0x80)
		dst[8] = byte(v >> 56)
		return 9
	}
	dst[0] = byte(v | 0x80)
	dst[1] = byte((v >> 7) | 0x80)
	dst[2] = byte((v >> 14) | 0x80)
	dst[3] = byte((v >> 21) | 0x80)
	dst[4] = byte((v >> 28) | 0x80)
	dst[5] = byte((v >> 35) | 0x80)
	dst[6] = byte((v >> 42) | 0x80)
	dst[7] = byte((v >> 49) | 0x80)
	dst[8] = byte((v >> 56) | 0x80)
	dst[9] = byte(v >> 63)
	return 10
}

// VarintLength returns the encoding length of v.
func VarintLength(v uint64) int {
	if v < 1<<7 {
		return 1
	}
	if v < 1<<14 {
		return 2
	}
	if v < 1<<21 {
		return 3
	}
	if v < 1<<28 {
		return 4
	}
	if v < 1<<35 {
		return 5
	}
	if v < 1<<42 {
		return 6
	}
	if v < 1<<49 {
		return 7
	}
	if v < 1<<56 {
		return 8
	}
	if v < 1<<63 {
		return 9
	}
	return 10
}

// Decodes varint32 from p and stores result in v.
// Returns (bytesRead, ok). If ok is false, v is undefined.
func DecodeVarint32(p []byte) (v uint32, n int, ok bool) {
	var result uint32
	var shift uint
	for i := 0; i < len(p) && i < MaxVarint32Length; i++ {
		b := p[i]
		if b < 0x80 {
			if i == MaxVarint32Length-1 && b > 1 {
				return 0, 0, false
			}
			return result | uint32(b)<<shift, i + 1, true
		}
		result |= uint32(b&0x7f) << shift
		shift += 7
	}
	return 0, 0, false
}

// Decodes varint64 from p and stores result in v.
// Returns (bytesRead, ok). If ok is false, v is undefined.
func DecodeVarint64(p []byte) (v uint64, n int, ok bool) {
	var result uint64
	var shift uint
	for i := 0; i < len(p) && i < MaxVarint64Length; i++ {
		b := p[i]
		if b < 0x80 {
			if i == MaxVarint64Length-1 && b > 1 {
				return 0, 0, false
			}
			return result | uint64(b)<<shift, i + 1, true
		}
		result |= uint64(b&0x7f) << shift
		shift += 7
	}
	return 0, 0, false
}

// PutFixed32 appends v to dst as 4-byte little-endian.
func PutFixed32(dst []byte, v uint32) []byte {
	origLen := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	EncodeFixed32(dst[origLen:], v)
	return dst
}

// PutFixed64 appends v to dst as 8-byte little-endian.
func PutFixed64(dst []byte, v uint64) []byte {
	origLen := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0)
	EncodeFixed64(dst[origLen:], v)
	return dst
}

// PutVarint32 appends varint-encoded v to dst.
func PutVarint32(dst []byte, v uint32) []byte {
	enc := make([]byte, MaxVarint32Length)
	n := EncodeVarint32(enc, v)
	return append(dst, enc[:n]...)
}

// PutVarint64 appends varint-encoded v to dst.
func PutVarint64(dst []byte, v uint64) []byte {
	enc := make([]byte, MaxVarint64Length)
	n := EncodeVarint64(enc, v)
	return append(dst, enc[:n]...)
}

// PutLengthPrefixedSlice appends len(s) as varint followed by s to dst.
// This matches C++ LevelDB encoding format.
func PutLengthPrefixedSlice(dst []byte, s Slice) []byte {
	dst = PutVarint32(dst, uint32(s.Size()))
	return append(dst, s.Data()...)
}

// GetLengthPrefixedSlice decodes a length-prefixed slice from p.
// Returns (slice, bytesConsumed, ok).
func GetLengthPrefixedSlice(p []byte) (Slice, int, bool) {
	v, n, ok := DecodeVarint32(p)
	// Invariant: v bytes of data starting at offset n must fit in p
	if !ok || n+int(v) > len(p) {
		return Slice{}, 0, false
	}
	return Slice{data: p[n : n+int(v)]}, n + int(v), true
}
