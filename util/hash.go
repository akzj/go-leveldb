package util

// Hash implements a hash function similar to MurmurHash.
// Used by BloomFilter and other internal data structures.
// Seed is used to allow multiple independent hash functions.
// Format matches C++ LevelDB util/hash.cc exactly.
func Hash(data []byte, seed uint32) uint32 {
	const m = uint32(0xc6a4a793)
	const r = uint32(24)
	h := seed ^ uint32(len(data))*m

	// Process 4 bytes at a time
	i := 0
	for ; i+4 <= len(data); i += 4 {
		w := DecodeFixed32(data[i:])
		h += w
		h *= m
		h ^= (h >> 16)
	}

	// Process remaining bytes
	switch len(data) - i {
	case 3:
		h += uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h += uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h += uint32(data[i])
		h *= m
		h ^= (h >> r)
	}

	return h
}
