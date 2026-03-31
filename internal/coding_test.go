package internal

import (
	"testing"
)

func TestPutUvarint(t *testing.T) {
	tests := []struct {
		x    uint64
		want []byte
	}{
		{0, []byte{0}},
		{1, []byte{1}},
		{127, []byte{127}},
		{128, []byte{128, 1}},                  // 128 = 0x80, little-endian base-128
		{300, []byte{172, 2}},                   // 300 = 0x12C = 2*128 + 44
		{1<<63 - 1, []byte{255, 255, 255, 255, 255, 255, 255, 255, 127}},
	}

	for _, tt := range tests {
		buf := make([]byte, 15)
		n := PutUvarint(buf, tt.x)
		if got := buf[:n]; !bytesEqual(got, tt.want) {
			t.Errorf("PutUvarint(%d) = %v, want %v", tt.x, got, tt.want)
		}
	}
}

func TestUvarint(t *testing.T) {
	tests := []struct {
		buf  []byte
		want uint64
		n    int
	}{
		{[]byte{0}, 0, 1},
		{[]byte{1}, 1, 1},
		{[]byte{127}, 127, 1},
		{[]byte{128, 1}, 128, 2},
		{[]byte{172, 2}, 300, 2},
	}

	for _, tt := range tests {
		x, n := Uvarint(tt.buf)
		if x != tt.want || n != tt.n {
			t.Errorf("Uvarint(%v) = (%d, %d), want (%d, %d)", tt.buf, x, n, tt.want, tt.n)
		}
	}
}

func TestAppendUvarint(t *testing.T) {
	tests := []struct {
		dst  []byte
		x    uint64
		want []byte
	}{
		{nil, 0, []byte{0}},
		{nil, 1, []byte{1}},
		{[]byte{10, 20}, 300, []byte{10, 20, 172, 2}},
		{[]byte{1, 2, 3}, 0, []byte{1, 2, 3, 0}},
	}

	for _, tt := range tests {
		got := AppendUvarint(tt.dst, tt.x)
		if !bytesEqual(got, tt.want) {
			t.Errorf("AppendUvarint(%v, %d) = %v, want %v", tt.dst, tt.x, got, tt.want)
		}
	}
}

func TestVarintRoundtrip(t *testing.T) {
	values := []uint64{0, 1, 127, 128, 255, 256, 1000, 10000, 1<<32 - 1, 1 << 56, 1<<63 - 1}

	for _, x := range values {
		// Encode
		var buf []byte
		buf = AppendUvarint(buf, x)

		// Decode
		decoded, n := Uvarint(buf)
		if decoded != x {
			t.Errorf("Roundtrip for %d: decoded = %d, n = %d", x, decoded, n)
		}
	}
}

// bytesEqual compares two byte slices, treating nil and empty slice as equal.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}