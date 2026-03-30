package util

import (
	"testing"
)

func TestEncodeDecodeFixed32(t *testing.T) {
	for _, v := range []uint32{0, 1, 100, 0xFFFFFFFF} {
		var buf [4]byte
		EncodeFixed32(buf[:], v)
		result := DecodeFixed32(buf[:])
		if result != v {
			t.Errorf("fixed32: expected %x, got %x", v, result)
		}
	}
}

func TestEncodeDecodeFixed64(t *testing.T) {
	for _, v := range []uint64{0, 1, 100, 0xFFFFFFFFFFFFFFFF} {
		var buf [8]byte
		EncodeFixed64(buf[:], v)
		result := DecodeFixed64(buf[:])
		if result != v {
			t.Errorf("fixed64: expected %x, got %x", v, result)
		}
	}
}

func TestVarint32(t *testing.T) {
	// Test values that fit in standard varint encoding
	testCases := []uint32{0, 1, 127, 128, 255, 1000, 10000, 100000, 1 << 24}
	for _, v := range testCases {
		var buf [MaxVarint32Length]byte
		n := EncodeVarint32(buf[:], v)
		result, n2, ok := DecodeVarint32(buf[:n])
		if !ok {
			t.Errorf("varint32 decode failed for %d", v)
		}
		if result != v {
			t.Errorf("varint32: expected %d, got %d", v, result)
		}
		if n != n2 {
			t.Errorf("varint32 length mismatch: wrote %d, read %d", n, n2)
		}
	}
}

func TestVarint64(t *testing.T) {
	// Test values that fit in standard varint encoding
	testCases := []uint64{0, 1, 127, 128, 255, 1000, 10000, 100000, 1 << 35, 1 << 56}
	for _, v := range testCases {
		var buf [MaxVarint64Length]byte
		n := EncodeVarint64(buf[:], v)
		result, n2, ok := DecodeVarint64(buf[:n])
		if !ok {
			t.Errorf("varint64 decode failed for %d", v)
		}
		if result != v {
			t.Errorf("varint64: expected %d, got %d", v, result)
		}
		if n != n2 {
			t.Errorf("varint64 length mismatch: wrote %d, read %d", n, n2)
		}
	}
}

func TestPutFixed(t *testing.T) {
	var buf []byte
	buf = PutFixed32(buf, 0x12345678)
	buf = PutFixed64(buf, 0x123456789ABCDEF0)

	if len(buf) != 12 {
		t.Errorf("expected 12 bytes, got %d", len(buf))
	}

	if DecodeFixed32(buf[:4]) != 0x12345678 {
		t.Error("fixed32 mismatch")
	}
	if DecodeFixed64(buf[4:]) != 0x123456789ABCDEF0 {
		t.Error("fixed64 mismatch")
	}
}

func TestPutVarint(t *testing.T) {
	var buf []byte
	buf = PutVarint32(buf, 300)
	buf = PutVarint64(buf, 1000000)

	result, n, ok := DecodeVarint32(buf)
	if !ok || result != 300 {
		t.Errorf("varint32 mismatch: got %d", result)
	}

	rest := buf[n:]
	result64, _, ok := DecodeVarint64(rest)
	if !ok || result64 != 1000000 {
		t.Errorf("varint64 mismatch: got %d", result64)
	}
}

func TestPutLengthPrefixedSlice(t *testing.T) {
	var buf []byte
	s := MakeSliceFromStr("hello world")
	buf = PutLengthPrefixedSlice(buf, s)

	result, n, ok := GetLengthPrefixedSlice(buf)
	if !ok {
		t.Fatal("GetLengthPrefixedSlice failed")
	}
	if n != len(buf) {
		t.Errorf("consumed %d bytes, expected %d", n, len(buf))
	}
	if string(result.Data()) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(result.Data()))
	}
}

func TestVarintLength(t *testing.T) {
	tests := []struct {
		v    uint64
		want int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
	}
	for _, tt := range tests {
		got := VarintLength(tt.v)
		if got != tt.want {
			t.Errorf("VarintLength(%d) = %d, want %d", tt.v, got, tt.want)
		}
	}
}
