package internal

import (
	"testing"
)

func TestMakeInternalKey(t *testing.T) {
	userKey := []byte("testkey")
	seq := uint64(100)
	vt := TypePut

	key := MakeInternalKey(userKey, seq, vt)

	// Check UserKey
	if got := key.UserKey(); !bytesEqual(got, userKey) {
		t.Errorf("UserKey() = %v, want %v", got, userKey)
	}

	// Check Sequence
	if got := key.Sequence(); got != seq {
		t.Errorf("Sequence() = %d, want %d", got, seq)
	}

	// Check Type
	if got := key.Type(); got != vt {
		t.Errorf("Type() = %v, want %v", got, vt)
	}
}

func TestInternalKeyFormat(t *testing.T) {
	userKey := []byte("abc")
	seq := uint64(0x123456789ABCDEF0)
	vt := TypeDelete

	key := MakeInternalKey(userKey, seq, vt)
	
	// Expected layout: "abc" + 8-byte BE seq + 1-byte type
	expectedLen := len(userKey) + SequenceSize + TypeSize
	if len(key.data) != expectedLen {
		t.Errorf("Key length = %d, want %d", len(key.data), expectedLen)
	}

	// Verify big-endian sequence encoding
	expectedSeq := uint64(0x123456789ABCDEF0)
	if got := key.Sequence(); got != expectedSeq {
		t.Errorf("Sequence = 0x%X, want 0x%X", got, expectedSeq)
	}
}

func TestInternalKeyUserKey(t *testing.T) {
	tests := []struct {
		name    string
		userKey []byte
		seq     uint64
		vt      ValueType
	}{
		{"empty key", []byte{}, 100, TypePut},
		{"short key", []byte("a"), 200, TypeDelete},
		{"long key", []byte("this is a longer user key"), 300, TypePut},
		{"binary key", []byte{0x00, 0xFF, 0x10, 0x20}, 400, TypeDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := MakeInternalKey(tt.userKey, tt.seq, tt.vt)
			if got := key.UserKey(); !bytesEqual(got, tt.userKey) {
				t.Errorf("UserKey() = %v, want %v", got, tt.userKey)
			}
		})
	}
}

func TestInternalKeySequence(t *testing.T) {
	tests := []uint64{0, 1, 127, 128, 255, 256, 1<<32 - 1, 1<<56, 1<<63 - 1}

	for _, seq := range tests {
		key := MakeInternalKey([]byte("test"), seq, TypePut)
		if got := key.Sequence(); got != seq {
			t.Errorf("Sequence() = %d for input %d", got, seq)
		}
	}
}

func TestInternalKeyType(t *testing.T) {
	tests := []ValueType{0, TypePut, TypeDelete, 255}

	for _, vt := range tests {
		key := MakeInternalKey([]byte("test"), 100, vt)
		if got := key.Type(); got != vt {
			t.Errorf("Type() = %d for input %d", got, vt)
		}
	}
}

func TestCompare(t *testing.T) {
	// Test user key comparison (ascending)
	t.Run("user key ascending", func(t *testing.T) {
		keyA := MakeInternalKey([]byte("aaa"), 100, TypePut)
		keyB := MakeInternalKey([]byte("bbb"), 100, TypePut)

		if cmp := Compare(keyA, keyB); cmp >= 0 {
			t.Errorf("Compare(aaa, bbb) = %d, want < 0", cmp)
		}
		if cmp := Compare(keyB, keyA); cmp <= 0 {
			t.Errorf("Compare(bbb, aaa) = %d, want > 0", cmp)
		}
	})

	// Test sequence comparison (descending - higher first)
	t.Run("sequence descending", func(t *testing.T) {
		keyA := MakeInternalKey([]byte("test"), 200, TypePut) // Higher sequence
		keyB := MakeInternalKey([]byte("test"), 100, TypePut) // Lower sequence

		if cmp := Compare(keyA, keyB); cmp >= 0 {
			t.Errorf("Compare(high_seq, low_seq) = %d, want < 0", cmp)
		}
		if cmp := Compare(keyB, keyA); cmp <= 0 {
			t.Errorf("Compare(low_seq, high_seq) = %d, want > 0", cmp)
		}
	})

	// Test equality
	t.Run("equal keys", func(t *testing.T) {
		keyA := MakeInternalKey([]byte("test"), 100, TypePut)
		keyB := MakeInternalKey([]byte("test"), 100, TypePut)

		if cmp := Compare(keyA, keyB); cmp != 0 {
			t.Errorf("Compare(equal, equal) = %d, want 0", cmp)
		}
	})

	// Test with different types when user key and sequence are same
	t.Run("type comparison", func(t *testing.T) {
		keyA := MakeInternalKey([]byte("test"), 100, TypePut)
		keyB := MakeInternalKey([]byte("test"), 100, TypeDelete)

		if cmp := Compare(keyA, keyB); cmp >= 0 {
			t.Errorf("Compare(TypePut, TypeDelete) = %d, want < 0", cmp)
		}
	})

	// Test prefix ordering with different sequences
	t.Run("prefix with different sequences", func(t *testing.T) {
		keyA := MakeInternalKey([]byte("aa"), 100, TypePut)
		keyB := MakeInternalKey([]byte("aaa"), 50, TypePut)

		// "aa" < "aaa" lexicographically
		if cmp := Compare(keyA, keyB); cmp >= 0 {
			t.Errorf("Compare(aa, aaa) = %d, want < 0", cmp)
		}
	})

	// Test sequence dominates over type
	t.Run("sequence dominates type", func(t *testing.T) {
		// Same user key, different sequences
		keyA := MakeInternalKey([]byte("test"), 200, TypeDelete) // High seq, delete
		keyB := MakeInternalKey([]byte("test"), 100, TypePut)     // Low seq, put

		// High sequence should come first regardless of type
		if cmp := Compare(keyA, keyB); cmp >= 0 {
			t.Errorf("Compare(high_seq+delete, low_seq+put) = %d, want < 0", cmp)
		}
	})
}

func TestCompareStability(t *testing.T) {
	keys := []InternalKey{
		MakeInternalKey([]byte("c"), 100, TypePut),
		MakeInternalKey([]byte("a"), 200, TypePut),
		MakeInternalKey([]byte("b"), 150, TypeDelete),
		MakeInternalKey([]byte("a"), 100, TypePut),
		MakeInternalKey([]byte("c"), 200, TypePut),
	}

	// Verify comparison is consistent
	for i := 0; i < len(keys); i++ {
		for j := 0; j < len(keys); j++ {
			cmp1 := Compare(keys[i], keys[j])
			cmp2 := Compare(keys[i], keys[j])
			if cmp1 != cmp2 {
				t.Errorf("Compare not stable: Compare(%d, %d) = %d then %d", i, j, cmp1, cmp2)
			}
		}
	}
}

func TestConstants(t *testing.T) {
	if SequenceSize != 8 {
		t.Errorf("SequenceSize = %d, want 8", SequenceSize)
	}
	if TypeSize != 1 {
		t.Errorf("TypeSize = %d, want 1", TypeSize)
	}
	if InternalKeyOverhead != 9 {
		t.Errorf("InternalKeyOverhead = %d, want 9", InternalKeyOverhead)
	}
}


// TestEmptyKeyUserKeyNotNil specifically verifies UserKey() returns non-nil empty slice
func TestEmptyKeyUserKeyNotNil(t *testing.T) {
	key := MakeInternalKey([]byte{}, 100, TypePut)
	got := key.UserKey()
	
	if got == nil {
		t.Error("UserKey() returned nil, must return []byte{} for empty key")
	}
	if len(got) != 0 {
		t.Errorf("UserKey() len = %d, want 0", len(got))
	}
}
