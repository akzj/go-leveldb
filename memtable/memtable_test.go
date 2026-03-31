package memtable

import (
	"bytes"
	"testing"

	"github.com/akzj/go-leveldb/internal"
)

func TestMemTableEmpty(t *testing.T) {
	mt := NewMemTable()

	if mt.Size() != 0 {
		t.Errorf("expected size 0, got %d", mt.Size())
	}

	_, err := mt.Get([]byte("nonexistent"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemTablePutGet(t *testing.T) {
	mt := NewMemTable()

	key := internal.MakeInternalKey([]byte("key1"), 1, internal.TypePut)
	mt.Put(key, []byte("value1"))

	if mt.Size() != 1 {
		t.Errorf("expected size 1, got %d", mt.Size())
	}

	val, err := mt.Get([]byte("key1"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got '%s'", val)
	}
}

func TestMemTableMultipleKeys(t *testing.T) {
	mt := NewMemTable()

	// Insert multiple keys
	mt.Put(internal.MakeInternalKey([]byte("key1"), 1, internal.TypePut), []byte("val1"))
	mt.Put(internal.MakeInternalKey([]byte("key2"), 1, internal.TypePut), []byte("val2"))
	mt.Put(internal.MakeInternalKey([]byte("key3"), 1, internal.TypePut), []byte("val3"))

	if mt.Size() != 3 {
		t.Errorf("expected size 3, got %d", mt.Size())
	}

	// Get each key
	for i := 1; i <= 3; i++ {
		key := []byte("key" + string(rune('0'+i)))
		val, err := mt.Get(key)
		if err != nil {
			t.Errorf("Get(%s) unexpected error: %v", key, err)
		}
		expected := []byte("val" + string(rune('0'+i)))
		if !bytes.Equal(val, expected) {
			t.Errorf("Get(%s) expected %s, got %s", key, expected, val)
		}
	}
}

func TestMemTableLatestVersion(t *testing.T) {
	mt := NewMemTable()

	userKey := []byte("samekey")

	// Insert same key with different sequences (higher seq = newer)
	mt.Put(internal.MakeInternalKey(userKey, 1, internal.TypePut), []byte("v1"))
	mt.Put(internal.MakeInternalKey(userKey, 2, internal.TypePut), []byte("v2"))
	mt.Put(internal.MakeInternalKey(userKey, 3, internal.TypePut), []byte("v3"))

	if mt.Size() != 3 {
		t.Errorf("expected size 3, got %d", mt.Size())
	}

	// Get should return latest version (highest seq)
	val, err := mt.Get(userKey)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if string(val) != "v3" {
		t.Errorf("expected 'v3' (latest), got '%s'", val)
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := NewMemTable()

	userKey := []byte("todelete")

	// Insert a value
	mt.Put(internal.MakeInternalKey(userKey, 1, internal.TypePut), []byte("value"))

	// Verify it exists
	val, err := mt.Get(userKey)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("expected 'value', got '%s'", val)
	}

	// Delete it (insert TypeDelete)
	mt.Put(internal.MakeInternalKey(userKey, 2, internal.TypeDelete), nil)

	// Get should return ErrNotFound
	_, err = mt.Get(userKey)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestMemTableDeleteWithExistingVersions(t *testing.T) {
	mt := NewMemTable()

	userKey := []byte("key")

	// Insert multiple versions
	mt.Put(internal.MakeInternalKey(userKey, 1, internal.TypePut), []byte("v1"))
	mt.Put(internal.MakeInternalKey(userKey, 2, internal.TypePut), []byte("v2"))

	// Delete with higher sequence
	mt.Put(internal.MakeInternalKey(userKey, 3, internal.TypeDelete), nil)

	// Get should return ErrNotFound (delete is latest)
	_, err := mt.Get(userKey)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemTableApproximateSize(t *testing.T) {
	mt := NewMemTable()

	// Initially empty
	if mt.ApproximateSize() != 0 {
		t.Errorf("expected size 0, got %d", mt.ApproximateSize())
	}

	// Add entry
	mt.Put(internal.MakeInternalKey([]byte("key"), 1, internal.TypePut), []byte("value"))

	// Size should include: len("key") + InternalKeyOverhead + len("value")
	expected := len("key") + internal.InternalKeyOverhead + len("value")
	if mt.ApproximateSize() != expected {
		t.Errorf("expected size %d, got %d", expected, mt.ApproximateSize())
	}
}

func TestMemTableIterator(t *testing.T) {
	mt := NewMemTable()

	// Insert in random order
	keys := []string{"banana", "apple", "cherry"}
	for i, k := range keys {
		mt.Put(internal.MakeInternalKey([]byte(k), uint64(i+1), internal.TypePut), []byte("val_"+k))
	}

	it := mt.NewIterator()

	// Iterate should return sorted order by user key
	expected := []string{"apple", "banana", "cherry"}
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if i >= len(expected) {
			t.Error("too many elements")
			break
		}
		got := string(it.Key().UserKey())
		if got != expected[i] {
			t.Errorf("at position %d: expected %s, got %s", i, expected[i], got)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("expected %d elements, got %d", len(expected), i)
	}
}

func TestMemTableNotFound(t *testing.T) {
	mt := NewMemTable()

	// Insert some keys
	mt.Put(internal.MakeInternalKey([]byte("key1"), 1, internal.TypePut), []byte("value1"))

	// Get non-existent key
	_, err := mt.Get([]byte("key2"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// Get empty key
	_, err = mt.Get([]byte(""))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for empty key, got %v", err)
	}
}

func TestMemTableGetAcrossVersions(t *testing.T) {
	mt := NewMemTable()

	// Insert different keys with interleaved sequences
	mt.Put(internal.MakeInternalKey([]byte("a"), 3, internal.TypePut), []byte("a_v3"))
	mt.Put(internal.MakeInternalKey([]byte("b"), 1, internal.TypePut), []byte("b_v1"))
	mt.Put(internal.MakeInternalKey([]byte("a"), 1, internal.TypePut), []byte("a_v1"))
	mt.Put(internal.MakeInternalKey([]byte("b"), 2, internal.TypePut), []byte("b_v2"))

	// Get 'a' should return latest (seq=3)
	val, err := mt.Get([]byte("a"))
	if err != nil {
		t.Errorf("Get(a) error: %v", err)
	}
	if string(val) != "a_v3" {
		t.Errorf("Get(a) expected 'a_v3', got '%s'", val)
	}

	// Get 'b' should return latest (seq=2)
	val, err = mt.Get([]byte("b"))
	if err != nil {
		t.Errorf("Get(b) error: %v", err)
	}
	if string(val) != "b_v2" {
		t.Errorf("Get(b) expected 'b_v2', got '%s'", val)
	}
}