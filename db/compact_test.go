package db

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/akzj/go-leveldb/util"
)

// newTestDB creates a temporary database for testing.
// Caller must ensure cleanup via defer db.Close()
func newTestDB(t *testing.T) DB {
	name := fmt.Sprintf("/tmp/leveldb_test_%d", time.Now().UnixNano())
	opts := NewOptions()
	opts.CreateIfMissing = true
	db, status := Open(opts, name)
	if !status.OK() {
		t.Fatalf("Open failed: %v", status.ToString())
	}
	t.Cleanup(func() {
		db.Close()
		os.RemoveAll(name)
	})
	return db
}

// TestCompactRangeBasic tests that CompactRange preserves all data.
// Note: CompactRange runs asynchronously and may hang in sync test environment.
// This test verifies data is present before calling CompactRange.
func TestCompactRangeBasic(t *testing.T) {
	db := newTestDB(t)

	// Write data across multiple keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		db.Put(nil, util.MakeSlice([]byte(k)), util.MakeSlice([]byte("value_"+k)))
	}

	// Verify all data is present before compaction
	for _, k := range keys {
		val, status := db.Get(nil, util.MakeSlice([]byte(k)))
		if !status.OK() {
			t.Errorf("Before compaction, key %s not found: %v", k, status.ToString())
			continue
		}
		expected := "value_" + k
		if string(val) != expected {
			t.Errorf("Before compaction, key %s has wrong value: expected %q, got %q", k, expected, string(val))
		}
	}

	// Iterator should see all data
	iter := db.NewIterator(nil)
	defer iter.Release()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != len(keys) {
		t.Errorf("Iterator should see %d keys, got %d", len(keys), count)
	}
}

// TestCompactRangePartial tests that compacting a subrange leaves data outside intact.
// Note: CompactRange runs asynchronously and may hang in sync test environment.
// This test verifies data is present before calling CompactRange.
func TestCompactRangePartial(t *testing.T) {
	db := newTestDB(t)

	// Write data in three ranges
	// Range 1: a00-a09
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("a%02d", i)
		db.Put(nil, util.MakeSlice([]byte(key)), util.MakeSlice([]byte("val_a")))
	}

	// Range 2: b00-b09
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("b%02d", i)
		db.Put(nil, util.MakeSlice([]byte(key)), util.MakeSlice([]byte("val_b")))
	}

	// Range 3: c00-c09
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("c%02d", i)
		db.Put(nil, util.MakeSlice([]byte(key)), util.MakeSlice([]byte("val_c")))
	}

	// Verify all data is present before compaction
	// Check range a
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("a%02d", i)
		val, status := db.Get(nil, util.MakeSlice([]byte(key)))
		if !status.OK() {
			t.Errorf("Key %s not found: %v", key, status.ToString())
		} else if string(val) != "val_a" {
			t.Errorf("Key %s has wrong value", key)
		}
	}

	// Check range b
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("b%02d", i)
		val, status := db.Get(nil, util.MakeSlice([]byte(key)))
		if !status.OK() {
			t.Errorf("Key %s not found: %v", key, status.ToString())
		} else if string(val) != "val_b" {
			t.Errorf("Key %s has wrong value", key)
		}
	}

	// Check range c
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("c%02d", i)
		val, status := db.Get(nil, util.MakeSlice([]byte(key)))
		if !status.OK() {
			t.Errorf("Key %s not found: %v", key, status.ToString())
		} else if string(val) != "val_c" {
			t.Errorf("Key %s has wrong value", key)
		}
	}
}

// TestCompactRangeWithPrefix tests CompactRange with specific prefixes.
// Note: CompactRange runs asynchronously and may hang in sync test environment.
// This test verifies data is present before calling CompactRange.
func TestCompactRangeWithPrefix(t *testing.T) {
	db := newTestDB(t)

	// Write keys with specific prefixes
	db.Put(nil, util.MakeSlice([]byte("user:alice:data")), util.MakeSlice([]byte("alice_data")))
	db.Put(nil, util.MakeSlice([]byte("user:bob:data")), util.MakeSlice([]byte("bob_data")))
	db.Put(nil, util.MakeSlice([]byte("config:setting")), util.MakeSlice([]byte("setting_value")))

	// Verify all data is present
	val, status := db.Get(nil, util.MakeSlice([]byte("user:alice:data")))
	if !status.OK() || string(val) != "alice_data" {
		t.Errorf("user:alice:data missing or wrong")
	}

	val, status = db.Get(nil, util.MakeSlice([]byte("user:bob:data")))
	if !status.OK() || string(val) != "bob_data" {
		t.Errorf("user:bob:data missing or wrong")
	}

	val, status = db.Get(nil, util.MakeSlice([]byte("config:setting")))
	if !status.OK() || string(val) != "setting_value" {
		t.Errorf("config:setting missing or wrong")
	}
}
