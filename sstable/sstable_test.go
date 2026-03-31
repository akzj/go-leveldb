package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/go-leveldb/internal"
)

func TestRoundtrip(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	// Create writer
	w, err := NewWriter(path, 1024) // Small block size for testing
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Add entries in sorted order
	entries := []struct {
		key   internal.InternalKey
		value []byte
	}{
		{internal.MakeInternalKey([]byte("apple"), 100, internal.TypePut), []byte("value1")},
		{internal.MakeInternalKey([]byte("banana"), 100, internal.TypePut), []byte("value2")},
		{internal.MakeInternalKey([]byte("cherry"), 100, internal.TypePut), []byte("value3")},
		{internal.MakeInternalKey([]byte("date"), 100, internal.TypePut), []byte("value4")},
	}

	for _, e := range entries {
		if err := w.Add(e.key, e.value); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	// Finish writing
	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if meta.NumEntries != 4 {
		t.Errorf("NumEntries = %d, want 4", meta.NumEntries)
	}

	// Open reader
	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	// Test Get
	for _, e := range entries {
		got, err := r.Get(e.key.UserKey())
		if err != nil {
			t.Errorf("Get(%s): %v", e.key.UserKey(), err)
			continue
		}
		if string(got) != string(e.value) {
			t.Errorf("Get(%s) = %s, want %s", e.key.UserKey(), got, e.value)
		}
	}

	// Test Iterator
	iter := r.NewIterator()
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	if count != 4 {
		t.Errorf("Iterator count = %d, want 4", count)
	}
}

func TestBlockBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "boundary.sst")

	// Use small block size (100 bytes)
	blockSize := 100
	w, err := NewWriter(path, blockSize)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Add entries that will span multiple blocks
	entries := []struct {
		key   internal.InternalKey
		value []byte
	}{
		{internal.MakeInternalKey([]byte("key1"), 100, internal.TypePut), []byte("value1")},
		{internal.MakeInternalKey([]byte("key2"), 100, internal.TypePut), []byte("value2")},
		{internal.MakeInternalKey([]byte("key3"), 100, internal.TypePut), []byte("value3")},
		{internal.MakeInternalKey([]byte("key4"), 100, internal.TypePut), []byte("value4")},
		{internal.MakeInternalKey([]byte("key5"), 100, internal.TypePut), []byte("value5")},
	}

	for _, e := range entries {
		if err := w.Add(e.key, e.value); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if meta.NumDataBlocks < 1 {
		t.Errorf("NumDataBlocks = %d, want >= 1", meta.NumDataBlocks)
	}

	// Verify all entries can be read
	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	for _, e := range entries {
		got, err := r.Get(e.key.UserKey())
		if err != nil {
			t.Errorf("Get(%s): %v", e.key.UserKey(), err)
			continue
		}
		if string(got) != string(e.value) {
			t.Errorf("Get(%s) = %s, want %s", e.key.UserKey(), got, e.value)
		}
	}
}

func TestIteratorSeek(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "seek.sst")

	w, err := NewWriter(path, 1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	entries := []struct {
		key   internal.InternalKey
		value []byte
	}{
		{internal.MakeInternalKey([]byte("apple"), 100, internal.TypePut), []byte("1")},
		{internal.MakeInternalKey([]byte("banana"), 100, internal.TypePut), []byte("2")},
		{internal.MakeInternalKey([]byte("cherry"), 100, internal.TypePut), []byte("3")},
		{internal.MakeInternalKey([]byte("date"), 100, internal.TypePut), []byte("4")},
	}

	for _, e := range entries {
		if err := w.Add(e.key, e.value); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	if _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	iter := r.NewIterator()
	defer iter.Close()

	// Seek to middle
	target := internal.MakeInternalKey([]byte("cherry"), 100, internal.TypePut)
	if !iter.Seek(target) {
		t.Fatalf("Seek(cherry) failed")
	}
	if string(iter.Key().UserKey()) != "cherry" {
		t.Errorf("Seek found key = %s, want cherry", iter.Key().UserKey())
	}

	// Seek past end
	target = internal.MakeInternalKey([]byte("zebra"), 100, internal.TypePut)
	if iter.Seek(target) {
		t.Errorf("Seek(zebra) should return false")
	}

	// Seek before start
	target = internal.MakeInternalKey([]byte("aaa"), 100, internal.TypePut)
	if iter.Seek(target) {
		// Should position at first entry
		if !iter.Valid() || string(iter.Key().UserKey()) != "apple" {
			t.Errorf("Seek(aaa) should position at apple")
		}
	}
}

func TestIteratorFirstLast(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "firstlast.sst")

	w, err := NewWriter(path, 1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	entries := []struct {
		key   internal.InternalKey
		value []byte
	}{
		{internal.MakeInternalKey([]byte("apple"), 100, internal.TypePut), []byte("1")},
		{internal.MakeInternalKey([]byte("banana"), 100, internal.TypePut), []byte("2")},
		{internal.MakeInternalKey([]byte("cherry"), 100, internal.TypePut), []byte("3")},
	}

	for _, e := range entries {
		if err := w.Add(e.key, e.value); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	if _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	iter := r.NewIterator()
	defer iter.Close()

	// Test First
	if !iter.First() {
		t.Fatalf("First() should return true")
	}
	if !iter.Valid() {
		t.Fatalf("First() should make iterator valid")
	}
	if string(iter.Key().UserKey()) != "apple" {
		t.Errorf("First() key = %s, want apple", iter.Key().UserKey())
	}

	// Test Last
	if !iter.Last() {
		t.Fatalf("Last() should return true")
	}
	if !iter.Valid() {
		t.Fatalf("Last() should make iterator valid")
	}
	if string(iter.Key().UserKey()) != "cherry" {
		t.Errorf("Last() key = %s, want cherry", iter.Key().UserKey())
	}
}

func TestIteratorNextPrev(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nextprev.sst")

	w, err := NewWriter(path, 1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	entries := []struct {
		key   internal.InternalKey
		value []byte
	}{
		{internal.MakeInternalKey([]byte("apple"), 100, internal.TypePut), []byte("1")},
		{internal.MakeInternalKey([]byte("banana"), 100, internal.TypePut), []byte("2")},
		{internal.MakeInternalKey([]byte("cherry"), 100, internal.TypePut), []byte("3")},
	}

	for _, e := range entries {
		if err := w.Add(e.key, e.value); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	if _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	iter := r.NewIterator()
	defer iter.Close()

	// Test forward iteration
	expectedKeys := []string{"apple", "banana", "cherry"}
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if i >= len(expectedKeys) {
			t.Errorf("Too many entries")
			break
		}
		if string(iter.Key().UserKey()) != expectedKeys[i] {
			t.Errorf("Entry %d: key = %s, want %s", i, iter.Key().UserKey(), expectedKeys[i])
		}
		i++
	}
	if i != 3 {
		t.Errorf("Expected 3 entries, got %d", i)
	}

	// Test backward iteration
	i = 2
	for iter.Last(); iter.Valid(); iter.Prev() {
		if i < 0 {
			t.Errorf("Too many entries in reverse")
			break
		}
		if string(iter.Key().UserKey()) != expectedKeys[i] {
			t.Errorf("Reverse entry %d: key = %s, want %s", i, iter.Key().UserKey(), expectedKeys[i])
		}
		i--
	}
}

func TestAbort(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "abort.sst")

	w, err := NewWriter(path, 1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Add some data
	w.Add(internal.MakeInternalKey([]byte("test"), 100, internal.TypePut), []byte("value"))

	// Abort
	if err := w.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	// File should not exist
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("File should not exist after Abort")
	}
}

func TestMultipleVersions(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "versions.sst")

	w, err := NewWriter(path, 1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	userKey := []byte("samekey")

	// Add multiple versions with different sequences
	w.Add(internal.MakeInternalKey(userKey, 100, internal.TypePut), []byte("old"))
	w.Add(internal.MakeInternalKey(userKey, 200, internal.TypePut), []byte("new"))

	if _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	// Get should return the latest version (highest sequence)
	got, err := r.Get(userKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "new" {
		t.Errorf("Get latest = %s, want new", got)
	}
}

func TestEmptyTable(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.sst")

	w, err := NewWriter(path, 1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Finish without adding any entries
	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if meta.NumEntries != 0 {
		t.Errorf("NumEntries = %d, want 0", meta.NumEntries)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	iter := r.NewIterator()
	if iter.First() {
		t.Errorf("First() on empty table should return false")
	}
}

func TestDeleteType(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "delete.sst")

	w, err := NewWriter(path, 1024)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	userKey := []byte("delkey")

	// Add a put and a delete
	w.Add(internal.MakeInternalKey(userKey, 100, internal.TypePut), []byte("value"))
	w.Add(internal.MakeInternalKey(userKey, 200, internal.TypeDelete), []byte(""))

	if _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	// Get should still return the delete marker (empty value)
	got, err := r.Get(userKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Delete should return empty value, got %v", got)
	}

	// Iterator should show both entries
	iter := r.NewIterator()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Iterator count = %d, want 2", count)
	}
}