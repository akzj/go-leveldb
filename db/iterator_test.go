package db

import (
	"testing"

	"github.com/akzj/go-leveldb/util"
)



// TestIteratorSeek tests that Seek positions the iterator at the correct key.
// Invariant: After Seek(target), iterator is valid at first key >= target.
func TestIteratorSeek(t *testing.T) {
	db := newTestDB(t)

	// Write sorted keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		db.Put(nil, util.MakeSlice([]byte(k)), util.MakeSlice([]byte("val_"+k)))
	}

	iter := db.NewIterator(nil)
	defer iter.Release()

	// Seek to middle key
	iter.Seek(util.MakeSlice([]byte("cherry")))
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after Seek(cherry)")
	}
	if string(iter.Key().Data()) != "cherry" {
		t.Errorf("Expected key cherry, got %q", string(iter.Key().Data()))
	}

	// Seek to non-existent key between banana and cherry
	iter.Seek(util.MakeSlice([]byte("blueberry")))
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after Seek(blueberry)")
	}
	if string(iter.Key().Data()) != "cherry" {
		t.Errorf("Expected key cherry, got %q", string(iter.Key().Data()))
	}

	// Seek to key before all keys
	iter.Seek(util.MakeSlice([]byte("aaa")))
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after Seek(aaa)")
	}
	if string(iter.Key().Data()) != "apple" {
		t.Errorf("Expected key apple, got %q", string(iter.Key().Data()))
	}

	// Seek to key after all keys
	iter.Seek(util.MakeSlice([]byte("zebra")))
	if iter.Valid() {
		t.Errorf("Iterator should be invalid after Seek(zebra)")
	}
}

// TestIteratorSeekToLast tests that SeekToLast positions at the last entry.
// Invariant: After SeekToLast, iterator is valid at the last key.
// Note: "last" alphabetically comes before "middle" (l < m), so the skiplist
// order is [first, last, middle] and SeekToLast returns "middle".
func TestIteratorSeekToLast(t *testing.T) {
	db := newTestDB(t)

	// Write sorted keys
	keys := []string{"first", "middle", "last"}
	for _, k := range keys {
		db.Put(nil, util.MakeSlice([]byte(k)), util.MakeSlice([]byte("val_"+k)))
	}

	iter := db.NewIterator(nil)
	defer iter.Release()

	// Seek to last (alphabetically last key = "middle" since l < m)
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after SeekToLast")
	}
	// Alphabetically: first < last < middle, so "middle" is the last
	if string(iter.Key().Data()) != "middle" {
		t.Errorf("Expected key middle, got %q", string(iter.Key().Data()))
	}
	if string(iter.Value().Data()) != "val_middle" {
		t.Errorf("Expected value val_middle, got %q", string(iter.Value().Data()))
	}

	// Next should make it invalid
	iter.Next()
	if iter.Valid() {
		t.Errorf("Iterator should be invalid after Next from last")
	}
}

// TestIteratorPrev tests that Prev returns to the previous key after Next.
// Invariant: After Next, Prev must return to previous key.
func TestIteratorPrev(t *testing.T) {
	db := newTestDB(t)

	// Write sorted keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		db.Put(nil, util.MakeSlice([]byte(k)), util.MakeSlice([]byte("val_"+k)))
	}

	iter := db.NewIterator(nil)
	defer iter.Release()

	// Seek to first
	iter.SeekToFirst()
	if !iter.Valid() || string(iter.Key().Data()) != "a" {
		t.Fatalf("Expected first key a")
	}

	// Next twice
	iter.Next()
	iter.Next()
	if !iter.Valid() || string(iter.Key().Data()) != "c" {
		t.Errorf("Expected key c after two Next, got %q", string(iter.Key().Data()))
	}

	// Prev once - should be at b
	iter.Prev()
	if !iter.Valid() || string(iter.Key().Data()) != "b" {
		t.Errorf("Expected key b after Prev, got %q", string(iter.Key().Data()))
	}

	// Prev again - should be at a
	iter.Prev()
	if !iter.Valid() || string(iter.Key().Data()) != "a" {
		t.Errorf("Expected key a after second Prev, got %q", string(iter.Key().Data()))
	}

	// Prev from first should be invalid
	iter.Prev()
	if iter.Valid() {
		t.Errorf("Iterator should be invalid after Prev from first")
	}
}

// TestIteratorRange tests iterating over a key range.
// Invariant: Seeking to range start and iterating should visit keys in that range.
func TestIteratorRange(t *testing.T) {
	db := newTestDB(t)

	// Write sorted keys
	keys := []string{"apple", "apricot", "banana", "blueberry", "cherry"}
	for _, k := range keys {
		db.Put(nil, util.MakeSlice([]byte(k)), util.MakeSlice([]byte("val_"+k)))
	}

	iter := db.NewIterator(nil)
	defer iter.Release()

	// Seek to start of "b*" range
	iter.Seek(util.MakeSlice([]byte("b")))

	// Collect all keys starting with "b"
	var visited []string
	for iter.Valid() {
		key := string(iter.Key().Data())
		if key[0] != 'b' {
			break // Past our range
		}
		visited = append(visited, key)
		iter.Next()
	}

	// Should have visited banana and blueberry
	if len(visited) != 2 {
		t.Errorf("Expected 2 keys in range, got %d: %v", len(visited), visited)
	}
	if visited[0] != "banana" {
		t.Errorf("Expected first key banana, got %q", visited[0])
	}
	if visited[1] != "blueberry" {
		t.Errorf("Expected second key blueberry, got %q", visited[1])
	}
}

// TestIteratorSeekToFirst tests that SeekToFirst positions at the first entry.
// Invariant: After SeekToFirst, iterator is valid at the first key.
func TestIteratorSeekToFirst(t *testing.T) {
	db := newTestDB(t)

	// Write sorted keys
	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		db.Put(nil, util.MakeSlice([]byte(k)), util.MakeSlice([]byte("val_"+k)))
	}

	iter := db.NewIterator(nil)
	defer iter.Release()

	// Seek to first
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after SeekToFirst")
	}
	if string(iter.Key().Data()) != "alpha" {
		t.Errorf("Expected key alpha, got %q", string(iter.Key().Data()))
	}

	// Traverse all
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	if count != 3 {
		t.Errorf("Expected 3 entries, got %d", count)
	}
}
