package memtable

import (
	"testing"

	"github.com/akzj/go-leveldb/internal"
)

func TestSkipListEmpty(t *testing.T) {
	sl := NewSkipList(123)
	if sl.Size() != 0 {
		t.Errorf("expected size 0, got %d", sl.Size())
	}
}

func TestSkipListPutAndGet(t *testing.T) {
	sl := NewSkipList(123)

	key1 := internal.MakeInternalKey([]byte("key1"), 1, internal.TypePut)
	sl.Put(key1, []byte("value1"))

	if sl.Size() != 1 {
		t.Errorf("expected size 1, got %d", sl.Size())
	}

	// Iterate and check
	it := sl.NewIterator()
	if !it.First() {
		t.Error("expected to find first element")
	}
	if string(it.Key().UserKey()) != "key1" {
		t.Errorf("expected key1, got %s", it.Key().UserKey())
	}
	if string(it.Value()) != "value1" {
		t.Errorf("expected value1, got %s", it.Value())
	}
}

func TestSkipListMultipleInserts(t *testing.T) {
	sl := NewSkipList(456)

	// Insert in random order
	keys := []string{"banana", "apple", "cherry", "date"}
	for i, k := range keys {
		key := internal.MakeInternalKey([]byte(k), uint64(i+1), internal.TypePut)
		sl.Put(key, []byte("value_"+k))
	}

	if sl.Size() != 4 {
		t.Errorf("expected size 4, got %d", sl.Size())
	}

	// Iterate in order (should be sorted by user key)
	it := sl.NewIterator()
	expected := []string{"apple", "banana", "cherry", "date"}
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

func TestSkipListIteratorFirstLast(t *testing.T) {
	sl := NewSkipList(789)

	// Empty list
	it := sl.NewIterator()
	if it.First() {
		t.Error("expected false for empty list First()")
	}
	if it.Last() {
		t.Error("expected false for empty list Last()")
	}

	// Single element
	key := internal.MakeInternalKey([]byte("only"), 1, internal.TypePut)
	sl.Put(key, []byte("value"))

	it = sl.NewIterator()
	if !it.First() {
		t.Error("expected true for First()")
	}
	if !it.Valid() {
		t.Error("expected valid after First()")
	}
	if string(it.Key().UserKey()) != "only" {
		t.Errorf("expected 'only', got %s", it.Key().UserKey())
	}

	if !it.Last() {
		t.Error("expected true for Last()")
	}
	if !it.Valid() {
		t.Error("expected valid after Last()")
	}
}

func TestSkipListIteratorNextPrev(t *testing.T) {
	sl := NewSkipList(999)

	for _, k := range []string{"a", "b", "c"} {
		key := internal.MakeInternalKey([]byte(k), 1, internal.TypePut)
		sl.Put(key, []byte("val_"+k))
	}

	it := sl.NewIterator()

	// Test Next
	expected := []string{"a", "b", "c"}
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if i >= len(expected) {
			t.Error("too many elements")
			break
		}
		got := string(it.Key().UserKey())
		if got != expected[i] {
			t.Errorf("Next: at position %d: expected %s, got %s", i, expected[i], got)
		}
		i++
	}

	// After last, Next should return false
	if it.Valid() {
		t.Error("expected invalid after last Next()")
	}

	// Test Prev from end
	i = len(expected) - 1
	for it.Last(); it.Valid(); it.Prev() {
		if i < 0 {
			t.Error("too many elements in Prev")
			break
		}
		got := string(it.Key().UserKey())
		if got != expected[i] {
			t.Errorf("Prev: at position %d: expected %s, got %s", i, expected[i], got)
		}
		i--
	}
}

func TestSkipListIteratorSeek(t *testing.T) {
	sl := NewSkipList(111)

	for _, k := range []string{"apple", "banana", "cherry", "date"} {
		key := internal.MakeInternalKey([]byte(k), 1, internal.TypePut)
		sl.Put(key, []byte("val_"+k))
	}

	it := sl.NewIterator()

	// Seek to middle
	target := internal.MakeInternalKey([]byte("cherry"), 1, internal.TypePut)
	if !it.Seek(target) {
		t.Error("expected Seek to find cherry")
	}
	if string(it.Key().UserKey()) != "cherry" {
		t.Errorf("expected cherry, got %s", it.Key().UserKey())
	}

	// Seek to non-existent key (should go to next higher)
	target = internal.MakeInternalKey([]byte("blueberry"), 1, internal.TypePut)
	if !it.Seek(target) {
		t.Error("expected Seek to find banana (next higher)")
	}
	if string(it.Key().UserKey()) != "cherry" {
		t.Errorf("expected cherry (next higher than blueberry), got %s", it.Key().UserKey())
	}

	// Seek beyond last
	target = internal.MakeInternalKey([]byte("zebra"), 1, internal.TypePut)
	if it.Seek(target) {
		t.Error("expected Seek to return false for key beyond last")
	}
}

func TestSkipListVersionHandling(t *testing.T) {
	sl := NewSkipList(222)

	// Insert same user key with different sequences
	userKey := []byte("samekey")
	sl.Put(internal.MakeInternalKey(userKey, 1, internal.TypePut), []byte("v1"))
	sl.Put(internal.MakeInternalKey(userKey, 2, internal.TypePut), []byte("v2"))
	sl.Put(internal.MakeInternalKey(userKey, 3, internal.TypePut), []byte("v3"))

	if sl.Size() != 3 {
		t.Errorf("expected size 3, got %d", sl.Size())
	}

	// Iterate - higher sequence should come first due to Compare (descending seq)
	it := sl.NewIterator()
	if !it.First() {
		t.Error("expected to find first element")
	}
	// First element should be highest sequence
	if it.Key().Sequence() != 3 {
		t.Errorf("expected first key to have sequence 3, got %d", it.Key().Sequence())
	}

	// Iterate all
	count := 0
	for it.First(); it.Valid(); it.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 elements, got %d", count)
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := NewSkipList(333)

	key := internal.MakeInternalKey([]byte("delkey"), 1, internal.TypePut)
	sl.Put(key, []byte("value"))

	if sl.Size() != 1 {
		t.Errorf("expected size 1, got %d", sl.Size())
	}

	// Note: SkipList doesn't support delete, but we can verify it exists
	it := sl.NewIterator()
	if !it.First() {
		t.Error("expected to find element")
	}
	if string(it.Key().UserKey()) != "delkey" {
		t.Errorf("expected delkey, got %s", it.Key().UserKey())
	}
}

func TestSkipListConsistency(t *testing.T) {
	// Ensure the list remains consistent after multiple operations
	sl := NewSkipList(444)

	// Insert many elements
	for i := 0; i < 100; i++ {
		key := internal.MakeInternalKey([]byte(string(rune('a'+i%26))), uint64(i), internal.TypePut)
		sl.Put(key, []byte("value"))
	}

	// Verify all elements are accessible
	it := sl.NewIterator()
	count := 0
	for it.First(); it.Valid(); it.Next() {
		count++
	}
	if count != sl.Size() {
		t.Errorf("iterator count %d != size %d", count, sl.Size())
	}

	// Verify all elements are in sorted order
	var prev internal.InternalKey
	first := true
	for it.First(); it.Valid(); it.Next() {
		if !first && internal.Compare(prev, it.Key()) >= 0 {
			t.Error("elements not in sorted order")
		}
		prev = it.Key()
		first = false
	}
}

func TestSkipListIteratorSeekForPrev(t *testing.T) {
	sl := NewSkipList(555)

	for _, k := range []string{"apple", "banana", "cherry"} {
		key := internal.MakeInternalKey([]byte(k), 1, internal.TypePut)
		sl.Put(key, []byte("val_"+k))
	}

	it := sl.NewIterator()

	// SeekForPrev to existing key
	target := internal.MakeInternalKey([]byte("banana"), 1, internal.TypePut)
	if !it.SeekForPrev(target) {
		t.Error("expected SeekForPrev to find banana")
	}
	if string(it.Key().UserKey()) != "banana" {
		t.Errorf("expected banana, got %s", it.Key().UserKey())
	}

	// SeekForPrev to non-existent key
	target = internal.MakeInternalKey([]byte("blueberry"), 1, internal.TypePut)
	if !it.SeekForPrev(target) {
		t.Error("expected SeekForPrev to find banana (prev of blueberry)")
	}
	if string(it.Key().UserKey()) != "banana" {
		t.Errorf("expected banana (prev of blueberry), got %s", it.Key().UserKey())
	}
}

func TestSkipListRandomHeight(t *testing.T) {
	// Test that randomHeight produces valid heights
	sl := NewSkipList(666)

	// Run multiple times to check distribution (not too strict)
	for i := 0; i < 100; i++ {
		h := sl.randomHeight()
		if h < 1 || h > MaxHeight {
			t.Errorf("invalid height %d, expected 1-%d", h, MaxHeight)
		}
	}

	// Check that heights vary (not all the same)
	heights := make(map[int]bool)
	for i := 0; i < 100; i++ {
		h := sl.randomHeight()
		heights[h] = true
	}

	if len(heights) < 2 {
		t.Error("randomHeight should produce varying heights")
	}
}