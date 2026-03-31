package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/go-leveldb/internal"
	"github.com/akzj/go-leveldb/manifest"
	"github.com/akzj/go-leveldb/sstable"
)

func TestMaybeCompactNoTrigger(t *testing.T) {
	// Create a temp directory
	tmpDir, err := os.MkdirTemp("", "compaction_test")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a manifest with no files
	m, err := manifest.Load(tmpDir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	m.Save(tmpDir)

	compactor := NewCompactor(tmpDir, m, nil)

	// Should not trigger compaction
	triggered, err := compactor.MaybeCompact()
	if err != nil {
		t.Fatalf("maybe compact: %v", err)
	}
	if triggered {
		t.Error("expected no compaction to be triggered")
	}
}

func TestCompactLevel0(t *testing.T) {
	// Create a temp directory
	tmpDir, err := os.MkdirTemp("", "compaction_test")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a manifest with 4 L0 files
	m := &manifest.Manifest{
		NextFileNumber: 10,
		Sequence:       0,
		Levels:         make(map[int][]*manifest.TableMeta),
		WALFileNumber:  1,
	}

	// Create SSTable files for L0
	blockSize := 4 * 1024
	for i := 0; i < 4; i++ {
		fileName := fmt.Sprintf("%06d.sst", i+1)
		writer, err := sstable.NewWriter(filepath.Join(tmpDir, fileName), blockSize)
		if err != nil {
			t.Fatalf("create writer: %v", err)
		}

		// Add some entries
		key := []byte{'a' + byte(i)}
		ikey := internal.MakeInternalKey(key, uint64(i+1), internal.TypePut)
		if err := writer.Add(ikey, []byte("value")); err != nil {
			t.Fatalf("add entry: %v", err)
		}

		meta, err := writer.Finish()
		if err != nil {
			t.Fatalf("finish writer: %v", err)
		}

		// Use the same key for smallest and largest
		largestKey := internal.MakeInternalKey(key, uint64(i+1), internal.TypePut)

		m.AddTable(0, &manifest.TableMeta{
			FileNum:     uint64(i + 1),
			FilePath:    meta.FilePath,
			FileSize:    meta.FileSize,
			SmallestKey: ikey,
			LargestKey:  largestKey,
		})
	}

	// Save manifest
	m.Save(tmpDir)

	compactor := NewCompactor(tmpDir, m, &Options{
		Level0CompactionTrigger: 4,
		Level1MaxSize:           10 * 1024 * 1024,
		CompactionOutputSize:    2 * 1024 * 1024,
		MaxLevels:               7,
	})

	// Should trigger compaction
	triggered, err := compactor.MaybeCompact()
	if err != nil {
		t.Fatalf("maybe compact: %v", err)
	}
	if !triggered {
		t.Error("expected compaction to be triggered")
	}

	// Check that L0 files are compacted
	if count := len(m.GetTablesForLevel(0)); count != 0 {
		t.Errorf("expected 0 L0 files, got %d", count)
	}

	// Check that L1 has new files
	if count := len(m.GetTablesForLevel(1)); count == 0 {
		t.Error("expected L1 files after compaction")
	}
}

func TestKeysOverlap(t *testing.T) {
	tests := []struct {
		name     string
		s1, l1   []byte
		s2, l2   []byte
		overlaps bool
	}{
		{"no overlap", []byte("a"), []byte("b"), []byte("c"), []byte("d"), false},
		{"complete overlap", []byte("a"), []byte("c"), []byte("b"), []byte("d"), true},
		{"partial overlap start", []byte("a"), []byte("c"), []byte("b"), []byte("b"), true},
		{"partial overlap end", []byte("a"), []byte("b"), []byte("a"), []byte("c"), true},
		{"identical", []byte("a"), []byte("c"), []byte("a"), []byte("c"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := keysOverlap(tt.s1, tt.l1, tt.s2, tt.l2)
			if result != tt.overlaps {
				t.Errorf("keysOverlap(%q, %q, %q, %q) = %v, want %v",
					tt.s1, tt.l1, tt.s2, tt.l2, result, tt.overlaps)
			}
		})
	}
}

func TestPow10(t *testing.T) {
	tests := []struct {
		n     int
		value int64
	}{
		{0, 1},
		{1, 10},
		{2, 100},
		{3, 1000},
		{4, 10000},
	}

	for _, tt := range tests {
		result := pow10(tt.n)
		if result != tt.value {
			t.Errorf("pow10(%d) = %d, want %d", tt.n, result, tt.value)
		}
	}
}

func TestMergeIterBasic(t *testing.T) {
	// Create temp files for testing
	tmpDir, err := os.MkdirTemp("", "merge_iter_test")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create two SSTable files
	blockSize := 4 * 1024

	// File 1: keys a, c
	writer1, err := sstable.NewWriter(filepath.Join(tmpDir, "000001.sst"), blockSize)
	if err != nil {
		t.Fatalf("create writer 1: %v", err)
	}
	writer1.Add(internal.MakeInternalKey([]byte("a"), 2, internal.TypePut), []byte("value1"))
	writer1.Add(internal.MakeInternalKey([]byte("c"), 1, internal.TypePut), []byte("value3"))
	writer1.Finish()

	// File 2: keys b, d
	writer2, err := sstable.NewWriter(filepath.Join(tmpDir, "000002.sst"), blockSize)
	if err != nil {
		t.Fatalf("create writer 2: %v", err)
	}
	writer2.Add(internal.MakeInternalKey([]byte("b"), 2, internal.TypePut), []byte("value2"))
	writer2.Add(internal.MakeInternalKey([]byte("d"), 1, internal.TypePut), []byte("value4"))
	writer2.Finish()

	// Open readers
	reader1, err := sstable.OpenReader(filepath.Join(tmpDir, "000001.sst"))
	if err != nil {
		t.Fatalf("open reader 1: %v", err)
	}
	defer reader1.Close()

	reader2, err := sstable.OpenReader(filepath.Join(tmpDir, "000002.sst"))
	if err != nil {
		t.Fatalf("open reader 2: %v", err)
	}
	defer reader2.Close()

	// Create iterators and position at first
	iter1 := &sstIter{iter: reader1.NewIterator()}
	iter1.iter.First()

	iter2 := &sstIter{iter: reader2.NewIterator()}
	iter2.iter.First()

	// Create merge iterator
	merge := newMergeIter([]internalIterator{iter1, iter2})

	// Check order
	expectedKeys := []string{"a", "b", "c", "d"}
	expectedSeqs := []uint64{2, 2, 1, 1}

	i := 0
	for merge.Valid() {
		if i >= len(expectedKeys) {
			break
		}
		key := merge.Key()
		userKey := string(key.UserKey())
		seq := key.Sequence()

		if userKey != expectedKeys[i] {
			t.Errorf("key %d: got %q, want %q", i, userKey, expectedKeys[i])
		}
		if seq != expectedSeqs[i] {
			t.Errorf("seq %d: got %d, want %d", i, seq, expectedSeqs[i])
		}
		i++
		merge.Next()
	}

	if i != len(expectedKeys) {
		t.Errorf("expected %d entries, got %d", len(expectedKeys), i)
	}
}

// TestMergeIterSortedOrder verifies the merge iterator returns entries
// sorted by InternalKey (user_key ASC, sequence DESC)
func TestMergeIterSortedOrder(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "merge_iter_test")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	blockSize := 4 * 1024

	// File 1: key "a" with seq 2 (higher sequence = comes first in sort order)
	writer1, err := sstable.NewWriter(filepath.Join(tmpDir, "000001.sst"), blockSize)
	if err != nil {
		t.Fatalf("create writer 1: %v", err)
	}
	writer1.Add(internal.MakeInternalKey([]byte("a"), 2, internal.TypePut), []byte("value1-2"))
	writer1.Finish()

	// File 2: key "a" with seq 1, then "b"
	writer2, err := sstable.NewWriter(filepath.Join(tmpDir, "000002.sst"), blockSize)
	if err != nil {
		t.Fatalf("create writer 2: %v", err)
	}
	writer2.Add(internal.MakeInternalKey([]byte("a"), 1, internal.TypePut), []byte("value1-1"))
	writer2.Add(internal.MakeInternalKey([]byte("b"), 1, internal.TypePut), []byte("value2"))
	writer2.Finish()

	reader1, err := sstable.OpenReader(filepath.Join(tmpDir, "000001.sst"))
	if err != nil {
		t.Fatalf("open reader 1: %v", err)
	}
	defer reader1.Close()

	reader2, err := sstable.OpenReader(filepath.Join(tmpDir, "000002.sst"))
	if err != nil {
		t.Fatalf("open reader 2: %v", err)
	}
	defer reader2.Close()

	// Position iterators at first
	iter1 := &sstIter{iter: reader1.NewIterator()}
	iter1.iter.First()

	iter2 := &sstIter{iter: reader2.NewIterator()}
	iter2.iter.First()

	merge := newMergeIter([]internalIterator{iter1, iter2})

	// Expected: "a" (seq 2), "a" (seq 1), "b" (seq 1)
	// The merge iterator does NOT deduplicate - it just merges in sorted order
	expectedKeys := []string{"a", "a", "b"}
	expectedSeqs := []uint64{2, 1, 1}

	i := 0
	for merge.Valid() {
		if i >= len(expectedKeys) {
			break
		}
		key := merge.Key()
		userKey := string(key.UserKey())
		seq := key.Sequence()

		if userKey != expectedKeys[i] {
			t.Errorf("key %d: got %q, want %q", i, userKey, expectedKeys[i])
		}
		if seq != expectedSeqs[i] {
			t.Errorf("seq %d: got %d, want %d", i, seq, expectedSeqs[i])
		}
		i++
		merge.Next()
	}

	if i != 3 {
		t.Errorf("expected 3 entries, got %d", i)
	}
}