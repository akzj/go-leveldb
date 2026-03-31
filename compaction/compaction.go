// Package compaction implements the compaction logic for the LSM-tree storage engine.
// Compaction merges SSTable files across levels to maintain level size constraints
// and clean up deleted or overwritten entries.
package compaction

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"

	"github.com/akzj/go-leveldb/internal"
	"github.com/akzj/go-leveldb/manifest"
	"github.com/akzj/go-leveldb/sstable"
)

// Default constants
const (
	// DefaultCompactionOutputSize is the target size for compacted SSTable files.
	DefaultCompactionOutputSize = 2 * 1024 * 1024 // 2MB
)

// Options holds compaction configuration.
type Options struct {
	// CompactionOutputSize is the target size for each compacted SSTable file.
	CompactionOutputSize int
	// Level0CompactionTrigger is the number of L0 files that triggers compaction.
	Level0CompactionTrigger int
	// Level1MaxSize is the target size for level 1.
	Level1MaxSize int64
	// MaxLevels is the maximum number of levels in the LSM tree.
	MaxLevels int
}

// DefaultOptions returns the default compaction options.
func DefaultOptions() *Options {
	return &Options{
		CompactionOutputSize:    DefaultCompactionOutputSize,
		Level0CompactionTrigger: 4,
		Level1MaxSize:           10 * 1024 * 1024,
		MaxLevels:               7,
	}
}

// Compactor performs compaction operations.
type Compactor struct {
	dbPath   string
	manifest *manifest.Manifest
	opts     *Options
}

// NewCompactor creates a new Compactor.
func NewCompactor(dbPath string, m *manifest.Manifest, opts *Options) *Compactor {
	if opts == nil {
		opts = DefaultOptions()
	}
	return &Compactor{
		dbPath:   dbPath,
		manifest: m,
		opts:     opts,
	}
}

// MaybeCompact checks if compaction is needed and executes it if so.
// Returns true if compaction was performed, false otherwise.
func (c *Compactor) MaybeCompact() (bool, error) {
	// Check Level 0 compaction
	if c.manifest.Level0Count() >= c.opts.Level0CompactionTrigger {
		if err := c.compactLevel0(); err != nil {
			return false, fmt.Errorf("compact level 0: %w", err)
		}
		return true, nil
	}

	// Check Level N compaction (N >= 1)
	for level := 1; level < c.opts.MaxLevels-1; level++ {
		maxSize := c.opts.Level1MaxSize * pow10(level-1)
		if c.manifest.TotalSize(level) > maxSize {
			if err := c.compactLevelN(level); err != nil {
				return false, fmt.Errorf("compact level %d: %w", level, err)
			}
			return true, nil
		}
	}

	return false, nil
}

// pow10 returns 10^n
func pow10(n int) int64 {
	result := int64(1)
	for i := 0; i < n; i++ {
		result *= 10
	}
	return result
}

// compactLevel0 merges all Level 0 files with overlapping Level 1 files.
func (c *Compactor) compactLevel0() error {
	level0Tables := c.manifest.GetTablesForLevel(0)
	if len(level0Tables) == 0 {
		return nil
	}

	// Find all Level 1 tables that overlap with any Level 0 table
	level1Tables := c.manifest.GetTablesForLevel(1)

	// Build a map of overlapping L1 tables to remove
	l1ToRemove := make(map[uint64]bool)
	for _, l0 := range level0Tables {
		for _, l1 := range level1Tables {
			if keysOverlap(l0.SmallestKey.UserKey(), l0.LargestKey.UserKey(),
				l1.SmallestKey.UserKey(), l1.LargestKey.UserKey()) {
				l1ToRemove[l1.FileNum] = true
			}
		}
	}

	// Collect files to compact
	filesToCompact := make([]*manifest.TableMeta, 0, len(level0Tables)+len(level1Tables))
	filesToCompact = append(filesToCompact, level0Tables...)

	level1ToCompact := make([]*manifest.TableMeta, 0)
	for _, l1 := range level1Tables {
		if l1ToRemove[l1.FileNum] {
			level1ToCompact = append(level1ToCompact, l1)
		}
	}
	filesToCompact = append(filesToCompact, level1ToCompact...)

	// Merge and write new SSTable
	newFiles, err := c.mergeAndWrite(1, filesToCompact)
	if err != nil {
		return fmt.Errorf("merge and write: %w", err)
	}

	// Update manifest - copy FileNums first to avoid slice mutation during iteration
	level0FileNums := make([]uint64, len(level0Tables))
	for i, t := range level0Tables {
		level0FileNums[i] = t.FileNum
	}
	for _, fn := range level0FileNums {
		c.manifest.RemoveTable(0, fn)
	}
	for _, t := range level1ToCompact {
		c.manifest.RemoveTable(1, t.FileNum)
	}
	for _, t := range newFiles {
		c.manifest.AddTable(1, t)
	}

	// Save manifest
	if err := c.manifest.Save(c.dbPath); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}

	// Delete old SSTable files
	for _, t := range level0Tables {
		os.Remove(t.FilePath)
	}
	for _, t := range level1ToCompact {
		os.Remove(t.FilePath)
	}

	return nil
}

// compactLevelN merges one Level N file with overlapping Level N+1 files.
func (c *Compactor) compactLevelN(level int) error {
	levelNTables := c.manifest.GetTablesForLevel(level)
	if len(levelNTables) == 0 {
		return nil
	}

	// Pick the oldest file (smallest file number)
	oldestFile := levelNTables[0]

	// Find all Level N+1 tables that overlap with the oldest file
	levelN1Tables := c.manifest.GetTablesForLevel(level + 1)

	// Build a map of overlapping L(N+1) tables to remove
	n1ToRemove := make(map[uint64]bool)
	for _, ln1 := range levelN1Tables {
		if keysOverlap(oldestFile.SmallestKey.UserKey(), oldestFile.LargestKey.UserKey(),
			ln1.SmallestKey.UserKey(), ln1.LargestKey.UserKey()) {
			n1ToRemove[ln1.FileNum] = true
		}
	}

	// Collect files to compact
	filesToCompact := []*manifest.TableMeta{oldestFile}

	levelN1ToCompact := make([]*manifest.TableMeta, 0)
	for _, ln1 := range levelN1Tables {
		if n1ToRemove[ln1.FileNum] {
			levelN1ToCompact = append(levelN1ToCompact, ln1)
		}
	}
	filesToCompact = append(filesToCompact, levelN1ToCompact...)

	// Merge and write new SSTable
	newFiles, err := c.mergeAndWrite(level+1, filesToCompact)
	if err != nil {
		return fmt.Errorf("merge and write: %w", err)
	}

	// Update manifest
	c.manifest.RemoveTable(level, oldestFile.FileNum)
	for _, t := range levelN1ToCompact {
		c.manifest.RemoveTable(level+1, t.FileNum)
	}
	for _, t := range newFiles {
		c.manifest.AddTable(level+1, t)
	}

	// Save manifest
	if err := c.manifest.Save(c.dbPath); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}

	// Delete old SSTable files
	os.Remove(oldestFile.FilePath)
	for _, t := range levelN1ToCompact {
		os.Remove(t.FilePath)
	}

	return nil
}

// keysOverlap checks if two key ranges overlap.
func keysOverlap(smallest1, largest1, smallest2, largest2 []byte) bool {
	return !(string(largest1) < string(smallest2) || string(largest2) < string(smallest1))
}

// itm holds an iterator item for the heap.
type itm struct {
	key    internal.InternalKey
	value  []byte
	iter   internalIterator
	index  int // index in the heap
}

// internalIterator is the interface for iterating over internal keys.
type internalIterator interface {
	Valid() bool
	Key() internal.InternalKey
	Value() []byte
	Next()
	Close() error
}

// mergeIter implements a heap-based merge iterator.
type mergeIter struct {
	items   []*itm
	keyComp func(a, b internal.InternalKey) int
}

func newMergeIter(iters []internalIterator) *mergeIter {
	m := &mergeIter{
		items:   make([]*itm, 0, len(iters)),
		keyComp: internal.Compare,
	}
	for i, iter := range iters {
		if iter.Valid() {
			m.items = append(m.items, &itm{
				key:   iter.Key(),
				value: iter.Value(),
				iter:  iter,
				index: i,
			})
		}
	}
	heap.Init(m)
	return m
}

func (m *mergeIter) Next() {
	if len(m.items) == 0 {
		return
	}
	item := m.items[0]
	item.iter.Next()
	if item.iter.Valid() {
		item.key = item.iter.Key()
		item.value = item.iter.Value()
		heap.Fix(m, 0)
	} else {
		item.iter.Close()
		heap.Remove(m, 0)
	}
}

func (m *mergeIter) Valid() bool {
	return len(m.items) > 0
}

func (m *mergeIter) Key() internal.InternalKey {
	if len(m.items) == 0 {
		return internal.InternalKey{}
	}
	return m.items[0].key
}

func (m *mergeIter) Value() []byte {
	if len(m.items) == 0 {
		return nil
	}
	return m.items[0].value
}

// Implement heap.Interface
func (m *mergeIter) Len() int { return len(m.items) }
func (m *mergeIter) Less(i, j int) bool {
	return m.keyComp(m.items[i].key, m.items[j].key) < 0
}
func (m *mergeIter) Swap(i, j int) {
	m.items[i], m.items[j] = m.items[j], m.items[i]
	m.items[i].index = i
	m.items[j].index = j
}
func (m *mergeIter) Push(x any) {
	item := x.(*itm)
	item.index = len(m.items)
	m.items = append(m.items, item)
}
func (m *mergeIter) Pop() any {
	old := m.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	m.items = old[0 : n-1]
	return item
}

// sstIter wraps sstable.TableIterator to implement internalIterator.
type sstIter struct {
	iter *sstable.TableIterator
}

func (s *sstIter) Valid() bool {
	return s.iter.Valid()
}

func (s *sstIter) Key() internal.InternalKey {
	return s.iter.Key()
}

func (s *sstIter) Value() []byte {
	return s.iter.Value()
}

func (s *sstIter) Next() {
	s.iter.Next()
}

func (s *sstIter) Close() error {
	return nil // TableIterator doesn't need closing
}

// mergeAndWrite merges multiple SSTable files and writes the result to new SSTable(s).
func (c *Compactor) mergeAndWrite(targetLevel int, tables []*manifest.TableMeta) ([]*manifest.TableMeta, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	// Open readers for all tables
	readers := make([]*sstable.Reader, 0, len(tables))
	iterators := make([]internalIterator, 0, len(tables))

	for _, t := range tables {
		reader, err := sstable.OpenReader(t.FilePath)
		if err != nil {
			// Close already opened readers
			for _, r := range readers {
				r.Close()
			}
			return nil, fmt.Errorf("open reader %s: %w", t.FilePath, err)
		}
		readers = append(readers, reader)
		sstIter := &sstIter{iter: reader.NewIterator()}
		sstIter.iter.First() // Position at first entry
		iterators = append(iterators, sstIter)
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	// Create merge iterator
	merge := newMergeIter(iterators)

	// Prepare for writing
	var currentWriter *sstable.Writer
	var smallestKey, largestKey internal.InternalKey
	var currentFileNum uint64
	var currentSize int64
	newFiles := make([]*manifest.TableMeta, 0)
	var smallestKeySet bool

	createNewWriter := func() (uint64, *sstable.Writer, error) {
		fileNum := c.manifest.NewFileNumber()
		path := filepath.Join(c.dbPath, fmt.Sprintf("%06d.sst", fileNum))
		w, err := sstable.NewWriter(path, 4*1024) // 4KB block size
		if err != nil {
			return 0, nil, err
		}
		return fileNum, w, nil
	}

	finishWriter := func(w *sstable.Writer, fileNum uint64, sKey, lKey internal.InternalKey) (*manifest.TableMeta, error) {
		meta, err := w.Finish()
		if err != nil {
			return nil, err
		}
		return &manifest.TableMeta{
			FileNum:     fileNum,
			FilePath:    meta.FilePath,
			FileSize:    meta.FileSize,
			SmallestKey: sKey,
			LargestKey:  lKey,
		}, nil
	}

	// Create first writer
	fileNum, w, err := createNewWriter()
	if err != nil {
		return nil, fmt.Errorf("create writer: %w", err)
	}
	currentWriter = w
	currentFileNum = fileNum
	smallestKeySet = false

	// Track last user key for deduplication
	var lastUserKey []byte

	for merge.Valid() {
		currKey := merge.Key()
		currValue := merge.Value()
		currUserKey := currKey.UserKey()

		// Check if same user key as last entry (deduplication)
		// Since merge iterator returns entries in sorted order with highest seq first,
		// we just skip entries with the same user key
		if lastUserKey != nil && string(currUserKey) == string(lastUserKey) {
			merge.Next()
			continue
		}

		// New user key
		lastUserKey = currUserKey

		// Set smallest key on first entry
		if !smallestKeySet {
			smallestKey = currKey
			smallestKeySet = true
		}
		largestKey = currKey

		// Write the entry
		if err := currentWriter.Add(currKey, currValue); err != nil {
			currentWriter.Abort()
			return nil, fmt.Errorf("add entry: %w", err)
		}
		currentSize += int64(len(currKey.Data()) + len(currValue))

		// Check if we should start a new SSTable
		if currentSize >= int64(c.opts.CompactionOutputSize) {
			// Finish current writer
			meta, err := finishWriter(currentWriter, currentFileNum, smallestKey, largestKey)
			if err != nil {
				return nil, fmt.Errorf("finish writer: %w", err)
			}
			newFiles = append(newFiles, meta)

			// Create new writer for remaining entries
			fileNum, w, err = createNewWriter()
			if err != nil {
				return nil, fmt.Errorf("create writer: %w", err)
			}
			currentWriter = w
			currentFileNum = fileNum
			currentSize = 0
			smallestKeySet = false
			// Reset lastUserKey so next entry becomes first
			lastUserKey = nil
		}

		merge.Next()
	}

	// Finish the last writer if it has entries
	if smallestKeySet {
		meta, err := finishWriter(currentWriter, currentFileNum, smallestKey, largestKey)
		if err != nil {
			return nil, fmt.Errorf("finish writer: %w", err)
		}
		newFiles = append(newFiles, meta)
	}

	return newFiles, nil
}

// GetManifest returns the manifest for external access.
func (c *Compactor) GetManifest() *manifest.Manifest {
	return c.manifest
}

// CompactTables merges a specific set of tables and returns the new table metadata.
// This is useful for the DB flush operation.
func (c *Compactor) CompactTables(level int, tables []*manifest.TableMeta) ([]*manifest.TableMeta, error) {
	return c.mergeAndWrite(level, tables)
}