// Package db provides the main database interface and implementation for LevelDB.
package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/akzj/go-leveldb/internal"
	"github.com/akzj/go-leveldb/memtable"
	"github.com/akzj/go-leveldb/sstable"
	"github.com/akzj/go-leveldb/wal"
)

// Errors
var (
	ErrNotFound = errors.New("goleveldb: key not found")
	ErrDBClosed = errors.New("goleveldb: database closed")
	ErrEmptyKey = errors.New("goleveldb: key cannot be empty")
)

// Options configures database behavior.
type Options struct {
	MemTableSize            int
	BlockSize               int
	MaxLevels               int
	Level0CompactionTrigger int
	Level1MaxSize           int64
}

// DefaultOptions returns Options with default values.
func DefaultOptions() *Options {
	return &Options{
		MemTableSize:            4 * 1024 * 1024,
		BlockSize:               4 * 1024,
		MaxLevels:               7,
		Level0CompactionTrigger: 4,
		Level1MaxSize:           10 * 1024 * 1024,
	}
}

// DB is the main database interface.
type DB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	NewIterator() Iterator
	Close() error
}

// Iterator iterates over key-value pairs in the database.
type Iterator interface {
	First()
	Last()
	Next()
	Prev()
	Seek(key []byte)
	Valid() bool
	Key() []byte
	Value() []byte
}

// TableMeta extends sstable.TableMeta with key range information.
type TableMeta struct {
	FileNum     uint64
	FilePath    string
	FileSize    int64
	SmallestKey internal.InternalKey
	LargestKey  internal.InternalKey
}

// Manifest stores database metadata.
type Manifest struct {
	NextFileNumber uint64
	Sequence       uint64
	Levels         map[int][]*TableMeta
	WALFileNumber  uint64
}

// dbImpl is the internal implementation of DB.
type dbImpl struct {
	path     string
	opts     *Options
	manifest *Manifest
	mem      *memtable.MemTable
	imm      *memtable.MemTable
	wal      *wal.Writer
	seq      uint64
	tables   map[uint64]*sstable.Reader
	closed   bool
	mu       sync.Mutex
}

// Open opens or creates a database at the given path.
func Open(path string, opts *Options) (DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	if opts.MemTableSize <= 0 {
		opts.MemTableSize = 4 * 1024 * 1024
	}
	if opts.BlockSize <= 0 {
		opts.BlockSize = 4 * 1024
	}
	if opts.MaxLevels <= 0 {
		opts.MaxLevels = 7
	}
	if opts.Level0CompactionTrigger <= 0 {
		opts.Level0CompactionTrigger = 4
	}
	if opts.Level1MaxSize <= 0 {
		opts.Level1MaxSize = 10 * 1024 * 1024
	}

	db := &dbImpl{
		path:   path,
		opts:   opts,
		tables: make(map[uint64]*sstable.Reader),
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	manifest, err := loadManifest(path)
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}
	db.manifest = manifest

	// Recover from WAL
	mem := memtable.NewMemTable()
	walPath := filepath.Join(path, fmt.Sprintf("%06d.wal", manifest.WALFileNumber))
	if _, err := os.Stat(walPath); err == nil {
		reader, err := wal.NewReader(walPath)
		if err != nil {
			return nil, fmt.Errorf("open WAL reader: %w", err)
		}

		records, err := reader.ReadAll()
		if err != nil {
			reader.Close()
			return nil, fmt.Errorf("read WAL: %w", err)
		}

		for _, rec := range records {
			ikey := internal.MakeInternalKeyFromBytes(rec.Key)
			mem.Put(ikey, rec.Value)
			if ikey.Sequence() > db.seq {
				db.seq = ikey.Sequence()
			}
		}
		reader.Close()
		os.Remove(walPath)
	}

	db.mem = mem

	// Open all SSTable readers (including Level 0!)
	if manifest.Levels != nil {
		for level, tables := range manifest.Levels {
			for _, meta := range tables {
				reader, err := sstable.OpenReader(meta.FilePath)
				if err != nil {
					for _, r := range db.tables {
						r.Close()
					}
					return nil, fmt.Errorf("open SSTable reader (level %d): %w", level, err)
				}
				db.tables[meta.FileNum] = reader
			}
		}
	}

	// Create new WAL
	newFileNum := manifest.NextFileNumber
	manifest.WALFileNumber = newFileNum
	walPath = filepath.Join(path, fmt.Sprintf("%06d.wal", newFileNum))
	db.wal, err = wal.NewWriter(walPath)
	if err != nil {
		return nil, fmt.Errorf("create WAL: %w", err)
	}

	if err := db.manifest.Save(path); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}

	return db, nil
}

// Put implements DB.Put.
func (db *dbImpl) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if db.closed {
		return ErrDBClosed
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.seq++
	ikey := internal.MakeInternalKey(key, db.seq, internal.TypePut)

	if err := db.wal.Append(internal.TypePut, ikey.Data(), value); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}

	db.mem.Put(ikey, value)

	if db.mem.ApproximateSize() >= db.opts.MemTableSize {
		if err := db.flush(); err != nil {
			return fmt.Errorf("flush memtable: %w", err)
		}
	}

	return nil
}

// Get implements DB.Get.
func (db *dbImpl) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	if db.closed {
		return nil, ErrDBClosed
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Check MemTable
	if val, err := db.mem.Get(key); err == nil {
		return val, nil
	}

	// Check Immutable MemTable
	if db.imm != nil {
		if val, err := db.imm.Get(key); err == nil {
			return val, nil
		}
	}

	// Check Level 0 (all files, newest first)
	if level0Tables := db.manifest.GetTablesForLevel(0); len(level0Tables) > 0 {
		for i := len(level0Tables) - 1; i >= 0; i-- {
			meta := level0Tables[i]
			if reader, ok := db.tables[meta.FileNum]; ok {
				if val, err := reader.Get(key); err == nil && val != nil {
					return val, nil
				}
			}
		}
	}

	// Check Level 1+ (binary search by key range)
	for level := 1; level < db.opts.MaxLevels; level++ {
		tables := db.manifest.GetTablesForLevel(level)
		if len(tables) == 0 {
			continue
		}

		idx := db.searchLevel(level, key)
		if idx < 0 || idx >= len(tables) {
			continue
		}

		meta := tables[idx]
		if string(meta.SmallestKey.UserKey()) > string(key) {
			continue
		}
		if string(meta.LargestKey.UserKey()) < string(key) {
			continue
		}

		if reader, ok := db.tables[meta.FileNum]; ok {
			if val, err := reader.Get(key); err == nil && val != nil {
				return val, nil
			}
		}
	}

	return nil, ErrNotFound
}

func (db *dbImpl) searchLevel(level int, key []byte) int {
	tables := db.manifest.GetTablesForLevel(level)
	if len(tables) == 0 {
		return -1
	}

	lo, hi := 0, len(tables)
	for lo < hi {
		mid := (lo + hi) / 2
		if string(tables[mid].LargestKey.UserKey()) < string(key) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// Delete implements DB.Delete.
func (db *dbImpl) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if db.closed {
		return ErrDBClosed
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.seq++
	ikey := internal.MakeInternalKey(key, db.seq, internal.TypeDelete)

	if err := db.wal.Append(internal.TypeDelete, ikey.Data(), nil); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}

	db.mem.Put(ikey, nil)

	if db.mem.ApproximateSize() >= db.opts.MemTableSize {
		if err := db.flush(); err != nil {
			return fmt.Errorf("flush memtable: %w", err)
		}
	}

	return nil
}

// NewIterator implements DB.NewIterator.
func (db *dbImpl) NewIterator() Iterator {
	return newDBIterator(db)
}

// Close implements DB.Close.
func (db *dbImpl) Close() error {
	if db.closed {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.closed = true

	if db.mem != nil && db.mem.ApproximateSize() > 0 {
		if err := db.flush(); err != nil {
			return fmt.Errorf("flush memtable on close: %w", err)
		}
	}

	if db.wal != nil {
		db.wal.Close()
	}

	for _, reader := range db.tables {
		reader.Close()
	}

	if err := db.manifest.Save(db.path); err != nil {
		return fmt.Errorf("save manifest on close: %w", err)
	}

	return nil
}

// flush writes the current MemTable to SSTable (Level 0).
func (db *dbImpl) flush() error {
	if db.mem == nil || db.mem.ApproximateSize() == 0 {
		return nil
	}

	db.imm = db.mem
	db.mem = memtable.NewMemTable()

	fileNum := db.manifest.NextFileNumber
	db.manifest.NextFileNumber++

	sstPath := filepath.Join(db.path, fmt.Sprintf("%06d.sst", fileNum))

	writer, err := sstable.NewWriter(sstPath, db.opts.BlockSize)
	if err != nil {
		return fmt.Errorf("create SSTable writer: %w", err)
	}

	iter := db.imm.NewIterator()
	var smallestKey, largestKey internal.InternalKey
	first := true

	for iter.First(); iter.Valid(); iter.Next() {
		ikey := iter.Key()
		value := iter.Value()

		if first {
			smallestKey = ikey
			first = false
		}
		largestKey = ikey

		if err := writer.Add(ikey, value); err != nil {
			writer.Abort()
			return fmt.Errorf("add to SSTable: %w", err)
		}
	}

	meta, err := writer.Finish()
	if err != nil {
		return fmt.Errorf("finish SSTable: %w", err)
	}

	tableMeta := &TableMeta{
		FileNum:     fileNum,
		FilePath:    sstPath,
		FileSize:    meta.FileSize,
		SmallestKey: smallestKey,
		LargestKey:  largestKey,
	}

	db.manifest.AddTable(0, tableMeta)

	reader, err := sstable.OpenReader(sstPath)
	if err != nil {
		return fmt.Errorf("open SSTable reader: %w", err)
	}
	db.tables[fileNum] = reader

	walPath := filepath.Join(db.path, fmt.Sprintf("%06d.wal", fileNum))
	newWal, err := wal.NewWriter(walPath)
	if err != nil {
		return fmt.Errorf("create new WAL: %w", err)
	}

	db.wal.Close()

	oldWalPath := filepath.Join(db.path, fmt.Sprintf("%06d.wal", db.manifest.WALFileNumber))
	os.Remove(oldWalPath)

	db.manifest.WALFileNumber = fileNum
	db.wal = newWal

	db.imm = nil

	if err := db.manifest.Save(db.path); err != nil {
		return fmt.Errorf("save manifest after flush: %w", err)
	}

	return nil
}
