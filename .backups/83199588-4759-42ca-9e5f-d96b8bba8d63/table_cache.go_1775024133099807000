package table

import (
	"fmt"

	"github.com/akzj/go-leveldb/cache"
	"github.com/akzj/go-leveldb/util"
)

// Env interface for file operations.
type Env interface {
	NewRandomAccessFile(name string) (RandomAccessFile, *util.Status)
	LockFile(name string) (interface{}, *util.Status)
	UnlockFile(lock interface{}) *util.Status
	ReadFile(name string, data []byte) (int, *util.Status)
	RenameFile(oldName, newName string) *util.Status
	DeleteFile(name string) *util.Status
	GetFileSize(name string) (uint64, *util.Status)
}

// TableCache caches open table files.
type TableCache struct {
	dbName     string
	env        Env
	cache      *cache.LRUCache
	comparator util.Comparator
}

// NewTableCache creates a new table cache.
func NewTableCache(dbName string, env Env, cache *cache.LRUCache, comparator util.Comparator) *TableCache {
	return &TableCache{
		dbName:     dbName,
		env:        env,
		cache:      cache,
		comparator: comparator,
	}
}

// GetTable returns a TableReader for the given file number.
func (tc *TableCache) GetTable(fileNum uint64, fileSize uint64) (*Reader, *util.Status) {
	key := util.MakeSliceFromStr(fmt.Sprintf("%d", fileNum))

	// Try cache
	if handle := tc.cache.Lookup(key); handle.Value() != nil {
		tc.cache.Release(handle)
		return handle.Value().(*Reader), util.NewStatusOK()
	}

	// Open table file
	tableName := TableFileName(fileNum)
	file, err := tc.env.NewRandomAccessFile(tableName)
	if !err.OK() {
		return nil, err
	}

	// Open table reader
	table, err := OpenReader(file, fileSize, tc.comparator)
	if !err.OK() {
		return nil, err
	}

	// Cache it
	tc.cache.Insert(key, table, int(fileSize))

	return table, util.NewStatusOK()
}

// Evict removes a table from the cache.
func (tc *TableCache) Evict(fileNum uint64) {
	key := util.MakeSliceFromStr(fmt.Sprintf("%d", fileNum))
	tc.cache.Erase(key)
}

// TableFileName returns the table file name.
func TableFileName(fileNum uint64) string {
	return fmt.Sprintf("%06d.ldb", fileNum)
}

// NewLRUCache creates a new LRU cache.
func NewLRUCache(capacity int) *cache.LRUCache {
	return cache.NewLRUCache(capacity)
}
