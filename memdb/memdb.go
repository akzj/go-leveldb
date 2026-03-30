package memdb

import (
	"github.com/akzj/go-leveldb/util"
)

// Iterator interface for memdb operations.
// Why not extend db.Iterator? memdb is a low-level package that shouldn't
// depend on db package to avoid import cycles.
type Iterator interface {
	Valid() bool
	SeekToFirst()
	SeekToLast()
	Seek(target util.Slice)
	Next()
	Prev()
	Key() util.Slice
	Value() util.Slice
	Status() *util.Status
	// Release releases the iterator resources.
	// No-op for memdb since skiplist iterator doesn't hold external resources.
	Release()
}

// MemDB is an in-memory skip-list based storage for memtable.
type MemDB struct {
	comparator util.Comparator
	skiplist   *SkipList
	arena      *util.Arena
}

// NewMemDB creates a new memtable with the given comparator.
func NewMemDB(comparator util.Comparator) *MemDB {
	arena := util.NewArena()
	return &MemDB{
		comparator: comparator,
		skiplist:   NewSkipList(comparator, arena),
		arena:      arena,
	}
}

// Put stores a key-value pair in the memtable.
func (m *MemDB) Put(key, value util.Slice) *util.Status {
	m.skiplist.Insert(key.Data(), value.Data())
	return util.NewStatusOK()
}

// Get looks up a key in the memtable.
func (m *MemDB) Get(key util.Slice) ([]byte, *util.Status) {
	node := m.skiplist.Find(key.Data())
	if node == nil {
		return nil, util.NotFound("")
	}
	return node.value, util.NewStatusOK()
}

// Delete removes a key from the memtable.
// In LevelDB, deletion is done by inserting a tombstone marker.
func (m *MemDB) Delete(key util.Slice) *util.Status {
	// LevelDB marks deletion with a special value
	// We use nil value with deletion semantics
	// The skiplist Insert will update existing key with new value
	m.skiplist.Insert(key.Data(), []byte{})
	return util.NewStatusOK()
}

// NewIterator returns an iterator over the memtable.
func (m *MemDB) NewIterator() Iterator {
	return m.skiplist.NewIterator()
}

// ApproximateMemoryUsage returns the approximate memory usage of the memtable.
func (m *MemDB) ApproximateMemoryUsage() int {
	return m.skiplist.ApproximateMemoryUsage()
}
