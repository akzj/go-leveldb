package db

import (
	"bytes"

	"github.com/akzj/go-leveldb/internal"
)

// dbIterator iterates over all key-value pairs in the database.
type dbIterator struct {
	db    *dbImpl
	items []*iteratorItem
	pos   int
	valid bool
}

type iteratorItem struct {
	key   []byte
	value []byte
	seq   uint64
}

func newDBIterator(db *dbImpl) *dbIterator {
	return &dbIterator{
		db:    db,
		items: nil,
		pos:   0,
		valid: false,
	}
}

func (it *dbIterator) First() {
	it.collectAll()
	if len(it.items) > 0 {
		it.pos = 0
		it.valid = true
	} else {
		it.valid = false
	}
}

func (it *dbIterator) Last() {
	it.collectAll()
	if len(it.items) > 0 {
		it.pos = len(it.items) - 1
		it.valid = true
	} else {
		it.valid = false
	}
}

func (it *dbIterator) Next() {
	if !it.valid {
		return
	}
	it.pos++
	if it.pos >= len(it.items) {
		it.valid = false
	} else {
		it.valid = true
	}
}

func (it *dbIterator) Prev() {
	if !it.valid {
		return
	}
	if it.pos <= 0 {
		it.valid = false
	} else {
		it.pos--
		it.valid = true
	}
}

func (it *dbIterator) Seek(key []byte) {
	it.collectAll()
	lo, hi := 0, len(it.items)
	for lo < hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(it.items[mid].key, key)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	it.pos = lo
	it.valid = it.pos < len(it.items)
}

func (it *dbIterator) Valid() bool {
	return it.valid && it.pos >= 0 && it.pos < len(it.items)
}

func (it *dbIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.items[it.pos].key
}

func (it *dbIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.items[it.pos].value
}

func (it *dbIterator) collectAll() {
	it.items = make([]*iteratorItem, 0)

	// Collect from MemTable
	if it.db.mem != nil {
		iter := it.db.mem.NewIterator()
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			if key.Type() == internal.TypeDelete {
				continue
			}
			it.items = append(it.items, &iteratorItem{
				key:   bytes.Clone(key.UserKey()),
				value: bytes.Clone(iter.Value()),
				seq:   key.Sequence(),
			})
		}
	}

	// Collect from Immutable MemTable
	if it.db.imm != nil {
		iter := it.db.imm.NewIterator()
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			if key.Type() == internal.TypeDelete {
				continue
			}
			it.items = append(it.items, &iteratorItem{
				key:   bytes.Clone(key.UserKey()),
				value: bytes.Clone(iter.Value()),
				seq:   key.Sequence(),
			})
		}
	}

	// Collect from SSTables
	for level := 0; level < it.db.opts.MaxLevels; level++ {
		tables := it.db.manifest.GetTablesForLevel(level)
		if tables == nil {
			continue
		}
		for _, meta := range tables {
			if reader, ok := it.db.tables[meta.FileNum]; ok {
				iter := reader.NewIterator()
				for iter.First(); iter.Valid(); iter.Next() {
					key := iter.Key()
					if key.Type() == internal.TypeDelete {
						continue
					}
					it.items = append(it.items, &iteratorItem{
						key:   bytes.Clone(key.UserKey()),
						value: bytes.Clone(iter.Value()),
						seq:   key.Sequence(),
					})
				}
			}
		}
	}

	// Sort by key (ascending), then seq (descending) - highest seq first
	for i := 0; i < len(it.items); i++ {
		for j := i + 1; j < len(it.items); j++ {
			cmp := bytes.Compare(it.items[i].key, it.items[j].key)
			if cmp > 0 || (cmp == 0 && it.items[i].seq < it.items[j].seq) {
				it.items[i], it.items[j] = it.items[j], it.items[i]
			}
		}
	}

	// Remove duplicates: keep only the first occurrence (highest seq due to sorting)
	unique := make([]*iteratorItem, 0, len(it.items))
	seen := make(map[string]bool)
	for _, item := range it.items {
		keyStr := string(item.key)
		if !seen[keyStr] {
			seen[keyStr] = true
			unique = append(unique, item)
		}
	}
	it.items = unique
}

var _ Iterator = (*dbIterator)(nil)
