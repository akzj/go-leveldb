// Package sstable provides SSTable (Sorted String Table) implementation.
// SSTable stores key-value pairs in a sorted order with block-based organization.
package sstable

import "github.com/akzj/go-leveldb/internal"

const (
	// FooterSize is the size of the SSTable footer in bytes.
	FooterSize = 48

	// MagicNumber is the magic number stored in the footer.
	MagicNumber = 0x1234567890ABCDEF

	// DefaultBlockSize is the default size of data blocks in bytes.
	DefaultBlockSize = 4 * 1024 // 4KB
)

// TableMeta contains metadata about a completed SSTable.
type TableMeta struct {
	// FilePath is the path to the SSTable file.
	FilePath string
	// FileSize is the size of the SSTable file in bytes.
	FileSize int64
	// NumDataBlocks is the number of data blocks in the table.
	NumDataBlocks int
	// NumEntries is the total number of key-value entries.
	NumEntries int
}

// indexEntry represents an entry in the index block.
type indexEntry struct {
	largestKey internal.InternalKey
	offset     uint64
	size       uint64
}