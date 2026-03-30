package table

import (
	"github.com/akzj/go-leveldb/util"
)

// File format constants for SSTable.
// Binary format compatible with C++ LevelDB v1.23.

// kTableMagicNumber is used to identify the file type.
// Computed as: echo "http://code.google.com/p/leveldb/" | sha1sum | head -c 16
const kTableMagicNumber uint64 = 0xdb4775248b80fb57

// kBlockTrailerSize is the size of the block trailer (1 byte type + 4 bytes CRC).
const kBlockTrailerSize = 5

// CompressionType for blocks.
type CompressionType uint8

const (
	// KNoCompression indicates no compression.
	KNoCompression CompressionType = 0x0
	// KSnappyCompression indicates snappy compression.
	KSnappyCompression CompressionType = 0x1
	// KZstdCompression indicates zstd compression.
	KZstdCompression CompressionType = 0x2
)

// BlockHandle represents a pointer to a block in a table file.
type BlockHandle struct {
	offset uint64 // Offset of block in file
	size   uint64 // Size of block data (without trailer)
}

// NewBlockHandle creates a new block handle.
func NewBlockHandle(offset, size uint64) BlockHandle {
	return BlockHandle{offset: offset, size: size}
}

// Offset returns the block offset.
func (h *BlockHandle) Offset() uint64 {
	return h.offset
}

// SetOffset sets the block offset.
func (h *BlockHandle) SetOffset(offset uint64) {
	h.offset = offset
}

// Size returns the block size.
func (h *BlockHandle) Size() uint64 {
	return h.size
}

// SetSize sets the block size.
func (h *BlockHandle) SetSize(size uint64) {
	h.size = size
}

// kMaxEncodedLength is the maximum encoding length of a BlockHandle.
// Max varint64 for offset (10 bytes) + Max varint64 for size (10 bytes) = 20 bytes.
const kMaxEncodedBlockHandleLength = 20

// MaxEncodedBlockHandleLength returns the maximum encoded block handle length.
func MaxEncodedBlockHandleLength() int {
	return kMaxEncodedBlockHandleLength
}

// EncodeTo encodes the block handle to dst.
// Format: varint64(offset) + varint64(size)
func (h *BlockHandle) EncodeTo(dst []byte) []byte {
	dst = util.PutVarint64(dst, h.offset)
	dst = util.PutVarint64(dst, h.size)
	return dst
}

// DecodeFrom decodes a block handle from src.
// Returns the number of bytes consumed.
func (h *BlockHandle) DecodeFrom(src []byte) bool {
	v, n, ok := util.DecodeVarint64(src)
	if !ok {
		return false
	}
	h.offset = v
	src = src[n:]
	v, n, ok = util.DecodeVarint64(src)
	if !ok {
		return false
	}
	h.size = v
	return true
}

// Footer encapsulates the fixed information stored at the tail end of every table file.
type Footer struct {
	metaindexHandle BlockHandle
	indexHandle    BlockHandle
}

// NewFooter creates a new footer.
func NewFooter() *Footer {
	return &Footer{}
}

// MetaindexHandle returns the metaindex block handle.
func (f *Footer) MetaindexHandle() *BlockHandle {
	return &f.metaindexHandle
}

// SetMetaindexHandle sets the metaindex block handle.
func (f *Footer) SetMetaindexHandle(h BlockHandle) {
	f.metaindexHandle = h
}

// IndexHandle returns the index block handle.
func (f *Footer) IndexHandle() *BlockHandle {
	return &f.indexHandle
}

// SetIndexHandle sets the index block handle.
func (f *Footer) SetIndexHandle(h BlockHandle) {
	f.indexHandle = h
}

// kEncodedLength is the encoded length of a Footer.
// 2 * kMaxEncodedBlockHandleLength + 8 (magic number) = 48 bytes
const kEncodedFooterLength = 2*kMaxEncodedBlockHandleLength + 8

// EncodedFooterLength returns the encoded footer length.
func EncodedFooterLength() int {
	return kEncodedFooterLength
}

// EncodeTo encodes the footer to dst.
func (f *Footer) EncodeTo(dst []byte) []byte {
	offset := 0
	// Encode metaindex handle
	origLen := len(dst)
	dst = f.metaindexHandle.EncodeTo(dst)
	// Encode index handle
	dst = f.indexHandle.EncodeTo(dst)
	// Append magic number (8 bytes)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0)
	util.EncodeFixed64(dst[origLen+20:], kTableMagicNumber)
	_ = offset // silence unused warning
	return dst
}

// DecodeFrom decodes a footer from src.
func (f *Footer) DecodeFrom(src []byte) bool {
	// Footer is at the end of the file
	// Format: [metaindex handle][index handle][padding][magic]
	// Last 48 bytes contain the footer
	if len(src) < kEncodedFooterLength {
		return false
	}
	// Magic number check
	magic := util.DecodeFixed64(src[kEncodedFooterLength-8:])
	if magic != kTableMagicNumber {
		return false
	}
	// Decode metaindex handle
	ok := f.metaindexHandle.DecodeFrom(src)
	if !ok {
		return false
	}
	// Decode index handle
	ok = f.indexHandle.DecodeFrom(src[20:])
	return ok
}

// BlockContents represents the contents of a block after reading from disk.
type BlockContents struct {
	Data          util.Slice // Actual contents of data
	Cachable      bool        // True iff data can be cached
	HeapAllocated bool        // True iff caller should free data
}

// BlockTrailerSize returns the block trailer size.
func BlockTrailerSize() int {
	return kBlockTrailerSize
}

// TableMagicNumber returns the table magic number.
func TableMagicNumber() uint64 {
	return kTableMagicNumber
}
