package table

import (
	"github.com/akzj/go-leveldb/util"
)

// Reader provides table file access.
type Reader struct {
	file            RandomAccessFile
	fileSize        uint64
	metaindexHandle *BlockHandle
	indexHandle     *BlockHandle
	filter          *FilterBlockReader
	options         *Options
	comparator      util.Comparator

	// Cached blocks
	indexBlock     *Block
	filterBlock    *Block
	metaindexBlock *Block

	cacheId    uint64
	pinnedData []byte
}

// Options for opening a table.
type Options struct {
	VerifyChecksums bool
	FillCache      bool
}

// NewOptions returns default table reader options.
func NewOptions() *Options {
	return &Options{
		VerifyChecksums: false,
		FillCache:      true,
	}
}

// OpenReader opens a table reader.
func OpenReader(file RandomAccessFile, fileSize uint64, comparator util.Comparator) (*Reader, *util.Status) {
	if fileSize < kEncodedFooterLength {
		return nil, util.Corruption("file too short for table")
	}

	// Read footer
	footerOffset := fileSize - kEncodedFooterLength
	var footer Footer
	footerData := make([]byte, kEncodedFooterLength)
	if _, err := file.ReadAt(footerData, int64(footerOffset)); !err.OK() {
		return nil, err
	}
	if !footer.DecodeFrom(footerData) {
		return nil, util.Corruption("bad table footer")
	}

	r := &Reader{
		file:            file,
		fileSize:        fileSize,
		comparator:      comparator,
		metaindexHandle: footer.MetaindexHandle(),
		indexHandle:     footer.IndexHandle(),
	}

	// Read index block
	indexBlock, err := r.readBlock(r.indexHandle)
	if !err.OK() {
		return nil, err
	}
	r.indexBlock = indexBlock
	r.indexBlock.SetComparator(comparator)

	return r, util.NewStatusOK()
}

// Get looks up a key in the table.
func (r *Reader) Get(key util.Slice) ([]byte, *util.Status) {
	// Check bloom filter first (nil is safe)
	if r.filter != nil && !r.filter.KeyMayMatch(key) {
		return nil, util.NotFound("")
	}

	// Binary search index block
	iter := NewBlockIterator(r.indexBlock)
	defer iter.Release()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, util.NotFound("")
	}

	// Get block handle
	handle := decodeBlockHandle(iter.Value().Data())

	// Read data block
	block, err := r.readBlock(handle)
	if !err.OK() {
		return nil, err
	}
	block.SetComparator(r.comparator)

	// Binary search in data block
	dataIter := NewBlockIterator(block)
	dataIter.Seek(key)

	if !dataIter.Valid() {
		return nil, util.NotFound("")
	}

	// Check key match
	dataKey := dataIter.Key()
	userKey := extractUserKey(dataKey)
	if r.comparator.Compare(userKey, key) != 0 {
		return nil, util.NotFound("")
	}

	return dataIter.Value().Data(), util.NewStatusOK()
}

// NewIterator returns an iterator over the table.
func (r *Reader) NewIterator() Iterator {
	if r.indexBlock == nil {
		return NewEmptyIterator()
	}

	indexIter := NewBlockIterator(r.indexBlock)
	return NewTwoLevelIterator(indexIter, tableBlockFunction, r)
}

// readBlock reads a block from the file.
func (r *Reader) readBlock(handle *BlockHandle) (*Block, *util.Status) {
	data := make([]byte, handle.Size()+kBlockTrailerSize)
	if _, err := r.file.ReadAt(data, int64(handle.Offset())); !err.OK() {
		return nil, err
	}
	return NewBlock(util.MakeSlice(data[:handle.Size()]))
}

// Size returns the approximate size of the table file.
func (r *Reader) Size() uint64 {
	return r.fileSize
}

// ApproximateOffsetOf returns approximate file offset for key.
func (r *Reader) ApproximateOffsetOf(key util.Slice) uint64 {
	iter := NewBlockIterator(r.indexBlock)
	iter.Seek(key)
	if !iter.Valid() {
		return r.fileSize
	}
	handle := decodeBlockHandle(iter.Value().Data())
	return handle.Offset()
}

// Close closes the table reader.
func (r *Reader) Close() *util.Status {
	return util.NewStatusOK() // RandomAccessFile doesn't have Close
}

// RandomAccessFile interface for table reading.
type RandomAccessFile interface {
	ReadAt(p []byte, offset int64) (n int, err *util.Status)
}

// extractUserKey extracts the user key from an internal key.
func extractUserKey(internalKey util.Slice) util.Slice {
	n := internalKey.Size()
	if n < 8 {
		return util.MakeSlice(nil)
	}
	return util.MakeSlice(internalKey.Data()[:n-8])
}

// tableBlockFunction creates a data block iterator from a block handle.
func tableBlockFunction(arg interface{}, indexValue util.Slice) Iterator {
	tbl := arg.(*Reader)
	handle := decodeBlockHandle(indexValue.Data())
	block, err := tbl.readBlock(handle)
	if !err.OK() {
		return NewErrorIterator(err)
	}
	block.SetComparator(tbl.comparator)
	return NewBlockIterator(block)
}

// decodeBlockHandle decodes a block handle from encoded form.
func decodeBlockHandle(data []byte) *BlockHandle {
	handle := &BlockHandle{}
	handle.DecodeFrom(data)
	return handle
}
