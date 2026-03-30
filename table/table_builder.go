package table

import (
	"github.com/akzj/go-leveldb/util"
)

// Comparator is the user comparator for table operations.
type Comparator = util.Comparator

// TableBuilderOptions controls the table builder behavior.
type TableBuilderOptions struct {
	// Comparator used to determine key ordering.
	Comparator Comparator

	// Compression algorithm for blocks.
	Compression CompressionType

	// Bloom filter policy.
	FilterPolicy FilterPolicy

	// Approximate size of user data per block.
	BlockSize int

	// Number of keys between restart points.
	BlockRestartInterval int

	// Maximum file size before switching to a new file.
	MaxFileSize int
}

// TableBuilderImpl is a basic implementation of TableBuilder.
type TableBuilderImpl struct {
	options          *TableBuilderOptions
	file             WritableFile
	offset           uint64
	comparator       Comparator
	dataBlock        *BlockBuilder
	indexBlock       *BlockBuilder
	filterBlock      *FilterBlockBuilder
	pendingIndexEntry bool
	pendingDataHandle BlockHandle
	lastKey          []byte
	entryCount       int
	finished         bool
	status           *util.Status
}

// NewTableBuilder creates a new table builder.
func NewTableBuilder(file WritableFile, options *TableBuilderOptions) *TableBuilderImpl {
	if options == nil {
		options = &TableBuilderOptions{}
	}
	blockSize := options.BlockSize
	if blockSize == 0 {
		blockSize = 4096
	}
	restartInterval := options.BlockRestartInterval
	if restartInterval == 0 {
		restartInterval = 16
	}

	t := &TableBuilderImpl{
		options:    options,
		file:       file,
		comparator: options.Comparator,
		dataBlock: NewBlockBuilder(blockSize, restartInterval),
		indexBlock: NewBlockBuilder(blockSize, 1), // Index blocks have restart interval 1
	}

	if options.FilterPolicy != nil {
		t.filterBlock = NewFilterBlockBuilder(options.FilterPolicy)
	}

	return t
}

// Add implements TableBuilder.
func (t *TableBuilderImpl) Add(key, value util.Slice) *util.Status {
	if t.finished {
		return util.Corruption("cannot add to finished table")
	}

	// Check if we need to finish current data block
	if t.dataBlock.CurrentSizeEstimate() >= t.options.BlockSize && len(t.lastKey) > 0 {
		t.flushDataBlock()
	}

	// Add to filter block if present
	if t.filterBlock != nil {
		t.filterBlock.AddKey(key)
	}

	// Add to data block
	t.dataBlock.Add(key, value)

	// Update last key for index
	t.lastKey = make([]byte, key.Size())
	key.CopyTo(t.lastKey)
	t.entryCount++
	t.pendingIndexEntry = true

	return util.NewStatusOK()
}

// flushDataBlock writes the current data block to file.
func (t *TableBuilderImpl) flushDataBlock() {
	// Add index entry for previous data block
	if t.pendingIndexEntry && len(t.lastKey) > 0 {
		handleData := make([]byte, kMaxEncodedBlockHandleLength)
	t.pendingDataHandle.EncodeTo(handleData)
		t.indexBlock.Add(util.MakeSlice(t.lastKey), util.MakeSlice(handleData))
		t.pendingIndexEntry = false
	}

	// Get current block data
	blockData := t.dataBlock.Finish()

	// Write block to file
	t.status = t.file.Append(util.MakeSlice(blockData))
	if !t.status.OK() {
		return
	}

	blockSize := uint64(len(blockData))
	t.offset += blockSize

	// Store handle for next index entry
	t.pendingDataHandle = NewBlockHandle(t.offset-blockSize, blockSize)

	// Reset data block
	restartInterval := t.options.BlockRestartInterval
	if restartInterval == 0 {
		restartInterval = 16
	}
	blockSizeOpt := t.options.BlockSize
	if blockSizeOpt == 0 {
		blockSizeOpt = 4096
	}
	t.dataBlock = NewBlockBuilder(blockSizeOpt, restartInterval)
}

// Flush implements TableBuilder.
func (t *TableBuilderImpl) Flush() *util.Status {
	if t.finished {
		return util.NewStatusOK()
	}

	// Write filter block if present
	if t.filterBlock != nil {
		filterData := t.filterBlock.Finish()
		if len(filterData) > 0 {
			filterHandle := NewBlockHandle(t.offset, uint64(len(filterData)))
			t.status = t.file.Append(util.MakeSlice(filterData))
			if !t.status.OK() {
				return t.status
			}
			t.offset += uint64(len(filterData))
			_ = filterHandle
		}
	}

	// Write final data block if not empty
	if !t.dataBlock.Empty() {
		t.flushDataBlock()
	}

	// Write index block
	if t.pendingIndexEntry && len(t.lastKey) > 0 {
		handleData := make([]byte, kMaxEncodedBlockHandleLength)
	t.pendingDataHandle.EncodeTo(handleData)
		t.indexBlock.Add(util.MakeSlice(t.lastKey), util.MakeSlice(handleData))
		t.pendingIndexEntry = false
	}

	indexData := t.indexBlock.Finish()
	t.status = t.file.Append(util.MakeSlice(indexData))
	if !t.status.OK() {
		return t.status
	}
	t.offset += uint64(len(indexData))

	indexHandle := NewBlockHandle(t.offset-uint64(len(indexData)), uint64(len(indexData)))

	// Write footer
	footer := NewFooter()
	footer.SetIndexHandle(indexHandle)
	footerBuf := make([]byte, EncodedFooterLength())
	footer.EncodeTo(footerBuf)
	t.status = t.file.Append(util.MakeSlice(footerBuf))
	if !t.status.OK() {
		return t.status
	}
	t.offset += uint64(len(footerBuf))

	return util.NewStatusOK()
}

// Finish implements TableBuilder.
func (t *TableBuilderImpl) Finish() (uint64, *util.Status) {
	if t.finished {
		return t.offset, t.status
	}

	t.finished = true
	t.Flush()
	return t.offset, t.status
}

// Abandon implements TableBuilder.
func (t *TableBuilderImpl) Abandon() {
	t.finished = true
}

// FileSize implements TableBuilder.
func (t *TableBuilderImpl) FileSize() uint64 {
	return t.offset
}

// Status implements TableBuilder.
func (t *TableBuilderImpl) Status() *util.Status {
	return t.status
}

// WritableFile interface for table operations.
type WritableFile interface {
	Append(data util.Slice) *util.Status
	Close() *util.Status
	Flush() *util.Status
	Sync() *util.Status
}
