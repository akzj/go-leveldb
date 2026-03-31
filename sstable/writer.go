package sstable

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/akzj/go-leveldb/internal"
)

// Writer writes SSTable files in sorted order.
type Writer struct {
	file           *os.File
	blockSize      int
	dataBlockBuf   []byte // current data block being built
	indexBlock     []indexEntry
	numEntries     int
	curBlockOffset uint64
	lastKey        internal.InternalKey // last key added, used for index block
	blockEntryCount int                 // entries in current data block
}

// NewWriter creates a new SSTable writer.
// The file is created and ready for writing. Data blocks are written
// when they reach approximately blockSize bytes.
func NewWriter(path string, blockSize int) (*Writer, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}

	w := &Writer{
		file:           file,
		blockSize:      blockSize,
		dataBlockBuf:   make([]byte, 0, blockSize*2),
		indexBlock:     make([]indexEntry, 0, 100),
		curBlockOffset: 0,
	}

	return w, nil
}

// Add writes a key-value entry to the SSTable.
// Keys must be added in sorted InternalKey order (user_key ascending,
// sequence descending).
func (w *Writer) Add(key internal.InternalKey, value []byte) error {
	// Use the FULL internal key (user_key + sequence + type), not just user key
	keyBytes := key.Data()
	keyLen := len(keyBytes)
	valLen := len(value)

	// Calculate space needed for varints (max 10 bytes each)
	// We'll use append which handles growth automatically
	var buf []byte
	buf = binary.AppendUvarint(buf, uint64(keyLen))
	buf = binary.AppendUvarint(buf, uint64(valLen))

	// Append key and value
	buf = append(buf, keyBytes...)
	buf = append(buf, value...)

	// Grow dataBlockBuf if needed
	if cap(w.dataBlockBuf)-len(w.dataBlockBuf) < len(buf) {
		newBuf := make([]byte, len(w.dataBlockBuf), cap(w.dataBlockBuf)+len(buf))
		copy(newBuf, w.dataBlockBuf)
		w.dataBlockBuf = newBuf
	}

	// Append to data block buffer
	w.dataBlockBuf = append(w.dataBlockBuf, buf...)

	w.numEntries++
	w.blockEntryCount++
	w.lastKey = key

	// Flush block if it reaches target size
	if len(w.dataBlockBuf) >= w.blockSize {
		return w.flushDataBlock()
	}

	return nil
}

// flushDataBlock writes the current data block to the file.
func (w *Writer) flushDataBlock() error {
	if len(w.dataBlockBuf) == 0 {
		return nil
	}

	// Add num_entries trailer (4 bytes LE)
	var numBuf [4]byte
	binary.LittleEndian.PutUint32(numBuf[:], uint32(w.blockEntryCount))
	w.dataBlockBuf = append(w.dataBlockBuf, numBuf[:]...)

	offset := w.curBlockOffset
	size := uint64(len(w.dataBlockBuf))

	// Write block data
	_, err := w.file.Write(w.dataBlockBuf)
	if err != nil {
		return fmt.Errorf("write data block: %w", err)
	}

	// Add to index (largest key is the last key added to this block)
	w.indexBlock = append(w.indexBlock, indexEntry{
		largestKey: w.lastKey,
		offset:     offset,
		size:       size,
	})

	w.curBlockOffset += size
	w.dataBlockBuf = w.dataBlockBuf[:0]
	w.blockEntryCount = 0

	return nil
}

// Finish completes the SSTable writing process.
// It writes the final data block (if any), the index block, and the footer.
// Returns TableMeta with file statistics.
func (w *Writer) Finish() (*TableMeta, error) {
	// Flush remaining data block
	if len(w.dataBlockBuf) > 0 {
		if err := w.flushDataBlock(); err != nil {
			return nil, err
		}
	}

	indexBlockOffset := w.curBlockOffset

	// Write index block
	indexBlockData, err := w.encodeIndexBlock()
	if err != nil {
		return nil, fmt.Errorf("encode index block: %w", err)
	}

	_, err = w.file.Write(indexBlockData)
	if err != nil {
		return nil, fmt.Errorf("write index block: %w", err)
	}

	// Write footer
	footer := w.encodeFooter(indexBlockOffset, uint64(len(indexBlockData)))
	_, err = w.file.Write(footer)
	if err != nil {
		return nil, fmt.Errorf("write footer: %w", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		return nil, fmt.Errorf("close file: %w", err)
	}

	// Get file size
	fileInfo, err := os.Stat(w.file.Name())
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	return &TableMeta{
		FilePath:      w.file.Name(),
		FileSize:      fileInfo.Size(),
		NumDataBlocks: len(w.indexBlock),
		NumEntries:    w.numEntries,
	}, nil
}

// encodeIndexBlock encodes the index block.
// Format: for each entry: key_len(varint) | key | offset(8 bytes) | size(8 bytes)
// End: num_entries (4 bytes uint32 LE)
func (w *Writer) encodeIndexBlock() ([]byte, error) {
	var buf []byte

	for _, entry := range w.indexBlock {
		// Use FULL internal key for index block
		keyBytes := entry.largestKey.Data()

		// key_len varint
		buf = binary.AppendUvarint(buf, uint64(len(keyBytes)))

		// key
		buf = append(buf, keyBytes...)

		// offset (8 bytes BE)
		var offsetBuf [8]byte
		binary.BigEndian.PutUint64(offsetBuf[:], entry.offset)
		buf = append(buf, offsetBuf[:]...)

		// size (8 bytes BE)
		var sizeBuf [8]byte
		binary.BigEndian.PutUint64(sizeBuf[:], entry.size)
		buf = append(buf, sizeBuf[:]...)
	}

	// Write num_entries (4 bytes LE)
	numEntries := uint32(len(w.indexBlock))
	var numBuf [4]byte
	binary.LittleEndian.PutUint32(numBuf[:], numEntries)
	buf = append(buf, numBuf[:]...)

	return buf, nil
}

// encodeFooter creates the 48-byte footer.
func (w *Writer) encodeFooter(indexOffset, indexSize uint64) []byte {
	footer := make([]byte, FooterSize)

	// Index Block Offset (8 bytes)
	binary.BigEndian.PutUint64(footer[0:8], indexOffset)

	// Index Block Size (8 bytes)
	binary.BigEndian.PutUint64(footer[8:16], indexSize)

	// Magic Number (8 bytes)
	binary.BigEndian.PutUint64(footer[16:24], MagicNumber)

	// Padding (24 bytes zeros) - footer[24:48] already zero

	return footer
}

// Abort cancels the SSTable writing process and deletes the incomplete file.
func (w *Writer) Abort() error {
	if w.file != nil {
		w.file.Close()
	}
	return os.Remove(w.file.Name())
}