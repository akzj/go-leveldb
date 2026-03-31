package sstable

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/akzj/go-leveldb/internal"
)

// Reader reads SSTable files.
type Reader struct {
	file       *os.File
	fileSize   int64
	indexBlock []indexEntry
}

// OpenReader opens an SSTable file for reading.
func OpenReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}
	fileSize := fileInfo.Size()

	// Read footer
	footer := make([]byte, FooterSize)
	_, err = file.ReadAt(footer, fileSize-FooterSize)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("read footer: %w", err)
	}

	// Verify magic number
	magic := binary.BigEndian.Uint64(footer[16:24])
	if magic != MagicNumber {
		file.Close()
		return nil, fmt.Errorf("invalid magic number: got 0x%X, want 0x%X", magic, MagicNumber)
	}

	// Read index block offset and size
	indexOffset := binary.BigEndian.Uint64(footer[0:8])
	indexSize := binary.BigEndian.Uint64(footer[8:16])

	// Read index block
	indexBlockData := make([]byte, indexSize)
	_, err = file.ReadAt(indexBlockData, int64(indexOffset))
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("read index block: %w", err)
	}

	indexBlock, err := decodeIndexBlock(indexBlockData)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("decode index block: %w", err)
	}

	return &Reader{
		file:       file,
		fileSize:   fileSize,
		indexBlock: indexBlock,
	}, nil
}

// decodeIndexBlock decodes the index block data.
func decodeIndexBlock(data []byte) ([]indexEntry, error) {
	var entries []indexEntry
	offset := 0

	if len(data) < 4 {
		return nil, fmt.Errorf("index block too short")
	}
	numEntries := int(binary.LittleEndian.Uint32(data[len(data)-4:]))

	for i := 0; i < numEntries && offset < len(data)-4; i++ {
		keyLen, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid key_len at entry %d", i)
		}
		offset += n

		if offset+int(keyLen)+16 > len(data)-4 {
			return nil, fmt.Errorf("truncated index entry %d", i)
		}

		keyData := data[offset : offset+int(keyLen)]
		offset += int(keyLen)

		off := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8

		size := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8

		entries = append(entries, indexEntry{
			largestKey: internal.MakeInternalKeyFromBytes(keyData),
			offset:     off,
			size:       size,
		})
	}

	return entries, nil
}

// Get looks up a user key in the SSTable.
func (r *Reader) Get(userKey []byte) ([]byte, error) {
	blockIndex := r.findBlockIndex(userKey)
	if blockIndex < 0 {
		return nil, nil
	}

	entry := r.indexBlock[blockIndex]
	blockData := make([]byte, entry.size)
	_, err := r.file.ReadAt(blockData, int64(entry.offset))
	if err != nil {
		return nil, fmt.Errorf("read data block: %w", err)
	}

	var latestValue []byte
	var latestSeq uint64

	for offset := 0; offset < len(blockData)-4; {
		keyLen, n := binary.Uvarint(blockData[offset:])
		if n <= 0 {
			break
		}
		offset += n

		valLen, n := binary.Uvarint(blockData[offset:])
		if n <= 0 {
			break
		}
		offset += n

		if offset+int(keyLen) > len(blockData) {
			break
		}
		keyData := blockData[offset : offset+int(keyLen)]
		offset += int(keyLen)

		valLenInt := int(valLen)
		if offset+valLenInt > len(blockData) {
			break
		}
		value := blockData[offset : offset+valLenInt]
		offset += valLenInt

		if len(keyData) < internal.InternalKeyOverhead {
			continue
		}

		ikey := internal.MakeInternalKeyFromBytes(keyData)
		extractedUserKey := ikey.UserKey()

		if string(extractedUserKey) == string(userKey) {
			seq := ikey.Sequence()
			if seq > latestSeq {
				latestSeq = seq
				latestValue = value
			}
		}
	}

	return latestValue, nil
}

// findBlockIndex finds the index of the data block that might contain the key.
func (r *Reader) findBlockIndex(userKey []byte) int {
	if len(r.indexBlock) == 0 {
		return -1
	}

	// Binary search for first block where largest key >= userKey
	lo, hi := 0, len(r.indexBlock)
	for lo < hi {
		mid := (lo + hi) / 2
		blockLargestKey := r.indexBlock[mid].largestKey.UserKey()
		if string(blockLargestKey) < string(userKey) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	if lo >= len(r.indexBlock) {
		return -1
	}
	return lo
}

// NewIterator returns a new iterator for traversing the SSTable.
func (r *Reader) NewIterator() *TableIterator {
	return &TableIterator{
		reader:    r,
		blockData: nil,
		offset:    0,
		key:       internal.InternalKey{},
		value:     nil,
		valid:     false,
	}
}

// Close closes the SSTable reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

// TableIterator provides ordered iteration over SSTable entries.
type TableIterator struct {
	reader     *Reader
	blockData  []byte
	blockIndex int
	offset     int
	key        internal.InternalKey
	value      []byte
	valid      bool
}

func (it *TableIterator) First() bool {
	if len(it.reader.indexBlock) == 0 {
		it.valid = false
		return false
	}

	it.blockIndex = 0
	return it.loadBlockAndSeek(0)
}

func (it *TableIterator) Last() bool {
	if len(it.reader.indexBlock) == 0 {
		it.valid = false
		return false
	}

	it.blockIndex = len(it.reader.indexBlock) - 1
	if !it.loadBlock(it.blockIndex) {
		return false
	}

	entries := it.countEntriesInBlock()
	if entries == 0 {
		it.valid = false
		return false
	}

	it.offset = 0
	for i := 0; i < entries-1; i++ {
		it.offset = it.skipEntryAt(it.offset)
	}

	return it.decodeEntryAt(it.offset)
}

func (it *TableIterator) Next() bool {
	if !it.valid {
		return false
	}

	if it.offset < len(it.blockData)-4 {
		return it.decodeEntry()
	}

	if it.blockIndex+1 < len(it.reader.indexBlock) {
		it.blockIndex++
		return it.loadBlockAndSeek(it.blockIndex)
	}

	it.valid = false
	return false
}

func (it *TableIterator) Prev() bool {
	if !it.valid {
		return false
	}

	prevEnd := 0
	currEnd := 0

	for currEnd < len(it.blockData)-4 {
		if currEnd >= it.offset {
			if prevEnd > 0 && prevEnd < it.offset {
				it.offset = prevEnd
				return it.decodeEntryAt(prevEnd)
			}
			break
		}
		prevEnd = currEnd
		currEnd = it.skipEntryAt(currEnd)
	}

	if it.blockIndex > 0 {
		it.blockIndex--
		if !it.loadBlock(it.blockIndex) {
			return false
		}
		entries := it.countEntriesInBlock()
		if entries == 0 {
			return false
		}
		it.offset = 0
		for i := 0; i < entries-1; i++ {
			it.offset = it.skipEntryAt(it.offset)
		}
		return it.decodeEntry()
	}

	it.valid = false
	return false
}

func (it *TableIterator) Seek(target internal.InternalKey) bool {
	targetUserKey := target.UserKey()

	lo, hi := 0, len(it.reader.indexBlock)
	for lo < hi {
		mid := (lo + hi) / 2
		blockLargestKey := it.reader.indexBlock[mid].largestKey.UserKey()
		if string(blockLargestKey) < string(targetUserKey) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	if lo < len(it.reader.indexBlock) {
		it.blockIndex = lo
		if !it.loadBlock(it.blockIndex) {
			return false
		}
		for it.offset < len(it.blockData)-4 {
			if !it.decodeEntry() {
				return false
			}
			if internal.Compare(it.key, target) >= 0 {
				return true
			}
			it.skipEntry()
		}
	}

	it.valid = false
	return false
}

func (it *TableIterator) Valid() bool {
	return it.valid
}

func (it *TableIterator) Key() internal.InternalKey {
	return it.key
}

func (it *TableIterator) Value() []byte {
	return it.value
}

func (it *TableIterator) Close() error {
	return nil
}

func (it *TableIterator) loadBlock(index int) bool {
	entry := it.reader.indexBlock[index]
	blockData := make([]byte, entry.size)
	_, err := it.reader.file.ReadAt(blockData, int64(entry.offset))
	if err != nil {
		it.valid = false
		return false
	}
	it.blockData = blockData
	it.offset = 0
	return true
}

func (it *TableIterator) loadBlockAndSeek(index int) bool {
	if !it.loadBlock(index) {
		return false
	}
	if it.offset < len(it.blockData)-4 {
		return it.decodeEntry()
	}
	it.valid = false
	return false
}

func (it *TableIterator) decodeEntry() bool {
	if it.offset >= len(it.blockData)-4 {
		it.valid = false
		return false
	}

	offset := it.offset

	keyLen, n := binary.Uvarint(it.blockData[offset:])
	if n <= 0 {
		it.valid = false
		return false
	}
	offset += n

	valLen, n := binary.Uvarint(it.blockData[offset:])
	if n <= 0 {
		it.valid = false
		return false
	}
	offset += n

	if offset+int(keyLen)+int(valLen) > len(it.blockData) {
		it.valid = false
		return false
	}

	keyBytes := make([]byte, keyLen)
	copy(keyBytes, it.blockData[offset:offset+int(keyLen)])
	offset += int(keyLen)

	valueBytes := make([]byte, valLen)
	copy(valueBytes, it.blockData[offset:offset+int(valLen)])

	it.key = internal.MakeInternalKeyFromBytes(keyBytes)
	it.value = valueBytes
	it.offset = offset + int(valLen)
	it.valid = true

	return true
}

func (it *TableIterator) decodeEntryAt(offset int) bool {
	keyLen, n := binary.Uvarint(it.blockData[offset:])
	if n <= 0 {
		return false
	}
	offset += n

	valLen, n := binary.Uvarint(it.blockData[offset:])
	if n <= 0 {
		return false
	}
	offset += n

	if offset+int(keyLen)+int(valLen) > len(it.blockData) {
		return false
	}

	keyBytes := make([]byte, keyLen)
	copy(keyBytes, it.blockData[offset:offset+int(keyLen)])
	offset += int(keyLen)

	valueBytes := make([]byte, valLen)
	copy(valueBytes, it.blockData[offset:offset+int(valLen)])

	it.key = internal.MakeInternalKeyFromBytes(keyBytes)
	it.value = valueBytes
	it.offset = offset + int(valLen)
	it.valid = true

	return true
}

func (it *TableIterator) skipEntry() {
	offset := it.offset

	keyLen, n := binary.Uvarint(it.blockData[offset:])
	if n <= 0 {
		return
	}
	offset += n

	valLen, n := binary.Uvarint(it.blockData[offset:])
	if n <= 0 {
		return
	}
	offset += n

	it.offset = offset + int(keyLen) + int(valLen)
}

func (it *TableIterator) skipEntryAt(offset int) int {
	if offset >= len(it.blockData)-4 {
		return offset
	}

	keyLen, n := binary.Uvarint(it.blockData[offset:])
	if n <= 0 {
		return offset
	}
	offset += n

	if offset >= len(it.blockData)-4 {
		return offset
	}

	valLen, n := binary.Uvarint(it.blockData[offset:])
	if n <= 0 {
		return offset
	}
	offset += n

	if offset+int(keyLen)+int(valLen) > len(it.blockData) {
		return offset
	}

	return offset + int(keyLen) + int(valLen)
}

func (it *TableIterator) countEntriesInBlock() int {
	if len(it.blockData) < 4 {
		return 0
	}
	return int(binary.LittleEndian.Uint32(it.blockData[len(it.blockData)-4:]))
}
