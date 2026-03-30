package table

import (
	"github.com/akzj/go-leveldb/util"
)

// Block represents a data block in an SSTable.
// Block format (C++ compatible):
//   [record1][record2]...[recordN][restart_point1]...[restart_pointN][num_restarts:4][crc32:4]
type Block struct {
	data           []byte
	restartOffsets []int
	numRestarts    int
	comparator     util.Comparator // Optional comparator for key comparison
}

// NewBlock creates a block from encoded data.
func NewBlock(data util.Slice) (*Block, *util.Status) {
	b := &Block{
		data:           data.Data(),
		restartOffsets: make([]int, 0),
		numRestarts:    0,
	}

	if len(b.data) < 4 {
		return nil, util.Corruption("block too short")
	}

	b.numRestarts = int(util.DecodeFixed32(b.data[len(b.data)-4:]))
	
	offsetBase := len(b.data) - 4*(b.numRestarts+1)
	for i := 0; i < b.numRestarts; i++ {
		b.restartOffsets = append(b.restartOffsets, int(util.DecodeFixed32(b.data[offsetBase+i*4:])))
	}

	return b, util.NewStatusOK()
}

// SetComparator sets the comparator for key comparison.
func (b *Block) SetComparator(cmp util.Comparator) {
	b.comparator = cmp
}

// BlockIterator provides iteration over a Block.
type BlockIterator struct {
	block       *Block
	pos         int
	key         []byte
	valueOffset int
	valueSize   int
}

// NewBlockIterator creates a new iterator over a block.
func NewBlockIterator(block *Block) Iterator {
	return &BlockIterator{block: block}
}

// Valid implements Iterator.
func (i *BlockIterator) Valid() bool {
	return i.key != nil
}

// SeekToFirst implements Iterator.
func (i *BlockIterator) SeekToFirst() {
	if i.block.numRestarts == 0 {
		i.key = nil
		i.pos = len(i.block.data)
		return
	}
	i.pos = int(util.DecodeFixed32(i.block.data[len(i.block.data)-4:]))
	i.parseNextEntry()
}

// SeekToLast implements Iterator.
func (i *BlockIterator) SeekToLast() {
	if i.block.numRestarts == 0 {
		i.key = nil
		i.pos = len(i.block.data)
		return
	}
	i.pos = int(util.DecodeFixed32(i.block.data[len(i.block.data)-8:]))
	i.parseNextEntry()
	
	for i.Valid() {
		nextPos := i.valueOffset + i.valueSize
		savedPos := i.pos
		savedKey := make([]byte, len(i.key))
		copy(savedKey, i.key)
		savedValueOffset := i.valueOffset
		savedValueSize := i.valueSize
		
		i.pos = nextPos
		if !i.parseNextEntry() {
			i.pos = savedPos
			i.key = savedKey
			i.valueOffset = savedValueOffset
			i.valueSize = savedValueSize
			return
		}
	}
}

// Seek implements Iterator.
func (i *BlockIterator) Seek(target util.Slice) {
	// Binary search restart points
	lo := 0
	hi := i.block.numRestarts

	for lo < hi {
		mid := (lo + hi) / 2
		offset := int(util.DecodeFixed32(i.block.data[len(i.block.data)-4*(mid+1):]))
		key := i.keyAt(offset)
		cmp := i.compareKey(key, target)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// Seek to restart point
	if i.block.numRestarts > 0 {
		i.pos = int(util.DecodeFixed32(i.block.data[len(i.block.data)-4*(lo+1):]))
	} else {
		i.pos = 0
	}
	i.key = nil

	// Linear search
	for i.parseNextEntry() {
		if i.compareKey(i.key, target) >= 0 {
			return
		}
	}
	i.key = nil
}

// Next implements Iterator.
func (i *BlockIterator) Next() {
	if i.key == nil {
		return
	}
	i.pos = i.valueOffset + i.valueSize
	i.parseNextEntry()
}

// Prev implements Iterator.
func (i *BlockIterator) Prev() {
	if i.key == nil {
		return
	}

	restartIndex := 0
	for restartIndex < i.block.numRestarts {
		offset := int(util.DecodeFixed32(i.block.data[len(i.block.data)-4*(restartIndex+1):]))
		if offset >= i.pos {
			break
		}
		restartIndex++
	}

	restartIndex--
	if restartIndex < 0 {
		i.key = nil
		return
	}

	i.pos = int(util.DecodeFixed32(i.block.data[len(i.block.data)-4*(restartIndex+1):]))
	i.key = nil

	var prevKey []byte
	var prevPos, prevValueOffset, prevValueSize int
	
	for i.parseNextEntry() {
		if i.valueOffset+i.valueSize > i.pos {
			break
		}
		prevKey = make([]byte, len(i.key))
		copy(prevKey, i.key)
		prevPos = i.pos
		prevValueOffset = i.valueOffset
		prevValueSize = i.valueSize
	}

	if prevKey != nil {
		i.pos = prevPos
		i.key = prevKey
		i.valueOffset = prevValueOffset
		i.valueSize = prevValueSize
	} else {
		i.key = nil
	}
}

// Key implements Iterator.
func (i *BlockIterator) Key() util.Slice {
	return util.MakeSlice(i.key)
}

// Value implements Iterator.
func (i *BlockIterator) Value() util.Slice {
	return util.MakeSlice(i.block.data[i.valueOffset : i.valueOffset+i.valueSize])
}

// Status implements Iterator.
func (i *BlockIterator) Status() *util.Status {
	return util.NewStatusOK()
}

// Release implements Iterator.
func (i *BlockIterator) Release() {
	i.block = nil
	i.key = nil
}

func (i *BlockIterator) parseNextEntry() bool {
	if i.pos >= len(i.block.data)-4*(i.block.numRestarts+1) {
		i.key = nil
		return false
	}

	p := i.pos
	
	v, n, ok := util.DecodeVarint32(i.block.data[p:])
	if !ok {
		i.key = nil
		return false
	}
	p += n
	shared := int(v)

	v, n, ok = util.DecodeVarint32(i.block.data[p:])
	if !ok {
		i.key = nil
		return false
	}
	p += n
	unshared := int(v)

	v, n, ok = util.DecodeVarint32(i.block.data[p:])
	if !ok {
		i.key = nil
		return false
	}
	p += n
	i.valueSize = int(v)
	i.valueOffset = p

	if shared > 0 && i.key != nil {
		if shared > len(i.key) {
			i.key = nil
			return false
		}
		i.key = append(i.key[:shared], i.block.data[p:p+unshared]...)
	} else {
		if unshared > len(i.block.data)-p {
			i.key = nil
			return false
		}
		i.key = i.block.data[p : p+unshared]
	}

	return true
}

func (i *BlockIterator) keyAt(offset int) []byte {
	if offset >= len(i.block.data)-4 {
		return nil
	}
	
	p := offset
	v, n, ok := util.DecodeVarint32(i.block.data[p:])
	if !ok {
		return nil
	}
	p += n
	shared := int(v)

	v, n, ok = util.DecodeVarint32(i.block.data[p:])
	if !ok {
		return nil
	}
	p += n
	unshared := int(v)

	if shared != 0 || p+unshared > len(i.block.data) {
		return nil
	}
	return i.block.data[p : p+unshared]
}

func (i *BlockIterator) compareKey(a []byte, b util.Slice) int {
	if i.block.comparator != nil {
		return i.block.comparator.Compare(util.MakeSlice(a), b)
	}
	return bytesCompare(a, b.Data())
}

func bytesCompare(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// BlockBuilder builds blocks with restart points.
type BlockBuilder struct {
	blockSize            int
	blockRestartInterval int
	buffer              []byte
	restarts            []int
	entriesSoFar        int
	finished            bool
	lastKey             []byte
}

// NewBlockBuilder creates a new block builder.
func NewBlockBuilder(blockSize, blockRestartInterval int) *BlockBuilder {
	return &BlockBuilder{
		blockSize:            blockSize,
		blockRestartInterval: blockRestartInterval,
		buffer:              make([]byte, 0, blockSize),
		restarts:            []int{0},
	}
}

// Add adds a key-value pair to the block.
func (b *BlockBuilder) Add(key, value util.Slice) []byte {
	keyData := key.Data()
	valueData := value.Data()

	shared := 0
	if b.entriesSoFar > 0 {
		minLen := len(b.lastKey)
		if len(keyData) < minLen {
			minLen = len(keyData)
		}
		for shared < minLen && b.lastKey[shared] == keyData[shared] {
			shared++
		}
	}

	unsharedKeyLen := len(keyData) - shared
	valueLen := len(valueData)

	estimatedSize := len(b.buffer) +
		util.MaxVarint32Length*3 +
		unsharedKeyLen + valueLen +
		len(b.restarts)*4 + 4

	if estimatedSize > b.blockSize && b.entriesSoFar > 0 {
		return nil
	}

	startLen := len(b.buffer)
	b.buffer = util.PutVarint32(b.buffer, uint32(shared))
	b.buffer = util.PutVarint32(b.buffer, uint32(unsharedKeyLen))
	b.buffer = util.PutVarint32(b.buffer, uint32(valueLen))
	b.buffer = append(b.buffer, keyData[shared:]...)
	b.buffer = append(b.buffer, valueData...)

	b.entriesSoFar++
	if b.entriesSoFar >= b.blockRestartInterval {
		b.restarts = append(b.restarts, len(b.buffer))
		b.entriesSoFar = 0
	}

	b.lastKey = make([]byte, len(keyData))
	copy(b.lastKey, keyData)

	return b.buffer[startLen:]
}

// Finish finishes building the block.
func (b *BlockBuilder) Finish() []byte {
	if !b.finished {
		b.finished = true
		for _, r := range b.restarts {
			b.buffer = util.PutFixed32(b.buffer, uint32(r))
		}
		b.buffer = util.PutFixed32(b.buffer, uint32(len(b.restarts)))
	}
	return b.buffer
}

// CurrentSizeEstimate returns the estimated current size.
func (b *BlockBuilder) CurrentSizeEstimate() int {
	return len(b.buffer) + len(b.restarts)*4 + 4
}

// Empty returns true if the block has no entries.
func (b *BlockBuilder) Empty() bool {
	return b.entriesSoFar == 0 && len(b.lastKey) == 0
}

// BlockSize returns the default block size.
func BlockSize() int {
	return 4 * 1024
}

// BlockRestartInterval returns the default restart interval.
func BlockRestartInterval() int {
	return 16
}
