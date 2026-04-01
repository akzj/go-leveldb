package util

import (
	"unsafe"
)

// Arena is a memory allocator optimized for typical LevelDB usage.
// It allocates memory in large blocks and subdivides them.
// This minimizes the number of memory allocations and reduces fragmentation.
type Arena struct {
	// Memory blocks
	blocks [][]byte
	// Current block
	allocPtr int
	allocBytesRemaining int
	// Total memory allocated
	totalBytes uint64
}

// NewArena creates a new arena with the default block size.
func NewArena() *Arena {
	const blockSize = 4096
	return NewArenaSize(blockSize)
}

// NewArenaSize creates a new arena with the specified block size.
func NewArenaSize(blockSize int) *Arena {
	return &Arena{
		blocks:             make([][]byte, 0, 1),
		allocBytesRemaining: blockSize,
	}
}

// Allocate allocates n bytes from the arena.
// Returns a byte slice of length n.
func (a *Arena) Allocate(n int) []byte {
	if n == 0 {
		return nil
	}
	// Align n to 8 bytes
	if n&7 != 0 {
		n = (n + 7) &^ 7
	}
	if n <= a.allocBytesRemaining {
		result := a.blocks[len(a.blocks)-1][a.allocPtr : a.allocPtr+n]
		a.allocPtr += n
		a.allocBytesRemaining -= n
		return result
	}
	// Allocate new block
	return a.allocateFallback(n)
}

func (a *Arena) allocateFallback(n int) []byte {
	if n > a.allocBytesRemaining/4 {
		// Allocate large block directly
		return a.allocateNewBlock(n)
	}
	// Allocate new block of default size
	block := a.allocateNewBlock(a.allocBytesRemaining)
	a.blocks = append(a.blocks, block)
	a.allocPtr = 0
	a.allocBytesRemaining = cap(block)
	// Re-check - n might still be larger than new block size
	if n > a.allocBytesRemaining {
		return a.allocateNewBlock(n)
	}
	result := block[:n]
	a.allocPtr = n
	a.allocBytesRemaining -= n
	return result
}

func (a *Arena) allocateNewBlock(n int) []byte {
	block := make([]byte, n)
	a.blocks = append(a.blocks, block)
	a.totalBytes += uint64(n)
	a.allocPtr = 0
	a.allocBytesRemaining = 0
	return block
}

// AllocateAligned allocates n bytes with natural alignment.
// Used for SkipList nodes.
func (a *Arena) AllocateAligned(n int) []byte {
	// Natural alignment is 8 bytes for 64-bit
	const alignMask = 7
	if a.allocPtr&alignMask != 0 {
		a.allocPtr = (a.allocPtr + alignMask) &^ alignMask
	}
	if a.allocPtr+n > cap(a.blocks[len(a.blocks)-1]) {
		block := a.allocateNewBlock(a.allocBytesRemaining)
		a.blocks = append(a.blocks, block)
		a.allocPtr = 0
	}
	result := a.blocks[len(a.blocks)-1][a.allocPtr : a.allocPtr+n]
	a.allocPtr += n
	a.allocBytesRemaining -= n
	return result
}

// MemoryUsage returns the approximate memory usage of the arena.
func (a *Arena) MemoryUsage() uint64 {
	return a.totalBytes
}

// PointerSize returns the size of a pointer on this platform.
var PointerSize = int(unsafe.Sizeof((*int)(nil)))
