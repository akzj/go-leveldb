package log

import (
	"encoding/binary"

	"github.com/akzj/go-leveldb/util"
)

// Reader reads log records from a file.
// Binary format compatible with C++ LevelDB v1.23.
type Reader struct {
	file          SequentialFile
	blockSize     int
	offset        uint64 // Current read position in file
	eof           bool
	partialRecord bool   // True if we have a partial record
}

// NewReader creates a new log reader.
func NewReader(file SequentialFile) *Reader {
	return &Reader{
		file:      file,
		blockSize: kBlockSize,
	}
}

// ReadRecord reads the next record from the log.
// Returns (record, status).
func (r *Reader) ReadRecord() ([]byte, *util.Status) {
	var result []byte

	for {
		// Check if we need to skip to next block
		offsetInBlock := r.offset % uint64(r.blockSize)
		if offsetInBlock == 0 && r.offset > 0 {
			// At block boundary, skip to next block
			if r.eof {
				return nil, util.NewStatusOK()
			}
			r.offset = (r.offset/uint64(r.blockSize) + 1) * uint64(r.blockSize)
			continue
		}

		// Check if we're at end of file
		if r.eof && offsetInBlock >= uint64(r.blockSize-1) {
			return nil, util.NewStatusOK()
		}

		// Calculate bytes remaining in current block
		remaining := uint64(r.blockSize) - offsetInBlock
		if remaining < kHeaderSize {
			// Skip to next block
			r.offset += remaining
			continue
		}

		// Read header
		header := make([]byte, kHeaderSize)
		data, status := r.file.Read(kHeaderSize)
		if !status.OK() {
			if len(data) == 0 {
				r.eof = true
				continue
			}
			return nil, status
		}
		copy(header, data)
		r.offset += kHeaderSize

		// Parse header
		checksum := binary.LittleEndian.Uint32(header[0:4])
		length := binary.LittleEndian.Uint16(header[4:6])
		recordType := RecordType(header[6])

		// Validate record type
		if recordType < kFullType || recordType > kLastType {
			// Skip this block on corruption
			r.offset = (r.offset/uint64(r.blockSize) + 1) * uint64(r.blockSize)
			continue
		}

		// Check for zero header (block without data)
		if checksum == 0 && length == 0 && recordType == kZeroType {
			// End of block with no data
			r.offset = (r.offset/uint64(r.blockSize) + 1) * uint64(r.blockSize)
			continue
		}

		// Read record data
		if length > 0 {
			readLen := int(length)
			if r.offset+uint64(readLen) > uint64(r.blockSize)*(r.offset/uint64(r.blockSize)+1) {
				// Would cross block boundary
				r.offset = (r.offset/uint64(r.blockSize) + 1) * uint64(r.blockSize)
				continue
			}

			recordData, status := r.file.Read(readLen)
			if !status.OK() {
				return nil, status
			}
			r.offset += uint64(readLen)

			// Verify checksum
			checksumData := append([]byte{header[6]}, recordData...)
			computedChecksum := util.Value(checksumData)
			if computedChecksum != checksum {
				// Checksum mismatch, skip block
				r.offset = (r.offset/uint64(r.blockSize) + 1) * uint64(r.blockSize)
				continue
			}

			// Handle record type
			switch recordType {
			case kFullType:
				return recordData, util.NewStatusOK()
			case kFirstType:
				result = recordData
				r.partialRecord = true
			case kMiddleType:
				if !r.partialRecord {
					continue
				}
				result = append(result, recordData...)
			case kLastType:
				if !r.partialRecord {
					continue
				}
				result = append(result, recordData...)
				r.partialRecord = false
				return result, util.NewStatusOK()
			}
		}
	}
}

// SkipToBlockBoundary skips to the next block boundary.
func (r *Reader) SkipToBlockBoundary() {
	blockStart := r.offset / uint64(r.blockSize)
	r.offset = (blockStart + 1) * uint64(r.blockSize)
}

// SequentialFile interface for log operations.
type SequentialFile interface {
	Read(n int) ([]byte, *util.Status)
	Skip(n uint64) *util.Status
}
