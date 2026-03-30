package log

import (
	"encoding/binary"

	"github.com/akzj/go-leveldb/util"
)

// Writer writes log records to a file.
// Binary format compatible with C++ LevelDB v1.23.
type Writer struct {
	file       WritableFile
	blockSize  int
	blockOffset uint64 // Current position in file
}

// NewWriter creates a new log writer.
func NewWriter(file WritableFile) *Writer {
	return &Writer{
		file:      file,
		blockSize: kBlockSize,
	}
}

// AddRecord writes a record to the log.
// Returns the offset of the record start and status.
func (w *Writer) AddRecord(data []byte) (uint64, *util.Status) {
	recordStart := w.blockOffset
	headerSize := kHeaderSize

	if len(data) == 0 {
		return 0, util.Corruption("empty record")
	}

	// Fragment the record if necessary
	remaining := len(data)
	offset := 0

	for {
		avail := w.blockSize - int(w.blockOffset%uint64(w.blockSize))
		// Need header space
		if avail < headerSize {
			// Fill rest of block with zeros
			if avail > 0 {
				zeros := make([]byte, avail)
				status := w.file.Append(util.MakeSlice(zeros))
				if !status.OK() {
					return 0, status
				}
				w.blockOffset += uint64(avail)
			}
			continue
		}

		// Calculate space available for record data
		fragmentLen := avail - headerSize

		var recordType RecordType
		if remaining <= fragmentLen {
			// Record fits in remaining space
			if w.blockOffset == 0 && remaining == len(data) {
				recordType = kFullType
			} else {
				recordType = kLastType
			}
			fragmentLen = remaining
		} else {
			// Need to fragment
			if w.blockOffset == 0 {
				recordType = kFirstType
			} else {
				recordType = kMiddleType
			}
		}

		// Write header
		header := make([]byte, kHeaderSize)
		// Bytes 0-3: checksum (will be filled after data)
		// Bytes 4-5: length of data
		binary.LittleEndian.PutUint16(header[4:6], uint16(fragmentLen))
		header[6] = byte(recordType)

		// Get fragment data
		fragment := data[offset : offset+fragmentLen]

		// Calculate checksum (covers header[6] and data)
		checksumData := append([]byte{header[6]}, fragment...)
		checksum := util.Value(checksumData)
		binary.LittleEndian.PutUint32(header[0:4], checksum)

		// Write header
		status := w.file.Append(util.MakeSlice(header))
		if !status.OK() {
			return 0, status
		}
		w.blockOffset += uint64(headerSize)

		// Write data
		status = w.file.Append(util.MakeSlice(fragment))
		if !status.OK() {
			return 0, status
		}
		w.blockOffset += uint64(fragmentLen)

		offset += fragmentLen
		remaining -= fragmentLen

		if recordType == kFullType || recordType == kLastType {
			return recordStart, util.NewStatusOK()
		}
	}
}

// WritableFile interface for log operations.
type WritableFile interface {
	Append(data util.Slice) *util.Status
	Close() *util.Status
	Flush() *util.Status
	Sync() *util.Status
}
