package log

// Record type constants for WAL format.
// See doc/log_format.md for detailed specification.
//
// Binary format compatible with C++ LevelDB v1.23.

// RecordType represents the type of log record.
type RecordType uint8

const (
	// kZeroType is reserved for preallocated files.
	kZeroType RecordType = 0
	// kFullType is a full record that fits in one block.
	kFullType RecordType = 1
	// kFirstType is the first fragment of a multi-block record.
	kFirstType RecordType = 2
	// kMiddleType is a middle fragment of a multi-block record.
	kMiddleType RecordType = 3
	// kLastType is the last fragment of a multi-block record.
	kLastType RecordType = 4
)

// kMaxRecordType is the maximum valid record type.
const kMaxRecordType = kLastType

// kBlockSize is the size of each block in the log file.
// Matches C++ LevelDB: 32768 bytes.
const kBlockSize = 32768

// kHeaderSize is the size of the record header.
// Format: checksum (4 bytes) + length (2 bytes) + type (1 byte) = 7 bytes.
const kHeaderSize = 7

// Record constants for compatibility checks.
const (
	// RecordTypeZero is the zero record type value.
	RecordTypeZero = kZeroType
	// RecordTypeFull is the full record type value.
	RecordTypeFull = kFullType
	// RecordTypeFirst is the first fragment record type.
	RecordTypeFirst = kFirstType
	// RecordTypeMiddle is the middle fragment record type.
	RecordTypeMiddle = kMiddleType
	// RecordTypeLast is the last fragment record type.
	RecordTypeLast = kLastType
)

// BlockSize returns the log block size for compatibility checks.
func BlockSize() int {
	return kBlockSize
}

// HeaderSize returns the log record header size for compatibility checks.
func HeaderSize() int {
	return kHeaderSize
}

// MaxRecordType returns the maximum valid record type.
func MaxRecordType() int {
	return int(kMaxRecordType)
}
