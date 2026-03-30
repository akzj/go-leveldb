package db

import (
	"github.com/akzj/go-leveldb/util"
)

// VersionEdit records a set of changes to a Version.
// Binary format compatible with C++ LevelDB v1.23.
// Format: sequence of tagged records
//   tag=1: comparator name (length-prefixed string)
//   tag=2: log number (fixed64)
//   tag=3: next file number (fixed64)
//   tag=4: last sequence (fixed64)
//   tag=5: compact pointer (varint level + internal key)
//   tag=6: deleted file (varint level + fixed64 file number)
//   tag=7: new file (see AddFile)
type VersionEdit struct {
	Comparator     string
	LogNumber      uint64
	PrevLogNumber  uint64
	NextFileNumber uint64
	LastSequence   SequenceNumber

	CompactPointers map[int]InternalKey
	DeletedFiles    map[DeletedFileKey]bool
	NewFiles        []NewFileInfo

	// Internal state flags
	hasComparator     bool
	hasLogNumber     bool
	hasPrevLogNumber bool
	hasNextFileNumber bool
	hasLastSequence  bool
}

// DeletedFileKey is used as map key for deleted files.
type DeletedFileKey struct {
	Level      int
	FileNumber uint64
}

// NewFileInfo represents a new SSTable file.
type NewFileInfo struct {
	Level int
	Meta  FileMetaData
}

// Tag constants matching C++ LevelDB
const (
	tagComparator = iota + 1
	tagLogNumber
	tagNextFile
	tagLastSeq
	tagCompactPtr
	tagDeleted
	tagNewFile
)

// NewVersionEdit creates a new version edit.
func NewVersionEdit() *VersionEdit {
	return &VersionEdit{
		CompactPointers: make(map[int]InternalKey),
		DeletedFiles:    make(map[DeletedFileKey]bool),
		NewFiles:        make([]NewFileInfo, 0),
	}
}

// Clear resets the version edit.
func (e *VersionEdit) Clear() {
	e.Comparator = ""
	e.LogNumber = 0
	e.PrevLogNumber = 0
	e.NextFileNumber = 0
	e.LastSequence = 0
	e.CompactPointers = make(map[int]InternalKey)
	e.DeletedFiles = make(map[DeletedFileKey]bool)
	e.NewFiles = e.NewFiles[:0]
	e.hasComparator = false
	e.hasLogNumber = false
	e.hasPrevLogNumber = false
	e.hasNextFileNumber = false
	e.hasLastSequence = false
}

// SetComparatorName sets the comparator name.
func (e *VersionEdit) SetComparatorName(name string) {
	e.Comparator = name
	e.hasComparator = true
}

// SetLogNumber sets the log number.
func (e *VersionEdit) SetLogNumber(num uint64) {
	e.LogNumber = num
	e.hasLogNumber = true
}

// SetPrevLogNumber sets the previous log number.
func (e *VersionEdit) SetPrevLogNumber(num uint64) {
	e.PrevLogNumber = num
	e.hasPrevLogNumber = true
}

// SetNextFile sets the next file number.
func (e *VersionEdit) SetNextFile(num uint64) {
	e.NextFileNumber = num
	e.hasNextFileNumber = true
}

// SetLastSequence sets the last sequence number.
func (e *VersionEdit) SetLastSequence(seq SequenceNumber) {
	e.LastSequence = seq
	e.hasLastSequence = true
}

// SetCompactPointer sets the compaction pointer for a level.
func (e *VersionEdit) SetCompactPointer(level int, key InternalKey) {
	e.CompactPointers[level] = key
}

// AddFile adds a new SSTable file to the version.
func (e *VersionEdit) AddFile(level int, meta FileMetaData) {
	e.NewFiles = append(e.NewFiles, NewFileInfo{Level: level, Meta: meta})
}

// RemoveFile marks a file for deletion.
func (e *VersionEdit) RemoveFile(level int, fileNum uint64) {
	e.DeletedFiles[DeletedFileKey{Level: level, FileNumber: fileNum}] = true
}

// EncodeTo encodes the version edit to a byte slice.
// Format matches C++ LevelDB exactly.
func (e *VersionEdit) EncodeTo() []byte {
	var result []byte

	// Comparator
	if e.hasComparator {
		result = append(result, tagComparator)
		result = util.PutLengthPrefixedSlice(result, util.MakeSliceFromStr(e.Comparator))
	}

	// Log number
	if e.hasLogNumber {
		result = append(result, tagLogNumber)
		result = util.PutFixed64(result, e.LogNumber)
	}

	// Prev log number
	if e.hasPrevLogNumber {
		result = append(result, tagLogNumber) // Same tag as LogNumber in C++
		result = util.PutFixed64(result, e.PrevLogNumber)
	}

	// Next file number
	if e.hasNextFileNumber {
		result = append(result, tagNextFile)
		result = util.PutFixed64(result, e.NextFileNumber)
	}

	// Last sequence
	if e.hasLastSequence {
		result = append(result, tagLastSeq)
		result = util.PutFixed64(result, uint64(e.LastSequence))
	}

	// Compact pointers
	for level := 0; level < kNumLevels; level++ {
		if key, ok := e.CompactPointers[level]; ok {
			result = append(result, tagCompactPtr)
			result = util.PutVarint32(result, uint32(level))
			result = append(result, key.Encode().Data()...)
		}
	}

	// Deleted files
	for key := range e.DeletedFiles {
		result = append(result, tagDeleted)
		result = util.PutVarint32(result, uint32(key.Level))
		result = util.PutFixed64(result, key.FileNumber)
	}

	// New files
	// NewFile format: level(varint) + file_number(fixed64) + file_size(fixed64)
	//                + smallest_key(varint+data) + largest_key(varint+data)
	for _, info := range e.NewFiles {
		result = append(result, tagNewFile)
		result = util.PutVarint32(result, uint32(info.Level))
		result = util.PutFixed64(result, info.Meta.Number)
		result = util.PutFixed64(result, info.Meta.FileSize)
		result = util.PutLengthPrefixedSlice(result, info.Meta.Smallest.Encode())
		result = util.PutLengthPrefixedSlice(result, info.Meta.Largest.Encode())
	}

	return result
}

// DecodeFrom decodes a version edit from a byte slice.
// Returns false if the data is corrupted.
// Uses C++ varint32 format (not uvarint).
func (e *VersionEdit) DecodeFrom(data []byte) bool {
	e.Clear()

	in := data
	for len(in) > 0 {
		if len(in) < 1 {
			return false
		}

		// Read tag as varint32
		tag, n, ok := util.DecodeVarint32(in)
		if n == 0 || !ok {
			return false
		}
		in = in[n:]

		switch tag {
		case tagComparator:
			slice, consumed, ok := util.GetLengthPrefixedSlice(in)
			if !ok {
				return false
			}
			e.Comparator = slice.ToString()
			e.hasComparator = true
			in = in[consumed:]

		case tagLogNumber:
			if !e.hasLogNumber {
				e.LogNumber, n, ok = util.DecodeVarint64(in)
				if !ok {
					return false
				}
				e.hasLogNumber = true
				in = in[n:]
			} else {
				// PrevLogNumber
				e.PrevLogNumber, n, ok = util.DecodeVarint64(in)
				if !ok {
					return false
				}
				e.hasPrevLogNumber = true
				in = in[n:]
			}

		case tagNextFile:
			e.NextFileNumber, n, ok = util.DecodeVarint64(in)
			if !ok {
				return false
			}
			e.hasNextFileNumber = true
			in = in[n:]

		case tagLastSeq:
			v, n2, ok := util.DecodeVarint64(in)
			if !ok {
				return false
			}
			e.LastSequence = SequenceNumber(v)
			e.hasLastSequence = true
			in = in[n2:]

		case tagCompactPtr:
			level, n3, ok := util.DecodeVarint32(in)
			if !ok {
				return false
			}
			in = in[n3:]
			if len(in) < 8 {
				return false
			}
			slice, consumed2, ok := util.GetLengthPrefixedSlice(in)
			if !ok {
				return false
			}
			var key InternalKey
			if !key.DecodeFrom(slice) {
				return false
			}
			e.CompactPointers[int(level)] = key
			in = in[consumed2:]

		case tagDeleted:
			level, n4, ok := util.DecodeVarint32(in)
			if !ok {
				return false
			}
			in = in[n4:]
			fileNum, n5, ok := util.DecodeVarint64(in)
			if !ok {
				return false
			}
			e.DeletedFiles[DeletedFileKey{Level: int(level), FileNumber: fileNum}] = true
			in = in[n5:]

		case tagNewFile:
			level, n6, ok := util.DecodeVarint32(in)
			if !ok {
				return false
			}
			in = in[n6:]
			fileNum, n7, ok := util.DecodeVarint64(in)
			if !ok {
				return false
			}
			in = in[n7:]
			fileSize, n8, ok := util.DecodeVarint64(in)
			if !ok {
				return false
			}
			in = in[n8:]

			smallestSlice, consumed3, ok := util.GetLengthPrefixedSlice(in)
			if !ok {
				return false
			}
			in = in[consumed3:]
			var smallest InternalKey
			if !smallest.DecodeFrom(smallestSlice) {
				return false
			}

			largestSlice, consumed4, ok := util.GetLengthPrefixedSlice(in)
			if !ok {
				return false
			}
			in = in[consumed4:]
			var largest InternalKey
			if !largest.DecodeFrom(largestSlice) {
				return false
			}

			e.NewFiles = append(e.NewFiles, NewFileInfo{
				Level: int(level),
				Meta: FileMetaData{
					Number:   fileNum,
					FileSize: fileSize,
					Smallest: smallest,
					Largest:  largest,
				},
			})

		default:
			return false
		}
	}

	return true
}
