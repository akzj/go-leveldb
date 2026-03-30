package db

import (
	"fmt"
	"strconv"
)

// FileType represents the type of a database file.
type FileType uint32

const (
	// kLogFile is a write-ahead log file.
	kLogFile FileType = 1
	// kDBLockFile is the database lock file.
	kDBLockFile = 2
	// kTableFile is an SSTable file.
	kTableFile = 3
	// kDescriptorFile is the MANIFEST file.
	kDescriptorFile = 4
	// kTempFile is a temporary file.
	kTempFile = 5
	// kInfoLogFile is the INFO log file.
	kInfoLogFile = 6
)

// FileTypeLogFile returns the log file type constant for compatibility.
func FileTypeLogFile() FileType {
	return kLogFile
}

// FileTypeDescriptorFile returns the descriptor file type.
func FileTypeDescriptorFile() FileType {
	return kDescriptorFile
}

// FileTypeTableFile returns the table file type.
func FileTypeTableFile() FileType {
	return kTableFile
}

// CurrentFileName returns the name of the CURRENT file.
func CurrentFileName() string {
	return "CURRENT"
}

// LockFileName returns the name of the LOCK file.
func LockFileName() string {
	return "LOCK"
}

// LogFileName returns the name of a log file with the given number.
func LogFileName(fileNum uint64) string {
	return fmt.Sprintf("%06d.log", fileNum)
}

// TableFileName returns the name of a table file with the given number.
func TableFileName(fileNum uint64) string {
	return fmt.Sprintf("%06d.ldb", fileNum)
}

// DescriptorFileName returns the name of a descriptor file with the given number.
func DescriptorFileName(fileNum uint64) string {
	return fmt.Sprintf("MANIFEST-%06d", fileNum)
}

// TempFileName returns the name of a temp file with the given number.
func TempFileName(fileNum uint64) string {
	return fmt.Sprintf("%06d.tmp", fileNum)
}

// InfoLogFileName returns the name of an INFO log file with the given suffix.
func InfoLogFileName(suffix string) string {
	if suffix == "" {
		return "LOG"
	}
	return "LOG." + suffix
}

// OldInfoLogFileName returns the old INFO log file name.
func OldInfoLogFileName() string {
	return "LOG.old"
}

// ParseFileName parses a filename and returns the file type and number.
// Returns (0, 0, false) if the filename doesn't match known patterns.
func ParseFileName(name string) (FileType, uint64, bool) {
	// Current file
	if name == "CURRENT" {
		return 0, 0, true
	}
	// Lock file
	if name == "LOCK" {
		return kDBLockFile, 0, true
	}
	// Table file: XXXXXX.ldb
	if len(name) == 12 && name[6] == '.' {
		if name[7:] == "ldb" {
			num, err := strconv.ParseUint(name[:6], 10, 64)
			if err == nil {
				return kTableFile, num, true
			}
		}
	}
	// Log file: XXXXXX.log
	if len(name) == 13 && name[6] == '.' {
		if name[7:] == "log" {
			num, err := strconv.ParseUint(name[:6], 10, 64)
			if err == nil {
				return kLogFile, num, true
			}
		}
	}
	// Descriptor file: MANIFEST-XXXXXX
	if len(name) > 9 && name[:8] == "MANIFEST" {
		num, err := strconv.ParseUint(name[9:], 10, 64)
		if err == nil {
			return kDescriptorFile, num, true
		}
	}
	// Temp file: XXXXXX.tmp
	if len(name) == 11 && name[6] == '.' {
		if name[7:] == "tmp" {
			num, err := strconv.ParseUint(name[:6], 10, 64)
			if err == nil {
				return kTempFile, num, true
			}
		}
	}
	// Info log file
	if len(name) > 4 && name[:4] == "LOG" {
		return kInfoLogFile, 0, true
	}
	return 0, 0, false
}

// SetFileType returns the string representation of a file type.
func SetFileType(t FileType) string {
	switch t {
	case kLogFile:
		return "log"
	case kDBLockFile:
		return "lock"
	case kTableFile:
		return "ldb"
	case kDescriptorFile:
		return "manifest"
	case kTempFile:
		return "tmp"
	case kInfoLogFile:
		return "log"
	default:
		return "unknown"
	}
}
