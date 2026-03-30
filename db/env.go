package db

import (
	"github.com/akzj/go-leveldb/util"
)

// Env provides access to operating system functionality.
// All implementations must be safe for concurrent access.
type Env interface {
	// NewSequentialFile opens a file for sequential reading.
	NewSequentialFile(name string) (SequentialFile, *util.Status)

	// NewRandomAccessFile opens a file for random access reading.
	NewRandomAccessFile(name string) (RandomAccessFile, *util.Status)

	// NewWritableFile opens a file for writing (truncates existing).
	NewWritableFile(name string) (WritableFile, *util.Status)

	// NewAppendableFile opens a file for appending.
	NewAppendableFile(name string) (AppendableFile, *util.Status)

	// FileExists returns true if the file exists.
	FileExists(name string) bool

	// GetChildren returns names of files in the directory.
	GetChildren(dir string) ([]string, *util.Status)

	// RemoveFile deletes the file.
	RemoveFile(name string) *util.Status

	// DeleteFile deletes the file (alias for RemoveFile).
	DeleteFile(name string) *util.Status

	// CreateDir creates a directory.
	CreateDir(dir string) *util.Status

	// ReadFile reads the entire contents of a file into data.
	// Returns the number of bytes read and any error encountered.
	ReadFile(name string, data []byte) (int, *util.Status)

	// RemoveDir deletes a directory.
	RemoveDir(dir string) *util.Status

	// GetFileSize returns the size of a file.
	GetFileSize(name string) (uint64, *util.Status)

	// RenameFile renames a file.
	RenameFile(src, target string) *util.Status

	// LockFile locks a file for exclusive access.
	LockFile(name string) (FileLock, *util.Status)

	// UnlockFile releases a file lock.
	UnlockFile(lock FileLock) *util.Status

	// Schedule schedules a function to run in a background thread.
	Schedule(func())

	// StartThread starts a new thread running the function.
	StartThread(func())

	// GetTestDirectory returns a directory for testing.
	GetTestDirectory() (string, *util.Status)

	// NewLogger creates a logger.
	NewLogger(name string) (Logger, *util.Status)

	// NowMicros returns the current time in microseconds.
	NowMicros() uint64

	// SleepForMicroseconds sleeps for the specified duration.
	SleepForMicroseconds(micros int)
}

// SequentialFile is for reading a file sequentially.
type SequentialFile interface {
	// Read reads up to n bytes. Returns (data, error).
	Read(n int) ([]byte, *util.Status)

	// Skip skips n bytes.
	Skip(n uint64) *util.Status
}

// RandomAccessFile is for random access reading.
type RandomAccessFile interface {
	// Read reads up to n bytes starting at offset.
	Read(offset uint64, n int) ([]byte, *util.Status)
}

// WritableFile is for sequential writing.
type WritableFile interface {
	// Append appends data to the file.
	Append(data util.Slice) *util.Status

	// Close closes the file.
	Close() *util.Status

	// Flush flushes buffered data.
	Flush() *util.Status

	// Sync synchronizes data to disk.
	Sync() *util.Status
}

// AppendableFile is for appending to a file.
type AppendableFile interface {
	WritableFile

	// AppendMore appends additional data.
	AppendMore(data util.Slice) *util.Status
}

// FileLock represents a file lock.
type FileLock interface {
	// Release releases the lock.
	Release()
}

// Logger is for writing log messages.
type Logger interface {
	// Logv writes a log message.
	Logv(format string, args ...interface{})
}

// GetEnv returns the environment from options or default.
func GetEnv(opts *Options) Env {
	if opts.Env != nil {
		return opts.Env
	}
	return DefaultEnv()
}

// DefaultEnv returns the default environment.
func DefaultEnv() Env {
	return NewPosixEnv()
}
