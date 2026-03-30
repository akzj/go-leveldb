package db

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/akzj/go-leveldb/util"
)

// posixEnv implements the Env interface using POSIX file operations.
type posixEnv struct{}

// NewPosixEnv creates a new POSIX environment.
func NewPosixEnv() *posixEnv {
	return &posixEnv{}
}

// NewSequentialFile implements Env.
func (e *posixEnv) NewSequentialFile(name string) (SequentialFile, *util.Status) {
	file, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, util.NotFound("file not found: " + name)
		}
		return nil, util.IOError(err.Error())
	}
	return &posixSequentialFile{file: file}, util.NewStatusOK()
}

// NewRandomAccessFile implements Env.
func (e *posixEnv) NewRandomAccessFile(name string) (RandomAccessFile, *util.Status) {
	file, err := os.Open(name)
	if err != nil {
		return nil, util.IOError(err.Error())
	}
	return &posixRandomAccessFile{file: file}, util.NewStatusOK()
}

// NewWritableFile implements Env.
func (e *posixEnv) NewWritableFile(name string) (WritableFile, *util.Status) {
	file, err := os.Create(name)
	if err != nil {
		return nil, util.IOError(err.Error())
	}
	return &posixWritableFile{file: file, filename: name}, util.NewStatusOK()
}

// NewAppendableFile implements Env.
func (e *posixEnv) NewAppendableFile(name string) (AppendableFile, *util.Status) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, util.IOError(err.Error())
	}
	return &posixAppendableFile{file: file, filename: name}, util.NewStatusOK()
}

// FileExists implements Env.
func (e *posixEnv) FileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// GetChildren implements Env.
func (e *posixEnv) GetChildren(dir string) ([]string, *util.Status) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, util.IOError(err.Error())
	}
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.Name())
	}
	return result, util.NewStatusOK()
}

// RemoveFile implements Env.
func (e *posixEnv) RemoveFile(name string) *util.Status {
	err := os.Remove(name)
	if err != nil && !os.IsNotExist(err) {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

// DeleteFile implements Env (alias for RemoveFile).
func (e *posixEnv) DeleteFile(name string) *util.Status {
	return e.RemoveFile(name)
}

// ReadFile implements Env.
func (e *posixEnv) ReadFile(name string, data []byte) (int, *util.Status) {
	file, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, util.NotFound("file not found: " + name)
		}
		return 0, util.IOError(err.Error())
	}
	defer file.Close()

	n, err := file.Read(data)
	if err != nil && err.Error() != "EOF" {
		return n, util.IOError(err.Error())
	}
	return n, util.NewStatusOK()
}

// CreateDir implements Env.
func (e *posixEnv) CreateDir(dir string) *util.Status {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

// RemoveDir implements Env.
func (e *posixEnv) RemoveDir(dir string) *util.Status {
	err := os.RemoveAll(dir)
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

// GetFileSize implements Env.
func (e *posixEnv) GetFileSize(name string) (uint64, *util.Status) {
	info, err := os.Stat(name)
	if err != nil {
		return 0, util.IOError(err.Error())
	}
	return uint64(info.Size()), util.NewStatusOK()
}

// RenameFile implements Env.
func (e *posixEnv) RenameFile(src, target string) *util.Status {
	err := os.Rename(src, target)
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

// LockFile implements Env.
func (e *posixEnv) LockFile(name string) (FileLock, *util.Status) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, util.IOError(err.Error())
	}
	err = flock(file, true)
	if err != nil {
		file.Close()
		return nil, util.IOError(err.Error())
	}
	return &posixFileLock{file: file}, util.NewStatusOK()
}

// UnlockFile implements Env.
func (e *posixEnv) UnlockFile(lock FileLock) *util.Status {
	flock := lock.(*posixFileLock)
	err := flockUnlock(flock.file)
	flock.file.Close()
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

// Schedule implements Env (no-op for sync environment).
func (e *posixEnv) Schedule(func()) {
	// No-op for synchronous POSIX env
}

// StartThread implements Env.
func (e *posixEnv) StartThread(f func()) {
	go f()
}

// GetTestDirectory implements Env.
func (e *posixEnv) GetTestDirectory() (string, *util.Status) {
	dir := filepath.Join(os.TempDir(), "leveldb_test")
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return "", util.IOError(err.Error())
	}
	return dir, util.NewStatusOK()
}

// NewLogger implements Env.
func (e *posixEnv) NewLogger(name string) (Logger, *util.Status) {
	return &posixLogger{filename: name}, util.NewStatusOK()
}

// NowMicros implements Env.
func (e *posixEnv) NowMicros() uint64 {
	return uint64(time.Now().UnixMicro())
}

// SleepForMicroseconds implements Env.
func (e *posixEnv) SleepForMicroseconds(micros int) {
	time.Sleep(time.Duration(micros) * time.Microsecond)
}

// posixSequentialFile implements SequentialFile.
type posixSequentialFile struct {
	file *os.File
}

func (f *posixSequentialFile) Read(n int) ([]byte, *util.Status) {
	data := make([]byte, n)
	n, err := f.file.Read(data)
	if err != nil && err.Error() != "EOF" {
		return nil, util.IOError(err.Error())
	}
	return data[:n], util.NewStatusOK()
}

func (f *posixSequentialFile) Skip(n uint64) *util.Status {
	_, err := f.file.Seek(int64(n), os.SEEK_CUR)
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

// posixRandomAccessFile implements RandomAccessFile.
type posixRandomAccessFile struct {
	file *os.File
}

func (f *posixRandomAccessFile) Read(offset uint64, n int) ([]byte, *util.Status) {
	data := make([]byte, n)
	_, err := f.file.ReadAt(data, int64(offset))
	if err != nil && err.Error() != "EOF" {
		return nil, util.IOError(err.Error())
	}
	return data, util.NewStatusOK()
}

// posixWritableFile implements WritableFile.
type posixWritableFile struct {
	file     *os.File
	filename string
	mu       sync.Mutex
}

func (f *posixWritableFile) Append(data util.Slice) *util.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.file.Write(data.Data())
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

func (f *posixWritableFile) Close() *util.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.file.Close()
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

func (f *posixWritableFile) Flush() *util.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.file.Sync()
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

func (f *posixWritableFile) Sync() *util.Status {
	return f.Flush()
}

// posixAppendableFile implements AppendableFile.
type posixAppendableFile struct {
	file     *os.File
	filename string
	mu       sync.Mutex
}

func (f *posixAppendableFile) Append(data util.Slice) *util.Status {
	return f.AppendMore(data)
}

func (f *posixAppendableFile) AppendMore(data util.Slice) *util.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.file.Write(data.Data())
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

func (f *posixAppendableFile) Close() *util.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.file.Close()
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

func (f *posixAppendableFile) Flush() *util.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.file.Sync()
	if err != nil {
		return util.IOError(err.Error())
	}
	return util.NewStatusOK()
}

func (f *posixAppendableFile) Sync() *util.Status {
	return f.Flush()
}

// posixFileLock implements FileLock.
type posixFileLock struct {
	file *os.File
}

func (l *posixFileLock) Release() {
	flockUnlock(l.file)
	l.file.Close()
}

// posixLogger implements Logger.
type posixLogger struct {
	filename string
}

func (l *posixLogger) Logv(format string, args ...interface{}) {
	// Simple logger implementation
	msg := format
	if len(args) > 0 {
		msg = format
	}
	os.Stderr.WriteString(msg + "\n")
}

// file locking helpers using flock
func flock(file *os.File, exclusive bool) error {
	lockType := syscall.LOCK_SH // shared lock
	if exclusive {
		lockType = syscall.LOCK_EX // exclusive lock
	}
	return syscall.Flock(int(file.Fd()), lockType|syscall.LOCK_NB)
}

func flockUnlock(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
