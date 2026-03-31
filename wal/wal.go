// Package wal provides Write-Ahead Log functionality for leveldb.
// WAL ensures durability by appending records to a log file before applying them to the database.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/akzj/go-leveldb/internal"
)

// ErrTruncatedRecord indicates that the last record in the WAL was truncated.
var ErrTruncatedRecord = errors.New("wal: truncated record")

// Record represents a single WAL record.
type Record struct {
	Type  internal.ValueType
	Key   []byte
	Value []byte
}

// Writer writes records to a WAL file.
type Writer struct {
	file *os.File
}

// NewWriter creates a new WAL Writer that appends to the specified file.
// If the file doesn't exist, it will be created.
func NewWriter(path string) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open file: %w", err)
	}
	return &Writer{file: file}, nil
}

// Append writes a record to the WAL file.
// For Put records, both key and value are written.
// For Delete records, only the key is written (value is ignored).
func (w *Writer) Append(vt internal.ValueType, key, value []byte) error {
	// Calculate total record size (excluding the 4-byte length field)
	// Format: type(1) + key_len(varint) + key + [val_len(varint) + value]
	keyLenLen := binary.PutUvarint(make([]byte, 10), uint64(len(key)))
	
	var recordSize int
	recordSize = 1 + keyLenLen + len(key) // type + key_len + key
	
	if vt == internal.TypePut {
		valLenLen := binary.PutUvarint(make([]byte, 10), uint64(len(value)))
		recordSize += valLenLen + len(value)
	}
	
	// Write length (4 bytes, little-endian)
	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, uint32(recordSize))
	if _, err := w.file.Write(lengthBuf); err != nil {
		return fmt.Errorf("wal: failed to write length: %w", err)
	}
	
	// Write type (1 byte)
	if _, err := w.file.Write([]byte{byte(vt)}); err != nil {
		return fmt.Errorf("wal: failed to write type: %w", err)
	}
	
	// Write key_len (varint)
	keyLenBuf := make([]byte, keyLenLen)
	binary.PutUvarint(keyLenBuf, uint64(len(key)))
	if _, err := w.file.Write(keyLenBuf); err != nil {
		return fmt.Errorf("wal: failed to write key length: %w", err)
	}
	
	// Write key
	if _, err := w.file.Write(key); err != nil {
		return fmt.Errorf("wal: failed to write key: %w", err)
	}
	
	// Write value (only for Put records)
	if vt == internal.TypePut {
		valLenBuf := make([]byte, 10)
		valLenLen := binary.PutUvarint(valLenBuf, uint64(len(value)))
		if _, err := w.file.Write(valLenBuf[:valLenLen]); err != nil {
			return fmt.Errorf("wal: failed to write value length: %w", err)
		}
		if _, err := w.file.Write(value); err != nil {
			return fmt.Errorf("wal: failed to write value: %w", err)
		}
	}
	
	return nil
}

// Sync flushes the WAL file to disk using fsync.
func (w *Writer) Sync() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync: %w", err)
	}
	return nil
}

// Close closes the WAL file.
func (w *Writer) Close() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close: %w", err)
	}
	return nil
}

// Reader reads records from a WAL file.
type Reader struct {
	file *os.File
}

// NewReader creates a new WAL Reader for the specified file.
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open file: %w", err)
	}
	return &Reader{file: file}, nil
}

// ReadAll reads all records from the WAL file.
// It handles truncated records gracefully by stopping at the last valid record.
func (r *Reader) ReadAll() ([]Record, error) {
	var records []Record
	buf := make([]byte, 4) // Buffer for reading length field
	
	for {
		// Read the 4-byte length field
		n, err := r.file.Read(buf)
		if err != nil {
			// EOF means we're done
			if n == 0 {
				break
			}
			// Partial read followed by EOF - possible truncation
			if n < 4 {
				break
			}
			return records, fmt.Errorf("wal: failed to read length: %w", err)
		}
		
		if n < 4 {
			// Not enough bytes for length field - truncated
			break
		}
		
		recordLen := binary.LittleEndian.Uint32(buf)
		
		// Read the rest of the record
		recordData := make([]byte, recordLen)
		readSoFar := 0
		
		for readSoFar < int(recordLen) {
			n, err := r.file.Read(recordData[readSoFar:])
			if err != nil {
				// EOF or error - truncated record
				return records, nil
			}
			if n == 0 {
				// EOF - truncated
				return records, nil
			}
			readSoFar += n
		}
		
		if readSoFar < int(recordLen) {
			// Didn't read full record - truncated
			break
		}
		
		// Parse the record
		record, err := parseRecord(recordData)
		if err != nil {
			return records, err
		}
		
		records = append(records, record)
	}
	
	return records, nil
}

// parseRecord parses a record from the given byte slice.
// Returns the record and any error.
func parseRecord(data []byte) (Record, error) {
	if len(data) < 1 {
		return Record{}, errors.New("wal: record too short for type")
	}
	
	var offset int
	
	// Read type (1 byte)
	vt := internal.ValueType(data[0])
	offset = 1
	
	// Read key_len (varint)
	keyLen, n := binary.Uvarint(data[offset:])
	if n <= 0 {
		return Record{}, errors.New("wal: invalid key length")
	}
	offset += n
	
	// Check if we have enough data for the key
	if offset+int(keyLen) > len(data) {
		return Record{}, errors.New("wal: truncated record (key)")
	}
	
	// Read key
	key := make([]byte, keyLen)
	copy(key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)
	
	// Read value (only for Put records)
	var value []byte
	if vt == internal.TypePut {
		valLen, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return Record{}, errors.New("wal: invalid value length")
		}
		offset += n
		
		if offset+int(valLen) > len(data) {
			return Record{}, errors.New("wal: truncated record (value)")
		}
		
		value = make([]byte, valLen)
		copy(value, data[offset:offset+int(valLen)])
	}
	
	return Record{
		Type:  vt,
		Key:   key,
		Value: value,
	}, nil
}

// Close closes the WAL file.
func (r *Reader) Close() error {
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close: %w", err)
	}
	return nil
}
