package wal

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/akzj/go-leveldb/internal"
)

func TestWriterReader(t *testing.T) {
	// Create a temp file
	tmpFile, err := ioutil.TempFile("", "wal-test-*.dat")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Test Write + Read roundtrip for Put
	t.Run("Put", func(t *testing.T) {
		// Write
		w, err := NewWriter(tmpPath)
		if err != nil {
			t.Fatalf("NewWriter failed: %v", err)
		}
		
		if err := w.Append(internal.TypePut, []byte("key1"), []byte("value1")); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if err := w.Append(internal.TypePut, []byte("key2"), []byte("value2")); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if err := w.Sync(); err != nil {
			t.Fatalf("Sync failed: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Read
		r, err := NewReader(tmpPath)
		if err != nil {
			t.Fatalf("NewReader failed: %v", err)
		}
		defer r.Close()

		records, err := r.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}

		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}

		if records[0].Type != internal.TypePut {
			t.Errorf("expected TypePut, got %v", records[0].Type)
		}
		if string(records[0].Key) != "key1" {
			t.Errorf("expected key 'key1', got '%s'", records[0].Key)
		}
		if string(records[0].Value) != "value1" {
			t.Errorf("expected value 'value1', got '%s'", records[0].Value)
		}

		if records[1].Type != internal.TypePut {
			t.Errorf("expected TypePut, got %v", records[1].Type)
		}
		if string(records[1].Key) != "key2" {
			t.Errorf("expected key 'key2', got '%s'", records[1].Key)
		}
		if string(records[1].Value) != "value2" {
			t.Errorf("expected value 'value2', got '%s'", records[1].Value)
		}
	})

	// Test Delete records
	t.Run("Delete", func(t *testing.T) {
		// Clear the file
		if err := os.Remove(tmpPath); err != nil {
			t.Fatalf("failed to remove file: %v", err)
		}

		// Write
		w, err := NewWriter(tmpPath)
		if err != nil {
			t.Fatalf("NewWriter failed: %v", err)
		}

		if err := w.Append(internal.TypeDelete, []byte("deleted_key"), nil); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Read
		r, err := NewReader(tmpPath)
		if err != nil {
			t.Fatalf("NewReader failed: %v", err)
		}
		defer r.Close()

		records, err := r.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}

		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}

		if records[0].Type != internal.TypeDelete {
			t.Errorf("expected TypeDelete, got %v", records[0].Type)
		}
		if string(records[0].Key) != "deleted_key" {
			t.Errorf("expected key 'deleted_key', got '%s'", records[0].Key)
		}
		if len(records[0].Value) != 0 {
			t.Errorf("expected empty value for Delete, got '%s'", records[0].Value)
		}
	})

	// Test mixed Put and Delete
	t.Run("Mixed", func(t *testing.T) {
		// Clear the file
		if err := os.Remove(tmpPath); err != nil {
			t.Fatalf("failed to remove file: %v", err)
		}

		// Write
		w, err := NewWriter(tmpPath)
		if err != nil {
			t.Fatalf("NewWriter failed: %v", err)
		}

		testCases := []struct {
			vt    internal.ValueType
			key   string
			value string
		}{
			{internal.TypePut, "key1", "value1"},
			{internal.TypeDelete, "key2", ""},
			{internal.TypePut, "key3", "value3"},
			{internal.TypeDelete, "key4", ""},
		}

		for _, tc := range testCases {
			if tc.vt == internal.TypePut {
				if err := w.Append(tc.vt, []byte(tc.key), []byte(tc.value)); err != nil {
					t.Fatalf("Append failed: %v", err)
				}
			} else {
				if err := w.Append(tc.vt, []byte(tc.key), nil); err != nil {
					t.Fatalf("Append failed: %v", err)
				}
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Read
		r, err := NewReader(tmpPath)
		if err != nil {
			t.Fatalf("NewReader failed: %v", err)
		}
		defer r.Close()

		records, err := r.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}

		if len(records) != len(testCases) {
			t.Fatalf("expected %d records, got %d", len(testCases), len(records))
		}

		for i, tc := range testCases {
			if records[i].Type != tc.vt {
				t.Errorf("record %d: expected type %v, got %v", i, tc.vt, records[i].Type)
			}
			if string(records[i].Key) != tc.key {
				t.Errorf("record %d: expected key '%s', got '%s'", i, tc.key, records[i].Key)
			}
			if string(records[i].Value) != tc.value {
				t.Errorf("record %d: expected value '%s', got '%s'", i, tc.value, records[i].Value)
			}
		}
	})
}

func TestTruncatedRecord(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "wal-truncated-*.dat")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Write a complete record
	w, err := NewWriter(tmpPath)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	if err := w.Append(internal.TypePut, []byte("complete"), []byte("record")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Now truncate the file to simulate a partial write
	file, err := os.OpenFile(tmpPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open file for truncation: %v", err)
	}
	
	info, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatalf("failed to stat file: %v", err)
	}
	
	// Truncate to half the file size
	truncateAt := info.Size() / 2
	if err := file.Truncate(truncateAt); err != nil {
		file.Close()
		t.Fatalf("failed to truncate file: %v", err)
	}
	file.Close()

	// Read - should handle truncated record gracefully
	r, err := NewReader(tmpPath)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// Should get 0 records since the truncated record is incomplete
	if len(records) != 0 {
		t.Errorf("expected 0 records for truncated file, got %d", len(records))
	}
}

func TestTruncatedLengthField(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "wal-trunc-len-*.dat")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Write only 2 bytes (truncated length field)
	file, err := os.OpenFile(tmpPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	
	partial := []byte{0x01, 0x02} // Only 2 bytes instead of 4
	if _, err := file.Write(partial); err != nil {
		file.Close()
		t.Fatalf("failed to write: %v", err)
	}
	file.Close()

	// Read - should handle gracefully
	r, err := NewReader(tmpPath)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("expected 0 records for truncated length field, got %d", len(records))
	}
}

func TestEmptyFile(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "wal-empty-*.dat")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	tmpFile.Close()

	r, err := NewReader(tmpPath)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("expected 0 records for empty file, got %d", len(records))
	}
}

func TestLargeKeyValue(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "wal-large-*.dat")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Create large key and value
	largeKey := make([]byte, 1000)
	largeValue := make([]byte, 10000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	w, err := NewWriter(tmpPath)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	if err := w.Append(internal.TypePut, largeKey, largeValue); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(tmpPath)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	if len(records[0].Key) != len(largeKey) {
		t.Errorf("expected key length %d, got %d", len(largeKey), len(records[0].Key))
	}
	if len(records[0].Value) != len(largeValue) {
		t.Errorf("expected value length %d, got %d", len(largeValue), len(records[0].Value))
	}
}
