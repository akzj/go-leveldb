package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/akzj/go-leveldb/util"
)

// TestDBOpenPutGet tests the basic Open/Put/Get cycle.
// Invariant: After successful Put, Get must return the same value.
func TestDBOpenPutGet(t *testing.T) {
	// Cleanup any previous test run
	os.RemoveAll("/tmp/testdb")
	defer os.RemoveAll("/tmp/testdb")

	// Create options with CreateIfMissing = true
	opts := NewOptions()
	opts.CreateIfMissing = true

	// Open the database
	db, status := Open(opts, "/tmp/testdb")
	if status == nil || !status.OK() {
		if status != nil {
			t.Fatalf("Open failed: %v", status.ToString())
		} else {
			t.Fatal("Open returned nil status")
		}
	}
	if db == nil {
		t.Fatal("Open returned nil db")
	}

	// Put a key-value pair
	if err := db.Put(nil, util.MakeSlice([]byte("key1")), util.MakeSlice([]byte("value1"))); err != nil && !err.OK() {
		t.Fatalf("Put failed: %v", err.ToString())
	}

	// Get it back
	value, status := db.Get(nil, util.MakeSlice([]byte("key1")))
	if !status.OK() {
		t.Fatalf("Get failed: %v", status.ToString())
	}

	// Verify the value matches
	if string(value) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(value))
	}

	// Close the database
	db.Close()
}

// TestWriteBatch tests atomic writes using WriteBatch.
// Invariant: After WriteBatch commits, all Put operations in batch are visible.
func TestWriteBatch(t *testing.T) {
	// Cleanup any previous test run
	os.RemoveAll("/tmp/testdb2")
	defer os.RemoveAll("/tmp/testdb2")

	// Create options with CreateIfMissing = true
	opts := NewOptions()
	opts.CreateIfMissing = true

	// Open the database
	db, status := Open(opts, "/tmp/testdb2")
	if !status.OK() {
		t.Fatalf("Open failed: %v", status.ToString())
	}

	// Create a write batch with 3 Put operations
	batch := NewWriteBatch()
	batch.Put(util.MakeSlice([]byte("a")), util.MakeSlice([]byte("1")))
	batch.Put(util.MakeSlice([]byte("b")), util.MakeSlice([]byte("2")))
	batch.Put(util.MakeSlice([]byte("c")), util.MakeSlice([]byte("3")))

	// Write the batch
	if status := db.Write(nil, batch); !status.OK() {
		t.Fatalf("Write failed: %v", status.ToString())
	}

	// Get "b" and verify value is "2"
	value, status := db.Get(nil, util.MakeSlice([]byte("b")))
	if !status.OK() {
		t.Fatalf("Get failed: %v", status.ToString())
	}

	if string(value) != "2" {
		t.Errorf("Expected '2', got '%s'", string(value))
	}

	// Also verify "a" and "c"
	valueA, _ := db.Get(nil, util.MakeSlice([]byte("a")))
	if string(valueA) != "1" {
		t.Errorf("Expected '1' for key 'a', got '%s'", string(valueA))
	}

	valueC, _ := db.Get(nil, util.MakeSlice([]byte("c")))
	if string(valueC) != "3" {
		t.Errorf("Expected '3' for key 'c', got '%s'", string(valueC))
	}

	// Close the database
	db.Close()
}

// TestIterator tests iterator traversal through all entries.
// Invariant: Iterator must visit exactly the number of entries written.
func TestIterator(t *testing.T) {
	// Cleanup any previous test run
	os.RemoveAll("/tmp/testdb3")
	defer os.RemoveAll("/tmp/testdb3")

	// Create options with CreateIfMissing = true
	opts := NewOptions()
	opts.CreateIfMissing = true

	// Open the database
	db, status := Open(opts, "/tmp/testdb3")
	if !status.OK() {
		t.Fatalf("Open failed: %v", status.ToString())
	}

	// Put 10 entries (key00-val00 through key09-val09)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%02d", i)
		val := fmt.Sprintf("val%02d", i)
		db.Put(nil, util.MakeSlice([]byte(key)), util.MakeSlice([]byte(val)))
	}

	// Create iterator and traverse
	iter := db.NewIterator(nil)
	iter.SeekToFirst()

	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	// Verify count is 10
	if count != 10 {
		t.Errorf("Expected 10 entries, got %d", count)
	}

	// Release the iterator
	iter.Release()

	// Close the database
	db.Close()
}
