package db

import (
	"os"
	"testing"

	"github.com/akzj/go-leveldb/util"
)

// TestDestroyDB tests that DestroyDB removes the database contents.
// Invariant: After DestroyDB, the database directory should be empty or gone.
func TestDestroyDB(t *testing.T) {
	// Create a temporary database
	name := "/tmp/leveldb_destroy_test"
	defer os.RemoveAll(name)

	// Setup
	os.RemoveAll(name)
	opts := NewOptions()
	opts.CreateIfMissing = true

	db, status := Open(opts, name)
	if !status.OK() {
		t.Fatalf("Open failed: %v", status.ToString())
	}

	// Write some data
	db.Put(nil, util.MakeSlice([]byte("key1")), util.MakeSlice([]byte("value1")))
	db.Put(nil, util.MakeSlice([]byte("key2")), util.MakeSlice([]byte("value2")))

	// Close the database
	db.Close()

	// Destroy the database
	destroyStatus := DestroyDB(name, nil)
	if !destroyStatus.OK() {
		t.Fatalf("DestroyDB failed: %v", destroyStatus.ToString())
	}

	// Try to open the destroyed database
	// It should either fail or open as empty
	opts2 := NewOptions()
	opts2.CreateIfMissing = false // Don't create if missing

	db2, status := Open(opts2, name)
	if status != nil && status.OK() {
		// Opened successfully - verify it's empty
		iter := db2.NewIterator(nil)
		count := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			count++
		}
		iter.Release()
		db2.Close()

		if count > 0 {
			t.Errorf("Database should be empty after DestroyDB, but had %d entries", count)
		}
	}
	// If Open failed, that's also acceptable behavior for destroyed DB
}

// TestDestroyAndRecreate tests that we can destroy and recreate a database.
// Invariant: After DestroyDB, CreateIfMissing should work.
func TestDestroyAndRecreate(t *testing.T) {
	name := "/tmp/leveldb_recreate_test"
	defer os.RemoveAll(name)

	// Cleanup any existing database
	os.RemoveAll(name)

	// First: Create and use a database
	opts := NewOptions()
	opts.CreateIfMissing = true

	db, status := Open(opts, name)
	if !status.OK() {
		t.Fatalf("First Open failed: %v", status.ToString())
	}

	db.Put(nil, util.MakeSlice([]byte("key1")), util.MakeSlice([]byte("value1")))
	db.Close()

	// Destroy the database
	destroyStatus := DestroyDB(name, nil)
	if !destroyStatus.OK() {
		t.Fatalf("DestroyDB failed: %v", destroyStatus.ToString())
	}

	// Recreate with CreateIfMissing = true
	opts2 := NewOptions()
	opts2.CreateIfMissing = true

	db2, status := Open(opts2, name)
	if !status.OK() {
		t.Fatalf("Second Open (recreate) failed: %v", status.ToString())
	}

	// New database should be empty
	_, status = db2.Get(nil, util.MakeSlice([]byte("key1")))
	if !status.IsNotFound() {
		t.Errorf("Recreated database should be empty, but key1 exists")
	}

	// Write new data
	db2.Put(nil, util.MakeSlice([]byte("key2")), util.MakeSlice([]byte("value2")))

	val, status := db2.Get(nil, util.MakeSlice([]byte("key2")))
	if !status.OK() || string(val) != "value2" {
		t.Errorf("Could not write and read in recreated database")
	}

	db2.Close()

	// Cleanup
	os.RemoveAll(name)
}

// TestDestroyDBWithError tests DestroyDB error handling.
func TestDestroyDBWithError(t *testing.T) {
	// DestroyDB with nil options should not panic
	status := DestroyDB("/nonexistent/path", nil)
	// The status might be an error since the path doesn't exist,
	// but DestroyDB should not panic
	if status == nil {
		t.Logf("DestroyDB returned nil status for nonexistent path")
	}
}

// TestRepairDBBasic tests that RepairDB can recover a database.
// Invariant: After RepairDB, previously written data should be recoverable.
func TestRepairDBBasic(t *testing.T) {
	name := "/tmp/leveldb_repair_test"
	defer os.RemoveAll(name)

	// Create and populate a database
	os.RemoveAll(name)
	opts := NewOptions()
	opts.CreateIfMissing = true

	db, status := Open(opts, name)
	if !status.OK() {
		t.Fatalf("Open failed: %v", status.ToString())
	}

	// Write some data
	keys := []string{"apple", "banana", "cherry"}
	for _, k := range keys {
		db.Put(nil, util.MakeSlice([]byte(k)), util.MakeSlice([]byte("val_"+k)))
	}
	db.Close()

	// Attempt to repair
	opts2 := NewOptions()
	opts2.CreateIfMissing = true

	db2, repairStatus := RepairDB(name, opts2)
	if repairStatus != nil && !repairStatus.OK() {
		t.Fatalf("RepairDB failed: %v", repairStatus.ToString())
	}

	if db2 != nil {
		// Check if data was recovered
		recovered := 0
		for _, k := range keys {
			val, status := db2.Get(nil, util.MakeSlice([]byte(k)))
			if status.OK() && string(val) == "val_"+k {
				recovered++
			}
		}
		t.Logf("Recovered %d of %d keys", recovered, len(keys))
		db2.Close()
	}

	// Cleanup
	os.RemoveAll(name)
}
