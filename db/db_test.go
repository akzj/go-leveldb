package db

import (
	"path/filepath"
	"testing"
)

func TestBasicPutGet(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}

	_, err = db.Get([]byte("nonexistent"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	keys := []string{"apple", "banana", "cherry", "date"}
	values := []string{"red", "yellow", "red", "brown"}

	for i := range keys {
		err := db.Put([]byte(keys[i]), []byte(values[i]))
		if err != nil {
			t.Fatalf("failed to put %s: %v", keys[i], err)
		}
	}

	for i := range keys {
		val, err := db.Get([]byte(keys[i]))
		if err != nil {
			t.Fatalf("failed to get %s: %v", keys[i], err)
		}
		if string(val) != values[i] {
			t.Errorf("for key %s: expected %s, got %s", keys[i], values[i], string(val))
		}
	}
}

func TestDelete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("failed to get before delete: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}

	err = db.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	_, err = db.Get([]byte("key1"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	{
		db, err := Open(dbPath, nil)
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}

		for i := 0; i < 100; i++ {
			key := []byte(string(rune('a'+i%26)) + string(rune('0'+i/26)))
			value := []byte(string(rune('A'+i%26)) + string(rune('0'+i/26)))
			err := db.Put(key, value)
			if err != nil {
				t.Fatalf("failed to put: %v", err)
			}
		}

		err = db.Close()
		if err != nil {
			t.Fatalf("failed to close database: %v", err)
		}
	}

	{
		db, err := Open(dbPath, nil)
		if err != nil {
			t.Fatalf("failed to reopen database: %v", err)
		}
		defer db.Close()

		for i := 0; i < 100; i++ {
			key := []byte(string(rune('a'+i%26)) + string(rune('0'+i/26)))
			value := []byte(string(rune('A'+i%26)) + string(rune('0'+i/26)))
			val, err := db.Get(key)
			if err != nil {
				t.Errorf("failed to get key %d (%s): %v", i, string(key), err)
				continue
			}
			if string(val) != string(value) {
				t.Errorf("for key %s: expected %s, got %s", string(key), string(value), string(val))
			}
		}
	}
}

func TestEmptyKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	err = db.Put([]byte(""), []byte("value"))
	if err != ErrEmptyKey {
		t.Errorf("expected ErrEmptyKey for empty key, got %v", err)
	}

	_, err = db.Get([]byte(""))
	if err != ErrEmptyKey {
		t.Errorf("expected ErrEmptyKey for empty key, got %v", err)
	}

	err = db.Delete([]byte(""))
	if err != ErrEmptyKey {
		t.Errorf("expected ErrEmptyKey for empty key, got %v", err)
	}
}

func TestDBClosed(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	err = db.Put([]byte("key"), []byte("value"))
	if err != ErrDBClosed {
		t.Errorf("expected ErrDBClosed, got %v", err)
	}

	_, err = db.Get([]byte("key"))
	if err != ErrDBClosed {
		t.Errorf("expected ErrDBClosed, got %v", err)
	}

	err = db.Delete([]byte("key"))
	if err != ErrDBClosed {
		t.Errorf("expected ErrDBClosed, got %v", err)
	}
}

func TestIterator(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	keys := []string{"apple", "banana", "cherry"}
	for i, k := range keys {
		err := db.Put([]byte(k), []byte(string(rune('A'+i))))
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}

	iter := db.NewIterator()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	if count != len(keys) {
		t.Errorf("expected %d entries, got %d", len(keys), count)
	}
}

func TestIteratorSeek(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	keys := []string{"apple", "banana", "cherry", "date"}
	for _, k := range keys {
		err := db.Put([]byte(k), []byte(k+"_value"))
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}

	iter := db.NewIterator()

	iter.Seek([]byte("banana"))
	if !iter.Valid() {
		t.Fatal("iterator should be valid after Seek")
	}
	if string(iter.Key()) != "banana" {
		t.Errorf("expected key 'banana', got '%s'", string(iter.Key()))
	}

	iter.Seek([]byte("blueberry"))
	if !iter.Valid() {
		t.Fatal("iterator should be valid after Seek to non-existent key")
	}
	if string(iter.Key()) != "cherry" {
		t.Errorf("expected key 'cherry', got '%s'", string(iter.Key()))
	}

	iter.Seek([]byte("zebra"))
	if iter.Valid() {
		t.Error("iterator should be invalid after Seek beyond last key")
	}
}

func TestMemTableFlush(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	opts := &Options{
		MemTableSize: 1024,
		BlockSize:    256,
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	for i := 0; i < 50; i++ {
		key := []byte(string(rune('k')) + string(rune('0'+i/10)) + string(rune('0'+i%10)))
		value := []byte(string(rune('v')) + string(rune('0'+i/10)) + string(rune('0'+i%10)))
		err := db.Put(key, value)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = Open(dbPath, opts)
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db.Close()

	for i := 0; i < 50; i++ {
		key := []byte(string(rune('k')) + string(rune('0'+i/10)) + string(rune('0'+i%10)))
		value := []byte(string(rune('v')) + string(rune('0'+i/10)) + string(rune('0'+i%10)))
		val, err := db.Get(key)
		if err != nil {
			t.Errorf("failed to get key %d: %v", i, err)
			continue
		}
		if string(val) != string(value) {
			t.Errorf("for key %d: expected %s, got %s", i, string(value), string(val))
		}
	}
}

func TestUpdateValue(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	err = db.Put([]byte("key"), []byte("value1"))
	if err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	val, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}

	err = db.Put([]byte("key"), []byte("value2"))
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	val, err = db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("failed to get updated value: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("expected value2, got %s", string(val))
	}
}
