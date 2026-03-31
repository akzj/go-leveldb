package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/go-leveldb/internal"
)

func TestLoadNewManifest(t *testing.T) {
	// Create a temp directory
	tmpDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Load should create new manifest when file doesn't exist
	m, err := Load(tmpDir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	if m.NextFileNumber != 1 {
		t.Errorf("expected NextFileNumber=1, got %d", m.NextFileNumber)
	}
	if m.Sequence != 0 {
		t.Errorf("expected Sequence=0, got %d", m.Sequence)
	}
	// Levels is initialized as empty map, not nil
	if m.Levels == nil {
		t.Errorf("expected Levels to be initialized, got nil")
	}
	if len(m.Levels) != 0 {
		t.Errorf("expected empty Levels, got %v", m.Levels)
	}
	if m.WALFileNumber != 1 {
		t.Errorf("expected WALFileNumber=1, got %d", m.WALFileNumber)
	}
}

func TestSaveAndLoad(t *testing.T) {
	// Create a temp directory
	tmpDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a manifest with some data
	m := &Manifest{
		NextFileNumber: 10,
		Sequence:       100,
		Levels:         make(map[int][]*TableMeta),
		WALFileNumber:  5,
	}

	// Add some tables
	m.Levels[0] = []*TableMeta{
		{
			FileNum:     1,
			FilePath:    filepath.Join(tmpDir, "000001.sst"),
			FileSize:    4096,
			SmallestKey: internal.MakeInternalKey([]byte("a"), 10, internal.TypePut),
			LargestKey:  internal.MakeInternalKey([]byte("c"), 5, internal.TypePut),
		},
		{
			FileNum:     2,
			FilePath:    filepath.Join(tmpDir, "000002.sst"),
			FileSize:    8192,
			SmallestKey: internal.MakeInternalKey([]byte("d"), 8, internal.TypePut),
			LargestKey:  internal.MakeInternalKey([]byte("f"), 3, internal.TypePut),
		},
	}

	m.Levels[1] = []*TableMeta{
		{
			FileNum:     3,
			FilePath:    filepath.Join(tmpDir, "000003.sst"),
			FileSize:    16384,
			SmallestKey: internal.MakeInternalKey([]byte("a"), 20, internal.TypePut),
			LargestKey:  internal.MakeInternalKey([]byte("z"), 15, internal.TypePut),
		},
	}

	// Save
	if err := m.Save(tmpDir); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	// Load it back
	m2, err := Load(tmpDir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	// Verify
	if m2.NextFileNumber != m.NextFileNumber {
		t.Errorf("NextFileNumber: expected %d, got %d", m.NextFileNumber, m2.NextFileNumber)
	}
	if m2.Sequence != m.Sequence {
		t.Errorf("Sequence: expected %d, got %d", m.Sequence, m2.Sequence)
	}
	if m2.WALFileNumber != m.WALFileNumber {
		t.Errorf("WALFileNumber: expected %d, got %d", m.WALFileNumber, m2.WALFileNumber)
	}

	// Check level 0
	if len(m2.Levels[0]) != 2 {
		t.Errorf("level 0: expected 2 tables, got %d", len(m2.Levels[0]))
	}
	if m2.Levels[0][0].FileNum != 1 {
		t.Errorf("level 0[0] FileNum: expected 1, got %d", m2.Levels[0][0].FileNum)
	}
	if m2.Levels[0][1].FileNum != 2 {
		t.Errorf("level 0[1] FileNum: expected 2, got %d", m2.Levels[0][1].FileNum)
	}

	// Check level 1
	if len(m2.Levels[1]) != 1 {
		t.Errorf("level 1: expected 1 table, got %d", len(m2.Levels[1]))
	}

	// Check key ranges are preserved
	expectedSmallest := "a"
	if string(m2.Levels[0][0].SmallestKey.UserKey()) != expectedSmallest {
		t.Errorf("SmallestKey: expected %s, got %s", expectedSmallest, string(m2.Levels[0][0].SmallestKey.UserKey()))
	}
}

func TestNewFileNumber(t *testing.T) {
	m := &Manifest{
		NextFileNumber: 1,
	}

	num1 := m.NewFileNumber()
	if num1 != 1 {
		t.Errorf("expected 1, got %d", num1)
	}
	if m.NextFileNumber != 2 {
		t.Errorf("expected NextFileNumber=2, got %d", m.NextFileNumber)
	}

	num2 := m.NewFileNumber()
	if num2 != 2 {
		t.Errorf("expected 2, got %d", num2)
	}
}

func TestAddTable(t *testing.T) {
	m := &Manifest{
		Levels: make(map[int][]*TableMeta),
	}

	// Add table to level 0
	m.AddTable(0, &TableMeta{FileNum: 3, FileSize: 100})
	m.AddTable(0, &TableMeta{FileNum: 1, FileSize: 200})
	m.AddTable(0, &TableMeta{FileNum: 2, FileSize: 150})

	// Should be sorted by file number
	tables := m.GetTablesForLevel(0)
	if len(tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(tables))
	}
	if tables[0].FileNum != 1 {
		t.Errorf("expected FileNum=1, got %d", tables[0].FileNum)
	}
	if tables[1].FileNum != 2 {
		t.Errorf("expected FileNum=2, got %d", tables[1].FileNum)
	}
	if tables[2].FileNum != 3 {
		t.Errorf("expected FileNum=3, got %d", tables[2].FileNum)
	}
}

func TestRemoveTable(t *testing.T) {
	m := &Manifest{
		Levels: map[int][]*TableMeta{
			0: {
				{FileNum: 1},
				{FileNum: 2},
				{FileNum: 3},
			},
		},
	}

	// Remove middle table
	m.RemoveTable(0, 2)

	tables := m.GetTablesForLevel(0)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}
	if tables[0].FileNum != 1 {
		t.Errorf("expected FileNum=1, got %d", tables[0].FileNum)
	}
	if tables[1].FileNum != 3 {
		t.Errorf("expected FileNum=3, got %d", tables[1].FileNum)
	}

	// Remove first table
	m.RemoveTable(0, 1)
	tables = m.GetTablesForLevel(0)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if tables[0].FileNum != 3 {
		t.Errorf("expected FileNum=3, got %d", tables[0].FileNum)
	}

	// Remove non-existent table (should not panic)
	m.RemoveTable(0, 999)
	tables = m.GetTablesForLevel(0)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table after removing non-existent, got %d", len(tables))
	}
}

func TestTotalSize(t *testing.T) {
	m := &Manifest{
		Levels: map[int][]*TableMeta{
			0: {
				{FileNum: 1, FileSize: 100},
				{FileNum: 2, FileSize: 200},
			},
			1: {
				{FileNum: 3, FileSize: 1000},
			},
		},
	}

	if size := m.TotalSize(0); size != 300 {
		t.Errorf("level 0 size: expected 300, got %d", size)
	}
	if size := m.TotalSize(1); size != 1000 {
		t.Errorf("level 1 size: expected 1000, got %d", size)
	}
	if size := m.TotalSize(2); size != 0 {
		t.Errorf("level 2 size: expected 0, got %d", size)
	}
}

func TestLevel0Count(t *testing.T) {
	m := &Manifest{
		Levels: map[int][]*TableMeta{
			0: {
				{FileNum: 1},
				{FileNum: 2},
				{FileNum: 3},
				{FileNum: 4},
			},
			1: {
				{FileNum: 5},
			},
		},
	}

	if count := m.Level0Count(); count != 4 {
		t.Errorf("expected 4, got %d", count)
	}
}

func TestGetTablesForLevel(t *testing.T) {
	m := &Manifest{
		Levels: map[int][]*TableMeta{
			0: {
				{FileNum: 1},
				{FileNum: 2},
			},
		},
	}

	// Get existing level
	tables := m.GetTablesForLevel(0)
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}

	// Get non-existing level
	tables = m.GetTablesForLevel(5)
	if tables != nil {
		t.Errorf("expected nil, got %v", tables)
	}

	// Get with nil levels
	m2 := &Manifest{}
	tables = m2.GetTablesForLevel(0)
	if tables != nil {
		t.Errorf("expected nil, got %v", tables)
	}
}