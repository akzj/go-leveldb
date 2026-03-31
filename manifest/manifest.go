// Package manifest manages database metadata including SSTable file tracking
// and level assignments. It provides atomic persistence of database state.
package manifest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/akzj/go-leveldb/internal"
)

// manifestFileName is the name of the manifest file.
const manifestFileName = "MANIFEST"

// TableMeta contains metadata about an SSTable file including its key range.
type TableMeta struct {
	FileNum     uint64
	FilePath    string
	FileSize    int64
	SmallestKey internal.InternalKey
	LargestKey  internal.InternalKey
}

// Manifest stores the database's metadata state.
type Manifest struct {
	NextFileNumber uint64
	Sequence       uint64
	Levels         map[int][]*TableMeta
	WALFileNumber  uint64
}

// manifestJSON is the on-disk representation of Manifest.
type manifestJSON struct {
	NextFileNumber uint64              `json:"next_file_number"`
	Sequence       uint64              `json:"sequence"`
	Levels         map[string][]*tableJSON `json:"levels"`
	WALFileNumber  uint64             `json:"wal_file_number"`
}

// tableJSON is the on-disk representation of TableMeta.
type tableJSON struct {
	FileNum     uint64 `json:"file_num"`
	FilePath    string `json:"file_path"`
	FileSize    int64  `json:"file_size"`
	SmallestKey string `json:"smallest_key"`
	LargestKey  string `json:"largest_key"`
}

// Load loads the manifest from disk, or creates a new one if not found.
func Load(dbPath string) (*Manifest, error) {
	manifestPath := filepath.Join(dbPath, manifestFileName)

	// Try to load existing manifest
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Create new manifest
			return &Manifest{
				NextFileNumber: 1,
				Sequence:       0,
				Levels:         make(map[int][]*TableMeta),
				WALFileNumber:  1,
			}, nil
		}
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	// Parse JSON
	var mj manifestJSON
	if err := json.Unmarshal(data, &mj); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	// Convert to Manifest
	m := &Manifest{
		NextFileNumber: mj.NextFileNumber,
		Sequence:       mj.Sequence,
		Levels:         make(map[int][]*TableMeta),
		WALFileNumber:  mj.WALFileNumber,
	}

	for levelStr, tables := range mj.Levels {
		var level int
		if _, err := fmt.Sscanf(levelStr, "%d", &level); err != nil {
			return nil, fmt.Errorf("parse level: %w", err)
		}
		m.Levels[level] = make([]*TableMeta, len(tables))
		for i, t := range tables {
			smallestKeyData, err := base64.StdEncoding.DecodeString(t.SmallestKey)
			if err != nil {
				return nil, fmt.Errorf("decode smallest key: %w", err)
			}
			largestKeyData, err := base64.StdEncoding.DecodeString(t.LargestKey)
			if err != nil {
				return nil, fmt.Errorf("decode largest key: %w", err)
			}
			m.Levels[level][i] = &TableMeta{
				FileNum:     t.FileNum,
				FilePath:    t.FilePath,
				FileSize:    t.FileSize,
				SmallestKey: internal.MakeInternalKeyFromBytes(smallestKeyData),
				LargestKey:  internal.MakeInternalKeyFromBytes(largestKeyData),
			}
		}
	}

	return m, nil
}

// Save atomically saves the manifest to disk.
// It writes to a temporary file first, then renames to the final name.
func (m *Manifest) Save(dbPath string) error {
	// Convert to JSON format
	mj := manifestJSON{
		NextFileNumber: m.NextFileNumber,
		Sequence:       m.Sequence,
		Levels:         make(map[string][]*tableJSON),
		WALFileNumber:  m.WALFileNumber,
	}

	for level, tables := range m.Levels {
		key := fmt.Sprintf("%d", level)
		mj.Levels[key] = make([]*tableJSON, len(tables))
		for i, t := range tables {
			mj.Levels[key][i] = &tableJSON{
				FileNum:     t.FileNum,
				FilePath:    t.FilePath,
				FileSize:    t.FileSize,
				SmallestKey: base64.StdEncoding.EncodeToString(t.SmallestKey.Data()),
				LargestKey:  base64.StdEncoding.EncodeToString(t.LargestKey.Data()),
			}
		}
	}

	// Serialize to JSON
	data, err := json.MarshalIndent(mj, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	// Write to temp file
	tmpPath := filepath.Join(dbPath, "MANIFEST.tmp")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write temp manifest: %w", err)
	}

	// Sync the temp file to ensure durability
	tmpFile, err := os.Open(tmpPath)
	if err == nil {
		tmpFile.Sync()
		tmpFile.Close()
	}

	// Rename to MANIFEST (atomic on most filesystems)
	manifestPath := filepath.Join(dbPath, manifestFileName)
	if err := os.Rename(tmpPath, manifestPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename manifest: %w", err)
	}

	return nil
}

// NewFileNumber allocates and returns a new unique file number.
func (m *Manifest) NewFileNumber() uint64 {
	num := m.NextFileNumber
	m.NextFileNumber++
	return num
}

// AddTable adds a table to the specified level.
func (m *Manifest) AddTable(level int, meta *TableMeta) {
	if m.Levels == nil {
		m.Levels = make(map[int][]*TableMeta)
	}
	m.Levels[level] = append(m.Levels[level], meta)
	// Sort by file number for consistency
	sort.Slice(m.Levels[level], func(i, j int) bool {
		return m.Levels[level][i].FileNum < m.Levels[level][j].FileNum
	})
}

// RemoveTable removes a table from the specified level by file number.
func (m *Manifest) RemoveTable(level int, fileNum uint64) {
	if m.Levels == nil {
		return
	}
	tables := m.Levels[level]
	for i, t := range tables {
		if t.FileNum == fileNum {
			m.Levels[level] = append(tables[:i], tables[i+1:]...)
			return
		}
	}
}

// GetTablesForLevel returns all tables at the specified level.
func (m *Manifest) GetTablesForLevel(level int) []*TableMeta {
	if m.Levels == nil {
		return nil
	}
	return m.Levels[level]
}

// TotalSize returns the total size of all tables at a given level.
func (m *Manifest) TotalSize(level int) int64 {
	var total int64
	tables := m.GetTablesForLevel(level)
	for _, t := range tables {
		total += t.FileSize
	}
	return total
}

// Level0Count returns the number of files in level 0.
func (m *Manifest) Level0Count() int {
	return len(m.GetTablesForLevel(0))
}