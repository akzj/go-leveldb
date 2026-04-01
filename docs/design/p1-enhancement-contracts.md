# P1 Enhancement Contracts

## Overview
Implementing remaining P1 functions: `GetProperty`, `GetApproximateSizes`, `GetSnapshot`/`ReleaseSnapshot`, `CompactRange`.

## Missing Methods (Prerequisites)

### 1. VersionSet.ApproximateOffsetOf
```go
// ApproximateOffsetOf returns approximate file offset for key within version.
// Invariant: result = sum of file sizes of files that may contain key
// Why not just return file offset? Callers need size for range calculations.
func (vs *VersionSet) ApproximateOffsetOf(v *Version, key InternalKey) uint64
```

### 2. Version.DebugString
```go
// DebugString returns multi-line string describing all SSTables in version.
// Used by GetProperty("leveldb.sstables").
// Format: per-level summary with file metadata.
func (v *Version) DebugString() string
```

### 3. VersionSet.CompactRange (exists but needs validation)
```go
// CompactRange should return a Compaction for the given level and key range.
// From C++: calls versions_->CompactRange(level, begin, end)
```

## Contract: GetProperty

**Interface**:
```go
func (db *DBImpl) GetProperty(property util.Slice) (string, bool)
```

**Supported Properties**:
| Property | Implementation | Requires |
|----------|---------------|----------|
| `leveldb.num-files-at-level<N>` | Count files at level N | `vs.NumLevelFiles(level)` |
| `leveldb.stats` | Multi-line compaction stats | `db.stats[level]` (micros, bytes_read, bytes_written) |
| `leveldb.sstables` | Multi-line SSTable description | `v.DebugString()` |
| `leveldb.approximate-memory-usage` | Memory in bytes | `blockCache.TotalCharge() + mem.ApproximateMemoryUsage() + imm.ApproximateMemoryUsage()` |

**Property Parsing**:
```go
// Parse "leveldb.num-files-at-level3" → level=3
// Parse "leveldb.stats" → return stats string
// Parse "leveldb.sstables" → return Version.DebugString()
// Parse "leveldb.approximate-memory-usage" → return memory estimate
```

**Stats Format** (from C++):
```
                               Compactions
Level  Files Size(MB) Time(sec) Read(MB) Write(MB)
--------------------------------------------------
  0        0      0.00      0.00      0.00      0.00
  ...
```

## Contract: GetApproximateSizes

**Interface**:
```go
func (db *DBImpl) GetApproximateSizes(ranges []Range) []uint64
```

**Algorithm** (from C++):
```go
// 1. Lock mutex
// 2. Get current version: v = db.versions.Current()
// 3. v.Ref() to hold version
// 4. For each range:
//    - k1 = NewInternalKey(range.Start, kMaxSequenceNumber, KTypeValue)
//    - k2 = NewInternalKey(range.Limit, kMaxSequenceNumber, KTypeValue)
//    - start = vs.ApproximateOffsetOf(v, k1)
//    - limit = vs.ApproximateOffsetOf(v, k2)
//    - sizes[i] = max(0, limit - start)
// 5. v.Unref()
// 6. Return sizes
```

**Range struct**:
```go
type Range struct {
    Start, Limit util.Slice
}
```

## Contract: CompactRange

**Interface**:
```go
func (db *DBImpl) CompactRange(begin, end util.Slice)
```

**Algorithm** (from C++ lines 582-650):
```go
// 1. Create ManualCompaction struct
// 2. Set manual.level = 0 (or find appropriate level)
// 3. If begin != nil: begin_internal = NewInternalKey(begin, kMaxSequenceNumber, KTypeValue)
// 4. If end != nil: end_internal = NewInternalKey(end, 0, KTypeDeletion)
// 5. Lock mutex
// 6. Loop while !manual.done && !shutting_down && bg_error.ok():
//    - if manualCompaction == nil:
//      - manualCompaction = &manual
//      - MaybeScheduleCompaction()
//    - else: wait on background_work_finished_signal
// 7. After loop: wait for background_compaction_scheduled to be false
// 8. Clear manualCompaction if it was ours
```

## Contract: Snapshot (Verification)

**Existing Implementation** (db_impl.go lines 443-454):
```go
func (db *DBImpl) GetSnapshot() Snapshot {
    db.mu.Lock()
    defer db.mu.Unlock()
    seq := db.versions.LastSequence()
    snapshot := NewSnapshot(seq)
    return snapshot
}

func (db *DBImpl) ReleaseSnapshot(snapshot Snapshot) {
    db.mu.Lock()
    defer db.mu.Unlock()
    snapshot.Release()
}
```

**Verification Checklist**:
- [ ] Snapshot stores sequence number correctly
- [ ] Ref count in snapshotImpl works
- [ ] ReadOptions.Snapshot is used in Get/Iterator

## CompactionStats Fields Required

```go
type CompactionStats struct {
    micros   int64  // Time spent in compaction (microseconds)
    bytes_read  uint64
    bytes_written uint64
}
```

## Dependencies

| Method | File | Status |
|--------|------|--------|
| `VersionSet.ApproximateOffsetOf` | version_set.go | **MISSING** |
| `Version.DebugString` | version_set.go | **MISSING** |
| `VersionSet.CompactRange` | version_set.go | May exist |
| `Reader.ApproximateOffsetOf` | table/reader.go | EXISTS (line 145) |
| `MemDB.ApproximateMemoryUsage` | memdb/memdb.go | EXISTS |
| `Cache.TotalCharge` | cache/cache.go | EXISTS |
| `kNumLevels` | version_set.go | EXISTS (=7) |
| `kMaxSequenceNumber` | db/format.go | EXISTS |
| `KTypeValue` | db/format.go | EXISTS |
