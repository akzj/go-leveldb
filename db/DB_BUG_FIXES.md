# DB Bug Fixes - Contract

## Overview
Fix 9 functional test failures in go-leveldb's db package.

---

## Bug Category 1: WriteBatch Delete (2 tests)

### TestWriteBatchDelete & TestWriteBatchMixed

**Root Cause**: `memtableWriter.Delete()` inserts a tombstone with empty value, but `MemDB.Get()` only returns the value - it doesn't check for deletion markers.

**Current buggy code** (db/db_impl.go):
```go
func (w *memtableWriter) Delete(key util.Slice) {
    ikey := NewInternalKey(key, kMaxSequenceNumber, KTypeDeletion)
    w.db.mem.Delete(ikey.Encode())  // ← Problem: MemDB.Delete stores []byte{}, not proper internal key
}
```

**Fix required**:
1. `MemDB.Delete(key)` should insert the internal key with deletion type into skiplist
2. OR `MemDB.Get()` should recognize deletion markers and return NotFound

**Why not just fix MemDB.Get()?** Because MemDB stores raw bytes without internal key structure.

**Correct fix**: Modify `MemDB.Delete()` to store a proper internal key (not just empty value), and modify `MemDB.Get()` to check for KTypeDeletion and return NotFound.

---

## Bug Category 2: Snapshot Isolation (3 tests)

### TestSnapshotBasic, TestSnapshotIterator, TestSnapshotMultiGet

**Root Cause**: Both `DBImpl.Get()` and `DBImpl.NewIterator()` use `kMaxSequenceNumber` for memtable/imm lookups instead of the snapshot's sequence number.

**Current buggy code** (db/db_impl.go):
```go
func (w *memtableWriter) Put(key, value util.Slice) {
    ikey := NewInternalKey(key, kMaxSequenceNumber, KTypeValue)  // ← Always uses kMaxSequenceNumber
    w.db.mem.Put(ikey.Encode(), value)
}
```

**Fix required**:
1. Store the actual batch sequence number in memtableWriter
2. Pass the sequence to memtableWriter from `writeBatchInternal()`
3. In `Get()` and `NewIterator()`, use snapshot's sequence for memtable lookups (not kMaxSequenceNumber)

**Invariant**: Each operation in a batch gets a unique, incrementing sequence number.

**Code locations to fix**:
- `memtableWriter.seq` - needs to be set per-batch, not hardcoded
- `DBImpl.writeBatchInternal()` - pass correct seq
- `DBImpl.Get()` - use snapshot's seq for memtable lookups

---

## Bug Category 3: Iterator SeekToLast (1 test)

### TestIteratorSeekToLast

**Root Cause**: In `findPrevUserEntry()`, the loop doesn't properly handle reaching the end of data.

**Expected behavior**: SeekToLast on keys ["first", "last", "middle"] should return "last".

**Current buggy behavior**: Returns "middle" (second-to-last).

**Fix required** in `db/db_iter.go` `findPrevUserEntry()`:
- When iterating backwards, ensure we stop at the LAST valid entry, not second-to-last
- The inner loop's `if valueType != KTypeDeletion && ...` condition may be incorrectly skipping entries

---

## Bug Category 4: GetApproximateSizes (1 test)

### TestGetApproximateSizes

**Root Cause**: `VersionSet.ApproximateOffsetOf()` returns 0 when there are no SST files (data only in memtable).

**Current behavior**: Returns 0 because no files exist in any level.

**Fix required**: Include memtable size in approximate size calculation.

**Code location**: `DBImpl.GetApproximateSizes()`

**Expected behavior**: After writing data, GetApproximateSizes should return non-zero even if data hasn't been flushed to SST files yet.

---

## Bug Category 5: DestroyDB Lock File (2 tests)

### TestDestroyDB, TestDestroyAndRecreate

**Root Cause**: `DestroyDB()` doesn't release the lock file before attempting to delete the directory.

**Error**: "resource temporarily unavailable" - means LOCK file is still held.

**Fix required**:
1. `DestroyDB()` must call `UnlockFile(lock)` before deleting files
2. Or skip locking entirely during DestroyDB (don't lock when opening for destruction)

**Code location**: `db/db.go` `DestroyDB()` function

---

## Implementation Order

1. **Lock file fix** (easiest, unblocks testing)
2. **WriteBatch Delete fix** (clear root cause)
3. **Snapshot isolation fix** (sequence number handling)
4. **Iterator SeekToLast fix** (logic error)
5. **GetApproximateSizes fix** (memtable size inclusion)

---

## Acceptance Criteria

```bash
cd go-leveldb && go test -run=. ./db/ 2>&1
# All 9 tests must pass:
# - TestWriteBatchDelete ✓
# - TestWriteBatchMixed ✓
# - TestSnapshotBasic ✓
# - TestSnapshotIterator ✓
# - TestSnapshotMultiGet ✓
# - TestIteratorSeekToLast ✓
# - TestGetApproximateSizes ✓
# - TestDestroyDB ✓
# - TestDestroyAndRecreate ✓
```
