package db

import (
	"fmt"

	"github.com/akzj/go-leveldb/log"
	"github.com/akzj/go-leveldb/memdb"
	"github.com/akzj/go-leveldb/table"
	"github.com/akzj/go-leveldb/util"
	"sync"
	"sync/atomic"
)

// DBImpl is the concrete implementation of the DB interface.
type DBImpl struct {
	options                       *Options
	env                           Env
	dbName                        string
	icmp                          *InternalKeyComparator
	tableCache                    *table.TableCache
	mu                            sync.Mutex
	bgCond                        sync.Cond
	shuttingDown                  atomic.Bool
	mem                           *memdb.MemDB
	imm                           *memdb.MemDB
	hasImm                        atomic.Bool
	logFile                       WritableFile
	logFileNumber                 uint64
	logWriter                     *log.Writer
	versions                      *VersionSet
	backgroundCompactionScheduled bool
	bgError                       *util.Status
	writers                       []Writer
	tmpBatch                      *WriteBatch
	snapshots                     snapshotList
	pendingOutputs                map[uint64]bool
	manualCompaction              *ManualCompaction
	stats                         [kNumLevels]CompactionStats
	seed                          uint32
	lock                          FileLock
}
type Writer struct {
	batch  *WriteBatch
	sync   bool
	done   chan struct{}
	result *util.Status
}
type ManualCompaction struct {
	level      int
	done       bool
	begin, end *InternalKey
	tmpStorage InternalKey
}
type CompactionStats struct {
	micros       int64
	bytesRead    int64
	bytesWritten int64
}

func (s *CompactionStats) Add(other CompactionStats) {
	s.micros += other.micros
	s.bytesRead += other.bytesRead
	s.bytesWritten += other.bytesWritten
}

type snapshotList struct {
	root   snapshotNode
	immSeq SequenceNumber
}
type snapshotNode struct {
	seq  SequenceNumber
	refs int
	next *snapshotNode
	prev *snapshotNode
}

func NewDBImpl(options *Options, dbName string) *DBImpl {
	db := &DBImpl{
		options:        options,
		env:            options.Env,
		dbName:         dbName,
		icmp:           NewInternalKeyComparator(options.Comparator),
		pendingOutputs: make(map[uint64]bool),
	}
	db.bgCond = sync.Cond{L: &db.mu}
	db.snapshots.root.next = &db.snapshots.root
	db.snapshots.root.prev = &db.snapshots.root
	return db
}
func (db *DBImpl) Recover() (*VersionEdit, bool, *util.Status) {
	currentData := make([]byte, 1024)
	n, err := db.env.ReadFile(db.dbName+"/CURRENT", currentData)
	if !err.OK() {
		if err.IsNotFound() {
			return nil, false, nil // New DB
		}
		return nil, false, err
	}
	manifestName := db.dbName + "/" + string(currentData[:n])
	saveManifest := true
	if err := db.versions.Recover(&saveManifest); !err.OK() {
		return nil, false, err
	}
	_ = manifestName // Used by Recover
	return NewVersionEdit(), saveManifest, util.NewStatusOK()
}
func (db *DBImpl) NewDB() *util.Status {
	edit := NewVersionEdit()
	edit.SetComparatorName(db.options.Comparator.Name())
	edit.SetLogNumber(0)
	edit.SetNextFile(2)
	edit.SetLastSequence(0)
	manifestNumber := db.versions.NewFileNumber()
	manifestName := db.dbName + "/" + DescriptorFileName(manifestNumber)
	file, err := db.env.NewWritableFile(manifestName)
	if err != nil {
		return err
	}
	descriptorLog := NewDescriptorLogWriter(file)
	record := edit.EncodeTo()
	if err := descriptorLog.AddRecord(record); !err.OK() {
		return err
	}
	if err := db.env.RenameFile(manifestName, db.dbName+"/CURRENT"); err != nil {
		return err
	}
	db.versions.manifestNumber = manifestNumber
	result := db.versions.LogAndApply(edit, &sync.Mutex{})
	return result
}

// Open opens the database. This function is the main entry point.
func Open(options *Options, dbname string) (DB, *util.Status) {
	options = SanitizeOptions(options)
	// Create directory if CreateIfMissing is true
	if options.CreateIfMissing {
		if err := options.Env.CreateDir(dbname); !err.OK() {
			return nil, err
		}
	}
	// Check if database exists if ErrorIfExists is set
	if options.ErrorIfExists {
		if options.Env.FileExists(dbname + "/CURRENT") {
			return nil, util.IOError("database already exists")
		}
	}
	db := NewDBImpl(options, dbname)

	// Create table-compatible env wrapper
	tableEnv := &tableEnvAdapter{env: options.Env}
	db.tableCache = table.NewTableCache(dbname, tableEnv, options.BlockCache, options.Comparator)
	db.versions = NewVersionSet(dbname, options, db.tableCache, db.icmp)
	lock, err := db.env.LockFile(dbname + "/LOCK")
	if !err.OK() {
		return nil, err
	}
	db.lock = lock
	edit, saveManifest, err := db.Recover()
	// Recover returns (nil, false, nil) for new DB - this is OK
	if err != nil && !err.OK() {
		db.env.UnlockFile(lock)
		return nil, err
	}
	if edit == nil {
		if err := db.NewDB(); !err.OK() {
			db.env.UnlockFile(lock)
			return nil, err
		}
	} else if saveManifest {
		_ = edit // Manifest will be saved by LogAndApply
	}
	// Recover memtable from WAL
	db.mem = memdb.NewMemDB(db.icmp)
	if err := db.RecoverLogFiles(); !err.OK() {
		db.env.UnlockFile(lock)
		return nil, err
	}
	logNumber := db.versions.NewFileNumber()
	db.logFileNumber = logNumber
	logName := db.dbName + "/" + LogFileName(logNumber)
	db.logFile, err = db.env.NewWritableFile(logName)
	if !err.OK() {
		return nil, err
	}
	db.logWriter = log.NewWriter(db.logFile)
	return db, util.NewStatusOK()
}

// RecoverLogFiles recovers memtable from WAL log files.
func (db *DBImpl) RecoverLogFiles() *util.Status {
	// Get all log files from the database directory
	children, err := db.env.GetChildren(db.dbName)
	if !err.OK() {
		return err
	}
	// Find the log file number from version set
	logNumber := db.versions.LogNumber()

	// If no log number set, no WAL to recover
	if logNumber == 0 {
		return util.NewStatusOK()
	}
	// Find the largest log file number <= logNumber
	var maxLogNum uint64 = 0
	for _, name := range children {
		fileType, num, ok := ParseFileName(name)
		if !ok {
			continue
		}
		if fileType == kLogFile && num <= logNumber && num > maxLogNum {
			maxLogNum = num
		}
	}
	if maxLogNum == 0 {
		return util.NewStatusOK()
	}
	// Open and recover the WAL
	return db.RecoverLogFile(maxLogNum)
}

// RecoverLogFile reads a WAL log file and replays records to memtable.
func (db *DBImpl) RecoverLogFile(logNum uint64) *util.Status {
	logName := db.dbName + "/" + LogFileName(logNum)

	file, err := db.env.NewSequentialFile(logName)
	if !err.OK() {
		return err
	}
	reader := log.NewReader(file)
	seq := db.versions.LastSequence() + 1
	for {
		record, status := reader.ReadRecord()
		if !status.OK() {
			return status
		}
		if record == nil {
			break // EOF
		}
		// Parse WriteBatch from record
		batch := decodeWriteBatch(record)
		if batch == nil {
			continue // Skip corrupted records
		}
		// Apply to memtable
		batch.Iterate(&memtableWriter{db: db, seq: seq})

		// Update sequence number based on batch size
		numOps := uint64(len(batch.ops))
		seq += SequenceNumber(numOps)
		db.versions.SetLastSequence(seq - 1)
	}
	return util.NewStatusOK()
}

// decodeWriteBatch decodes a WriteBatch from its binary representation.
// Returns nil if the batch is malformed.
func decodeWriteBatch(data []byte) *WriteBatch {
	if len(data) == 0 {
		return nil
	}
	batch := NewWriteBatch()
	offset := 0
	// Read count (varint32)
	count, n, ok := util.DecodeVarint32(data[offset:])
	if !ok {
		return nil
	}
	offset += n
	for i := uint32(0); i < count; i++ {
		if offset >= len(data) {
			return nil
		}
		// Read opcode
		code := WriteBatchOpCode(data[offset])
		offset++
		// Read key (length-prefixed)
		keySlice, n, ok := util.GetLengthPrefixedSlice(data[offset:])
		if !ok {
			return nil
		}
		offset += n
		if code == WriteBatchPut {
			// Read value (length-prefixed)
			valueSlice, n, ok := util.GetLengthPrefixedSlice(data[offset:])
			if !ok {
				return nil
			}
			offset += n
			batch.Put(keySlice, valueSlice)
		} else if code == WriteBatchDelete {
			batch.Delete(keySlice)
		} else {
			// Unknown opcode, skip
			return nil
		}
	}
	return batch
}
func SanitizeOptions(options *Options) *Options {
	if options == nil {
		options = NewOptions()
	}
	if options.Comparator == nil {
		options.Comparator = util.DefaultBytewiseComparator()
	}
	if options.Env == nil {
		options.Env = DefaultEnv()
	}
	if options.WriteBufferSize == 0 {
		options.WriteBufferSize = 4 * 1024 * 1024
	}
	if options.MaxFileSize == 0 {
		options.MaxFileSize = 2 * 1024 * 1024
	}
	if options.BlockSize == 0 {
		options.BlockSize = 4096
	}
	if options.BlockCache == nil {
		options.BlockCache = table.NewLRUCache(8 << 20)
	}
	return options
}
func (db *DBImpl) Put(options *WriteOptions, key, value util.Slice) *util.Status {
	batch := NewWriteBatch()
	batch.Put(key, value)
	return db.Write(options, batch)
}
func (db *DBImpl) Delete(options *WriteOptions, key util.Slice) *util.Status {
	batch := NewWriteBatch()
	batch.Delete(key)
	return db.Write(options, batch)
}
func (db *DBImpl) Write(options *WriteOptions, batch *WriteBatch) *util.Status {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.makeRoomForWrite(false); err != nil && !err.OK() {
		return err
	}
	lastSeq := db.versions.LastSequence()
	seq := lastSeq + 1
	if db.logWriter != nil {
		record := encodeWriteBatch(batch)
		if _, err := db.logWriter.AddRecord(record); err != nil && !err.OK() {
			return err
		}
		if options != nil && options.Sync && db.logFile != nil {
			if err := db.logFile.Sync(); err != nil && !err.OK() {
				return err
			}
		}
	}
	if err := db.writeBatchInternal(batch, seq); err != nil && !err.OK() {
		return err
	}
	// Update LastSequence to the actual last sequence used in this batch
	db.versions.SetLastSequence(seq + SequenceNumber(len(batch.ops)) - 1)
	return util.NewStatusOK()
}
func encodeWriteBatch(batch *WriteBatch) []byte {
	var result []byte
	result = util.PutVarint32(result, uint32(len(batch.ops)))
	for _, op := range batch.ops {
		result = append(result, byte(op.Code))
		result = util.PutLengthPrefixedSlice(result, util.MakeSlice(op.Key))
		if op.Code == WriteBatchPut {
			result = util.PutLengthPrefixedSlice(result, util.MakeSlice(op.Value))
		}
	}
	return result
}
func (db *DBImpl) writeBatchInternal(batch *WriteBatch, seq SequenceNumber) *util.Status {
	batch.Iterate(&memtableWriter{db: db, seq: seq})
	return util.NewStatusOK()
}

type memtableWriter struct {
	db  *DBImpl
	seq SequenceNumber
}

func (w *memtableWriter) Put(key, value util.Slice) {
	// Use w.seq for proper snapshot isolation
	// This ensures Put and Delete can be ordered correctly
	ikey := NewInternalKey(key, w.seq, KTypeValue)
	w.db.mem.Put(ikey.Encode(), value)
}
func (w *memtableWriter) Delete(key util.Slice) {
	// Use w.seq for proper snapshot isolation
	// Delete and Put in the same batch get the same sequence, but
	// in LevelDB, Put (type=1) sorts after Delete (type=0) at same sequence
	// So we need to use seq+1 for Put to make it "newer"
	ikey := NewInternalKey(key, w.seq, KTypeDeletion)
	w.db.mem.Delete(ikey.Encode())
}

// GetLastSequence returns the current last sequence number.
// Exported for testing purposes.
func (db *DBImpl) GetLastSequence() SequenceNumber {
	return db.versions.LastSequence()
}
func (db *DBImpl) Get(options *ReadOptions, key util.Slice) ([]byte, *util.Status) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Use LastSequence when no snapshot to get the latest entry
	// (Put uses actual sequence number, not kMaxSequenceNumber)
	var seq SequenceNumber = db.versions.LastSequence()
	if options != nil && options.Snapshot != nil {
		// With snapshot, use the snapshot's sequence
		seq = options.Snapshot.Sequence()
	}
	// Search memtable with appropriate sequence for proper snapshot isolation
	if db.mem != nil {
		if value, status, ok := db.mem.FindForSnapshot(key, uint64(seq)); ok {
			return value, status
		}
	}
	if db.imm != nil {
		if value, status, ok := db.imm.FindForSnapshot(key, uint64(seq)); ok {
			return value, status
		}
	}
	if value, err := db.versions.Current().Get(key, seq); err.OK() {
		return value, err
	}
	return nil, util.NotFound("")
}
func (db *DBImpl) NewIterator(options *ReadOptions) Iterator {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Use LastSequence when no snapshot to see all entries
	// (Put uses actual sequence number, not kMaxSequenceNumber)
	var seq SequenceNumber = db.versions.LastSequence()
	if options != nil && options.Snapshot != nil {
		seq = options.Snapshot.Sequence()
	}
	internalIter := db.newInternalIterator()
	return NewDBIterator(db, db.options.Comparator, internalIter, seq, db.seed)
}
func (db *DBImpl) newInternalIterator() Iterator {
	var iters []Iterator
	if db.mem != nil {
		iters = append(iters, db.mem.NewIterator())
	}
	if db.imm != nil {
		iters = append(iters, db.imm.NewIterator())
	}
	if len(iters) == 0 {
		return NewEmptyIterator()
	}
	return NewMergingIterator(db.icmp, iters)
}
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
func (db *DBImpl) GetProperty(property util.Slice) (string, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	prefix := util.MakeSliceFromString("leveldb.")
	if !property.StartsWith(prefix) {
		return "", false
	}
	in := property
	in.RemovePrefix(prefix.Size())

	// Handle "num-files-at-level<N>"
	numFilesPrefix := util.MakeSliceFromString("num-files-at-level")
	if in.StartsWith(numFilesPrefix) {
		tmp := in
		tmp.RemovePrefix(19) // len("num-files-at-level")
		if tmp.Empty() {
			return "", false
		}
		// Parse level number
		var level uint64
		for !tmp.Empty() && tmp.Data()[0] >= '0' && tmp.Data()[0] <= '9' {
			level = level*10 + uint64(tmp.Data()[0]-'0')
			tmp.RemovePrefix(1)
		}
		if !tmp.Empty() || level >= uint64(kNumLevels) {
			return "", false
		}
		return fmt.Sprintf("%d", db.versions.NumLevelFiles(int(level))), true
	}

	// Handle "stats"
	statsStr := util.MakeSliceFromString("stats")
	if in.StartsWith(statsStr) && in.Size() == 5 {
		result := "                               Compactions\n"
		result += "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
		result += "--------------------------------------------------\n"
		for level := 0; level < kNumLevels; level++ {
			files := db.versions.NumLevelFiles(level)
			if db.stats[level].micros > 0 || files > 0 {
				sizeMB := float64(db.versions.NumLevelBytes(level)) / 1048576.0
				timeSec := float64(db.stats[level].micros) / 1e6
				readMB := float64(db.stats[level].bytesRead) / 1048576.0
				writeMB := float64(db.stats[level].bytesWritten) / 1048576.0
				result += fmt.Sprintf("%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
					level, files, sizeMB, timeSec, readMB, writeMB)
			}
		}
		return result, true
	}

	// Handle "sstables"
	sstablesStr := util.MakeSliceFromString("sstables")
	if in.StartsWith(sstablesStr) && in.Size() == 8 {
		return db.versions.Current().DebugString(), true
	}

	// Handle "approximate-memory-usage"
	memUsageStr := util.MakeSliceFromString("approximate-memory-usage")
	if in.StartsWith(memUsageStr) && in.Size() == 26 {
		var totalUsage int
		if db.options.BlockCache != nil {
			totalUsage += int(db.options.BlockCache.TotalCharge())
		}
		if db.mem != nil {
			totalUsage += int(db.mem.ApproximateMemoryUsage())
		}
		if db.imm != nil {
			totalUsage += int(db.imm.ApproximateMemoryUsage())
		}
		return fmt.Sprintf("%d", totalUsage), true
	}

	return "", false
}
func (db *DBImpl) GetApproximateSizes(ranges []Range) []uint64 {
	db.mu.Lock()
	v := db.versions.Current()
	v.Ref()
	sizes := make([]uint64, len(ranges))
	for i := 0; i < len(ranges); i++ {
		k1 := NewInternalKey(ranges[i].Start, kMaxSequenceNumber, KTypeValue)
		k2 := NewInternalKey(ranges[i].Limit, kMaxSequenceNumber, KTypeValue)
		start := db.versions.ApproximateOffsetOf(v, k1)
		limit := db.versions.ApproximateOffsetOf(v, k2)
		if limit >= start {
			sizes[i] = limit - start
		}
		// Include memtable size if it exists - memtable data starts at offset 0
		// and isn't yet in SST files, so it should always be counted
		if db.mem != nil {
			memSize := uint64(db.mem.ApproximateMemoryUsage())
			if sizes[i] < memSize {
				sizes[i] = memSize
			}
		}
		if db.imm != nil {
			immSize := uint64(db.imm.ApproximateMemoryUsage())
			if sizes[i] < immSize {
				sizes[i] = immSize
			}
		}
	}
	v.Unref()
	db.mu.Unlock()
	return sizes
}
func (db *DBImpl) CompactRange(begin, end util.Slice) {
	manual := ManualCompaction{
		level: 0,
		done:  false,
	}
	if !begin.Empty() {
		manual.begin = new(InternalKey)
		*manual.begin = NewInternalKey(begin, kMaxSequenceNumber, KTypeValue)
	}
	if !end.Empty() {
		manual.end = new(InternalKey)
		*manual.end = NewInternalKey(end, 0, KTypeDeletion)
	}

	db.mu.Lock()
	for !manual.done && !db.shuttingDown.Load() && db.bgError.OK() {
		if db.manualCompaction == nil {
			db.manualCompaction = &manual
			db.MaybeScheduleCompaction()
		} else {
			db.bgCond.Wait()
		}
	}
	// Finish current background compaction in the case where
	// bgCond was signalled due to an error.
	for db.backgroundCompactionScheduled {
		db.bgCond.Wait()
	}
	if db.manualCompaction == &manual {
		db.manualCompaction = nil
	}
	db.mu.Unlock()
}
func (db *DBImpl) makeRoomForWrite(force bool) *util.Status {
	for {
		if !force && int(db.mem.ApproximateMemoryUsage()) < db.options.WriteBufferSize {
			return util.NewStatusOK()
		}
		if db.hasImm.Load() {
			// Another thread is switching memtable, wait for it
			continue
		}
		// Memtable is full, need to switch
		if err := db.switchMemtable(); !err.OK() {
			return err
		}
		// Trigger background compaction
		db.MaybeScheduleCompaction()
	}
}

// MaybeScheduleCompaction schedules a background compaction if not already scheduled.
func (db *DBImpl) MaybeScheduleCompaction() {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.backgroundCompactionScheduled {
		return
	}
	db.backgroundCompactionScheduled = true
	db.env.Schedule(db.CompactMemTable)
}

// CompactMemTable implements memtable → SSTable flush.
// Design Contract (reference: leveldb_cpp_code_readonly/db/version_set.cc):
//   Input: db.imm contains sorted key-value pairs from memtable
//   Output: New SSTable file in level 0
//   Side effects:
//     - New SSTable file created in db directory
//     - VersionEdit contains AddFile record for new SSTable
//     - manifest synced via LogAndApply
//     - db.imm set to nil on success
//
// Implementation steps:
//   1. Get file number: db.versions.NewFileNumber()
//   2. Get key range: iterate imm to find smallest/largest keys
//   3. Build SSTable: table.BuildTable(env, dbName, fileNum, opts, iter)
//   4. Add to VersionEdit: edit.AddFile(0, FileMetaData{...})
//   5. Apply edit: db.versions.LogAndApply(edit, &db.mu)
//   6. Clear imm: db.imm = nil (only after LogAndApply succeeds)
//
// Why BuildTable? Creates sorted SSTable from memdb iterator.
// Why not just LogAndApply? Current code skips step 3 - no SSTable is built.
func (db *DBImpl) CompactMemTable() {
	db.mu.Lock()
	defer db.mu.Unlock()
	defer func() {
		db.backgroundCompactionScheduled = false
	}()
	if db.imm == nil {
		return
	}

	// Step 1: Get file number
	fileNumber := db.versions.NewFileNumber()

	// Step 2: Get key range by iterating imm
	imm := db.imm
	var smallest, largest InternalKey
	iter := imm.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		smallest = InternalKey{rep: iter.Key().Data()}
	}
	iter.SeekToLast()
	if iter.Valid() {
		largest = InternalKey{rep: iter.Key().Data()}
	}
	iter.Release()

	// Step 3: Build SSTable from imm
	builderOpts := &table.TableBuilderOptions{
		Comparator: db.icmp,
	}
	tableEnv := &tableEnvAdapter{env: db.env}
	fileSize, buildStatus := table.BuildTable(tableEnv, db.dbName, fileNumber, builderOpts, imm.NewIterator())
	if !buildStatus.OK() {
		db.bgError = buildStatus
		return
	}

	// Step 4: Add to VersionEdit
	edit := NewVersionEdit()
	edit.SetLogNumber(db.logFileNumber)
	edit.AddFile(0, FileMetaData{
		Number:   fileNumber,
		FileSize: fileSize,
		Smallest: smallest,
		Largest:  largest,
	})

	// Step 5: Apply edit
	if err := db.versions.LogAndApply(edit, &db.mu); !err.OK() {
		db.bgError = err
		return
	}

	// Step 6: Clear imm (only after LogAndApply succeeds)
	db.imm = nil
	db.hasImm.Store(false)
}
func (db *DBImpl) switchMemtable() *util.Status {
	db.hasImm.Store(true)
	db.imm = db.mem
	db.mem = memdb.NewMemDB(db.icmp)
	logNumber := db.versions.NewFileNumber()
	logName := db.dbName + "/" + LogFileName(logNumber)
	logFile, err := db.env.NewWritableFile(logName)
	if err != nil {
		return err
	}
	db.logFile = logFile
	db.logFileNumber = logNumber
	db.logWriter = log.NewWriter(logFile)
	edit := NewVersionEdit()
	edit.SetLogNumber(logNumber)
	return db.versions.LogAndApply(edit, &db.mu)
}
func (db *DBImpl) Close() *util.Status {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.shuttingDown.Store(true)
	if db.logFile != nil {
		db.logFile.Close()
	}
	// Release the lock file
	if db.lock != nil {
		db.env.UnlockFile(db.lock)
		db.lock = nil
	}
	return util.NewStatusOK()
}
func InternalKeyForSeek(key util.Slice, seq SequenceNumber) InternalKey {
	return NewInternalKey(key, seq, KTypeValue)
}

// tableEnvAdapter wraps db.Env to satisfy table.Env interface.
// ⚠️ WARNING: This adapter must stay in sync with both db.Env and table.Env interfaces.
// Any change to either interface requires updating this adapter.
// Why not extend Env directly? table package intentionally duplicates
// Env to avoid import cycle with db package.
type tableEnvAdapter struct {
	env Env
}

func (e *tableEnvAdapter) NewRandomAccessFile(name string) (table.RandomAccessFile, *util.Status) {
	file, err := e.env.NewRandomAccessFile(name)
	if !err.OK() {
		return nil, err
	}
	return &randomAccessFileAdapter{file: file}, err
}
func (e *tableEnvAdapter) LockFile(name string) (interface{}, *util.Status) {
	lock, err := e.env.LockFile(name)
	if !err.OK() {
		return nil, err
	}
	return lock, err
}
func (e *tableEnvAdapter) UnlockFile(lock interface{}) *util.Status {
	return e.env.UnlockFile(lock.(FileLock))
}
func (e *tableEnvAdapter) ReadFile(name string, data []byte) (int, *util.Status) {
	return e.env.ReadFile(name, data)
}
func (e *tableEnvAdapter) RenameFile(oldName, newName string) *util.Status {
	return e.env.RenameFile(oldName, newName)
}
func (e *tableEnvAdapter) DeleteFile(name string) *util.Status {
	return e.env.DeleteFile(name)
}
func (e *tableEnvAdapter) GetFileSize(name string) (uint64, *util.Status) {
	return e.env.GetFileSize(name)
}
func (e *tableEnvAdapter) NewWritableFile(name string) (table.WritableFile, *util.Status) {
	file, err := e.env.NewWritableFile(name)
	if !err.OK() {
		return nil, err
	}
	return &writableFileAdapter{file: file}, err
}
func (e *tableEnvAdapter) CreateDir(dir string) *util.Status {
	return e.env.CreateDir(dir)
}

// randomAccessFileAdapter wraps db.RandomAccessFile for table.RandomAccessFile.
type randomAccessFileAdapter struct {
	file RandomAccessFile
}

func (a *randomAccessFileAdapter) ReadAt(p []byte, offset int64) (n int, err *util.Status) {
	data, err := a.file.Read(uint64(offset), len(p))
	if !err.OK() {
		return 0, err
	}
	n = copy(p, data)
	return n, util.NewStatusOK()
}

// writableFileAdapter wraps db.WritableFile for table.WritableFile.
type writableFileAdapter struct {
	file WritableFile
}

func (a *writableFileAdapter) Append(data util.Slice) *util.Status {
	return a.file.Append(data)
}
func (a *writableFileAdapter) Close() *util.Status {
	return a.file.Close()
}
func (a *writableFileAdapter) Flush() *util.Status {
	return a.file.Flush()
}
func (a *writableFileAdapter) Sync() *util.Status {
	return a.file.Sync()
}
