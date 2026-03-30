package db

import (
	"sync"

	"github.com/akzj/go-leveldb/table"
	"github.com/akzj/go-leveldb/util"
)

// kNumLevels is the number of levels in the LSM tree.
// Level 0: memtable flushes go here (may have overlapping files)
// Levels 1+: sorted, non-overlapping files
// Why 7? C++ LevelDB uses 7 levels. Memory/IO tradeoff.
const kNumLevels = 7

// kLevelMultiplier is the size multiplier between levels.
const kLevelMultiplier = 10

// kMaxMemCompactionLevel is the max level for memtable compaction output.
const kMaxMemCompactionLevel = 2

// FileMetaData stores metadata for an SSTable file.
// Invariant: smallest key < largest key (in user comparator order)
type FileMetaData struct {
	Number       uint64
	FileSize     uint64
	Smallest     InternalKey
	Largest      InternalKey
	AllowedSeeks int
	RefCount     int
}

// Version stores the files at each level for a point-in-time snapshot.
// Thread-compatible (requires external synchronization with DB mutex).
type Version struct {
	vset  *VersionSet
	refs  int
	files [kNumLevels][]*FileMetaData

	// Compaction scheduling
	fileToCompact      *FileMetaData
	fileToCompactLevel int
	compactionScore    float64
	compactionLevel    int

	// Doubly-linked list for version management
	next, prev *Version
}

// VersionSet manages the versions of the database.
// All members are protected by DB mutex.
type VersionSet struct {
	dbName       string
	env          Env
	options      *Options
	tableCache   *table.TableCache
	icmp         *InternalKeyComparator

	mu sync.Mutex

	// File numbers
	nextFileNumber  uint64
	manifestNumber  uint64

	// Sequence number
	lastSequence SequenceNumber

	// Log numbers
	logNumber    uint64
	prevLogNumber uint64

	// Descriptor file
	descriptorFile WritableFile
	descriptorLog  *DescriptorLogWriter

	// Version list (circular doubly-linked)
	dummyVersions *Version
	current      *Version

	// Compact pointers per level
	compactPointers [kNumLevels]string
}

// DescriptorLogWriter writes VersionEdits to the manifest.
type DescriptorLogWriter struct {
	file WritableFile
}

// NewDescriptorLogWriter creates a new descriptor log writer.
func NewDescriptorLogWriter(file WritableFile) *DescriptorLogWriter {
	return &DescriptorLogWriter{file: file}
}

// AddRecord writes a VersionEdit to the manifest.
func (w *DescriptorLogWriter) AddRecord(record []byte) *util.Status {
	return w.file.Append(util.MakeSlice(record))
}

// NewVersionSet creates a new version set.
func NewVersionSet(dbName string, options *Options, tableCache *table.TableCache, icmp *InternalKeyComparator) *VersionSet {
	vs := &VersionSet{
		dbName:         dbName,
		env:            options.Env,
		options:        options,
		tableCache:     tableCache,
		icmp:           icmp,
		nextFileNumber: 1,
		lastSequence:   0,
		logNumber:      0,
		prevLogNumber:  0,
	}

	// Initialize circular doubly-linked list
	dummy := &Version{vset: vs, refs: 0}
	dummy.next = dummy
	dummy.prev = dummy
	vs.dummyVersions = dummy
	vs.current = vs.dummyVersions

	return vs
}

// Current returns the current version.
func (vs *VersionSet) Current() *Version {
	return vs.current
}

// LogNumber returns the current log number.
func (vs *VersionSet) LogNumber() uint64 {
	return vs.logNumber
}

// PrevLogNumber returns the previous log number.
func (vs *VersionSet) PrevLogNumber() uint64 {
	return vs.prevLogNumber
}

// LastSequence returns the last assigned sequence number.
func (vs *VersionSet) LastSequence() SequenceNumber {
	return vs.lastSequence
}

// SetLastSequence updates the last sequence number.
func (vs *VersionSet) SetLastSequence(s SequenceNumber) {
	vs.lastSequence = s
}

// NewFileNumber allocates a new file number.
func (vs *VersionSet) NewFileNumber() uint64 {
	n := vs.nextFileNumber
	vs.nextFileNumber++
	return n
}

// MarkFileNumberUsed records that a file number is in use.
func (vs *VersionSet) MarkFileNumberUsed(fileNum uint64) {
	if fileNum >= vs.nextFileNumber {
		vs.nextFileNumber = fileNum + 1
	}
}

// ManifestFileNumber returns the current manifest file number.
func (vs *VersionSet) ManifestFileNumber() uint64 {
	return vs.manifestNumber
}

// NumLevelFiles returns the number of files at the specified level.
func (vs *VersionSet) NumLevelFiles(level int) int {
	return len(vs.current.files[level])
}

// NumLevelBytes returns approximate bytes at the specified level.
func (vs *VersionSet) NumLevelBytes(level int) uint64 {
	var total uint64
	for _, f := range vs.current.files[level] {
		total += f.FileSize
	}
	return total
}

// Ref increments the reference count.
func (v *Version) Ref() {
	v.refs++
}

// Unref decrements the reference count.
func (v *Version) Unref() {
	v.refs--
	if v.refs <= 0 {
		// Version will be deleted by caller
	}
}

// NumFiles returns number of files at a level.
func (v *Version) NumFiles(level int) int {
	return len(v.files[level])
}

// Get looks up a key in this version's SSTables.
// Invariant: searches levels from bottom (kNumLevels-1) to top (0)
// Returns first match where internal key seq <= targetSeq.
// Uses v.vset.tableCache to open SSTable readers.
// Why bottom-up? Later levels have older data, newer keys should be found in upper levels first.
func (v *Version) Get(key util.Slice, seq SequenceNumber) ([]byte, *util.Status) {
	for level := kNumLevels - 1; level >= 0; level-- {
		files := v.files[level]
		if len(files) == 0 {
			continue
		}
		idx := findFile(v.vset.icmp, files, key)
		if idx >= len(files) {
			continue
		}
		f := files[idx]
		// Check if key is in file's range
		if v.vset.icmp.user.Compare(key, f.Smallest.UserKey()) < 0 ||
			v.vset.icmp.user.Compare(key, f.Largest.UserKey()) > 0 {
			continue
		}
		// Open table and lookup using table cache
		table, err := v.vset.tableCache.GetTable(f.Number, f.FileSize)
		if !err.OK() {
			continue
		}
		value, s := table.Get(key)
		if s != nil && s.OK() {
			return value, s
		}
	}
	return nil, util.NotFound("")
}

// GetOverlappingInputs finds all files in level that may contain keys in range.
func (v *Version) GetOverlappingInputs(level int, begin, end *InternalKey) []*FileMetaData {
	return getOverlappingInputs(v.vset.icmp, v.files[level], begin, end)
}

// getOverlappingInputs implements the overlap check.
func getOverlappingInputs(icmp *InternalKeyComparator, files []*FileMetaData, begin, end *InternalKey) []*FileMetaData {
	var result []*FileMetaData

	if len(files) == 0 {
		return result
	}

	// Binary search for start
	startIndex := 0
	if begin != nil {
		startIndex = findFile(icmp, files, begin.Encode())
		if startIndex >= len(files) {
			return result
		}
	}

	// Binary search for end
	endIndex := len(files) - 1
	if end != nil {
		// Find first file with smallest > end
		tmp := findFile(icmp, files, end.Encode())
		if tmp < len(files) {
			endIndex = tmp
		}
	}

	// Collect all overlapping files
	for i := startIndex; i <= endIndex && i < len(files); i++ {
		f := files[i]
		if end != nil && icmp.Compare(f.Smallest.Encode(), end.Encode()) > 0 {
			break
		}
		result = append(result, f)
	}

	return result
}

// findFile returns the index of the first file with largest key >= target.
// Uses binary search on sorted file list.
func findFile(icmp *InternalKeyComparator, files []*FileMetaData, target util.Slice) int {
	lo := 0
	hi := len(files)

	for lo < hi {
		mid := (lo + hi) / 2
		if icmp.Compare(files[mid].Largest.Encode(), target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// SomeFileOverlapsRange checks if any file in level overlaps the key range.
func (v *Version) SomeFileOverlapsRange(smallestUserKey, largestUserKey *util.Slice) bool {
	// Check each level that might have overlapping files
	for level := 0; level < kNumLevels; level++ {
		if someFileOverlapsRange(v.vset.icmp, v.files[level], smallestUserKey, largestUserKey) {
			return true
		}
	}
	return false
}

// someFileOverlapsRange implements overlap check.
func someFileOverlapsRange(icmp *InternalKeyComparator, files []*FileMetaData, smallest, largest *util.Slice) bool {
	if len(files) == 0 {
		return false
	}

	// Check if smallest is past all files
	if smallest != nil {
		idx := findFile(icmp, files, util.MakeSliceFromString(string(smallest.Data())))
		if idx < len(files) {
			if icmp.user.Compare(*largest, files[idx].Smallest.UserKey()) >= 0 {
				return true
			}
		}
	}

	return false
}

// AppendVersion adds v to the circular list of versions.
func (vs *VersionSet) AppendVersion(v *Version) {
	v.prev = vs.dummyVersions.prev
	v.next = vs.dummyVersions
	v.prev.next = v
	v.next.prev = v
	vs.current = v
}

// NewVersion creates a new version with zero references.
func (vs *VersionSet) NewVersion() *Version {
	return &Version{vset: vs}
}

// PickLevelForMemTableOutput picks the best level for a new memtable compaction.
func (v *Version) PickLevelForMemTableOutput(smallest, largest util.Slice) int {
	level := 0

	// If level 0 has overlap, go to level 1
	if v.vset.NumLevelFiles(0) > 0 {
		level = 1
	}

	// Stop if compaction would overlap with next level
	for level < kMaxMemCompactionLevel {
		if v.vset.NumLevelBytes(level) > uint64(level)*1024*1024*1024 {
			break
		}
		level++
	}

	return level
}

// OverlapInLevel checks if any file in level overlaps [smallest, largest].
func (v *Version) OverlapInLevel(level int, smallest, largest *util.Slice) bool {
	return someFileOverlapsRange(v.vset.icmp, v.files[level], smallest, largest)
}

// Finalize calculates compaction scores for all levels.
func (vs *VersionSet) Finalize(v *Version) {
	v.compactionScore = 1.0
	v.compactionLevel = 0

	// Level 0: score based on file count
	score := float64(vs.NumLevelFiles(0))
	if score < 1 {
		// Other levels: score based on size
		score = float64(vs.NumLevelBytes(1)) / float64(64*1024*1024)
		v.compactionLevel = 1
	}

	v.compactionScore = score
}

// Compaction represents a compaction operation.
// Invariant: files_[0] are level-0 files, files_[1] are level-1 files, etc.
// VersionEdit will delete input files and add output files.
type Compaction struct {
	vset           *VersionSet
	level          int
	inputVersions  [2]*Version
	inputFiles    [2][]*FileMetaData
	grandparents   []*FileMetaData
	overlappedBytes uint64
	outputLevel   int
}

// NewCompaction creates a new compaction for the given level.
func (vs *VersionSet) NewCompaction(level int) *Compaction {
	return &Compaction{
		vset:  vs,
		level: level,
	}
}

// IsTrivialMove returns true if compaction can be done by simply moving files.
func (c *Compaction) IsTrivialMove() bool {
	return len(c.inputFiles[0]) == 1 && len(c.inputFiles[1]) == 0
}

// PickCompaction selects the next compaction to run.
// Level 0: picks oldest files if count > kL0CompactionTrigger
// Level > 0: picks file that exceeds allowed seeks
func (vs *VersionSet) PickCompaction() *Compaction {
	v := vs.current

	// Level 0 compaction
	if vs.NumLevelFiles(0) >= 4 {
		// Pick the oldest file from level 0
		c := vs.NewCompaction(0)
		c.inputFiles[0] = v.files[0][:1]
		return c
	}

	// Otherwise, pick from the level with highest score
	level := v.compactionLevel
	if level == 0 || level >= kNumLevels-1 {
		level = 1
	}

	c := vs.NewCompaction(level)
	c.inputFiles[0] = v.files[level]
	return c
}

// RunCompaction executes the compaction and returns a VersionEdit.
// TODO: implement actual compaction logic
func (vs *VersionSet) RunCompaction(c *Compaction) (*VersionEdit, *util.Status) {
	edit := NewVersionEdit()
	return edit, util.NewStatusOK()
}

// NeedsCompaction returns true if current version needs compaction.
func (vs *VersionSet) NeedsCompaction() bool {
	v := vs.current
	return v.compactionScore >= 1.0 || v.fileToCompact != nil
}

// LogAndApply applies a VersionEdit to the current version.
// Writes the edit to the manifest and installs the new version.
func (vs *VersionSet) LogAndApply(edit *VersionEdit, mu *sync.Mutex) *util.Status {
	// Create new version
	v := vs.NewVersion()

	// Apply edit to new version
	if err := vs.applyEdit(v, edit); !err.OK() {
		return err
	}

	// Finalize compaction scores
	vs.Finalize(v)

	// Save to manifest
	if vs.descriptorLog != nil {
		record := edit.EncodeTo()
		if err := vs.descriptorLog.AddRecord(record); !err.OK() {
			return err
		}
		if err := vs.descriptorFile.Sync(); !err.OK() {
			return err
		}
	}

	// Install new version
	vs.AppendVersion(v)
	v.Ref()

	// Unref old version
	vs.current.Unref()

	return util.NewStatusOK()
}

// applyEdit applies a VersionEdit to a version.
func (vs *VersionSet) applyEdit(v *Version, edit *VersionEdit) *util.Status {
	// Apply log number
	if edit.hasLogNumber {
		vs.logNumber = edit.LogNumber
	}

	// Apply prev log number
	if edit.hasPrevLogNumber {
		vs.prevLogNumber = edit.PrevLogNumber
	}

	// Apply next file number
	if edit.hasNextFileNumber {
		vs.nextFileNumber = edit.NextFileNumber
	}

	// Apply last sequence
	if edit.hasLastSequence {
		vs.lastSequence = edit.LastSequence
	}

	// Apply deleted files
	for key := range edit.DeletedFiles {
		// Find and remove file from version
		files := v.files[key.Level]
		for i, f := range files {
			if f.Number == key.FileNumber {
				// Remove file
				copy(files[i:], files[i+1:])
				v.files[key.Level] = files[:len(files)-1]
				break
			}
		}
	}

	// Apply new files
	for _, info := range edit.NewFiles {
		meta := &info.Meta
		meta.AllowedSeeks = 1 << 30
		meta.RefCount = 0
		v.files[info.Level] = append(v.files[info.Level], meta)
	}

	// Update compact pointers
	for level, key := range edit.CompactPointers {
		vs.compactPointers[level] = string(key.Encode().Data())
	}

	return util.NewStatusOK()
}

// Recover reads the manifest and reconstructs the version set state.
func (vs *VersionSet) Recover(saveManifest *bool) *util.Status {
	// Read CURRENT file to find manifest
	currentData := make([]byte, 1024)
	n, err := vs.env.ReadFile(vs.dbName+"/CURRENT", currentData)
	if !err.OK() {
		if err.IsNotFound() {
			return util.NewStatusOK() // New DB, no manifest
		}
		return err
	}

	manifestName := vs.dbName + "/" + string(currentData[:n])

	// Open manifest
	manifestFile, err := vs.env.NewSequentialFile(manifestName)
	if !err.OK() {
		return err
	}

	// Read all records
	logReader := NewManifestReader(manifestFile)
	for {
		record, err := logReader.ReadRecord()
		if !err.OK() {
			break // EOF or error
		}

		// Parse version edit
		edit := NewVersionEdit()
		if !edit.DecodeFrom(record) {
			return util.Corruption("bad version edit")
		}

		// Apply to current version
		if err := vs.applyEdit(vs.current, edit); !err.OK() {
			return err
		}
	}

	// Update file numbers
	vs.manifestNumber = parseFileNumber(manifestName)

	return util.NewStatusOK()
}

// ManifestReader reads VersionEdit records from a manifest.
type ManifestReader struct {
	file SequentialFile
}

// NewManifestReader creates a new manifest reader.
func NewManifestReader(file SequentialFile) *ManifestReader {
	return &ManifestReader{file: file}
}

// ReadRecord reads the next record from the manifest.
func (r *ManifestReader) ReadRecord() ([]byte, *util.Status) {
	// Read record header (length)
	header := make([]byte, 4)
	data, err := r.file.Read(4)
	if !err.OK() {
		if len(data) == 0 {
			return nil, nil // EOF
		}
		return nil, err
	}
	if len(data) < 4 {
		return nil, nil // EOF
	}
	copy(header, data)

	length := util.DecodeFixed32(header)
	if length == 0 {
		return nil, nil // EOF
	}

	// Read record data
	record := make([]byte, length)
	remaining := int(length)
	offset := 0
	for remaining > 0 {
		n := remaining
		if n > 4096 {
			n = 4096
		}
		data, err := r.file.Read(n)
		if !err.OK() {
			return nil, err
		}
		copy(record[offset:], data)
		offset += len(data)
		remaining -= len(data)
	}

	return record, util.NewStatusOK()
}

// parseFileNumber extracts the file number from a manifest path.
func parseFileNumber(name string) uint64 {
	var num uint64
	for i := len(name) - 1; i >= 0 && name[i] >= '0' && name[i] <= '9'; i-- {
		num = uint64(name[i]-'0') + num*10
	}
	return num
}
