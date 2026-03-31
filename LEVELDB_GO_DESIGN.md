# GoLevelDB — Go 重写 LevelDB 设计文档

> **目的**：作为 A/B 实验的标准化输入。两个不同配置的 agent 拿到完全相同的文档，独立实现。
> 文档必须足够详细和明确，避免实现差异来自"理解不同"。

---

## 1. 项目概述

用 Go 实现一个简化版 LevelDB（LSM-Tree 存储引擎）。

**目标**：
- 功能正确
- 可测试（每个组件都有单元测试）
- 代码清晰（Go 惯用风格）

**不需要**（保持范围可控）：
- 性能优化（无 block cache、无 bloom filter）
- 并发写入（单写者模型）
- 压缩（snappy/zstd）
- 自定义 comparator（固定使用 bytes.Compare）
- Table cache
- Rate limiting

**Go Module**：`goleveldb`

---

## 2. 核心 API

### 2.1 DB 接口

```go
package goleveldb

// Options 配置数据库选项
type Options struct {
    // MemTable 大小阈值（字节），达到后触发 flush
    // 默认值：4 * 1024 * 1024 (4MB)
    MemTableSize int

    // SSTable Data Block 大小目标（字节）
    // 默认值：4 * 1024 (4KB)
    BlockSize int

    // LSM-Tree 最大层级数（Level 0 到 Level N）
    // 默认值：7 (Level 0 ~ Level 6)
    MaxLevels int

    // Level 0 文件数达到此值时触发 compaction
    // 默认值：4
    Level0CompactionTrigger int

    // Level 1 的最大总大小（字节），后续每层 x10
    // 默认值：10 * 1024 * 1024 (10MB)
    Level1MaxSize int64
}

// DefaultOptions 返回默认配置
func DefaultOptions() *Options {
    return &Options{
        MemTableSize:            4 * 1024 * 1024,
        BlockSize:               4 * 1024,
        MaxLevels:               7,
        Level0CompactionTrigger: 4,
        Level1MaxSize:           10 * 1024 * 1024,
    }
}

// DB 是数据库的主接口
type DB interface {
    // Put 写入一个 key-value 对。key 和 value 不能为 nil。
    // Put 内部会先写 WAL，再写 MemTable。
    Put(key, value []byte) error

    // Get 读取 key 对应的 value。
    // 如果 key 不存在，返回 (nil, ErrNotFound)。
    // 读取顺序：MemTable → Immutable MemTable → Level 0 → Level 1 → ... → Level N
    Get(key []byte) ([]byte, error)

    // Delete 删除一个 key。实际写入一个 deletion marker（tombstone）。
    // Delete 内部会先写 WAL，再写 MemTable（标记为删除）。
    Delete(key []byte) error

    // NewIterator 创建一个遍历所有有效 key-value 的迭代器。
    // 迭代器创建时获取当前快照（snapshot），后续写入不影响迭代器。
    // 简化实现：迭代器创建时合并所有数据源，不需要真正的 snapshot 机制。
    NewIterator() Iterator

    // Close 关闭数据库。Flush MemTable 到 SSTable，关闭 WAL，释放资源。
    Close() error
}

// Open 打开或创建一个数据库。
// 如果 path 不存在，创建目录和初始文件。
// 如果 path 存在，从 Manifest 恢复状态，从 WAL 恢复未 flush 的数据。
// opts 为 nil 时使用 DefaultOptions()。
func Open(path string, opts *Options) (DB, error)

// 错误定义
var (
    ErrNotFound     = errors.New("goleveldb: key not found")
    ErrDBClosed     = errors.New("goleveldb: database closed")
    ErrEmptyKey     = errors.New("goleveldb: key cannot be empty")
)
```

### 2.2 Iterator 接口

```go
package goleveldb

// Iterator 遍历数据库中的 key-value 对（按 key 字典序）。
// 使用完毕后不需要显式关闭（简化实现）。
type Iterator interface {
    // First 移动到第一个 key（最小 key）。
    First()

    // Last 移动到最后一个 key（最大 key）。
    Last()

    // Next 移动到下一个 key。如果已经在最后，Valid() 返回 false。
    Next()

    // Prev 移动到上一个 key。如果已经在第一个，Valid() 返回 false。
    Prev()

    // Seek 移动到 >= key 的第一个位置。
    // 如果没有 >= key 的 entry，Valid() 返回 false。
    Seek(key []byte)

    // Valid 返回迭代器是否指向有效的 entry。
    Valid() bool

    // Key 返回当前 entry 的 key。只在 Valid() == true 时调用。
    // 返回的 slice 在下次迭代器操作后可能失效（调用方应复制）。
    Key() []byte

    // Value 返回当前 entry 的 value。只在 Valid() == true 时调用。
    // 返回的 slice 在下次迭代器操作后可能失效（调用方应复制）。
    Value() []byte
}
```

---

## 3. 内部 Key 编码

所有写入 MemTable 和 SSTable 的 key 都使用 InternalKey 格式，而非原始 user key。

### 3.1 InternalKey 格式

```
┌──────────────────────┬──────────────────────┬───────────┐
│      user_key        │  sequence (8 bytes)  │ type (1)  │
│    (变长, 原始key)    │   uint64 big-endian  │ Put=1     │
│                      │                      │ Delete=2  │
└──────────────────────┴──────────────────────┴───────────┘
```

**总长度** = len(user_key) + 8 + 1 = len(user_key) + 9

### 3.2 Sequence Number

- 全局递增的 uint64，每次写入操作（Put 或 Delete）递增 1
- 用于区分同一个 user_key 的不同版本
- 读取时，对于同一个 user_key，取 sequence number 最大的版本

### 3.3 InternalKey 排序规则

```go
// InternalKey 的排序规则（用于 MemTable 和 SSTable）：
// 1. 先按 user_key 升序（bytes.Compare）
// 2. user_key 相同时，按 sequence number 降序（新版本在前）
// 3. sequence 相同时，按 type 降序（Delete > Put，但实际不会出现）
func CompareInternalKey(a, b []byte) int {
    // 提取 user_key（去掉最后 9 字节）
    aUserKey := a[:len(a)-9]
    bUserKey := b[:len(b)-9]

    cmp := bytes.Compare(aUserKey, bUserKey)
    if cmp != 0 {
        return cmp
    }

    // user_key 相同，按 sequence 降序
    aSeq := binary.BigEndian.Uint64(a[len(a)-9 : len(a)-1])
    bSeq := binary.BigEndian.Uint64(b[len(b)-9 : len(b)-1])

    if aSeq > bSeq {
        return -1 // 新版本排在前面
    }
    if aSeq < bSeq {
        return 1
    }
    return 0
}
```

### 3.4 Go 接口

```go
package internal

// ValueType 表示写入类型
type ValueType byte

const (
    TypePut    ValueType = 1
    TypeDelete ValueType = 2
)

// InternalKey 编解码
type InternalKey []byte

// MakeInternalKey 创建 InternalKey
func MakeInternalKey(userKey []byte, seq uint64, vt ValueType) InternalKey

// UserKey 提取 user key
func (k InternalKey) UserKey() []byte

// Sequence 提取 sequence number
func (k InternalKey) Sequence() uint64

// ValueType 提取类型
func (k InternalKey) Type() ValueType

// Compare 比较两个 InternalKey
func Compare(a, b InternalKey) int
```

---

## 4. Varint 编码

用于 SSTable 和 WAL 中编码变长整数，节省空间。

### 4.1 编码规则

使用标准的 LEB128 变长编码（与 Go 标准库 `encoding/binary` 的 `PutUvarint`/`Uvarint` 兼容）：

```go
package internal

// PutUvarint 编码 uint64 到 buf，返回写入的字节数
// 直接使用 encoding/binary.PutUvarint
func PutUvarint(buf []byte, x uint64) int

// Uvarint 从 buf 解码 uint64，返回值和消耗的字节数
// 直接使用 encoding/binary.Uvarint
func Uvarint(buf []byte) (uint64, int)

// AppendUvarint 追加编码后的 varint 到 slice
func AppendUvarint(dst []byte, x uint64) []byte
```

**直接使用 Go 标准库 `encoding/binary` 的实现，不要自己写。**

---

## 5. MemTable

### 5.1 跳表（SkipList）

MemTable 的底层数据结构。存储 InternalKey → Value 的有序映射。

```go
package memtable

const MaxHeight = 12 // 跳表最大高度

// SkipList 是一个有序的 key-value 存储
type SkipList struct {
    head   *node
    height int        // 当前最大高度
    size   int        // 当前存储的数据总大小（字节，用于判断 MemTable 是否满）
    rng    *rand.Rand // 随机数生成器（确定性种子，方便测试）
}

type node struct {
    key   internal.InternalKey
    value []byte
    next  [MaxHeight]*node // 每层的下一个节点
}

// NewSkipList 创建跳表。seed 用于随机高度生成。
func NewSkipList(seed int64) *SkipList

// Put 插入或更新。key 是 InternalKey。
// 注意：因为 InternalKey 包含 sequence number，同一个 user_key 的不同版本是不同的 key，
// 所以 Put 总是插入新节点，不会覆盖。
func (sl *SkipList) Put(key internal.InternalKey, value []byte)

// NewIterator 创建跳表迭代器
func (sl *SkipList) NewIterator() *SkipListIterator

// Size 返回当前数据总大小（字节）
func (sl *SkipList) Size() int

// SkipListIterator 跳表迭代器
type SkipListIterator struct { ... }
func (it *SkipListIterator) First()
func (it *SkipListIterator) Last()
func (it *SkipListIterator) Next()
func (it *SkipListIterator) Prev()
func (it *SkipListIterator) Seek(key internal.InternalKey)
func (it *SkipListIterator) Valid() bool
func (it *SkipListIterator) Key() internal.InternalKey
func (it *SkipListIterator) Value() []byte
```

**跳表高度随机算法**：
```go
func (sl *SkipList) randomHeight() int {
    h := 1
    for h < MaxHeight && sl.rng.Float64() < 0.25 {
        h++
    }
    return h
}
```

### 5.2 MemTable 封装

```go
package memtable

// MemTable 封装 SkipList，提供 user-facing API
type MemTable struct {
    sl  *SkipList
    seq uint64 // 当前 sequence number（由外部 DB 管理，这里只存引用）
}

// NewMemTable 创建 MemTable
func NewMemTable() *MemTable

// Put 写入 key-value（调用方负责传入正确的 InternalKey）
func (m *MemTable) Put(ikey internal.InternalKey, value []byte)

// Get 查找 user_key，返回最新版本的 value
// 如果最新版本是 Delete，返回 (nil, ErrNotFound)
// 如果找不到，返回 (nil, ErrNotFound)
func (m *MemTable) Get(userKey []byte) ([]byte, error)

// NewIterator 创建迭代器
func (m *MemTable) NewIterator() *SkipListIterator

// ApproximateSize 返回大约的内存使用量（字节）
func (m *MemTable) ApproximateSize() int
```

---

## 6. WAL（Write-Ahead Log）

### 6.1 WAL 记录格式

```
┌──────────────────┬───────────┬──────────────────┬─────────┬───────────────────┬──────────┐
│ length (4 bytes) │ type (1)  │ key_len (varint) │   key   │ val_len (varint)  │  value   │
│  uint32 little-  │ Put=1     │                  │         │ (Delete时=0,      │(Delete时 │
│  endian, 整条    │ Delete=2  │                  │         │  无value字段)      │ 无此字段)│
│  记录的剩余长度  │           │                  │         │                   │          │
└──────────────────┴───────────┴──────────────────┴─────────┴───────────────────┴──────────┘
```

**length 字段**：从 type 开始到 record 结束的字节数（不包含 length 自身的 4 字节）。

**Delete 记录**：没有 val_len 和 value 字段。length = 1 + varint_size(key_len) + key_len。

### 6.2 Go 接口

```go
package wal

// Writer WAL 写入器
type Writer struct {
    file *os.File
}

// NewWriter 创建或追加 WAL 文件
func NewWriter(path string) (*Writer, error)

// Append 追加一条记录
func (w *Writer) Append(vt internal.ValueType, key, value []byte) error

// Sync 强制刷盘（fsync）
func (w *Writer) Sync() error

// Close 关闭文件
func (w *Writer) Close() error

// Reader WAL 读取器（用于恢复）
type Reader struct {
    file *os.File
}

// NewReader 打开 WAL 文件用于读取
func NewReader(path string) (*Reader, error)

// ReadAll 读取所有记录，返回 (type, key, value) 列表
// 遇到损坏的记录时停止（truncated write = 崩溃，忽略不完整的最后一条）
func (r *Reader) ReadAll() ([]Record, error)

// Record 一条 WAL 记录
type Record struct {
    Type  internal.ValueType
    Key   []byte
    Value []byte // Delete 时为 nil
}

// Close 关闭文件
func (r *Reader) Close() error
```

### 6.3 WAL 生命周期

- 每个 MemTable 对应一个 WAL 文件
- 文件命名：`{sequence_number}.wal`（sequence_number 是创建时的全局序号）
- MemTable flush 到 SSTable 后，对应的 WAL 文件被删除
- 恢复时：读取所有未删除的 WAL 文件，按文件名排序，重放到 MemTable

---

## 7. SSTable

### 7.1 文件整体格式

```
┌─────────────────────────┐
│      Data Block 0       │
├─────────────────────────┤
│      Data Block 1       │
├─────────────────────────┤
│         ...             │
├─────────────────────────┤
│      Data Block N       │
├─────────────────────────┤
│      Index Block        │
├─────────────────────────┤
│    Footer (48 bytes)    │
└─────────────────────────┘
```

### 7.2 Data Block 格式

每个 Data Block 包含多个有序的 key-value entry：

```
┌──────────────────────────────────────────────────────┐
│ Entry 0: key_len(varint) | val_len(varint) | key | value │
├──────────────────────────────────────────────────────┤
│ Entry 1: key_len(varint) | val_len(varint) | key | value │
├──────────────────────────────────────────────────────┤
│ ...                                                  │
├──────────────────────────────────────────────────────┤
│ Entry M: key_len(varint) | val_len(varint) | key | value │
├──────────────────────────────────────────────────────┤
│ num_entries (4 bytes, uint32 little-endian)           │
└──────────────────────────────────────────────────────┘
```

- **key** 是完整的 InternalKey（user_key + sequence + type）
- **value** 是原始 value（Delete 类型时 value 为空，val_len=0）
- **num_entries**：block 末尾 4 字节，记录 entry 数量（用于读取时解析）
- **Block 大小目标**：4KB（默认）。当累积大小 >= BlockSize 时，结束当前 block，开始新 block。最后一个 block 可以小于目标大小。

### 7.3 Index Block 格式

Index Block 记录每个 Data Block 的位置和最大 key：

```
┌────────────────────────────────────────────────────────────────────┐
│ Index Entry 0: key_len(varint) | key(该block最大key) |              │
│                offset(8 bytes, uint64 LE) | size(8 bytes, uint64 LE)│
├────────────────────────────────────────────────────────────────────┤
│ Index Entry 1: ...                                                 │
├────────────────────────────────────────────────────────────────────┤
│ ...                                                                │
├────────────────────────────────────────────────────────────────────┤
│ num_entries (4 bytes, uint32 little-endian)                        │
└────────────────────────────────────────────────────────────────────┘
```

- **key**：该 Data Block 中最大的 InternalKey（最后一个 entry 的 key）
- **offset**：该 Data Block 在文件中的起始偏移（uint64 little-endian）
- **size**：该 Data Block 的字节大小（uint64 little-endian）

### 7.4 Footer 格式（固定 48 字节）

```
┌───────────────────────────────────────────────────────┐
│ Index Block Offset  (8 bytes, uint64 little-endian)   │
├───────────────────────────────────────────────────────┤
│ Index Block Size    (8 bytes, uint64 little-endian)   │
├───────────────────────────────────────────────────────┤
│ Magic Number        (8 bytes) = 0x1234567890ABCDEF    │
├───────────────────────────────────────────────────────┤
│ Padding             (24 bytes, all zeros)             │
└───────────────────────────────────────────────────────┘
```

### 7.5 SSTable 文件命名

- 格式：`{file_number}.sst`
- file_number 是全局递增的 uint64
- 例如：`000001.sst`、`000002.sst`

### 7.6 Go 接口

```go
package sstable

// Writer 创建 SSTable 文件
type Writer struct {
    file      *os.File
    blockSize int
    // 内部状态...
}

// NewWriter 创建 SSTable Writer
func NewWriter(path string, blockSize int) (*Writer, error)

// Add 添加一个 key-value entry（必须按 InternalKey 排序顺序调用）
func (w *Writer) Add(key internal.InternalKey, value []byte) error

// Finish 完成写入（写入 Index Block 和 Footer），关闭文件
// 返回 SSTable 的元信息（最小key、最大key、文件大小）
func (w *Writer) Finish() (*TableMeta, error)

// Abort 放弃写入，删除未完成的文件
func (w *Writer) Abort() error

// TableMeta SSTable 元信息
type TableMeta struct {
    FileNum    uint64
    FilePath   string
    FileSize   int64
    SmallestKey internal.InternalKey // 文件中最小的 key
    LargestKey  internal.InternalKey // 文件中最大的 key
}

// Reader 读取 SSTable 文件
type Reader struct {
    file      *os.File
    fileSize  int64
    indexBlock []indexEntry
}

// OpenReader 打开 SSTable 文件，读取 Footer 和 Index Block
func OpenReader(path string) (*Reader, error)

// Get 在 SSTable 中查找 user_key（返回最新版本）
// 利用 Index Block 定位到正确的 Data Block，然后顺序扫描
func (r *Reader) Get(userKey []byte) ([]byte, error)

// NewIterator 创建 SSTable 迭代器（遍历所有 InternalKey entry）
func (r *Reader) NewIterator() *TableIterator

// Close 关闭文件
func (r *Reader) Close() error

// TableIterator SSTable 迭代器
type TableIterator struct { ... }
func (it *TableIterator) First()
func (it *TableIterator) Last()
func (it *TableIterator) Next()
func (it *TableIterator) Prev()
func (it *TableIterator) Seek(key internal.InternalKey)
func (it *TableIterator) Valid() bool
func (it *TableIterator) Key() internal.InternalKey
func (it *TableIterator) Value() []byte
```

---

## 8. Manifest

### 8.1 职责

记录数据库的当前状态：
- 所有 SSTable 文件的列表和层级归属
- 每个文件的元信息（file number, size, key range）
- 当前的 sequence number
- 下一个 file number

### 8.2 Manifest 文件格式

使用简单的 JSON 格式（简化实现，不追求性能）：

```json
{
  "next_file_number": 7,
  "sequence": 1024,
  "levels": {
    "0": [
      {"file_num": 3, "file_path": "000003.sst", "file_size": 4096, "smallest_key": "...", "largest_key": "..."},
      {"file_num": 5, "file_path": "000005.sst", "file_size": 8192, "smallest_key": "...", "largest_key": "..."}
    ],
    "1": [
      {"file_num": 1, "file_path": "000001.sst", "file_size": 16384, "smallest_key": "...", "largest_key": "..."}
    ]
  },
  "wal_file_number": 6
}
```

- **smallest_key / largest_key**：Base64 编码的 InternalKey
- Manifest 文件名固定为 `MANIFEST`
- 每次变更时，原子写入（写临时文件 → rename）

### 8.3 Go 接口

```go
package manifest

// Manifest 数据库元信息
type Manifest struct {
    NextFileNumber uint64                     `json:"next_file_number"`
    Sequence       uint64                     `json:"sequence"`
    Levels         map[int][]*sstable.TableMeta `json:"levels"`
    WALFileNumber  uint64                     `json:"wal_file_number"`
}

// Load 从文件加载 Manifest
func Load(dbPath string) (*Manifest, error)

// Save 原子保存 Manifest（写临时文件 → rename）
func (m *Manifest) Save(dbPath string) error

// NewFileNumber 分配一个新的 file number
func (m *Manifest) NewFileNumber() uint64

// AddTable 添加一个 SSTable 到指定 level
func (m *Manifest) AddTable(level int, meta *sstable.TableMeta)

// RemoveTable 从指定 level 移除一个 SSTable
func (m *Manifest) RemoveTable(level int, fileNum uint64)

// GetTablesForLevel 获取指定 level 的所有 SSTable
func (m *Manifest) GetTablesForLevel(level int) []*sstable.TableMeta
```

---

## 9. Compaction

### 9.1 触发条件

- **Level 0 → Level 1**：Level 0 的 SSTable 文件数 >= `Level0CompactionTrigger`（默认 4）
- **Level N → Level N+1**（N >= 1）：Level N 的总文件大小 > `Level1MaxSize * 10^(N-1)`

### 9.2 Compaction 过程

#### Level 0 → Level 1 Compaction

1. 选择 Level 0 中所有文件（Level 0 的文件之间可能有 key 重叠）
2. 在 Level 1 中找到所有与 Level 0 文件 key range 重叠的文件
3. 合并排序所有选中的文件（使用合并迭代器）
4. 输出新的 Level 1 SSTable 文件（每个文件大小目标：2MB）
5. 更新 Manifest：删除旧文件，添加新文件
6. 删除旧的 SSTable 文件

#### Level N → Level N+1 Compaction (N >= 1)

1. 选择 Level N 中最老的一个文件（file_num 最小的）
2. 在 Level N+1 中找到所有与选中文件 key range 重叠的文件
3. 合并排序
4. 输出新的 Level N+1 SSTable 文件
5. 更新 Manifest，删除旧文件

### 9.3 合并排序中的去重

- 同一个 user_key 的多个版本，只保留 sequence number 最大的
- 如果最新版本是 Delete（tombstone），在非最底层时仍然保留（因为下层可能有旧版本）
- 在最底层（Level N-1）时，Delete 标记可以丢弃

### 9.4 Go 接口

```go
package compaction

// Compactor 执行 compaction
type Compactor struct {
    dbPath    string
    manifest  *manifest.Manifest
    opts      *goleveldb.Options
}

// NewCompactor 创建 Compactor
func NewCompactor(dbPath string, m *manifest.Manifest, opts *goleveldb.Options) *Compactor

// MaybeCompact 检查是否需要 compaction，如果需要则执行
// 返回是否执行了 compaction
func (c *Compactor) MaybeCompact() (bool, error)

// compactLevel0 执行 Level 0 → Level 1 compaction
func (c *Compactor) compactLevel0() error

// compactLevelN 执行 Level N → Level N+1 compaction
func (c *Compactor) compactLevelN(level int) error
```

---

## 10. Iterator（合并迭代器）

### 10.1 职责

将多个有序数据源合并为一个有序的迭代器，同时处理 key 去重和 deletion marker。

### 10.2 数据源

DB.NewIterator() 需要合并以下数据源：
1. MemTable 的迭代器
2. Immutable MemTable 的迭代器（如果存在）
3. 所有 SSTable 的迭代器（按 level 从低到高）

### 10.3 合并策略

- 使用最小堆（min-heap）合并多个有序迭代器
- 对于同一个 user_key，只返回 sequence number 最大的版本
- 如果最新版本是 Delete，跳过该 key（不返回给用户）
- 迭代器只返回 user_key 和 value（不暴露 InternalKey）

### 10.4 Go 接口

```go
package iterator

// InternalIterator 内部迭代器接口（MemTable 和 SSTable 的迭代器都实现此接口）
type InternalIterator interface {
    First()
    Last()
    Next()
    Prev()
    Seek(key internal.InternalKey)
    Valid() bool
    Key() internal.InternalKey
    Value() []byte
}

// MergingIterator 合并多个 InternalIterator
type MergingIterator struct {
    iters []InternalIterator
    // 内部使用 heap...
}

// NewMergingIterator 创建合并迭代器
func NewMergingIterator(iters []InternalIterator) *MergingIterator

// MergingIterator 实现 InternalIterator 接口
func (m *MergingIterator) First()
func (m *MergingIterator) Next()
// ... 等等

// DBIterator 面向用户的迭代器（包装 MergingIterator，处理去重和 deletion）
type DBIterator struct {
    inner *MergingIterator
}

// NewDBIterator 创建用户迭代器
func NewDBIterator(inner *MergingIterator) *DBIterator

// DBIterator 实现 goleveldb.Iterator 接口
func (d *DBIterator) First()
func (d *DBIterator) Last()
func (d *DBIterator) Next()
func (d *DBIterator) Prev()
func (d *DBIterator) Seek(key []byte)  // 注意：参数是 user_key，内部转换
func (d *DBIterator) Valid() bool
func (d *DBIterator) Key() []byte      // 返回 user_key
func (d *DBIterator) Value() []byte
```

---

## 11. DB 实现

### 11.1 内部结构

```go
// dbImpl 是 DB 接口的实现
type dbImpl struct {
    path     string
    opts     *Options
    manifest *manifest.Manifest

    // MemTable
    mem    *memtable.MemTable  // 当前活跃的 MemTable
    imm    *memtable.MemTable  // Immutable MemTable（正在 flush 或等待 flush）

    // WAL
    wal    *wal.Writer

    // Sequence
    seq    uint64  // 全局递增的 sequence number

    // SSTable readers cache（简化：不做 LRU，打开后一直持有）
    tables map[uint64]*sstable.Reader  // file_num → Reader

    // 状态
    closed bool
}
```

### 11.2 Open 流程

```
1. 创建目录（如果不存在）
2. 加载 Manifest（如果存在）
   - 不存在：创建新 Manifest
3. 恢复 WAL（如果存在）
   - 读取 WAL 记录，重放到新 MemTable
   - 恢复 sequence number
4. 打开所有 SSTable Reader
5. 创建新 WAL Writer
6. 返回 DB 实例
```

### 11.3 Put 流程

```
1. 检查 key 不为空
2. seq++
3. 构造 InternalKey(key, seq, TypePut)
4. 写入 WAL
5. 写入 MemTable
6. 检查 MemTable 大小，如果 >= MemTableSize：
   a. 将当前 MemTable 设为 Immutable
   b. Flush Immutable MemTable 到 SSTable（Level 0）
   c. 删除旧 WAL 文件
   d. 创建新 MemTable 和新 WAL
   e. 更新 Manifest
   f. 检查是否需要 Compaction
```

### 11.4 Get 流程

```
1. 在 MemTable 中查找 → 找到则返回
2. 在 Immutable MemTable 中查找（如果存在）→ 找到则返回
3. 在 Level 0 的 SSTable 中查找（按 file_num 从大到小，即从新到旧）
   - Level 0 的文件之间可能有 key 重叠，所以需要检查所有文件
4. 在 Level 1 ~ Level N 的 SSTable 中查找
   - Level 1+ 的文件之间没有 key 重叠，可以用二分查找定位文件
5. 都找不到 → 返回 ErrNotFound
```

### 11.5 Delete 流程

与 Put 相同，只是 ValueType 为 TypeDelete，value 为 nil。

### 11.6 Close 流程

```
1. 如果 Immutable MemTable 存在，flush 到 SSTable
2. 如果 MemTable 不为空，flush 到 SSTable
3. 更新 Manifest
4. 关闭 WAL
5. 关闭所有 SSTable Reader
6. 设置 closed = true
```

---

## 12. 项目结构

```
goleveldb/
├── go.mod                  # module goleveldb
├── db.go                   # DB/Iterator 接口、Options、错误定义、Open 函数
├── db_impl.go              # dbImpl 结构体和所有方法实现
├── db_test.go              # DB 集成测试
├── memtable/
│   ├── skiplist.go         # SkipList 实现
│   ├── skiplist_test.go    # SkipList 单元测试
│   ├── memtable.go         # MemTable 封装
│   └── memtable_test.go    # MemTable 单元测试
├── wal/
│   ├── wal.go              # WAL Writer + Reader
│   └── wal_test.go         # WAL 单元测试
├── sstable/
│   ├── format.go           # 常量定义（Magic Number, Footer Size 等）
│   ├── writer.go           # SSTable Writer
│   ├── reader.go           # SSTable Reader + TableIterator
│   └── sstable_test.go     # SSTable 单元测试
├── manifest/
│   ├── manifest.go         # Manifest 读写
│   └── manifest_test.go    # Manifest 单元测试
├── compaction/
│   ├── compaction.go       # Compaction 逻辑
│   └── compaction_test.go  # Compaction 单元测试
├── iterator/
│   ├── iterator.go         # InternalIterator 接口、MergingIterator、DBIterator
│   └── iterator_test.go    # Iterator 单元测试
└── internal/
    ├── keys.go             # InternalKey 编解码、Compare
    ├── keys_test.go        # InternalKey 单元测试
    ├── coding.go           # Varint 编码工具（封装标准库）
    └── coding_test.go      # Coding 单元测试
```

---

## 13. 分阶段实现计划

每个 Phase 完成后必须：**编译通过 + 所有测试通过**。

### Phase 1：基础设施（internal/）

**文件**：
- `internal/coding.go` — Varint 编码（封装 `encoding/binary`）
- `internal/coding_test.go` — 测试 varint 编解码
- `internal/keys.go` — InternalKey 编解码、Compare 函数
- `internal/keys_test.go` — 测试 InternalKey 创建、提取、比较

**验收标准**：
- `MakeInternalKey` 创建正确的 InternalKey
- `UserKey()`、`Sequence()`、`Type()` 正确提取
- `Compare` 排序正确（user_key 升序，sequence 降序）
- Varint 编解码正确

### Phase 2：MemTable（memtable/）

**文件**：
- `memtable/skiplist.go` — SkipList 实现
- `memtable/skiplist_test.go` — SkipList 测试
- `memtable/memtable.go` — MemTable 封装
- `memtable/memtable_test.go` — MemTable 测试

**验收标准**：
- SkipList Put/Get/Iterator 正确
- SkipList 排序遵循 InternalKey 排序规则
- MemTable.Get 返回最新版本
- MemTable.Get 对 Delete 标记返回 ErrNotFound
- SkipList.Size() 正确追踪大小

### Phase 3：WAL（wal/）

**文件**：
- `wal/wal.go` — Writer + Reader
- `wal/wal_test.go` — 测试

**验收标准**：
- Writer.Append 写入正确格式
- Reader.ReadAll 正确解析所有记录
- 处理 truncated record（崩溃模拟）
- Put 和 Delete 两种记录类型都正确

### Phase 4：SSTable（sstable/）

**文件**：
- `sstable/format.go` — 常量
- `sstable/writer.go` — Writer
- `sstable/reader.go` — Reader + TableIterator
- `sstable/sstable_test.go` — 测试

**验收标准**：
- Writer 按 block 大小目标分割 Data Block
- Footer 格式正确（Magic Number 验证）
- Index Block 正确记录每个 Data Block 的位置和最大 key
- Reader.Get 通过 Index Block 定位正确的 Data Block
- TableIterator 正确遍历所有 entry（First/Next/Seek）
- 写入后读取数据一致

### Phase 5：DB 核心（db.go, db_impl.go）

**文件**：
- `db.go` — 接口定义、Open 函数
- `db_impl.go` — 实现
- `db_test.go` — 集成测试

**验收标准**：
- Open 创建新数据库
- Put/Get/Delete 基本功能正确
- MemTable 满时自动 flush 到 SSTable
- Close 后 Reopen 数据不丢失（WAL 恢复 + SSTable 读取）
- **关键集成测试**：Open → Put 1000 条 → Get 验证 → Close → Reopen → Get 验证

### Phase 6：Iterator（iterator/）

**文件**：
- `iterator/iterator.go` — MergingIterator + DBIterator
- `iterator/iterator_test.go` — 测试

**验收标准**：
- MergingIterator 正确合并多个有序源
- DBIterator 正确去重（同一 user_key 只返回最新版本）
- DBIterator 正确处理 Delete（跳过已删除的 key）
- First/Last/Next/Prev/Seek 都正确
- 集成到 DB.NewIterator()

### Phase 7：Compaction + Manifest（compaction/, manifest/）

**文件**：
- `manifest/manifest.go` — Manifest 读写
- `manifest/manifest_test.go` — 测试
- `compaction/compaction.go` — Compaction 逻辑
- `compaction/compaction_test.go` — 测试

**验收标准**：
- Manifest 正确序列化/反序列化
- Manifest 原子写入（写临时文件 → rename）
- Level 0 → Level 1 compaction 正确合并
- Compaction 后数据完整性不变
- **关键集成测试**：写入大量数据触发多次 compaction → 验证所有数据可读

---

## 14. 常量汇总

```go
// internal/keys.go
const (
    SequenceSize  = 8  // sequence number 占 8 字节
    TypeSize      = 1  // value type 占 1 字节
    InternalKeyOverhead = SequenceSize + TypeSize  // = 9
)

// sstable/format.go
const (
    FooterSize   = 48
    MagicNumber  = 0x1234567890ABCDEF
    DefaultBlockSize = 4 * 1024  // 4KB
)

// 默认配置
const (
    DefaultMemTableSize            = 4 * 1024 * 1024   // 4MB
    DefaultMaxLevels               = 7
    DefaultLevel0CompactionTrigger = 4
    DefaultLevel1MaxSize           = 10 * 1024 * 1024   // 10MB
    DefaultCompactionOutputSize    = 2 * 1024 * 1024    // 2MB per output SSTable
)
```

---

## 15. 依赖

**零外部依赖**。只使用 Go 标准库：
- `encoding/binary` — 字节序转换、varint
- `encoding/json` — Manifest 序列化
- `encoding/base64` — Manifest 中的 key 编码
- `os` — 文件操作
- `path/filepath` — 路径处理
- `sort` — 排序
- `container/heap` — 合并迭代器的最小堆
- `math/rand` — 跳表随机高度
- `bytes` — key 比较
- `errors` — 错误定义
- `fmt` — 格式化
- `io` — IO 操作
- `sync` — Mutex（保护 DB 状态，虽然是单写者，但 Get 和 Iterator 可能并发读）