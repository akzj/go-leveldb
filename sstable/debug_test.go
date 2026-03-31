package sstable

import (
    "encoding/binary"
    "os"
    "testing"
    "github.com/akzj/go-leveldb/internal"
)

func TestDebugIterator(t *testing.T) {
    tmpDir := t.TempDir()
    path := tmpDir + "/debug.sst"
    
    w, _ := NewWriter(path, 1024)
    w.Add(internal.MakeInternalKey([]byte("apple"), 100, internal.TypePut), []byte("value1"))
    w.Add(internal.MakeInternalKey([]byte("banana"), 100, internal.TypePut), []byte("value2"))
    w.Add(internal.MakeInternalKey([]byte("cherry"), 100, internal.TypePut), []byte("value3"))
    w.Add(internal.MakeInternalKey([]byte("date"), 100, internal.TypePut), []byte("value4"))
    w.Finish()
    
    data, _ := os.ReadFile(path)
    
    // Get data block
    footer := data[len(data)-48:]
    indexOffset := binary.BigEndian.Uint64(footer[0:8])
    blockData := data[:indexOffset]
    
    t.Logf("Block data: %x", blockData)
    
    // Check bytes at key offsets
    t.Logf("Byte at 0: 0x%x (should be key_len varint for apple)", blockData[0])
    t.Logf("Byte at 22: 0x%x (should be key_len varint for banana)", blockData[22])
    t.Logf("Byte at 45: 0x%x (should be key_len varint for cherry)", blockData[45])
    
    // Decode varints
    keyLen0, n := binary.Uvarint(blockData[0:])
    t.Logf("At offset 0: key_len=%d, n=%d", keyLen0, n)
    keyLen22, n := binary.Uvarint(blockData[22:])
    t.Logf("At offset 22: key_len=%d, n=%d", keyLen22, n)
    keyLen45, n := binary.Uvarint(blockData[45:])
    t.Logf("At offset 45: key_len=%d, n=%d", keyLen45, n)
    
    // Decode keys
    offset := 0
    for i := 0; i < 4; i++ {
        keyLen, n := binary.Uvarint(blockData[offset:]); offset += n
        valLen, n := binary.Uvarint(blockData[offset:]); offset += n
        keyBytes := blockData[offset:offset+int(keyLen)]; offset += int(keyLen)
        offset += int(valLen)
        ikey := internal.MakeInternalKeyFromBytes(keyBytes)
        t.Logf("Entry %d: at offset %d, key=%s, keyLen=%d", i, offset-int(keyLen)-int(valLen)-n*2, ikey.UserKey(), keyLen)
    }
}
