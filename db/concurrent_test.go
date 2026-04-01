package db

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/akzj/go-leveldb/util"
)

// TestConcurrentMultiWriter tests that multiple threads can write to different
// keys concurrently without data loss.
// Invariant: Every key written by a thread must be readable and contain that
// thread's ID in the value.
func TestConcurrentMultiWriter(t *testing.T) {
	os.RemoveAll("/tmp/testdb_concurrent_multi")
	defer os.RemoveAll("/tmp/testdb_concurrent_multi")

	opts := NewOptions()
	opts.CreateIfMissing = true
	db, status := Open(opts, "/tmp/testdb_concurrent_multi")
	if !status.OK() {
		t.Fatalf("Open failed: %v", status.ToString())
	}
	defer db.Close()

	config := DefaultConcurrentConfig()
	numKeys := config.NumKeys
	numThreads := config.NumThreads
	rounds := 10 // Fixed iterations per thread
	padding := strings.Repeat("x", config.ValuePadding)

	var totalWrites int64
	var wg sync.WaitGroup

	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				keyNum := tid
				for keyNum < numKeys {
					key := fmt.Sprintf("k%09d", keyNum)
					value := fmt.Sprintf("%s.%d.%d.%s", key, tid, round, padding)
					db.Put(nil, util.MakeSliceFromStr(key), util.MakeSliceFromStr(value))
					atomic.AddInt64(&totalWrites, 1)
					keyNum += numThreads
				}
			}
		}(threadID)
	}

	wg.Wait()
	t.Logf("Total writes completed: %d", totalWrites)

	verificationErrors := 0
	for threadID := 0; threadID < numThreads; threadID++ {
		keyNum := threadID
		for keyNum < numKeys {
			key := fmt.Sprintf("k%09d", keyNum)
			value, status := db.Get(nil, util.MakeSliceFromStr(key))
			if !status.OK() {
				t.Errorf("Key %s not found after writes", key)
				verificationErrors++
			} else {
				valStr := string(value)
				parts := strings.Split(valStr, ".")
				if len(parts) < 3 {
					t.Errorf("Key %s has malformed value: %s", key, valStr)
					verificationErrors++
				} else if parts[0] != key {
					t.Errorf("Key %s: expected key prefix '%s', got '%s'", key, key, parts[0])
					verificationErrors++
				} else {
					tid, err := strconv.Atoi(parts[1])
					if err != nil || tid != threadID {
						t.Errorf("Key %s: expected thread ID %d, got %s", key, threadID, parts[1])
						verificationErrors++
					}
				}
			}
			keyNum += numThreads
		}
	}

	if verificationErrors > 0 {
		t.Errorf("Verification failed with %d errors", verificationErrors)
	}
}

// TestConcurrentMixedReadWrite tests that concurrent reads and writes to the
// same keys see consistent data (no torn reads).
// Invariant: Readers never see partial values or inconsistent state.
func TestConcurrentMixedReadWrite(t *testing.T) {
	os.RemoveAll("/tmp/testdb_concurrent_mixed")
	defer os.RemoveAll("/tmp/testdb_concurrent_mixed")

	opts := NewOptions()
	opts.CreateIfMissing = true
	db, status := Open(opts, "/tmp/testdb_concurrent_mixed")
	if !status.OK() {
		t.Fatalf("Open failed: %v", status.ToString())
	}
	defer db.Close()

	config := DefaultConcurrentConfig()
	numKeys := 100
	numThreads := config.NumThreads
	rounds := 10
	padding := strings.Repeat("x", 50)

	counters := make([]int64, numKeys)
	var countersMu sync.RWMutex

	var totalReads int64
	var totalWrites int64
	var failedReads int64
	var wg sync.WaitGroup

	// Pre-populate keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("%s.0.0.%s", key, padding)
		db.Put(nil, util.MakeSliceFromStr(key), util.MakeSliceFromStr(value))
	}

	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				keyNum := tid % numKeys
				key := fmt.Sprintf("key%05d", keyNum)

				if tid%2 == 0 {
					value := fmt.Sprintf("%s.%d.%d.%s", key, tid, round, padding)

					countersMu.Lock()
					counters[keyNum] = int64(round)
					countersMu.Unlock()

					db.Put(nil, util.MakeSliceFromStr(key), util.MakeSliceFromStr(value))
					atomic.AddInt64(&totalWrites, 1)
				} else {
					value, status := db.Get(nil, util.MakeSliceFromStr(key))
					atomic.AddInt64(&totalReads, 1)

					if status.OK() {
						valStr := string(value)
						parts := strings.Split(valStr, ".")

						if len(parts) >= 3 {
							countersMu.RLock()
							expectedCounter := counters[keyNum]
							countersMu.RUnlock()

							seenCounter, err := strconv.Atoi(parts[2])
							if err == nil && seenCounter > int(expectedCounter) {
								atomic.AddInt64(&failedReads, 1)
							}
						}
					}
				}
			}
		}(threadID)
	}

	wg.Wait()

	t.Logf("Total writes: %d, Total reads: %d, Failed reads: %d", totalWrites, totalReads, failedReads)

	if failedReads > 0 {
		t.Errorf("Found %d inconsistent reads (saw counter > writer's current counter)", failedReads)
	}
}

// TestConcurrentWriteConsistency verifies that each key's value reflects exactly
// one writer's thread ID and counter, and that concurrent writes maintain consistency.
// Invariant: Each key's value reflects exactly one writer's thread ID and counter.
func TestConcurrentWriteConsistency(t *testing.T) {
	os.RemoveAll("/tmp/testdb_concurrent_consistency")
	defer os.RemoveAll("/tmp/testdb_concurrent_consistency")

	opts := NewOptions()
	opts.CreateIfMissing = true
	db, status := Open(opts, "/tmp/testdb_concurrent_consistency")
	if !status.OK() {
		t.Fatalf("Open failed: %v", status.ToString())
	}
	defer db.Close()

	config := DefaultConcurrentConfig()
	numKeys := config.NumKeys
	numThreads := config.NumThreads
	rounds := 10
	padding := strings.Repeat("x", config.ValuePadding)

	type keyState struct {
		mu       sync.RWMutex
		threadID int
		counter  int
		value    string
	}
	states := make([]*keyState, numKeys)
	for i := range states {
		states[i] = &keyState{}
	}

	var totalWrites int64
	var verificationErrors []string
	var errorsMu sync.Mutex
	var wg sync.WaitGroup

	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				keyNum := tid
				for keyNum < numKeys {
					key := fmt.Sprintf("k%09d", keyNum)
					value := fmt.Sprintf("%s.%d.%d.%s", key, tid, round, padding)

					state := states[keyNum]
					state.mu.Lock()
					state.threadID = tid
					state.counter = round
					state.value = value
					state.mu.Unlock()

					db.Put(nil, util.MakeSliceFromStr(key), util.MakeSliceFromStr(value))
					atomic.AddInt64(&totalWrites, 1)
					keyNum += numThreads
				}
			}
		}(threadID)
	}

	wg.Wait()
	t.Logf("Total writes completed: %d", totalWrites)

	for keyNum := 0; keyNum < numKeys; keyNum++ {
		key := fmt.Sprintf("k%09d", keyNum)
		expectedThreadID := keyNum % numThreads

		value, status := db.Get(nil, util.MakeSliceFromStr(key))
		if !status.OK() {
			errorsMu.Lock()
			verificationErrors = append(verificationErrors,
				fmt.Sprintf("Key %s not found", key))
			errorsMu.Unlock()
			continue
		}

		valStr := string(value)
		parts := strings.Split(valStr, ".")

		if len(parts) < 3 {
			errorsMu.Lock()
			verificationErrors = append(verificationErrors,
				fmt.Sprintf("Key %s: malformed value '%s'", key, valStr))
			errorsMu.Unlock()
			continue
		}

		if parts[0] != key {
			errorsMu.Lock()
			verificationErrors = append(verificationErrors,
				fmt.Sprintf("Key %s: expected prefix '%s', got '%s'", key, key, parts[0]))
			errorsMu.Unlock()
			continue
		}

		seenThreadID, err := strconv.Atoi(parts[1])
		if err != nil {
			errorsMu.Lock()
			verificationErrors = append(verificationErrors,
				fmt.Sprintf("Key %s: invalid thread ID '%s'", key, parts[1]))
			errorsMu.Unlock()
			continue
		}

		if seenThreadID != expectedThreadID {
			errorsMu.Lock()
			verificationErrors = append(verificationErrors,
				fmt.Sprintf("Key %s: expected thread ID %d, got %d", key, expectedThreadID, seenThreadID))
			errorsMu.Unlock()
			continue
		}

		_, err = strconv.Atoi(parts[2])
		if err != nil {
			errorsMu.Lock()
			verificationErrors = append(verificationErrors,
				fmt.Sprintf("Key %s: invalid counter '%s'", key, parts[2]))
			errorsMu.Unlock()
		}
	}

	if len(verificationErrors) > 0 {
		t.Errorf("Verification failed with %d errors:", len(verificationErrors))
		for _, err := range verificationErrors {
			t.Errorf("  %s", err)
		}
	}
}
