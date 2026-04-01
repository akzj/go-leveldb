package db

// Concurrent Safety Test Contract
//
// Invariants to verify:
//   1. Concurrent writes to different keys succeed without data loss
//   2. Concurrent reads during writes see consistent data (no torn reads)
//   3. Each key's value reflects exactly one writer's thread ID and counter
//   4. Readers never see values with counters higher than the writer's current counter
//
// Why not put all tests in db_test.go?
//   - Tests are complex enough to warrant separate files
//   - Sibling test files (snapshot_test.go, write_batch_test.go) follow same pattern
//   - Avoids conflicts with existing test functions
//
// Test naming convention:
//   - All test functions MUST contain "Concurrent" in name (per acceptance criteria filter)
//   - Pattern: TestConcurrent<Scenario>
//
// Reference: C++ LevelDB db/db_test.cc MultiThreaded test pattern
//   - kNumThreads = 4
//   - kNumKeys = 1000
//   - Each key written by exactly one thread (thread ID encoded in value)
//   - Value format: "<key>.<thread_id>.<counter>"
//   - Readers verify counter consistency

// TestConfig holds concurrent test configuration.
// Invariant: NumThreads >= 1 && NumKeys >= 1 && TestSeconds >= 1
type TestConfig struct {
	NumKeys       int // Number of distinct keys to write (default: 1000)
	NumThreads    int // Number of concurrent writer/reader threads (default: 4)
	TestSeconds   int // Duration of test in seconds (default: 3)
	ValuePadding  int // Padding size to force compactions (default: 100)
}

// DefaultConcurrentConfig returns safe defaults for concurrent tests.
// Why defaults vs required params? Simplifies test callers, sane for CI.
func DefaultConcurrentConfig() TestConfig {
	return TestConfig{
		NumKeys:      1000,
		NumThreads:   4,
		TestSeconds:  3,
		ValuePadding: 100,
	}
}

// ConcurrentTestResult holds test outcome data for verification.
type ConcurrentTestResult struct {
	TotalWrites   int64  // Total successful write operations
	TotalReads    int64  // Total read operations performed
	FailedReads   int64  // Reads that saw inconsistent data
	VerificationErrors []string // List of consistency violations
}

// Invariant: FailedReads == 0 && len(VerificationErrors) == 0 for a passing test
