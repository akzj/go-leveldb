// db_test_contract.go - Phase 3 E2E Test Contract
// This contract defines the end-to-end tests for DB Open/Put/Get, WriteBatch, and Iterator.
// All tests must compile and pass before Phase 3 is considered complete.
//
// Contract Invariants:
// 1. Tests use util.MakeSlice([]byte("...")) for all key/value wrapping
// 2. All API calls return Status that must be checked with status.OK()
// 3. Temp DB paths use /tmp/testdb* to avoid workspace pollution
// 4. DB must be closed with d.Close() before cleanup with os.RemoveAll()
// 5. Iterator must be released with iter.Release() when done
//
// Why not test in /test/ directory? The acceptance criteria explicitly requires
// tests in db/ directory with *_test.go naming convention.
//
// Why require explicit status.OK() checks? Debugging database issues requires
// knowing exactly which operation failed.
//
// Implementation note: This contract has been fulfilled by db_test.go
// which contains the actual test implementations.
package db
