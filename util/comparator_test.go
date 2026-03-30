package util

import (
	"testing"
)

func TestBytewiseComparator(t *testing.T) {
	c := DefaultBytewiseComparator()
	
	// Test Compare
	if c.Compare(MakeSliceFromStr("a"), MakeSliceFromStr("b")) >= 0 {
		t.Error("expected a < b")
	}
	if c.Compare(MakeSliceFromStr("abc"), MakeSliceFromStr("abc")) != 0 {
		t.Error("expected abc == abc")
	}
	if c.Compare(MakeSliceFromStr("z"), MakeSliceFromStr("a")) <= 0 {
		t.Error("expected z > a")
	}
	
	// Test Name
	if c.Name() != "leveldb.BytewiseComparator" {
		t.Errorf("unexpected name: %s", c.Name())
	}
}

func TestBytewiseComparatorFindShortestSeparator(t *testing.T) {
	c := DefaultBytewiseComparator()
	
	start := MakeSliceFromStr("hello")
	limit := MakeSliceFromStr("hello world")
	
	// FindShortestSeparator should find a key between start and limit
	// Simple implementation just returns start for now
	c.FindShortestSeparator(start, limit)
}

func TestBytewiseComparatorFindShortestSuccessor(t *testing.T) {
	c := DefaultBytewiseComparator()
	
	key := MakeSliceFromStr("abc")
	c.FindShortestSuccessor(key)
	// Simple implementation may not modify key
}

func TestSliceCompareEdgeCases(t *testing.T) {
	empty := MakeSliceFromStr("")
	nonEmpty := MakeSliceFromStr("a")
	
	if empty.Compare(nonEmpty) >= 0 {
		t.Error("expected empty < 'a'")
	}
	if nonEmpty.Compare(empty) <= 0 {
		t.Error("expected 'a' > empty")
	}
	if empty.Compare(empty) != 0 {
		t.Error("expected empty == empty")
	}
}
