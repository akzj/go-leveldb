package util

import (
	"testing"
)

func TestSliceBasic(t *testing.T) {
	s := MakeSliceFromStr("hello")
	if s.Size() != 5 {
		t.Errorf("expected size 5, got %d", s.Size())
	}
	if s.Empty() {
		t.Error("expected non-empty slice")
	}
	if string(s.Data()) != "hello" {
		t.Errorf("expected 'hello', got '%s'", string(s.Data()))
	}
}

func TestSliceCompare(t *testing.T) {
	a := MakeSliceFromStr("abc")
	b := MakeSliceFromStr("abd")
	c := MakeSliceFromStr("abc")
	
	if a.Compare(b) >= 0 {
		t.Error("expected abc < abd")
	}
	if a.Compare(c) != 0 {
		t.Error("expected abc == abc")
	}
	if b.Compare(a) <= 0 {
		t.Error("expected abd > abc")
	}
}

func TestSliceStartsWith(t *testing.T) {
	s := MakeSliceFromStr("hello world")
	prefix := MakeSliceFromStr("hello")
	
	if !s.StartsWith(prefix) {
		t.Error("expected 'hello world' to start with 'hello'")
	}
	
	notPrefix := MakeSliceFromStr("world")
	if s.StartsWith(notPrefix) {
		t.Error("expected 'hello world' to not start with 'world'")
	}
}

func TestSliceRemovePrefix(t *testing.T) {
	s := MakeSliceFromStr("hello")
	s.RemovePrefix(2)
	if string(s.Data()) != "llo" {
		t.Errorf("expected 'llo', got '%s'", string(s.Data()))
	}
}

func TestSliceClear(t *testing.T) {
	s := MakeSliceFromStr("hello")
	s.Clear()
	if !s.Empty() {
		t.Error("expected empty slice after Clear()")
	}
}
