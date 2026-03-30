package util

import (
	"testing"
)

func TestStatusOK(t *testing.T) {
	s := NewStatusOK()
	if !s.OK() {
		t.Error("expected OK status")
	}
	if !s.OK() {
		t.Error("OK() should return true")
	}
}

func TestStatusNotFound(t *testing.T) {
	s := NotFound("key not found")
	if s.OK() {
		t.Error("expected non-OK status")
	}
	if !s.IsNotFound() {
		t.Error("expected IsNotFound() to return true")
	}
	if s.Code() != StatusCodeNotFound {
		t.Errorf("expected NotFound code, got %d", s.Code())
	}
}

func TestStatusCorruption(t *testing.T) {
	s := Corruption("data corrupted")
	if !s.IsCorruption() {
		t.Error("expected IsCorruption() to return true")
	}
}

func TestStatusIOError(t *testing.T) {
	s := IOError("read failed")
	if !s.IsIOError() {
		t.Error("expected IsIOError() to return true")
	}
}

func TestStatusMessage(t *testing.T) {
	msg := "test error message"
	s := NotFound(msg)
	if s.Message() != msg {
		t.Errorf("expected message '%s', got '%s'", msg, s.Message())
	}
}

func TestStatusToString(t *testing.T) {
	s := NotFound("key")
	str := s.ToString()
	if str == "" || str == "OK" {
		t.Error("expected non-empty error string")
	}
	
	ok := NewStatusOK()
	if ok.ToString() != "OK" {
		t.Error("expected OK status to print 'OK'")
	}
}

func TestStatusClone(t *testing.T) {
	s := NotFound("original")
	c := s.Clone()
	if c.Message() != s.Message() {
		t.Error("clone should have same message")
	}
	if c.Code() != s.Code() {
		t.Error("clone should have same code")
	}
}

func TestStatusBinaryEncoding(t *testing.T) {
	// Test that status encoding uses 4-byte little-endian length
	// This is critical for C++ binary compatibility
	s := NotFound("test")
	
	// Verify the state structure: [4 bytes len][1 byte code][msg]
	if len(s.state) < 5 {
		t.Fatalf("state too short: %d bytes", len(s.state))
	}
	
	// Length should be 4 bytes (little-endian)
	msgLen := int(s.state[0]) | int(s.state[1])<<8 | int(s.state[2])<<16 | int(s.state[3])<<24
	if msgLen != 4 {
		t.Errorf("expected message length 4, got %d", msgLen)
	}
	
	// Code should be at byte 4
	if s.state[4] != byte(StatusCodeNotFound) {
		t.Error("code not at expected position")
	}
}
