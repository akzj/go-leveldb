package util

import (
	"fmt"
)

// StatusCode represents the type of status.
type StatusCode int

const (
	StatusCodeOK              StatusCode = 0
	StatusCodeNotFound        StatusCode = 1
	StatusCodeCorruption     StatusCode = 2
	StatusCodeNotSupported    StatusCode = 3
	StatusCodeInvalidArgument StatusCode = 4
	StatusCodeIOError         StatusCode = 5
)

// Status encapsulates the result of an operation.
// It may indicate success, or it may indicate an error with an associated error message.
//
// Binary format (C++ LevelDB v1.23 compatible):
//   - If OK: state is nil
//   - Otherwise: state[0:4] = message length (LE uint32),
//                state[4] = code (uint8),
//                state[5:] = message bytes
//
// Invariant: msgLen stored in bytes [0,1,2,3] as little-endian uint32.
//            Code stored at byte [4].
//            Message starts at byte [5].
type Status struct {
	state []byte // nil for OK, otherwise: [len:4][code:1][msg]
}

// Create a success status.
func NewStatusOK() *Status {
	return &Status{state: nil}
}

// newStatus creates an error status with the given code and message.
// Why not expose publicly? Use factory functions below.
func newStatus(code StatusCode, msg string) *Status {
	if code == StatusCodeOK {
		return NewStatusOK()
	}
	msgLen := len(msg)
	// Binary format: [4 bytes len LE][1 byte code][msg bytes]
	state := make([]byte, 5+msgLen)
	// FIXED: Use 4-byte little-endian encoding (C++ compatible)
	state[0] = byte(msgLen)
	state[1] = byte(msgLen >> 8)
	state[2] = byte(msgLen >> 16)
	state[3] = byte(msgLen >> 24)
	state[4] = byte(code)
	copy(state[5:], msg)
	return &Status{state: state}
}

// Code returns the status code.
func (s *Status) Code() StatusCode {
	if s.state == nil {
		return StatusCodeOK
	}
	return StatusCode(s.state[4])
}

// Message returns the error message.
func (s *Status) Message() string {
	if s.state == nil {
		return ""
	}
	// Decode 4-byte little-endian length
	msgLen := int(s.state[0]) | int(s.state[1])<<8 | int(s.state[2])<<16 | int(s.state[3])<<24
	if msgLen == 0 {
		return ""
	}
	return string(s.state[5 : 5+msgLen])
}

// OK returns true iff the status indicates success.
func (s *Status) OK() bool {
	return s.state == nil
}

// IsNotFound returns true iff the status indicates a NotFound error.
func (s *Status) IsNotFound() bool {
	return s.Code() == StatusCodeNotFound
}

// IsCorruption returns true iff the status indicates a Corruption error.
func (s *Status) IsCorruption() bool {
	return s.Code() == StatusCodeCorruption
}

// IsIOError returns true iff the status indicates an IOError.
func (s *Status) IsIOError() bool {
	return s.Code() == StatusCodeIOError
}

// IsNotSupported returns true iff the status indicates a NotSupportedError.
func (s *Status) IsNotSupported() bool {
	return s.Code() == StatusCodeNotSupported
}

// IsInvalidArgument returns true iff the status indicates an InvalidArgument.
func (s *Status) IsInvalidArgument() bool {
	return s.Code() == StatusCodeInvalidArgument
}

// ToString returns a string representation of this status.
// Returns "OK" for success.
func (s *Status) ToString() string {
	if s.OK() {
		return "OK"
	}
	return fmt.Sprintf("%s: %s", s.Code().String(), s.Message())
}

// CodeString returns the string name of the code.
func (c StatusCode) String() string {
	switch c {
	case StatusCodeOK:
		return "OK"
	case StatusCodeNotFound:
		return "NotFound"
	case StatusCodeCorruption:
		return "Corruption"
	case StatusCodeNotSupported:
		return "NotSupported"
	case StatusCodeInvalidArgument:
		return "InvalidArgument"
	case StatusCodeIOError:
		return "IOError"
	default:
		return fmt.Sprintf("Unknown(%d)", c)
	}
}

// Factory functions for common status types.
func NotFound(msg string) *Status       { return newStatus(StatusCodeNotFound, msg) }
func Corruption(msg string) *Status     { return newStatus(StatusCodeCorruption, msg) }
func NotSupported(msg string) *Status    { return newStatus(StatusCodeNotSupported, msg) }
func InvalidArgument(msg string) *Status { return newStatus(StatusCodeInvalidArgument, msg) }
func IOError(msg string) *Status        { return newStatus(StatusCodeIOError, msg) }

// Pre-defined common status value (singleton for OK).
var OK = NewStatusOK()

// Clone creates a deep copy of the status.
func (s *Status) Clone() *Status {
	if s == nil || s.state == nil {
		return NewStatusOK()
	}
	clone := &Status{state: make([]byte, len(s.state))}
	copy(clone.state, s.state)
	return clone
}
