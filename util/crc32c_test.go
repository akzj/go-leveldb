package util

import (
	"testing"
)

func TestCRC32c(t *testing.T) {
	// Test that CRC32c produces consistent results
	testCases := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("abc"),
		[]byte("hello world"),
	}
	
	for _, data := range testCases {
		got := Value(data)
		// Verify idempotency
		got2 := Value(data)
		if got != got2 {
			t.Errorf("CRC32c non-idempotent for %q", string(data))
		}
		// Verify Extend equals Value
		ext := Extend(0, data)
		if got != ext {
			t.Errorf("Extend(0, data) != Value(data) for %q", string(data))
		}
	}
}

func TestCRC32cExtend(t *testing.T) {
	data1 := []byte("hello")
	data2 := []byte(" world")
	
	crc1 := Value(data1)
	crc2 := Extend(crc1, data2)
	crcCombined := Value(append(data1, data2...))
	
	if crc2 != crcCombined {
		t.Error("Extend should produce same result as single Value")
	}
}

func TestCRC32cMask(t *testing.T) {
	crc := Value([]byte("test"))
	
	masked := Mask(crc)
	unmasked := Unmask(masked)
	
	if unmasked != crc {
		t.Error("Unmask(Mask(crc)) should equal crc")
	}
	
	// Verify mask/unmask are inverses
	if masked == crc && crc != 0 {
		t.Error("Mask should modify the CRC")
	}
}
