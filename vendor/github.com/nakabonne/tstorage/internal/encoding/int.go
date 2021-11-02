package encoding

import "encoding/binary"

// MarshalUint16 appends marshaled v to dst and returns the result.
func MarshalUint16(dst []byte, u uint16) []byte {
	return append(dst, byte(u>>8), byte(u))
}

// UnmarshalUint16 returns unmarshaled uint32 from src.
func UnmarshalUint16(src []byte) uint16 {
	// This is faster than the manual conversion.
	return binary.BigEndian.Uint16(src)
}
