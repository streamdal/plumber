package frame

import (
	"strings"
	"unsafe"
)

var (
	replacerForEncodeValue = strings.NewReplacer(
		"\\", "\\\\",
		"\r", "\\r",
		"\n", "\\n",
		":", "\\c",
	)
	replacerForUnencodeValue = strings.NewReplacer(
		"\\r", "\r",
		"\\n", "\n",
		"\\c", ":",
		"\\\\", "\\",
	)
)

// Reduce one allocation on copying bytes to string
func bytesToString(b []byte) string {
	/* #nosec G103 */
	return *(*string)(unsafe.Pointer(&b))
}

// Unencodes a header value using STOMP value encoding
// TODO: return error if invalid sequences found (eg "\t")
func unencodeValue(b []byte) (string, error) {
	s := replacerForUnencodeValue.Replace(bytesToString(b))
	return s, nil
}
