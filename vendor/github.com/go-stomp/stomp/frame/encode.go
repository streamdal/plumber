package frame

import (
	"bytes"
	"strings"
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

// Encodes a header value using STOMP value encoding
func encodeValue(s string) []byte {
	var buf bytes.Buffer
	buf.Grow(len(s))
	replacerForEncodeValue.WriteString(&buf, s)
	return buf.Bytes()
}

// Unencodes a header value using STOMP value encoding
// TODO: return error if invalid sequences found (eg "\t")
func unencodeValue(b []byte) (string, error) {
	s := replacerForUnencodeValue.Replace(string(b))
	return s, nil
}
