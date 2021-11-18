/*
Package frame provides functionality for manipulating STOMP frames.
*/
package frame

// A Frame represents a STOMP frame. A frame consists of a command
// followed by a collection of header entries, and then an optional
// body.
type Frame struct {
	Command string
	Header  *Header
	Body    []byte
}

// New creates a new STOMP frame with the specified command and headers.
// The headers should contain an even number of entries. Each even index is
// the header name, and the odd indexes are the assocated header values.
func New(command string, headers ...string) *Frame {
	f := &Frame{Command: command, Header: &Header{}}
	for index := 0; index < len(headers); index += 2 {
		f.Header.Add(headers[index], headers[index+1])
	}
	return f
}

// Clone creates a deep copy of the frame and its header. The cloned
// frame shares the body with the original frame.
func (f *Frame) Clone() *Frame {
	fc := &Frame{Command: f.Command}
	if f.Header != nil {
		fc.Header = f.Header.Clone()
	}
	if f.Body != nil {
		fc.Body = make([]byte, len(f.Body))
		copy(fc.Body, f.Body)
	}
	return fc
}
