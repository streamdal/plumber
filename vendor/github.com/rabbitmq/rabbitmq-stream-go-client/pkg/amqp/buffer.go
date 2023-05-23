//MIT License
//
//Copyright (C) 2017 Kale Blankenship
//Portions Copyright (C) Microsoft Corporation
//
//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:
//
//The above copyright notice and this permission notice shall be included in all
//copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//SOFTWARE

package amqp

import (
	"encoding/binary"
	"io"
)

// buffer is similar to bytes.Buffer but specialized for this package
type buffer struct {
	b []byte
	i int
}

func (b *buffer) next(n int64) ([]byte, bool) {
	if b.readCheck(n) {
		buf := b.b[b.i:len(b.b)]
		b.i = len(b.b)
		return buf, false
	}

	buf := b.b[b.i : b.i+int(n)]
	b.i += int(n)
	return buf, true
}

func (b *buffer) skip(n int) {
	b.i += n
}

func (b *buffer) readCheck(n int64) bool {
	return int64(b.i)+n > int64(len(b.b))
}

func (b *buffer) readByte() (byte, error) {
	if b.readCheck(1) {
		return 0, io.EOF
	}

	byte_ := b.b[b.i]
	b.i++
	return byte_, nil
}

func (b *buffer) readType() (amqpType, error) {
	n, err := b.readByte()
	return amqpType(n), err
}

func (b *buffer) peekType() (amqpType, error) {
	if b.readCheck(1) {
		return 0, io.EOF
	}

	return amqpType(b.b[b.i]), nil
}

func (b *buffer) readUint16() (uint16, error) {
	if b.readCheck(2) {
		return 0, io.EOF
	}

	n := binary.BigEndian.Uint16(b.b[b.i:])
	b.i += 2
	return n, nil
}

func (b *buffer) readUint32() (uint32, error) {
	if b.readCheck(4) {
		return 0, io.EOF
	}

	n := binary.BigEndian.Uint32(b.b[b.i:])
	b.i += 4
	return n, nil
}

func (b *buffer) readUint64() (uint64, error) {
	if b.readCheck(8) {
		return 0, io.EOF
	}

	n := binary.BigEndian.Uint64(b.b[b.i : b.i+8])
	b.i += 8
	return n, nil
}

func (b *buffer) write(p []byte) {
	b.b = append(b.b, p...)
}

func (b *buffer) writeByte(byte_ byte) {
	b.b = append(b.b, byte_)
}

func (b *buffer) writeString(s string) {
	b.b = append(b.b, s...)
}

func (b *buffer) len() int {
	return len(b.b) - b.i
}

func (b *buffer) bytes() []byte {
	return b.b[b.i:]
}

func (b *buffer) writeUint16(n uint16) {
	b.b = append(b.b,
		byte(n>>8),
		byte(n),
	)
}

func (b *buffer) writeUint32(n uint32) {
	b.b = append(b.b,
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}

func (b *buffer) writeUint64(n uint64) {
	b.b = append(b.b,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}
