package binary

import (
	"fmt"
	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/spi"
	"io"
	"math"
)

type Stream struct {
	spi.ValEncoderProvider
	writer io.Writer
	buf    []byte
	err    error
}

func NewStream(provider spi.ValEncoderProvider, writer io.Writer, buf []byte) *Stream {
	return &Stream{
		ValEncoderProvider: provider,
		writer:             writer,
		buf:                buf,
	}
}

func (stream *Stream) Spawn() spi.Stream {
	return &Stream{
		ValEncoderProvider: stream.ValEncoderProvider,
	}
}

func (stream *Stream) Error() error {
	return stream.err
}

func (stream *Stream) ReportError(operation string, err string) {
	if stream.err == nil {
		stream.err = fmt.Errorf("%s: %s", operation, err)
	}
}

func (stream *Stream) Buffer() []byte {
	return stream.buf
}

func (stream *Stream) Reset(writer io.Writer) {
	stream.writer = writer
	stream.err = nil
	stream.buf = stream.buf[:0]
}

func (stream *Stream) Flush() {
	if stream.writer == nil {
		return
	}
	_, err := stream.writer.Write(stream.buf)
	if err != nil {
		stream.ReportError("Flush", err.Error())
		return
	}
	if f, ok := stream.writer.(protocol.Flusher); ok {
		if err = f.Flush(); err != nil {
			stream.ReportError("Flush", err.Error())
		}
	}
	stream.buf = stream.buf[:0]
}

func (stream *Stream) Write(buf []byte) error {
	stream.buf = append(stream.buf, buf...)
	stream.Flush()
	return stream.Error()
}

func (stream *Stream) WriteMessageHeader(header protocol.MessageHeader) {
	versionAndMessageType := uint32(protocol.BINARY_VERSION_1) | uint32(header.MessageType)
	stream.WriteUint32(versionAndMessageType)
	stream.WriteString(header.MessageName)
	stream.WriteInt32(int32(header.SeqId))
}

func (stream *Stream) WriteListHeader(elemType protocol.TType, length int) {
	stream.buf = append(stream.buf, byte(elemType),
		byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
}

func (stream *Stream) WriteStructHeader() {
}

func (stream *Stream) WriteStructField(fieldType protocol.TType, fieldId protocol.FieldId) {
	stream.buf = append(stream.buf, byte(fieldType), byte(fieldId>>8), byte(fieldId))
}

func (stream *Stream) WriteStructFieldStop() {
	stream.buf = append(stream.buf, byte(protocol.TypeStop))
}

func (stream *Stream) WriteMapHeader(keyType protocol.TType, elemType protocol.TType, length int) {
	stream.buf = append(stream.buf, byte(keyType), byte(elemType),
		byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
}

func (stream *Stream) WriteBool(val bool) {
	if val {
		stream.WriteUint8(1)
	} else {
		stream.WriteUint8(0)
	}
}

func (stream *Stream) WriteInt8(val int8) {
	stream.WriteUint8(uint8(val))
}

func (stream *Stream) WriteUint8(val uint8) {
	stream.buf = append(stream.buf, byte(val))
}

func (stream *Stream) WriteInt16(val int16) {
	stream.WriteUint16(uint16(val))
}

func (stream *Stream) WriteUint16(val uint16) {
	stream.buf = append(stream.buf, byte(val>>8), byte(val))
}

func (stream *Stream) WriteInt32(val int32) {
	stream.WriteUint32(uint32(val))
}

func (stream *Stream) WriteUint32(val uint32) {
	stream.buf = append(stream.buf, byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
}

func (stream *Stream) WriteInt64(val int64) {
	stream.WriteUint64(uint64(val))
}

func (stream *Stream) WriteUint64(val uint64) {
	stream.buf = append(stream.buf,
		byte(val>>56), byte(val>>48), byte(val>>40), byte(val>>32),
		byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
}

func (stream *Stream) WriteInt(val int) {
	stream.WriteInt64(int64(val))
}

func (stream *Stream) WriteUint(val uint) {
	stream.WriteUint64(uint64(val))
}

func (stream *Stream) WriteFloat64(val float64) {
	stream.WriteUint64(math.Float64bits(val))
}

func (stream *Stream) WriteBinary(val []byte) {
	stream.WriteUint32(uint32(len(val)))
	stream.buf = append(stream.buf, val...)
}

func (stream *Stream) WriteString(val string) {
	stream.WriteUint32(uint32(len(val)))
	stream.buf = append(stream.buf, val...)
}
