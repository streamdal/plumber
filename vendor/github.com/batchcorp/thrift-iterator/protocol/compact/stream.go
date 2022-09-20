package compact

import (
	"fmt"
	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/spi"
	"io"
	"math"
)

type Stream struct {
	spi.ValEncoderProvider
	writer           io.Writer
	buf              []byte
	err              error
	fieldIdStack     []protocol.FieldId
	lastFieldId      protocol.FieldId
	pendingBoolField protocol.FieldId
}

func NewStream(provider spi.ValEncoderProvider, writer io.Writer, buf []byte) *Stream {
	return &Stream{
		ValEncoderProvider: provider,
		writer:             writer,
		buf:                buf,
		pendingBoolField:   -1,
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
	stream.buf = append(stream.buf, protocol.COMPACT_PROTOCOL_ID)
	stream.buf = append(stream.buf, (protocol.COMPACT_VERSION&protocol.COMPACT_VERSION_MASK)|((byte(header.MessageType)<<5)&0x0E0))
	stream.writeVarInt32(int32(header.SeqId))
	stream.WriteString(header.MessageName)
}

func (stream *Stream) WriteListHeader(elemType protocol.TType, length int) {
	if length <= 14 {
		stream.WriteUint8(uint8(int32(length<<4) | int32(compactTypes[elemType])))
		return
	}
	stream.WriteUint8(0xf0 | uint8(compactTypes[elemType]))
	stream.writeVarInt32(int32(length))
}

func (stream *Stream) WriteStructHeader() {
	stream.fieldIdStack = append(stream.fieldIdStack, stream.lastFieldId)
	stream.lastFieldId = 0
}

func (stream *Stream) WriteStructField(fieldType protocol.TType, fieldId protocol.FieldId) {
	if fieldType == protocol.TypeBool {
		stream.pendingBoolField = fieldId
		return
	}
	compactType := uint8(compactTypes[fieldType])
	// check if we can use delta encoding for the field id
	if fieldId > stream.lastFieldId && fieldId-stream.lastFieldId <= 15 {
		stream.WriteUint8(uint8((fieldId-stream.lastFieldId)<<4) | compactType)
	} else {
		stream.WriteUint8(compactType)
		stream.WriteInt16(int16(fieldId))
	}
	stream.lastFieldId = fieldId
}

func (stream *Stream) WriteStructFieldStop() {
	stream.buf = append(stream.buf, byte(TypeStop))
	stream.lastFieldId = stream.fieldIdStack[len(stream.fieldIdStack)-1]
	stream.fieldIdStack = stream.fieldIdStack[:len(stream.fieldIdStack)-1]
	stream.pendingBoolField = -1
}

func (stream *Stream) WriteMapHeader(keyType protocol.TType, elemType protocol.TType, length int) {
	if length == 0 {
		stream.WriteUint8(0)
		return
	}
	stream.writeVarInt32(int32(length))
	stream.WriteUint8(uint8(compactTypes[keyType]<<4 | TCompactType(compactTypes[elemType])))
}

func (stream *Stream) WriteBool(val bool) {
	if stream.pendingBoolField == -1 {
		if val {
			stream.WriteUint8(1)
		} else {
			stream.WriteUint8(0)
		}
		return
	}
	var compactType TCompactType
	if val {
		compactType = TypeBooleanTrue
	} else {
		compactType = TypeBooleanFalse
	}
	fieldId := stream.pendingBoolField
	// check if we can use delta encoding for the field id
	if fieldId > stream.lastFieldId && fieldId-stream.lastFieldId <= 15 {
		stream.WriteUint8(uint8((fieldId-stream.lastFieldId)<<4) | uint8(compactType))
	} else {
		stream.WriteUint8(uint8(compactType))
		stream.WriteInt16(int16(fieldId))
	}
	stream.lastFieldId = fieldId
	stream.pendingBoolField = -1
}

func (stream *Stream) WriteInt8(val int8) {
	stream.WriteUint8(uint8(val))
}

func (stream *Stream) WriteUint8(val uint8) {
	stream.buf = append(stream.buf, byte(val))
}

func (stream *Stream) WriteInt16(val int16) {
	stream.WriteInt32(int32(val))
}

func (stream *Stream) WriteUint16(val uint16) {
	stream.WriteInt32(int32(val))
}

func (stream *Stream) WriteInt32(val int32) {
	stream.writeVarInt32((val << 1) ^ (val >> 31))
}

func (stream *Stream) WriteUint32(val uint32) {
	stream.WriteInt32(int32(val))
}

// Write an i32 as a varint. Results in 1-5 bytes on the wire.
func (stream *Stream) writeVarInt32(n int32) {
	for {
		if (n & ^0x7F) == 0 {
			stream.buf = append(stream.buf, byte(n))
			break
		} else {
			stream.buf = append(stream.buf, byte((n&0x7F)|0x80))
			u := uint64(n)
			n = int32(u >> 7)
		}
	}
}

func (stream *Stream) WriteInt64(val int64) {
	stream.writeVarInt64((val << 1) ^ (val >> 63))
}

// Write an i64 as a varint. Results in 1-10 bytes on the wire.
func (stream *Stream) writeVarInt64(n int64) {
	for {
		if (n & ^0x7F) == 0 {
			stream.buf = append(stream.buf, byte(n))
			break
		} else {
			stream.buf = append(stream.buf, byte((n&0x7F)|0x80))
			u := uint64(n)
			n = int64(u >> 7)
		}
	}
}

func (stream *Stream) WriteUint64(val uint64) {
	stream.WriteInt64(int64(val))
}

func (stream *Stream) WriteInt(val int) {
	stream.WriteInt64(int64(val))
}

func (stream *Stream) WriteUint(val uint) {
	stream.WriteUint64(uint64(val))
}

func (stream *Stream) WriteFloat64(val float64) {
	bits := math.Float64bits(val)
	stream.buf = append(stream.buf,
		byte(bits),
		byte(bits>>8),
		byte(bits>>16),
		byte(bits>>24),
		byte(bits>>32),
		byte(bits>>40),
		byte(bits>>48),
		byte(bits>>56),
	)
}

func (stream *Stream) WriteBinary(val []byte) {
	stream.writeVarInt32(int32(len(val)))
	stream.buf = append(stream.buf, val...)
}

func (stream *Stream) WriteString(val string) {
	stream.writeVarInt32(int32(len(val)))
	stream.buf = append(stream.buf, val...)
}
