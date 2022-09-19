package binary

import (
	"fmt"
	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/spi"
	"io"
	"math"
)

type Iterator struct {
	spi.ValDecoderProvider
	reader  io.Reader
	tmp     []byte
	preread []byte
	skipped []byte
	err     error
}

func NewIterator(provider spi.ValDecoderProvider, reader io.Reader, buf []byte) *Iterator {
	return &Iterator{
		ValDecoderProvider: provider,
		reader:             reader,
		tmp:                make([]byte, 8),
		preread:            buf,
	}
}

func (iter *Iterator) readByte() byte {
	tmp := iter.tmp[:1]
	if len(iter.preread) > 0 {
		tmp[0] = iter.preread[0]
		iter.preread = iter.preread[1:]
	} else {
		_, err := iter.reader.Read(tmp)
		if err != nil {
			iter.ReportError("read", err.Error())
			return 0
		}
	}
	if iter.skipped != nil {
		iter.skipped = append(iter.skipped, tmp[0])
	}
	return tmp[0]
}

func (iter *Iterator) readSmall(nBytes int) []byte {
	tmp := iter.tmp[:nBytes]
	wantBytes := nBytes
	if len(iter.preread) > 0 {
		if len(iter.preread) > nBytes {
			copy(tmp, iter.preread[:nBytes])
			iter.preread = iter.preread[nBytes:]
			wantBytes = 0
		} else {
			prelength := len(iter.preread)
			copy(tmp[:prelength], iter.preread)
			wantBytes -= prelength
			iter.preread = nil
		}
	}
	if wantBytes > 0 {
		_, err := io.ReadFull(iter.reader, tmp[nBytes-wantBytes:nBytes])
		if err != nil {
			for i := 0; i < len(tmp); i++ {
				tmp[i] = 0
			}
			iter.ReportError("read", err.Error())
			return tmp
		}
	}
	if iter.skipped != nil {
		iter.skipped = append(iter.skipped, tmp...)
	}
	return tmp
}

func (iter *Iterator) readLarge(nBytes int) []byte {
	// allocate new buffer if not enough
	if len(iter.tmp) < nBytes {
		iter.tmp = make([]byte, nBytes)
	}
	return iter.readSmall(nBytes)
}

func (iter *Iterator) Spawn() spi.Iterator {
	return NewIterator(iter.ValDecoderProvider, nil, nil)
}

func (iter *Iterator) Error() error {
	return iter.err
}

func (iter *Iterator) ReportError(operation string, err string) {
	if iter.err == nil {
		iter.err = fmt.Errorf("%s: %s", operation, err)
	}
}

func (iter *Iterator) Reset(reader io.Reader, buf []byte) {
	iter.reader = reader
	iter.preread = buf
	iter.err = nil
}

func (iter *Iterator) ReadMessageHeader() protocol.MessageHeader {
	versionAndMessageType := iter.ReadInt32()
	messageType := protocol.TMessageType(versionAndMessageType & 0x0ff)
	version := int64(int64(versionAndMessageType) & 0xffff0000)
	if version != protocol.BINARY_VERSION_1 {
		iter.ReportError("ReadMessageHeader", "unexpected version")
		return protocol.MessageHeader{}
	}
	messageName := iter.ReadString()
	seqId := protocol.SeqId(iter.ReadInt32())
	return protocol.MessageHeader{
		MessageName: messageName,
		MessageType: messageType,
		SeqId:       seqId,
	}
}

func (iter *Iterator) ReadStructHeader() {
	// noop
}

func (iter *Iterator) ReadStructField() (fieldType protocol.TType, fieldId protocol.FieldId) {
	firstByte := iter.readByte()
	fieldType = protocol.TType(firstByte)
	if fieldType == protocol.TypeStop {
		return protocol.TypeStop, 0
	}
	fieldId = protocol.FieldId(iter.ReadUint16())
	return fieldType, fieldId
}

func (iter *Iterator) ReadListHeader() (elemType protocol.TType, size int) {
	b := iter.readSmall(5)
	elemType = protocol.TType(b[0])
	size = int(uint32(b[4]) | uint32(b[3])<<8 | uint32(b[2])<<16 | uint32(b[1])<<24)
	return elemType, size
}

func (iter *Iterator) ReadMapHeader() (keyType protocol.TType, elemType protocol.TType, size int) {
	b := iter.readSmall(6)
	keyType = protocol.TType(b[0])
	elemType = protocol.TType(b[1])
	size = int(uint32(b[5]) | uint32(b[4])<<8 | uint32(b[3])<<16 | uint32(b[2])<<24)
	return keyType, elemType, size
}

func (iter *Iterator) ReadBool() bool {
	return iter.ReadUint8() == 1
}

func (iter *Iterator) ReadInt() int {
	return int(iter.ReadInt64())
}

func (iter *Iterator) ReadUint() uint {
	return uint(iter.ReadUint64())
}

func (iter *Iterator) ReadInt8() int8 {
	return int8(iter.ReadUint8())
}

func (iter *Iterator) ReadUint8() uint8 {
	return iter.readByte()
}

func (iter *Iterator) ReadInt16() int16 {
	return int16(iter.ReadUint16())
}

func (iter *Iterator) ReadUint16() uint16 {
	b := iter.readSmall(2)
	return uint16(b[1]) | uint16(b[0])<<8
}

func (iter *Iterator) ReadInt32() int32 {
	return int32(iter.ReadUint32())
}

func (iter *Iterator) ReadUint32() uint32 {
	b := iter.readSmall(4)
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}

func (iter *Iterator) ReadInt64() int64 {
	return int64(iter.ReadUint64())
}

func (iter *Iterator) ReadUint64() uint64 {
	b := iter.readSmall(8)
	return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
}

func (iter *Iterator) ReadFloat64() float64 {
	return math.Float64frombits(iter.ReadUint64())
}

func (iter *Iterator) ReadString() string {
	length := iter.ReadUint32()
	return string(iter.readLarge(int(length)))
}

func (iter *Iterator) ReadBinary() []byte {
	length := iter.ReadUint32()
	tmp := make([]byte, length)
	copy(tmp, iter.readLarge(int(length)))
	return tmp
}
