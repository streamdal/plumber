package compact

import (
	"encoding/binary"
	"fmt"
	"github.com/thrift-iterator/go/protocol"
	"github.com/thrift-iterator/go/spi"
	"io"
	"math"
)

type Iterator struct {
	spi.ValDecoderProvider
	reader  io.Reader
	tmp     []byte
	preread []byte
	skipped []byte

	err              error
	fieldIdStack     []protocol.FieldId
	lastFieldId      protocol.FieldId
	pendingBoolField uint8
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

func (iter *Iterator) readVarInt32() int32 {
	return int32(iter.readVarInt64())
}

func (iter *Iterator) readVarInt64() int64 {
	shift := uint(0)
	result := int64(0)
	for {
		b := iter.readByte()
		if iter.err != nil {
			return 0
		}
		result |= int64(b&0x7f) << shift
		if (b & 0x80) != 0x80 {
			break
		}
		shift += 7
	}
	return result
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
	protocolId := iter.readByte()
	if protocolId != protocol.COMPACT_PROTOCOL_ID {
		iter.ReportError("ReadMessageHeader", "invalid protocol")
		return protocol.MessageHeader{}
	}
	versionAndType := iter.readByte()
	version := versionAndType & protocol.COMPACT_VERSION_MASK
	messageType := protocol.TMessageType((versionAndType >> 5) & 0x07)
	if version != protocol.COMPACT_VERSION {
		iter.ReportError("ReadMessageHeader", fmt.Sprintf("expected version %02x but got %02x", protocol.COMPACT_VERSION, version))
		return protocol.MessageHeader{}
	}
	seqId := protocol.SeqId(iter.readVarInt32())
	messageName := iter.ReadString()
	return protocol.MessageHeader{
		MessageName: messageName,
		MessageType: messageType,
		SeqId:       seqId,
	}
}

func (iter *Iterator) ReadStructHeader() {
	iter.fieldIdStack = append(iter.fieldIdStack, iter.lastFieldId)
	iter.lastFieldId = 0
}

func (iter *Iterator) ReadStructField() (fieldType protocol.TType, fieldId protocol.FieldId) {
	firstByte := iter.readByte()
	if firstByte == 0 {
		if iter.Error() != nil {
			return protocol.TypeStop, 0
		}
		iter.lastFieldId = iter.fieldIdStack[len(iter.fieldIdStack)-1]
		iter.fieldIdStack = iter.fieldIdStack[:len(iter.fieldIdStack)-1]
		iter.pendingBoolField = 0
		return protocol.TType(firstByte), 0
	}
	// mask off the 4 MSB of the type header. it could contain a field id delta.
	modifier := int16((firstByte & 0xf0) >> 4)
	if modifier == 0 {
		// not a delta, look ahead for the zigzag varint field id.
		fieldId = protocol.FieldId(iter.ReadInt16())
	} else {
		// has a delta. add the delta to the last read field id.
		fieldId = iter.lastFieldId + protocol.FieldId(modifier)
	}
	switch tType := TCompactType(firstByte & 0x0f); tType {
	case TypeBooleanTrue:
		fieldType = protocol.TypeBool
		iter.pendingBoolField = 1
	case TypeBooleanFalse:
		fieldType = protocol.TypeBool
		iter.pendingBoolField = 2
	default:
		fieldType = tType.ToTType()
		iter.pendingBoolField = 0
	}

	// push the new field onto the field stack so we can keep the deltas going.
	iter.lastFieldId = fieldId
	return fieldType, fieldId
}

func (iter *Iterator) ReadListHeader() (elemType protocol.TType, size int) {
	lenAndType := iter.readByte()
	length := int((lenAndType >> 4) & 0x0f)
	if length == 15 {
		length2 := iter.readVarInt32()
		if length2 < 0 {
			iter.ReportError("ReadListHeader", "invalid length")
			return protocol.TypeStop, 0
		}
		length = int(length2)
	}
	elemType = TCompactType(lenAndType).ToTType()
	return elemType, length
}

func (iter *Iterator) ReadMapHeader() (keyType protocol.TType, elemType protocol.TType, size int) {
	length := int(iter.readVarInt32())
	if length == 0 {
		return protocol.TypeStop, protocol.TypeStop, length
	}
	keyAndElemType := iter.readByte()
	keyType = TCompactType(keyAndElemType >> 4).ToTType()
	elemType = TCompactType(keyAndElemType & 0xf).ToTType()
	return keyType, elemType, length
}

func (iter *Iterator) ReadBool() bool {
	if iter.pendingBoolField == 0 {
		return iter.ReadUint8() == 1
	}
	return iter.pendingBoolField == 1
}

func (iter *Iterator) ReadInt() int {
	return int(iter.ReadInt64())
}

func (iter *Iterator) ReadUint() uint {
	return uint(iter.ReadInt64())
}

func (iter *Iterator) ReadInt8() int8 {
	return int8(iter.ReadUint8())
}

func (iter *Iterator) ReadUint8() uint8 {
	return iter.readByte()
}

func (iter *Iterator) ReadInt16() int16 {
	return int16(iter.ReadInt32())
}

func (iter *Iterator) ReadUint16() uint16 {
	return uint16(iter.ReadUint32())
}

func (iter *Iterator) ReadInt32() int32 {
	result := iter.readVarInt32()
	u := uint32(result)
	return int32(u>>1) ^ -(result & 1)
}

func (iter *Iterator) ReadUint32() uint32 {
	return uint32(iter.ReadInt32())
}

func (iter *Iterator) ReadInt64() int64 {
	result := iter.readVarInt64()
	u := uint64(result)
	return int64(u>>1) ^ -(result & 1)
}

func (iter *Iterator) ReadUint64() uint64 {
	return uint64(iter.ReadInt64())
}

func (iter *Iterator) ReadFloat64() float64 {
	tmp := iter.readSmall(8)
	return math.Float64frombits(binary.LittleEndian.Uint64(tmp))
}

func (iter *Iterator) ReadString() string {
	length := iter.readVarInt32()
	return string(iter.readLarge(int(length)))
}

func (iter *Iterator) ReadBinary() []byte {
	length := iter.readVarInt32()
	tmp := make([]byte, length)
	copy(tmp, iter.readLarge(int(length)))
	return tmp
}
