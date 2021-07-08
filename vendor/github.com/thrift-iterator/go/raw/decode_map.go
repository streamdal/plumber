package raw

import (
	"github.com/thrift-iterator/go/spi"
	"github.com/thrift-iterator/go/protocol"
)

type rawMapDecoder struct {
}

func (decoder *rawMapDecoder) Decode(val interface{}, iter spi.Iterator) {
	keyType, elemType, length := iter.ReadMapHeader()
	entries := make(map[interface{}]MapEntry, length)
	generalKeyReader := readerOf(keyType)
	keyIter := iter.Spawn()
	for i := 0; i < length; i++ {
		keyBuf := iter.Skip(keyType, nil)
		key := generalKeyReader(keyBuf, keyIter)
		elemBuf := iter.Skip(elemType, nil)
		entries[key] = MapEntry{
			Key: keyBuf,
			Element: elemBuf,
		}
	}
	obj := val.(*Map)
	obj.KeyType = keyType
	obj.ElementType = elemType
	obj.Entries = entries
}

func readerOf(valType protocol.TType) func([]byte, spi.Iterator) interface{} {
	switch valType {
	case protocol.TypeBool:
		return readBool
	case protocol.TypeI08:
		return readInt8
	case protocol.TypeI16:
		return readInt16
	case protocol.TypeI32:
		return readInt32
	case protocol.TypeI64:
		return readInt64
	case protocol.TypeDouble:
		return readFloat64
	case protocol.TypeString:
		return readString
	default:
		panic("unsupported type")
	}
}

func readBool(buf []byte, iter spi.Iterator) interface{} {
	iter.Reset(nil, buf)
	return iter.ReadBool()
}

func readInt8(buf []byte, iter spi.Iterator) interface{} {
	iter.Reset(nil, buf)
	return iter.ReadInt8()
}

func readInt16(buf []byte, iter spi.Iterator) interface{} {
	iter.Reset(nil, buf)
	return iter.ReadInt16()
}

func readInt32(buf []byte, iter spi.Iterator) interface{} {
	iter.Reset(nil,buf)
	return iter.ReadInt32()
}

func readInt64(buf []byte, iter spi.Iterator) interface{} {
	iter.Reset(nil, buf)
	return iter.ReadInt64()
}

func readFloat64(buf []byte, iter spi.Iterator) interface{} {
	iter.Reset(nil,buf)
	return iter.ReadFloat64()
}

func readString(buf []byte, iter spi.Iterator) interface{} {
	iter.Reset(nil,buf)
	return iter.ReadString()
}