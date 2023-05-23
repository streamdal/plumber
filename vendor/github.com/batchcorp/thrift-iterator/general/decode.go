package general

import (
	"fmt"

	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/spi"
)

func generalReaderOf(ttype protocol.TType) func(iter spi.Iterator) interface{} {
	switch ttype {
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
	case protocol.TypeString:
		return readString
	case protocol.TypeDouble:
		return readFloat64
	case protocol.TypeList:
		return readList
	case protocol.TypeMap:
		return readMap
	case protocol.TypeStruct:
		return readStruct
	case protocol.TypeSet:
		return readList
	default:
		panic(fmt.Sprintf("unsupported type: %d", ttype))
	}
}

func readFloat64(iter spi.Iterator) interface{} {
	return iter.ReadFloat64()
}

func readBool(iter spi.Iterator) interface{} {
	return iter.ReadBool()
}

func readInt8(iter spi.Iterator) interface{} {
	return iter.ReadInt8()
}

func readInt16(iter spi.Iterator) interface{} {
	return iter.ReadInt16()
}

func readInt32(iter spi.Iterator) interface{} {
	return iter.ReadInt32()
}

func readInt64(iter spi.Iterator) interface{} {
	return iter.ReadInt64()
}

func readString(iter spi.Iterator) interface{} {
	return iter.ReadString()
}
