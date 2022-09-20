package compact

import (
	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/spi"
)

func (iter *Iterator) Discard(ttype protocol.TType) {
	switch ttype {
	case protocol.TypeBool, protocol.TypeI08:
		iter.ReadInt8()
	case protocol.TypeI16:
		iter.ReadInt16()
	case protocol.TypeI32:
		iter.ReadInt32()
	case protocol.TypeI64:
		iter.ReadInt64()
	case protocol.TypeDouble:
		iter.ReadFloat64()
	case protocol.TypeString:
		iter.SkipBinary(nil)
	case protocol.TypeList:
		spi.DiscardList(iter)
	case protocol.TypeStruct:
		spi.DiscardStruct(iter)
	case protocol.TypeMap:
		spi.DiscardMap(iter)
	default:
		panic("unsupported type")
	}
}
