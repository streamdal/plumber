package binary

import (
	"github.com/thrift-iterator/go/protocol"
	"github.com/thrift-iterator/go/spi"
)

func (iter *Iterator) Discard(ttype protocol.TType) {
	switch ttype {
	case protocol.TypeBool, protocol.TypeI08:
		iter.readByte()
	case protocol.TypeI16:
		iter.readSmall(2)
	case protocol.TypeI32:
		iter.readSmall(4)
	case protocol.TypeI64, protocol.TypeDouble:
		iter.readSmall(8)
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
