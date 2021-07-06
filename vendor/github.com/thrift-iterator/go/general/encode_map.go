package general

import (
	"github.com/thrift-iterator/go/spi"
	"github.com/thrift-iterator/go/protocol"
)

type generalMapEncoder struct {
}

func (encoder *generalMapEncoder) Encode(val interface{}, stream spi.Stream) {
	writeMap(val, stream)
}

func (encoder *generalMapEncoder) ThriftType() protocol.TType {
	return protocol.TypeMap
}

func takeSampleFromMap(sample Map) (interface{}, interface{}){
	for key, elem := range sample {
		return key, elem
	}
	panic("should not reach here")
}

func writeMap(val interface{}, stream spi.Stream) {
	obj := val.(Map)
	length := len(obj)
	if length == 0 {
		stream.WriteMapHeader(protocol.TypeI64, protocol.TypeI64, 0)
		return
	}
	keySample, elemSample := takeSampleFromMap(obj)
	keyType, generalKeyWriter := generalWriterOf(keySample)
	elemType, generalElemWriter := generalWriterOf(elemSample)
	stream.WriteMapHeader(keyType, elemType, length)
	for key, elem := range obj {
		generalKeyWriter(key, stream)
		generalElemWriter(elem, stream)
	}
}