package general

import (
	"github.com/thrift-iterator/go/spi"
	"github.com/thrift-iterator/go/protocol"
)

type generalStructEncoder struct {
}

func (encoder *generalStructEncoder) Encode(val interface{}, stream spi.Stream) {
	writeStruct(val, stream)
}

func (encoder *generalStructEncoder) ThriftType() protocol.TType {
	return protocol.TypeStruct
}

func writeStruct(val interface{}, stream spi.Stream) {
	obj := val.(Struct)
	stream.WriteStructHeader()
	for fieldId, elem := range obj {
		fieldType, generalWriter := generalWriterOf(elem)
		stream.WriteStructField(fieldType, fieldId)
		generalWriter(elem, stream)
	}
	stream.WriteStructFieldStop()
}
