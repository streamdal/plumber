package reflection

import (
	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/spi"
	"reflect"
	"unsafe"
)

type pointerEncoder struct {
	valType    reflect.Type
	valEncoder internalEncoder
}

func (encoder *pointerEncoder) encode(ptr unsafe.Pointer, stream spi.Stream) {
	valPtr := *(*unsafe.Pointer)(ptr)
	if encoder.valType.Kind() == reflect.Map {
		valPtr = *(*unsafe.Pointer)(valPtr)
	}
	encoder.valEncoder.encode(valPtr, stream)
}

func (encoder *pointerEncoder) thriftType() protocol.TType {
	return encoder.valEncoder.thriftType()
}
