package reflection

import (
	"github.com/batchcorp/thrift-iterator/spi"
	"reflect"
	"unsafe"
)

type mapDecoder struct {
	mapType      reflect.Type
	mapInterface emptyInterface
	keyType      reflect.Type
	keyDecoder   internalDecoder
	elemType     reflect.Type
	elemDecoder  internalDecoder
}

func (decoder *mapDecoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	mapInterface := decoder.mapInterface
	mapInterface.word = ptr
	realInterface := (*interface{})(unsafe.Pointer(&mapInterface))
	mapVal := reflect.ValueOf(*realInterface).Elem()
	if mapVal.IsNil() {
		mapVal.Set(reflect.MakeMap(decoder.mapType))
	}
	_, _, length := iter.ReadMapHeader()
	for i := 0; i < length; i++ {
		keyVal := reflect.New(decoder.keyType)
		decoder.keyDecoder.decode(unsafe.Pointer(keyVal.Pointer()), iter)
		elemVal := reflect.New(decoder.elemType)
		decoder.elemDecoder.decode(unsafe.Pointer(elemVal.Pointer()), iter)
		mapVal.SetMapIndex(keyVal.Elem(), elemVal.Elem())
	}
}
