package general

import "github.com/batchcorp/thrift-iterator/spi"

type generalMapDecoder struct {
}

func (decoder *generalMapDecoder) Decode(val interface{}, iter spi.Iterator) {
	*val.(*Map) = readMap(iter).(Map)
}

func readMap(iter spi.Iterator) interface{} {
	keyType, elemType, length := iter.ReadMapHeader()
	generalMap := Map{}
	if length == 0 {
		return generalMap
	}
	keyReader := generalReaderOf(keyType)
	elemReader := generalReaderOf(elemType)
	for i := 0; i < length; i++ {
		key := keyReader(iter)
		elem := elemReader(iter)
		generalMap[key] = elem
	}
	return generalMap
}