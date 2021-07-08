package general

import "github.com/thrift-iterator/go/spi"

type generalListDecoder struct {
}

func (decoder *generalListDecoder) Decode(val interface{}, iter spi.Iterator) {
	*val.(*List) = readList(iter).(List)
}

func readList(iter spi.Iterator) interface{} {
	elemType, length := iter.ReadListHeader()
	generalReader := generalReaderOf(elemType)
	var generalList List
	for i := 0; i < length; i++ {
		generalList = append(generalList, generalReader(iter))
	}
	return generalList
}
