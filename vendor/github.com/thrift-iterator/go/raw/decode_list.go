package raw

import (
	"github.com/thrift-iterator/go/spi"
)

type rawListDecoder struct {
}

func (decoder *rawListDecoder) Decode(val interface{}, iter spi.Iterator) {
	elemType, length := iter.ReadListHeader()
	elements := make([][]byte, length)
	for i := 0; i < length; i++ {
		elements[i] = iter.Skip(elemType, nil)
	}
	obj := val.(*List)
	obj.ElementType = elemType
	obj.Elements = elements
}