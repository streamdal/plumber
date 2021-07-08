package raw

import (
	"github.com/thrift-iterator/go/spi"
	"github.com/thrift-iterator/go/protocol"
)

type rawStructDecoder struct {
}

func (decoder *rawStructDecoder) Decode(val interface{}, iter spi.Iterator) {
	fields := Struct{}
	iter.ReadStructHeader()
	for {
		fieldType, fieldId := iter.ReadStructField()
		if fieldType == protocol.TypeStop {
			*val.(*Struct) = fields
			return
		}
		fields[fieldId] = StructField{
			Type: fieldType,
			Buffer: iter.Skip(fieldType, nil),
		}
	}
}