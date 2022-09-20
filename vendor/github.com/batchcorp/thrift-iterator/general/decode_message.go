package general

import (
	"github.com/batchcorp/thrift-iterator/spi"
	"github.com/batchcorp/thrift-iterator/protocol"
)

type messageDecoder struct {
}

func (decoder *messageDecoder) Decode(val interface{}, iter spi.Iterator) {
	*val.(*Message) = Message{
		MessageHeader: iter.ReadMessageHeader(),
		Arguments:     readStruct(iter).(Struct),
	}
}

type messageHeaderDecoder struct {
}

func (decoder *messageHeaderDecoder) Decode(val interface{}, iter spi.Iterator) {
	*val.(*protocol.MessageHeader) = iter.ReadMessageHeader()
}