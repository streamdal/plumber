package thrifter

import (
	"github.com/batchcorp/thrift-iterator/general"
	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/spi"
	"io"
	"reflect"
)

type Encoder struct {
	cfg    *frozenConfig
	stream spi.Stream
}

func (encoder *Encoder) Encode(val interface{}) error {
	cfg := encoder.cfg
	valType := reflect.TypeOf(val)
	valEncoder := cfg.getGenEncoder(valType)
	if valEncoder == nil {
		valEncoder = cfg.encoderOf(valType)
		cfg.addGenEncoder(valType, valEncoder)
	}
	valEncoder.Encode(val, encoder.stream)
	encoder.stream.Flush()
	if encoder.stream.Error() != nil {
		return encoder.stream.Error()
	}
	return nil
}

func (encoder *Encoder) EncodeMessage(msg general.Message) error {
	return encoder.Encode(msg)
}

func (encoder *Encoder) EncodeMessageHeader(msgHeader protocol.MessageHeader) error {
	return encoder.Encode(msgHeader)
}

func (encoder *Encoder) EncodeMessageArguments(msgArgs general.Struct) error {
	return encoder.Encode(msgArgs)
}

func (encoder *Encoder) Reset(writer io.Writer) {
	encoder.stream.Reset(writer)
}

func (encoder *Encoder) Buffer() []byte {
	return encoder.stream.Buffer()
}
