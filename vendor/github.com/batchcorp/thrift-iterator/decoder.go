package thrifter

import (
	"github.com/batchcorp/thrift-iterator/general"
	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/spi"
	"io"
	"reflect"
)

type Decoder struct {
	cfg  *frozenConfig
	iter spi.Iterator
}

func (decoder *Decoder) Decode(val interface{}) error {
	cfg := decoder.cfg
	valType := reflect.TypeOf(val)
	valDecoder := cfg.getGenDecoder(valType)
	if valDecoder == nil {
		valDecoder = cfg.decoderOf(valType)
		cfg.addGenDecoder(valType, valDecoder)
	}
	valDecoder.Decode(val, decoder.iter)
	if decoder.iter.Error() != nil {
		return decoder.iter.Error()
	}
	return nil
}

func (decoder *Decoder) DecodeMessage() (general.Message, error) {
	var msg general.Message
	err := decoder.Decode(&msg)
	return msg, err
}

func (decoder *Decoder) DecodeMessageHeader() (protocol.MessageHeader, error) {
	var msgHeader protocol.MessageHeader
	err := decoder.Decode(&msgHeader)
	return msgHeader, err
}

func (decoder *Decoder) DecodeMessageArguments() (general.Struct, error) {
	var msgArgs general.Struct
	err := decoder.Decode(&msgArgs)
	return msgArgs, err
}

func (decoder *Decoder) Reset(reader io.Reader, buf []byte) {
	decoder.iter.Reset(reader, buf)
}
