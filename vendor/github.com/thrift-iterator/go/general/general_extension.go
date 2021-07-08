package general

import (
	"reflect"
	"github.com/thrift-iterator/go/spi"
	"github.com/thrift-iterator/go/protocol"
)

type Extension struct {
}

func (ext *Extension) EncoderOf(valType reflect.Type) spi.ValEncoder {
	switch valType {
	case reflect.TypeOf(List(nil)):
		return &generalListEncoder{}
	case reflect.TypeOf(Map(nil)):
		return &generalMapEncoder{}
	case reflect.TypeOf(Struct(nil)):
		return &generalStructEncoder{}
	case reflect.TypeOf((*Message)(nil)).Elem():
		return &messageEncoder{}
	case reflect.TypeOf((*protocol.MessageHeader)(nil)).Elem():
		return &messageHeaderEncoder{}
	}
	return nil
}

func (ext *Extension) DecoderOf(valType reflect.Type) spi.ValDecoder {
	switch valType {
	case reflect.TypeOf((*List)(nil)):
		return &generalListDecoder{}
	case reflect.TypeOf((*Map)(nil)):
		return &generalMapDecoder{}
	case reflect.TypeOf((*Struct)(nil)):
		return &generalStructDecoder{}
	case reflect.TypeOf((*Message)(nil)):
		return &messageDecoder{}
	case reflect.TypeOf((*protocol.MessageHeader)(nil)):
		return &messageHeaderDecoder{}
	}
	return nil
}
