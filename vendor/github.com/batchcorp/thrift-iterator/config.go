package thrifter

import (
	"encoding/json"
	"errors"
	"github.com/batchcorp/thrift-iterator/binding/codegen"
	"github.com/batchcorp/thrift-iterator/binding/reflection"
	"github.com/batchcorp/thrift-iterator/general"
	"github.com/batchcorp/thrift-iterator/protocol"
	"github.com/batchcorp/thrift-iterator/protocol/binary"
	"github.com/batchcorp/thrift-iterator/protocol/compact"
	"github.com/batchcorp/thrift-iterator/raw"
	"github.com/batchcorp/thrift-iterator/spi"
	"github.com/v2pro/wombat/generic"
	"io"
	"reflect"
	"sync"
)

type frozenConfig struct {
	extension     spi.Extension
	protocol      Protocol
	genDecoders   sync.Map
	genEncoders   sync.Map
	extDecoders   sync.Map
	extEncoders   sync.Map
	staticCodegen bool
}

func (cfg Config) AddExtension(extension spi.Extension) Config {
	cfg.Extensions = append(cfg.Extensions, extension)
	return cfg
}

func (cfg Config) Froze() API {
	extensions := append(cfg.Extensions, &general.Extension{})
	extensions = append(extensions, &raw.Extension{})
	api := &frozenConfig{
		extension:     extensions,
		protocol:      cfg.Protocol,
		staticCodegen: cfg.StaticCodegen,
	}
	api.extDecoders = sync.Map{}
	api.genDecoders = sync.Map{}
	api.extEncoders = sync.Map{}
	api.genEncoders = sync.Map{}
	return api
}

func (cfg *frozenConfig) addGenDecoder(cacheKey reflect.Type, decoder spi.ValDecoder) {
	cfg.genDecoders.Store(cacheKey, decoder)
}

func (cfg *frozenConfig) addExtDecoder(cacheKey string, decoder spi.ValDecoder) {
	cfg.extDecoders.Store(cacheKey, decoder)
}

func (cfg *frozenConfig) addGenEncoder(cacheKey reflect.Type, encoder spi.ValEncoder) {
	cfg.genEncoders.Store(cacheKey, encoder)
}

func (cfg *frozenConfig) addExtEncoder(cacheKey string, encoder spi.ValEncoder) {
	cfg.extEncoders.Store(cacheKey, encoder)
}

func (cfg *frozenConfig) PrepareDecoder(valType reflect.Type) {
	cacheKey := valType.String()
	if cfg.GetDecoder(cacheKey) != nil {
		return
	}
	decoder := cfg.extension.DecoderOf(valType)
	cfg.addExtDecoder(cacheKey, decoder)
	cfg.addGenDecoder(valType, decoder)
}

func (cfg *frozenConfig) GetDecoder(cacheKey string) spi.ValDecoder {
	decoder, found := cfg.extDecoders.Load(cacheKey)
	if found {
		return decoder.(spi.ValDecoder)
	}
	return nil
}

func (cfg *frozenConfig) getGenDecoder(cacheKey reflect.Type) spi.ValDecoder {
	decoder, found := cfg.genDecoders.Load(cacheKey)
	if found {
		return decoder.(spi.ValDecoder)
	}
	return nil
}

func (cfg *frozenConfig) PrepareEncoder(valType reflect.Type) {
	cacheKey := valType.String()
	if cfg.GetEncoder(cacheKey) != nil {
		return
	}
	encoder := cfg.extension.EncoderOf(valType)
	cfg.addExtEncoder(cacheKey, encoder)
	cfg.addGenEncoder(valType, encoder)
}

func (cfg *frozenConfig) GetEncoder(cacheKey string) spi.ValEncoder {
	encoder, found := cfg.extEncoders.Load(cacheKey)
	if found {
		return encoder.(spi.ValEncoder)
	}
	return nil
}

func (cfg *frozenConfig) getGenEncoder(cacheKey reflect.Type) spi.ValEncoder {
	encoder, found := cfg.genEncoders.Load(cacheKey)
	if found {
		return encoder.(spi.ValEncoder)
	}
	return nil
}

func (cfg *frozenConfig) NewStream(writer io.Writer, buf []byte) spi.Stream {
	switch cfg.protocol {
	case ProtocolBinary:
		return binary.NewStream(cfg, writer, buf)
	case ProtocolCompact:
		return compact.NewStream(cfg, writer, buf)
	}
	panic("unsupported protocol")
}

func (cfg *frozenConfig) NewIterator(reader io.Reader, buf []byte) spi.Iterator {
	switch cfg.protocol {
	case ProtocolBinary:
		return binary.NewIterator(cfg, reader, buf)
	case ProtocolCompact:
		return compact.NewIterator(cfg, reader, buf)
	}
	panic("unsupported protocol")
}

func (cfg *frozenConfig) WillDecodeFromBuffer(samples ...interface{}) {
	if !cfg.staticCodegen {
		panic("this config is using dynamic codegen, can not do static codegen")
	}
	for _, sample := range samples {
		cfg.staticDecoderOf(reflect.TypeOf(sample))
	}
}

func (cfg *frozenConfig) WillDecodeFromReader(samples ...interface{}) {
	if !cfg.staticCodegen {
		panic("this config is using dynamic codegen, can not do static codegen")
	}
	for _, sample := range samples {
		cfg.staticDecoderOf(reflect.TypeOf(sample))
	}
}

func (cfg *frozenConfig) WillEncode(samples ...interface{}) {
	if !cfg.staticCodegen {
		panic("this config is using dynamic codegen, can not do static codegen")
	}
	for _, sample := range samples {
		cfg.staticEncoderOf(reflect.TypeOf(sample))
	}
}

func (cfg *frozenConfig) decoderOf(valType reflect.Type) spi.ValDecoder {
	if cfg.staticCodegen {
		return cfg.staticDecoderOf(valType)
	}
	return reflection.DecoderOf(cfg.extension, valType)
}

func (cfg *frozenConfig) staticDecoderOf(valType reflect.Type) spi.ValDecoder {
	iteratorType := reflect.TypeOf((*binary.Iterator)(nil))
	if cfg.protocol == ProtocolCompact {
		iteratorType = reflect.TypeOf((*compact.Iterator)(nil))
	}
	funcObj := generic.Expand(codegen.Decode,
		"EXT", &codegen.Extension{Extension: cfg.extension},
		"ST", iteratorType,
		"DT", valType)
	f := funcObj.(func(interface{}, interface{}))
	return &funcDecoder{f}
}

func (cfg *frozenConfig) encoderOf(valType reflect.Type) spi.ValEncoder {
	if cfg.staticCodegen {
		return cfg.staticEncoderOf(valType)
	}
	return reflection.EncoderOf(cfg.extension, valType)
}

func (cfg *frozenConfig) staticEncoderOf(valType reflect.Type) spi.ValEncoder {
	streamType := reflect.TypeOf((*binary.Stream)(nil))
	if cfg.protocol == ProtocolCompact {
		streamType = reflect.TypeOf((*compact.Stream)(nil))
	}
	funcObj := generic.Expand(codegen.Encode,
		"EXT", &codegen.Extension{Extension: cfg.extension},
		"ST", valType,
		"DT", streamType)
	f := funcObj.(func(interface{}, interface{}))
	return &funcEncoder{f}
}

type funcDecoder struct {
	f func(dst interface{}, src interface{})
}

func (decoder *funcDecoder) Decode(val interface{}, iter spi.Iterator) {
	decoder.f(val, iter)
}

type funcEncoder struct {
	f func(dst interface{}, src interface{})
}

func (encoder *funcEncoder) Encode(val interface{}, stream spi.Stream) {
	encoder.f(stream, val)
}

func (encoder *funcEncoder) ThriftType() protocol.TType {
	panic("funcEncoder is not composable")
}

func (cfg *frozenConfig) Unmarshal(buf []byte, val interface{}) error {
	valType := reflect.TypeOf(val)
	decoder := cfg.getGenDecoder(valType)
	if decoder == nil {
		decoder = cfg.decoderOf(valType)
		cfg.addGenDecoder(valType, decoder)
	}
	if buf == nil {
		return errors.New("empty input")
	}
	iter := cfg.NewIterator(nil, buf)
	decoder.Decode(val, iter)
	if iter.Error() != nil {
		return iter.Error()
	}
	return nil
}

func (cfg *frozenConfig) Marshal(val interface{}) ([]byte, error) {
	valType := reflect.TypeOf(val)
	encoder := cfg.getGenEncoder(valType)
	if encoder == nil {
		encoder = cfg.encoderOf(valType)
		cfg.addGenEncoder(valType, encoder)
	}
	stream := cfg.NewStream(nil, nil)
	encoder.Encode(val, stream)
	if stream.Error() != nil {
		return nil, stream.Error()
	}
	buf := stream.Buffer()
	return buf, nil
}

func (cfg *frozenConfig) NewDecoder(reader io.Reader, buf []byte) *Decoder {
	return &Decoder{
		cfg:  cfg,
		iter: cfg.NewIterator(reader, buf),
	}
}

func (cfg *frozenConfig) NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{
		cfg:    cfg,
		stream: cfg.NewStream(writer, nil),
	}
}

func (cfg *frozenConfig) ToJSON(buf []byte) (string, error) {
	msg, err := UnmarshalMessage(buf)
	if err != nil {
		return "", err
	}
	jsonEncoded, err := json.MarshalIndent(msg, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonEncoded), nil
}

func (cfg *frozenConfig) MarshalMessage(msg general.Message) ([]byte, error) {
	return cfg.Marshal(msg)
}

func (cfg *frozenConfig) UnmarshalMessage(buf []byte) (general.Message, error) {
	var msg general.Message
	err := cfg.Unmarshal(buf, &msg)
	return msg, err
}
