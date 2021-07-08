package thrifter

import (
	"io"
	"github.com/thrift-iterator/go/spi"
	"github.com/thrift-iterator/go/general"
)

type Protocol int

var ProtocolBinary Protocol = 1
var ProtocolCompact Protocol = 2

type Config struct {
	Protocol      Protocol
	StaticCodegen bool
	Extensions    spi.Extensions
}

type API interface {
	// NewStream is low level streaming api
	NewStream(writer io.Writer, buf []byte) spi.Stream
	// NewIterator is low level streaming api
	NewIterator(reader io.Reader, buf []byte) spi.Iterator
	// Unmarshal from []byte
	Unmarshal(buf []byte, obj interface{}) error
	// UnmarshalMessage from []byte
	UnmarshalMessage(buf []byte) (general.Message, error)
	// Marshal to []byte
	Marshal(obj interface{}) ([]byte, error)
	// ToJSON convert thrift message to JSON string
	ToJSON(buf []byte) (string, error)
	// MarshalMessage to []byte
	MarshalMessage(msg general.Message) ([]byte, error)
	// NewDecoder to unmarshal from []byte or io.Reader
	NewDecoder(reader io.Reader, buf []byte) *Decoder
	// NewEncoder to marshal to io.Writer
	NewEncoder(writer io.Writer) *Encoder
	// WillDecodeFromBuffer should only be used in generic.Declare
	WillDecodeFromBuffer(sample ...interface{})
	// WillDecodeFromReader should only be used in generic.Declare
	WillDecodeFromReader(sample ...interface{})
	// WillEncode should only be used in generic.Declare
	WillEncode(sample ...interface{})
}

var DefaultConfig = Config{Protocol: ProtocolBinary, StaticCodegen: false}.Froze()

func NewStream(writer io.Writer, buf []byte) spi.Stream {
	return DefaultConfig.NewStream(writer, buf)
}

func NewIterator(reader io.Reader, buf []byte) spi.Iterator {
	return DefaultConfig.NewIterator(reader, buf)
}

func Unmarshal(buf []byte, obj interface{}) error {
	return DefaultConfig.Unmarshal(buf, obj)
}

// UnmarshalMessage demonstrate how to decode thrift binary without IDL into a general message struct
func UnmarshalMessage(buf []byte) (general.Message, error) {
	return DefaultConfig.UnmarshalMessage(buf)
}

// ToJSON convert the thrift message to JSON string
func ToJSON(buf []byte) (string, error) {
	return DefaultConfig.ToJSON(buf)
}

func Marshal(obj interface{}) ([]byte, error) {
	return DefaultConfig.Marshal(obj)
}

// MarshalMessage is just a shortcut to demonstrate message decoded by UnmarshalMessage can be encoded back
func MarshalMessage(msg general.Message) ([]byte, error) {
	return DefaultConfig.MarshalMessage(msg)
}

func NewDecoder(reader io.Reader, buf []byte) *Decoder {
	return DefaultConfig.NewDecoder(reader, buf)
}

func NewEncoder(writer io.Writer) *Encoder {
	return DefaultConfig.NewEncoder(writer)
}
