package protocol

// Type constants in the Thrift protocol
type TType byte
type TMessageType int32
type SeqId int32
type FieldId int16

const (
	BINARY_VERSION_MASK = 0xffff0000
	BINARY_VERSION_1    = 0x80010000

	COMPACT_PROTOCOL_ID      = 0x082
	COMPACT_VERSION          = 0x01
	COMPACT_VERSON_BE        = 0x02
	COMPACT_VERSION_MASK     = 0x1f
	COMPACT_TYPE_BITS        = 0x07
	COMPACT_TYPE_SHIFT_AMOUT = 5
)

const (
	MessgeTypeInvalid    TMessageType = 0
	MessageTypeCall      TMessageType = 1
	MessageTypeReply     TMessageType = 2
	MessageTypeException TMessageType = 3
	MessageTypeOneWay    TMessageType = 4
)

const (
	TypeStop   TType = 0
	TypeVoid   TType = 1
	TypeBool   TType = 2
	TypeByte   TType = 3
	TypeI08    TType = 3
	TypeDouble TType = 4
	TypeI16    TType = 6
	TypeI32    TType = 8
	TypeI64    TType = 10
	TypeString TType = 11
	TypeUTF7   TType = 11
	TypeStruct TType = 12
	TypeMap    TType = 13
	TypeSet    TType = 14
	TypeList   TType = 15
	TypeUTF8   TType = 16
	TypeUTF16  TType = 17
)

var typeNames = map[TType]string{
	TypeStop:   "Stop",
	TypeVoid:   "Void",
	TypeBool:   "Bool",
	TypeByte:   "Byte",
	TypeDouble: "Double",
	TypeI16:    "I16",
	TypeI32:    "I32",
	TypeI64:    "I64",
	TypeString: "String",
	TypeStruct: "Struct",
	TypeMap:    "Map",
	TypeSet:    "Set",
	TypeList:   "List",
	TypeUTF8:   "UTF8",
	TypeUTF16:  "UTF16",
}

func (p TType) String() string {
	if s, ok := typeNames[p]; ok {
		return s
	}
	return "Unknown"
}

type MessageHeader struct {
	MessageName string
	MessageType TMessageType
	SeqId       SeqId
}

type Flusher interface {
	Flush() error
}
