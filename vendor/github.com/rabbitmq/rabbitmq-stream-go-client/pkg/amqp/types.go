package amqp

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"time"
	"unicode/utf8"
)

type amqpType uint8
type ProtoID uint8

// Type codes
const (
	typeCodeNull amqpType = 0x40

	// Bool
	typeCodeBool      amqpType = 0x56 // boolean with the octet 0x00 being false and octet 0x01 being true
	typeCodeBoolTrue  amqpType = 0x41
	typeCodeBoolFalse amqpType = 0x42

	// Unsigned
	typeCodeUbyte      amqpType = 0x50 // 8-bit unsigned integer (1)
	typeCodeUshort     amqpType = 0x60 // 16-bit unsigned integer in network byte order (2)
	typeCodeUint       amqpType = 0x70 // 32-bit unsigned integer in network byte order (4)
	typeCodeSmallUint  amqpType = 0x52 // unsigned integer value in the range 0 to 255 inclusive (1)
	typeCodeUint0      amqpType = 0x43 // the uint value 0 (0)
	typeCodeUlong      amqpType = 0x80 // 64-bit unsigned integer in network byte order (8)
	typeCodeSmallUlong amqpType = 0x53 // unsigned long value in the range 0 to 255 inclusive (1)
	typeCodeUlong0     amqpType = 0x44 // the ulong value 0 (0)

	// Signed
	typeCodeByte      amqpType = 0x51 // 8-bit two's-complement integer (1)
	typeCodeShort     amqpType = 0x61 // 16-bit two's-complement integer in network byte order (2)
	typeCodeInt       amqpType = 0x71 // 32-bit two's-complement integer in network byte order (4)
	typeCodeSmallint  amqpType = 0x54 // 8-bit two's-complement integer (1)
	typeCodeLong      amqpType = 0x81 // 64-bit two's-complement integer in network byte order (8)
	typeCodeSmalllong amqpType = 0x55 // 8-bit two's-complement integer

	// Decimal
	typeCodeFloat      amqpType = 0x72 // IEEE 754-2008 binary32 (4)
	typeCodeDouble     amqpType = 0x82 // IEEE 754-2008 binary64 (8)
	typeCodeDecimal32  amqpType = 0x74 // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)
	typeCodeDecimal64  amqpType = 0x84 // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)
	typeCodeDecimal128 amqpType = 0x94 // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)

	// Other
	typeCodeChar      amqpType = 0x73 // a UTF-32BE encoded Unicode character (4)
	typeCodeTimestamp amqpType = 0x83 // 64-bit two's-complement integer representing milliseconds since the unix epoch
	typeCodeUUID      amqpType = 0x98 // UUID as defined in section 4.1.2 of RFC-4122

	// Variable Length
	typeCodeVbin8  amqpType = 0xa0 // up to 2^8 - 1 octets of binary data (1 + variable)
	typeCodeVbin32 amqpType = 0xb0 // up to 2^32 - 1 octets of binary data (4 + variable)
	typeCodeStr8   amqpType = 0xa1 // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)
	typeCodeStr32  amqpType = 0xb1 // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)
	typeCodeSym8   amqpType = 0xa3 // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)
	typeCodeSym32  amqpType = 0xb3 // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)

	// Compound
	typeCodeList0   amqpType = 0x45 // the empty list (i.e. the list with no elements) (0)
	typeCodeList8   amqpType = 0xc0 // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)
	typeCodeList32  amqpType = 0xd0 // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)
	typeCodeMap8    amqpType = 0xc1 // up to 2^8 - 1 octets of encoded map data (1 + compound)
	typeCodeMap32   amqpType = 0xd1 // up to 2^32 - 1 octets of encoded map data (4 + compound)
	typeCodeArray8  amqpType = 0xe0 // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)
	typeCodeArray32 amqpType = 0xf0 // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)

	// Composites
	typeCodeOpen        amqpType = 0x10
	typeCodeBegin       amqpType = 0x11
	typeCodeAttach      amqpType = 0x12
	typeCodeFlow        amqpType = 0x13
	typeCodeTransfer    amqpType = 0x14
	typeCodeDisposition amqpType = 0x15
	typeCodeDetach      amqpType = 0x16
	typeCodeEnd         amqpType = 0x17
	typeCodeClose       amqpType = 0x18

	typeCodeSource amqpType = 0x28
	typeCodeTarget amqpType = 0x29
	typeCodeError  amqpType = 0x1d

	typeCodeMessageHeader         amqpType = 0x70
	typeCodeDeliveryAnnotations   amqpType = 0x71
	typeCodeMessageAnnotations    amqpType = 0x72
	typeCodeMessageProperties     amqpType = 0x73
	typeCodeApplicationProperties amqpType = 0x74
	typeCodeApplicationData       amqpType = 0x75
	typeCodeAMQPSequence          amqpType = 0x76
	typeCodeAMQPValue             amqpType = 0x77
	typeCodeFooter                amqpType = 0x78

	typeCodeStateReceived amqpType = 0x23
	typeCodeStateAccepted amqpType = 0x24
	typeCodeStateRejected amqpType = 0x25
	typeCodeStateReleased amqpType = 0x26
	typeCodeStateModified amqpType = 0x27

	typeCodeSASLMechanism amqpType = 0x40
	typeCodeSASLInit      amqpType = 0x41
	typeCodeSASLChallenge amqpType = 0x42
	typeCodeSASLResponse  amqpType = 0x43
	typeCodeSASLOutcome   amqpType = 0x44

	typeCodeDeleteOnClose             amqpType = 0x2b
	typeCodeDeleteOnNoLinks           amqpType = 0x2c
	typeCodeDeleteOnNoMessages        amqpType = 0x2d
	typeCodeDeleteOnNoLinksOrMessages amqpType = 0x2e
)

// Frame structure:
//
//     header (8 bytes)
//       0-3: SIZE (total size, at least 8 bytes for header, uint32)
//       4:   DOFF (data offset,at least 2, count of 4 bytes words, uint8)
//       5:   TYPE (frame type)
//                0x0: AMQP
//                0x1: SASL
//       6-7: type dependent (channel for AMQP)
//     extended header (opt)
//     body (opt)

type deliveryState interface{} // TODO: http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#type-declared

type unsettled map[string]deliveryState

func (u unsettled) marshal(wr *buffer) error {
	return writeMap(wr, u)
}

func (u *unsettled) unmarshal(r *buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	m := make(unsettled, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readString(r)
		if err != nil {
			return err
		}
		var value deliveryState
		err = unmarshal(r, &value)
		if err != nil {
			return err
		}
		m[key] = value
	}
	*u = m
	return nil
}

type filter map[symbol]*describedType

func (f filter) marshal(wr *buffer) error {
	return writeMap(wr, f)
}

func (f *filter) unmarshal(r *buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	m := make(filter, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readString(r)
		if err != nil {
			return err
		}
		var value describedType
		err = unmarshal(r, &value)
		if err != nil {
			return err
		}
		m[symbol(key)] = &value
	}
	*f = m
	return nil
}

// ErrorCondition is one of the error conditions defined in the AMQP spec.
type ErrorCondition string

func (ec ErrorCondition) marshal(wr *buffer) error {
	return (symbol)(ec).marshal(wr)
}

func (ec *ErrorCondition) unmarshal(r *buffer) error {
	s, err := readString(r)
	*ec = ErrorCondition(s)
	return err
}

// Error Conditions
const (
	// AMQP Errors
	ErrorInternalError         ErrorCondition = "amqp:internal-error"
	ErrorNotFound              ErrorCondition = "amqp:not-found"
	ErrorUnauthorizedAccess    ErrorCondition = "amqp:unauthorized-access"
	ErrorDecodeError           ErrorCondition = "amqp:decode-error"
	ErrorResourceLimitExceeded ErrorCondition = "amqp:resource-limit-exceeded"
	ErrorNotAllowed            ErrorCondition = "amqp:not-allowed"
	ErrorInvalidField          ErrorCondition = "amqp:invalid-field"
	ErrorNotImplemented        ErrorCondition = "amqp:not-implemented"
	ErrorResourceLocked        ErrorCondition = "amqp:resource-locked"
	ErrorPreconditionFailed    ErrorCondition = "amqp:precondition-failed"
	ErrorResourceDeleted       ErrorCondition = "amqp:resource-deleted"
	ErrorIllegalState          ErrorCondition = "amqp:illegal-state"
	ErrorFrameSizeTooSmall     ErrorCondition = "amqp:frame-size-too-small"

	// Connection Errors
	ErrorConnectionForced   ErrorCondition = "amqp:connection:forced"
	ErrorFramingError       ErrorCondition = "amqp:connection:framing-error"
	ErrorConnectionRedirect ErrorCondition = "amqp:connection:redirect"

	// Session Errors
	ErrorWindowViolation  ErrorCondition = "amqp:session:window-violation"
	ErrorErrantLink       ErrorCondition = "amqp:session:errant-link"
	ErrorHandleInUse      ErrorCondition = "amqp:session:handle-in-use"
	ErrorUnattachedHandle ErrorCondition = "amqp:session:unattached-handle"

	// Link Errors
	ErrorDetachForced          ErrorCondition = "amqp:link:detach-forced"
	ErrorTransferLimitExceeded ErrorCondition = "amqp:link:transfer-limit-exceeded"
	ErrorMessageSizeExceeded   ErrorCondition = "amqp:link:message-size-exceeded"
	ErrorLinkRedirect          ErrorCondition = "amqp:link:redirect"
	ErrorStolen                ErrorCondition = "amqp:link:stolen"
)

/*
<type name="error" class="composite" source="list">
    <descriptor name="amqp:error:list" code="0x00000000:0x0000001d"/>
    <field name="condition" type="symbol" requires="error-condition" mandatory="true"/>
    <field name="description" type="string"/>
    <field name="info" type="fields"/>
</type>
*/

// Error is an AMQP error.
type Error struct {
	// A symbolic value indicating the error condition.
	Condition ErrorCondition

	// descriptive text about the error condition
	//
	// This text supplies any supplementary details not indicated by the condition field.
	// This text can be logged as an aid to resolving issues.
	Description string

	// map carrying information about the error condition
	Info map[string]interface{}
}

func (e *Error) marshal(wr *buffer) error {
	return marshalComposite(wr, typeCodeError, []marshalField{
		{value: &e.Condition, omit: false},
		{value: &e.Description, omit: e.Description == ""},
		{value: e.Info, omit: len(e.Info) == 0},
	})
}

func (e *Error) unmarshal(r *buffer) error {
	return unmarshalComposite(r, typeCodeError, []unmarshalField{
		{field: &e.Condition, handleNull: func() error { return errorNew("Error.Condition is required") }},
		{field: &e.Description},
		{field: &e.Info},
	}...)
}

func (e *Error) String() string {
	if e == nil {
		return "*Error(nil)"
	}
	return fmt.Sprintf("*Error{Condition: %s, Description: %s, Info: %v}",
		e.Condition,
		e.Description,
		e.Info,
	)
}

func (e *Error) Error() string {
	return e.String()
}

// Message is an AMQP message.
type Message struct {
	// Message format code.
	//
	// The upper three octets of a message format code identify a particular message
	// format. The lowest octet indicates the version of said message format. Any
	// given version of a format is forwards compatible with all higher versions.
	Format uint32

	// The DeliveryTag can be up to 32 octets of binary data.
	// Note that when mode one is enabled there will be no delivery tag.
	DeliveryTag []byte

	// The header section carries standard delivery details about the transfer
	// of a message through the AMQP network.
	Header *MessageHeader
	// If the header section is omitted the receiver MUST assume the appropriate
	// default values (or the meaning implied by no value being set) for the
	// fields within the header unless other target or node specific defaults
	// have otherwise been set.

	// The delivery-annotations section is used for delivery-specific non-standard
	// properties at the head of the message. Delivery annotations convey information
	// from the sending peer to the receiving peer.
	DeliveryAnnotations Annotations
	// If the recipient does not understand the annotation it cannot be acted upon
	// and its effects (such as any implied propagation) cannot be acted upon.
	// Annotations might be specific to one implementation, or common to multiple
	// implementations. The capabilities negotiated on link attach and on the source
	// and target SHOULD be used to establish which annotations a peer supports. A
	// registry of defined annotations and their meanings is maintained [AMQPDELANN].
	// The symbolic key "rejected" is reserved for the use of communicating error
	// information regarding rejected messages. Any values associated with the
	// "rejected" key MUST be of type error.
	//
	// If the delivery-annotations section is omitted, it is equivalent to a
	// delivery-annotations section containing an empty map of annotations.

	// The message-annotations section is used for properties of the message which
	// are aimed at the infrastructure.
	Annotations Annotations
	// The message-annotations section is used for properties of the message which
	// are aimed at the infrastructure and SHOULD be propagated across every
	// delivery step. UnConfirmedMessage annotations convey information about the message.
	// Intermediaries MUST propagate the annotations unless the annotations are
	// explicitly augmented or modified (e.g., by the use of the modified outcome).
	//
	// The capabilities negotiated on link attach and on the source and target can
	// be used to establish which annotations a peer understands; however, in a
	// network of AMQP intermediaries it might not be possible to know if every
	// intermediary will understand the annotation. Note that for some annotations
	// it might not be necessary for the intermediary to understand their purpose,
	// i.e., they could be used purely as an attribute which can be filtered on.
	//
	// A registry of defined annotations and their meanings is maintained [AMQPMESSANN].
	//
	// If the message-annotations section is omitted, it is equivalent to a
	// message-annotations section containing an empty map of annotations.

	// The properties section is used for a defined set of standard properties of
	// the message.
	Properties *MessageProperties
	// The properties section is part of the bare message; therefore,
	// if retransmitted by an intermediary, it MUST remain unaltered.

	// The application-properties section is a part of the bare message used for
	// structured application data. Intermediaries can use the data within this
	// structure for the purposes of filtering or routing.
	ApplicationProperties map[string]interface{}
	// The keys of this map are restricted to be of type string (which excludes
	// the possibility of a null key) and the values are restricted to be of
	// simple types only, that is, excluding map, list, and array types.

	// Data payloads.
	Data [][]byte
	// A data section contains opaque binary data.
	// TODO: this could be data(s), amqp-sequence(s), amqp-value rather than single data:
	// "The body consists of one of the following three choices: one or more data
	//  sections, one or more amqp-sequence sections, or a single amqp-value section."

	// Value payload.
	Value interface{}
	// An amqp-value section contains a single AMQP value.

	// The footer section is used for details about the message or delivery which
	// can only be calculated or evaluated once the whole bare message has been
	// constructed or seen (for example message hashes, HMACs, signatures and
	// encryption details).
	Footer Annotations

	// Mark the message as settled when LinkSenderSettle is ModeMixed.
	//
	// This field is ignored when LinkSenderSettle is not ModeMixed.
	SendSettled bool

	//receiver   *Receiver // Receiver the message was received from
	settled bool // whether transfer was settled by sender

	// doneSignal is a channel that indicate when a message is considered acted upon by downstream handler
	doneSignal chan struct{}
}

type AMQP10 struct {
	publishingId int64
	message      *Message
	Properties   *MessageProperties
}

func NewMessage(data []byte) *AMQP10 {
	return &AMQP10{
		message:      newMessage(data),
		publishingId: -1,
	}
}

func (amqp *AMQP10) SetPublishingId(id int64) {
	amqp.publishingId = id
}

func (amqp *AMQP10) GetPublishingId() int64 {
	return amqp.publishingId
}

func (amqp *AMQP10) MarshalBinary() ([]byte, error) {
	amqp.message.Properties = amqp.Properties
	return amqp.message.MarshalBinary()
}

func (amqp *AMQP10) UnmarshalBinary(data []byte) error {
	return amqp.message.UnmarshalBinary(data)
}

func (amqp *AMQP10) GetData() [][]byte {
	return amqp.message.Data
}

func (amqp *AMQP10) Message() *Message {
	return amqp.message

}

// NewMessage returns a *Message with data as the payload.
//
// This constructor is intended as a helper for basic Messages with a
// single data payload. It is valid to construct a Message directly for
// more complex usages.
func newMessage(data []byte) *Message {
	return &Message{
		Data:       [][]byte{data},
		doneSignal: make(chan struct{}),
	}
}

// done closes the internal doneSignal channel to let the receiver know that this message has been acted upon
func (m *Message) done() {
	// TODO: move initialization in ctor and use ctor everywhere?
	if m.doneSignal != nil {
		close(m.doneSignal)
	}
}

// GetData returns the first []byte from the Data field
// or nil if Data is empty.
func (m *Message) GetData() []byte {
	if len(m.Data) < 1 {
		return nil
	}
	return m.Data[0]
}

// Ignore notifies the amqp message pump that the message has been handled
// without any disposition. It frees the amqp receiver to get the next message
// this is implicitly done after calling message dispositions (Accept/Release/Reject/Modify)
func (m *Message) Ignore() {
	if m.shouldSendDisposition() {
		m.done()
	}
}

// MarshalBinary encodes the message into binary form.
func (m *Message) MarshalBinary() ([]byte, error) {
	buf := new(buffer)
	err := m.marshal(buf)
	return buf.b, err
}

func (m *Message) shouldSendDisposition() bool {
	return !m.settled
}

func (m *Message) marshal(wr *buffer) error {
	if m.Header != nil {
		err := m.Header.marshal(wr)
		if err != nil {
			return err
		}
	}

	if m.DeliveryAnnotations != nil {
		writeDescriptor(wr, typeCodeDeliveryAnnotations)
		err := marshal(wr, m.DeliveryAnnotations)
		if err != nil {
			return err
		}
	}

	if m.Annotations != nil {
		writeDescriptor(wr, typeCodeMessageAnnotations)
		err := marshal(wr, m.Annotations)
		if err != nil {
			return err
		}
	}

	if m.Properties != nil {
		err := marshal(wr, m.Properties)
		if err != nil {
			return err
		}
	}

	if m.ApplicationProperties != nil {
		writeDescriptor(wr, typeCodeApplicationProperties)
		err := marshal(wr, m.ApplicationProperties)
		if err != nil {
			return err
		}
	}

	for _, data := range m.Data {
		writeDescriptor(wr, typeCodeApplicationData)
		err := writeBinary(wr, data)
		if err != nil {
			return err
		}
	}

	if m.Value != nil {
		writeDescriptor(wr, typeCodeAMQPValue)
		err := marshal(wr, m.Value)
		if err != nil {
			return err
		}
	}

	if m.Footer != nil {
		writeDescriptor(wr, typeCodeFooter)
		err := marshal(wr, m.Footer)
		if err != nil {
			return err
		}
	}

	return nil
}

// UnmarshalBinary decodes the message from binary form.
func (m *Message) UnmarshalBinary(data []byte) error {
	buf := &buffer{
		b: data,
	}
	return m.unmarshal(buf)
}

func (m *Message) unmarshal(r *buffer) error {
	// loop, decoding sections until bytes have been consumed
	for r.len() > 0 {
		// determine type
		type_, err := peekMessageType(r.bytes())
		if err != nil {
			return err
		}

		var (
			section interface{}
			// section header is read from r before
			// unmarshaling section is set to true
			discardHeader = true
		)
		switch amqpType(type_) {

		case typeCodeMessageHeader:
			discardHeader = false
			section = &m.Header

		case typeCodeDeliveryAnnotations:
			section = &m.DeliveryAnnotations

		case typeCodeMessageAnnotations:
			section = &m.Annotations

		case typeCodeMessageProperties:
			discardHeader = false
			section = &m.Properties

		case typeCodeApplicationProperties:
			section = &m.ApplicationProperties

		case typeCodeApplicationData:
			r.skip(3)

			var data []byte
			err = unmarshal(r, &data)
			if err != nil {
				return err
			}

			m.Data = append(m.Data, data)
			continue

		case typeCodeFooter:
			section = &m.Footer

		case typeCodeAMQPValue:
			section = &m.Value

		default:
			return errorErrorf("unknown message section %#02x", type_)
		}

		if discardHeader {
			r.skip(3)
		}

		err = unmarshal(r, section)
		if err != nil {
			return err
		}
	}
	return nil
}

// peekMessageType reads the message type without
// modifying any data.
func peekMessageType(buf []byte) (uint8, error) {
	if len(buf) < 3 {
		return 0, errorNew("invalid message")
	}

	if buf[0] != 0 {
		return 0, errorErrorf("invalid composite header %02x", buf[0])
	}

	// copied from readUlong to avoid allocations
	t := amqpType(buf[1])
	if t == typeCodeUlong0 {
		return 0, nil
	}

	if t == typeCodeSmallUlong {
		if len(buf[2:]) == 0 {
			return 0, errorNew("invalid ulong")
		}
		return buf[2], nil
	}

	if t != typeCodeUlong {
		return 0, errorErrorf("invalid type for uint32 %02x", t)
	}

	if len(buf[2:]) < 8 {
		return 0, errorNew("invalid ulong")
	}
	v := binary.BigEndian.Uint64(buf[2:10])

	return uint8(v), nil
}

func tryReadNull(r *buffer) bool {
	if r.len() > 0 && amqpType(r.bytes()[0]) == typeCodeNull {
		r.skip(1)
		return true
	}
	return false
}

// Annotations keys must be of type string, int, or int64.
//
// String keys are encoded as AMQP Symbols.
type Annotations map[interface{}]interface{}

func (a Annotations) marshal(wr *buffer) error {
	return writeMap(wr, a)
}

func (a *Annotations) unmarshal(r *buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	m := make(Annotations, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readAny(r)
		if err != nil {
			return err
		}
		value, err := readAny(r)
		if err != nil {
			return err
		}
		m[key] = value
	}
	*a = m
	return nil
}

/*
<type name="header" class="composite" source="list" provides="section">
    <descriptor name="amqp:header:list" code="0x00000000:0x00000070"/>
    <field name="durable" type="boolean" default="false"/>
    <field name="priority" type="ubyte" default="4"/>
    <field name="ttl" type="milliseconds"/>
    <field name="first-acquirer" type="boolean" default="false"/>
    <field name="delivery-count" type="uint" default="0"/>
</type>
*/

// MessageHeader carries standard delivery details about the transfer
// of a message.
type MessageHeader struct {
	Durable       bool
	Priority      uint8
	TTL           time.Duration // from milliseconds
	FirstAcquirer bool
	DeliveryCount uint32
}

func (h *MessageHeader) marshal(wr *buffer) error {
	return marshalComposite(wr, typeCodeMessageHeader, []marshalField{
		{value: &h.Durable, omit: !h.Durable},
		{value: &h.Priority, omit: h.Priority == 4},
		{value: (*milliseconds)(&h.TTL), omit: h.TTL == 0},
		{value: &h.FirstAcquirer, omit: !h.FirstAcquirer},
		{value: &h.DeliveryCount, omit: h.DeliveryCount == 0},
	})
}

func (h *MessageHeader) unmarshal(r *buffer) error {
	return unmarshalComposite(r, typeCodeMessageHeader, []unmarshalField{
		{field: &h.Durable},
		{field: &h.Priority, handleNull: func() error { h.Priority = 4; return nil }},
		{field: (*milliseconds)(&h.TTL)},
		{field: &h.FirstAcquirer},
		{field: &h.DeliveryCount},
	}...)
}

/*
<type name="properties" class="composite" source="list" provides="section">
    <descriptor name="amqp:properties:list" code="0x00000000:0x00000073"/>
    <field name="message-id" type="*" requires="message-id"/>
    <field name="user-id" type="binary"/>
    <field name="to" type="*" requires="address"/>
    <field name="subject" type="string"/>
    <field name="reply-to" type="*" requires="address"/>
    <field name="correlation-id" type="*" requires="message-id"/>
    <field name="content-type" type="symbol"/>
    <field name="content-encoding" type="symbol"/>
    <field name="absolute-expiry-time" type="timestamp"/>
    <field name="creation-time" type="timestamp"/>
    <field name="group-id" type="string"/>
    <field name="group-sequence" type="sequence-no"/>
    <field name="reply-to-group-id" type="string"/>
</type>
*/

// MessageProperties is the defined set of properties for AMQP messages.
type MessageProperties struct {
	// Message-id, if set, uniquely identifies a message within the message system.
	// The message producer is usually responsible for setting the message-id in
	// such a way that it is assured to be globally unique. A broker MAY discard a
	// message as a duplicate if the value of the message-id matches that of a
	// previously received message sent to the same node.
	MessageID interface{} // uint64, UUID, []byte, or string

	// The identity of the user responsible for producing the message.
	// The client sets this value, and it MAY be authenticated by intermediaries.
	UserID []byte

	// The to field identifies the node that is the intended destination of the message.
	// On any given transfer this might not be the node at the receiving end of the link.
	To string

	// A common field for summary information about the message content and purpose.
	Subject string

	// The address of the node to send replies to.
	ReplyTo string

	// This is a client-specific id that can be used to mark or identify messages
	// between clients.
	CorrelationID interface{} // uint64, UUID, []byte, or string

	// The RFC-2046 [RFC2046] MIME type for the message's application-data section
	// (body). As per RFC-2046 [RFC2046] this can contain a charset parameter defining
	// the character encoding used: e.g., 'text/plain; charset="utf-8"'.
	//
	// For clarity, as per section 7.2.1 of RFC-2616 [RFC2616], where the content type
	// is unknown the content-type SHOULD NOT be set. This allows the recipient the
	// opportunity to determine the actual type. Where the section is known to be truly
	// opaque binary data, the content-type SHOULD be set to application/octet-stream.
	//
	// When using an application-data section with a section code other than data,
	// content-type SHOULD NOT be set.
	ContentType string

	// The content-encoding property is used as a modifier to the content-type.
	// When present, its value indicates what additional content encodings have been
	// applied to the application-data, and thus what decoding mechanisms need to be
	// applied in order to obtain the media-type referenced by the content-type header
	// field.
	//
	// Content-encoding is primarily used to allow a document to be compressed without
	// losing the identity of its underlying content type.
	//
	// Content-encodings are to be interpreted as per section 3.5 of RFC 2616 [RFC2616].
	// Valid content-encodings are registered at IANA [IANAHTTPPARAMS].
	//
	// The content-encoding MUST NOT be set when the application-data section is other
	// than data. The binary representation of all other application-data section types
	// is defined completely in terms of the AMQP type system.
	//
	// Implementations MUST NOT use the identity encoding. Instead, implementations
	// SHOULD NOT set this property. Implementations SHOULD NOT use the compress encoding,
	// except as to remain compatible with messages originally sent with other protocols,
	// e.g. HTTP or SMTP.
	//
	// Implementations SHOULD NOT specify multiple content-encoding values except as to
	// be compatible with messages originally sent with other protocols, e.g. HTTP or SMTP.
	ContentEncoding string

	// An absolute time when this message is considered to be expired.
	AbsoluteExpiryTime time.Time

	// An absolute time when this message was created.
	CreationTime time.Time

	// Identifies the group the message belongs to.
	GroupID string

	// The relative position of this message within its group.
	GroupSequence uint32 // RFC-1982 sequence number

	// This is a client-specific id that is used so that client can send replies to this
	// message to a specific group.
	ReplyToGroupID string
}

func (p *MessageProperties) marshal(wr *buffer) error {
	return marshalComposite(wr, typeCodeMessageProperties, []marshalField{
		{value: p.MessageID, omit: p.MessageID == nil},
		{value: &p.UserID, omit: len(p.UserID) == 0},
		{value: &p.To, omit: p.To == ""},
		{value: &p.Subject, omit: p.Subject == ""},
		{value: &p.ReplyTo, omit: p.ReplyTo == ""},
		{value: p.CorrelationID, omit: p.CorrelationID == nil},
		{value: (*symbol)(&p.ContentType), omit: p.ContentType == ""},
		{value: (*symbol)(&p.ContentEncoding), omit: p.ContentEncoding == ""},
		{value: &p.AbsoluteExpiryTime, omit: p.AbsoluteExpiryTime.IsZero()},
		{value: &p.CreationTime, omit: p.CreationTime.IsZero()},
		{value: &p.GroupID, omit: p.GroupID == ""},
		{value: &p.GroupSequence},
		{value: &p.ReplyToGroupID, omit: p.ReplyToGroupID == ""},
	})
}

func (p *MessageProperties) unmarshal(r *buffer) error {
	return unmarshalComposite(r, typeCodeMessageProperties, []unmarshalField{
		{field: &p.MessageID},
		{field: &p.UserID},
		{field: &p.To},
		{field: &p.Subject},
		{field: &p.ReplyTo},
		{field: &p.CorrelationID},
		{field: &p.ContentType},
		{field: &p.ContentEncoding},
		{field: &p.AbsoluteExpiryTime},
		{field: &p.CreationTime},
		{field: &p.GroupID},
		{field: &p.GroupSequence},
		{field: &p.ReplyToGroupID},
	}...)
}

/*
<type name="received" class="composite" source="list" provides="delivery-state">
    <descriptor name="amqp:received:list" code="0x00000000:0x00000023"/>
    <field name="section-number" type="uint" mandatory="true"/>
    <field name="section-offset" type="ulong" mandatory="true"/>
</type>
*/

type stateReceived struct {
	// When sent by the sender this indicates the first section of the message
	// (with section-number 0 being the first section) for which data can be resent.
	// Data from sections prior to the given section cannot be retransmitted for
	// this delivery.
	//
	// When sent by the receiver this indicates the first section of the message
	// for which all data might not yet have been received.
	SectionNumber uint32

	// When sent by the sender this indicates the first byte of the encoded section
	// data of the section given by section-number for which data can be resent
	// (with section-offset 0 being the first byte). Bytes from the same section
	// prior to the given offset section cannot be retransmitted for this delivery.
	//
	// When sent by the receiver this indicates the first byte of the given section
	// which has not yet been received. Note that if a receiver has received all of
	// section number X (which contains N bytes of data), but none of section number
	// X + 1, then it can indicate this by sending either Received(section-number=X,
	// section-offset=N) or Received(section-number=X+1, section-offset=0). The state
	// Received(section-number=0, section-offset=0) indicates that no message data
	// at all has been transferred.
	SectionOffset uint64
}

func (sr *stateReceived) marshal(wr *buffer) error {
	return marshalComposite(wr, typeCodeStateReceived, []marshalField{
		{value: &sr.SectionNumber, omit: false},
		{value: &sr.SectionOffset, omit: false},
	})
}

func (sr *stateReceived) unmarshal(r *buffer) error {
	return unmarshalComposite(r, typeCodeStateReceived, []unmarshalField{
		{field: &sr.SectionNumber, handleNull: func() error { return errorNew("StateReceiver.SectionNumber is required") }},
		{field: &sr.SectionOffset, handleNull: func() error { return errorNew("StateReceiver.SectionOffset is required") }},
	}...)
}

/*
<type name="accepted" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:accepted:list" code="0x00000000:0x00000024"/>
</type>
*/

type stateAccepted struct{}

func (sa *stateAccepted) marshal(wr *buffer) error {
	return marshalComposite(wr, typeCodeStateAccepted, nil)
}

func (sa *stateAccepted) unmarshal(r *buffer) error {
	return unmarshalComposite(r, typeCodeStateAccepted)
}

func (sa *stateAccepted) String() string {
	return "Accepted"
}

/*
<type name="rejected" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:rejected:list" code="0x00000000:0x00000025"/>
    <field name="error" type="error"/>
</type>
*/

type stateRejected struct {
	Error *Error
}

func (sr *stateRejected) marshal(wr *buffer) error {
	return marshalComposite(wr, typeCodeStateRejected, []marshalField{
		{value: sr.Error, omit: sr.Error == nil},
	})
}

func (sr *stateRejected) unmarshal(r *buffer) error {
	return unmarshalComposite(r, typeCodeStateRejected,
		unmarshalField{field: &sr.Error},
	)
}

func (sr *stateRejected) String() string {
	return fmt.Sprintf("Rejected{Error: %v}", sr.Error)
}

/*
<type name="released" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:released:list" code="0x00000000:0x00000026"/>
</type>
*/

type stateReleased struct{}

func (sr *stateReleased) marshal(wr *buffer) error {
	return marshalComposite(wr, typeCodeStateReleased, nil)
}

func (sr *stateReleased) unmarshal(r *buffer) error {
	return unmarshalComposite(r, typeCodeStateReleased)
}

func (sr *stateReleased) String() string {
	return "Released"
}

/*
<type name="modified" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:modified:list" code="0x00000000:0x00000027"/>
    <field name="delivery-failed" type="boolean"/>
    <field name="undeliverable-here" type="boolean"/>
    <field name="message-annotations" type="fields"/>
</type>
*/

type stateModified struct {
	// count the transfer as an unsuccessful delivery attempt
	//
	// If the delivery-failed flag is set, any messages modified
	// MUST have their delivery-count incremented.
	DeliveryFailed bool

	// prevent redelivery
	//
	// If the undeliverable-here is set, then any messages released MUST NOT
	// be redelivered to the modifying link endpoint.
	UndeliverableHere bool

	// message attributes
	// Map containing attributes to combine with the existing message-annotations
	// held in the message's header section. Where the existing message-annotations
	// of the message contain an entry with the same key as an entry in this field,
	// the value in this field associated with that key replaces the one in the
	// existing headers; where the existing message-annotations has no such value,
	// the value in this map is added.
	MessageAnnotations Annotations
}

func (sm *stateModified) marshal(wr *buffer) error {
	return marshalComposite(wr, typeCodeStateModified, []marshalField{
		{value: &sm.DeliveryFailed, omit: !sm.DeliveryFailed},
		{value: &sm.UndeliverableHere, omit: !sm.UndeliverableHere},
		{value: sm.MessageAnnotations, omit: sm.MessageAnnotations == nil},
	})
}

func (sm *stateModified) unmarshal(r *buffer) error {
	return unmarshalComposite(r, typeCodeStateModified, []unmarshalField{
		{field: &sm.DeliveryFailed},
		{field: &sm.UndeliverableHere},
		{field: &sm.MessageAnnotations},
	}...)
}

func (sm *stateModified) String() string {
	return fmt.Sprintf("Modified{DeliveryFailed: %t, UndeliverableHere: %t, MessageAnnotations: %v}", sm.DeliveryFailed, sm.UndeliverableHere, sm.MessageAnnotations)
}

/*
<type name="sasl-init" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-init:list" code="0x00000000:0x00000041"/>
    <field name="mechanism" type="symbol" mandatory="true"/>
    <field name="initial-response" type="binary"/>
    <field name="hostname" type="string"/>
</type>
*/

// symbol is an AMQP symbolic string.
type symbol string

func (s symbol) marshal(wr *buffer) error {
	l := len(s)
	switch {
	// Sym8
	case l < 256:
		wr.write([]byte{
			byte(typeCodeSym8),
			byte(l),
		})
		wr.writeString(string(s))

	// Sym32
	case uint(l) < math.MaxUint32:
		wr.writeByte(uint8(typeCodeSym32))
		wr.writeUint32(uint32(l))
		wr.writeString(string(s))
	default:
		return errorNew("too long")
	}
	return nil
}

type milliseconds time.Duration

func (m milliseconds) marshal(wr *buffer) error {
	writeUint32(wr, uint32(m/milliseconds(time.Millisecond)))
	return nil
}

func (m *milliseconds) unmarshal(r *buffer) error {
	n, err := readUint(r)
	*m = milliseconds(time.Duration(n) * time.Millisecond)
	return err
}

// mapAnyAny is used to decode AMQP maps who's keys are undefined or
// inconsistently typed.
type mapAnyAny map[interface{}]interface{}

func (m mapAnyAny) marshal(wr *buffer) error {
	return writeMap(wr, map[interface{}]interface{}(m))
}

func (m *mapAnyAny) unmarshal(r *buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	mm := make(mapAnyAny, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readAny(r)
		if err != nil {
			return err
		}
		value, err := readAny(r)
		if err != nil {
			return err
		}

		// https://golang.org/ref/spec#Map_types:
		// The comparison operators == and != must be fully defined
		// for operands of the key type; thus the key type must not
		// be a function, map, or slice.
		switch reflect.ValueOf(key).Kind() {
		case reflect.Slice, reflect.Func, reflect.Map:
			return errorNew("invalid map key")
		}

		mm[key] = value
	}
	*m = mm
	return nil
}

// mapStringAny is used to decode AMQP maps that have string keys
type mapStringAny map[string]interface{}

func (m mapStringAny) marshal(wr *buffer) error {
	return writeMap(wr, map[string]interface{}(m))
}

func (m *mapStringAny) unmarshal(r *buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	mm := make(mapStringAny, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readString(r)
		if err != nil {
			return err
		}
		value, err := readAny(r)
		if err != nil {
			return err
		}
		mm[key] = value
	}
	*m = mm

	return nil
}

// mapStringAny is used to decode AMQP maps that have Symbol keys
type mapSymbolAny map[symbol]interface{}

func (m mapSymbolAny) marshal(wr *buffer) error {
	return writeMap(wr, map[symbol]interface{}(m))
}

func (m *mapSymbolAny) unmarshal(r *buffer) error {
	count, err := readMapHeader(r)
	if err != nil {
		return err
	}

	mm := make(mapSymbolAny, count/2)
	for i := uint32(0); i < count; i += 2 {
		key, err := readString(r)
		if err != nil {
			return err
		}
		value, err := readAny(r)
		if err != nil {
			return err
		}
		mm[symbol(key)] = value
	}
	*m = mm
	return nil
}

// UUID is a 128 bit identifier as defined in RFC 4122.
type UUID [16]byte

// String returns the hex encoded representation described in RFC 4122, Section 3.
func (u UUID) String() string {
	var buf [36]byte
	hex.Encode(buf[:8], u[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])
	return string(buf[:])
}

func (u UUID) marshal(wr *buffer) error {
	wr.writeByte(byte(typeCodeUUID))
	wr.write(u[:])
	return nil
}

func (u *UUID) unmarshal(r *buffer) error {
	un, err := readUUID(r)
	*u = un
	return err
}

type lifetimePolicy uint8

const (
	deleteOnClose             = lifetimePolicy(typeCodeDeleteOnClose)
	deleteOnNoLinks           = lifetimePolicy(typeCodeDeleteOnNoLinks)
	deleteOnNoMessages        = lifetimePolicy(typeCodeDeleteOnNoMessages)
	deleteOnNoLinksOrMessages = lifetimePolicy(typeCodeDeleteOnNoLinksOrMessages)
)

func (p lifetimePolicy) marshal(wr *buffer) error {
	wr.write([]byte{
		0x0,
		byte(typeCodeSmallUlong),
		byte(p),
		byte(typeCodeList0),
	})
	return nil
}

func (p *lifetimePolicy) unmarshal(r *buffer) error {
	typ, fields, err := readCompositeHeader(r)
	if err != nil {
		return err
	}
	if fields != 0 {
		return errorErrorf("invalid size %d for lifetime-policy")
	}
	*p = lifetimePolicy(typ)
	return nil
}

// Sender Settlement Modes
const (
	// Sender will send all deliveries initially unsettled to the receiver.
	ModeUnsettled SenderSettleMode = 0

	// Sender will send all deliveries settled to the receiver.
	ModeSettled SenderSettleMode = 1

	// Sender MAY send a mixture of settled and unsettled deliveries to the receiver.
	ModeMixed SenderSettleMode = 2
)

// SenderSettleMode specifies how the sender will settle messages.
type SenderSettleMode uint8

func (m *SenderSettleMode) String() string {
	if m == nil {
		return "<nil>"
	}

	switch *m {
	case ModeUnsettled:
		return "unsettled"

	case ModeSettled:
		return "settled"

	case ModeMixed:
		return "mixed"

	default:
		return fmt.Sprintf("unknown sender mode %d", uint8(*m))
	}
}

func (m SenderSettleMode) marshal(wr *buffer) error {
	return marshal(wr, uint8(m))
}

func (m *SenderSettleMode) unmarshal(r *buffer) error {
	n, err := readUbyte(r)
	*m = SenderSettleMode(n)
	return err
}

// Receiver Settlement Modes
const (
	// Receiver will spontaneously settle all incoming transfers.
	ModeFirst ReceiverSettleMode = 0

	// Receiver will only settle after sending the disposition to the
	// sender and receiving a disposition indicating settlement of
	// the delivery from the sender.
	ModeSecond ReceiverSettleMode = 1
)

// ReceiverSettleMode specifies how the receiver will settle messages.
type ReceiverSettleMode uint8

func (m *ReceiverSettleMode) String() string {
	if m == nil {
		return "<nil>"
	}

	switch *m {
	case ModeFirst:
		return "first"

	case ModeSecond:
		return "second"

	default:
		return fmt.Sprintf("unknown receiver mode %d", uint8(*m))
	}
}

func (m ReceiverSettleMode) marshal(wr *buffer) error {
	return marshal(wr, uint8(m))
}

func (m *ReceiverSettleMode) unmarshal(r *buffer) error {
	n, err := readUbyte(r)
	*m = ReceiverSettleMode(n)
	return err
}

// Durability Policies
const (
	// No terminus state is retained durably.
	DurabilityNone Durability = 0

	// Only the existence and configuration of the terminus is
	// retained durably.
	DurabilityConfiguration Durability = 1

	// In addition to the existence and configuration of the
	// terminus, the unsettled state for durable messages is
	// retained durably.
	DurabilityUnsettledState Durability = 2
)

// Durability specifies the durability of a link.
type Durability uint32

func (d *Durability) String() string {
	if d == nil {
		return "<nil>"
	}

	switch *d {
	case DurabilityNone:
		return "none"
	case DurabilityConfiguration:
		return "configuration"
	case DurabilityUnsettledState:
		return "unsettled-state"
	default:
		return fmt.Sprintf("unknown durability %d", *d)
	}
}

func (d Durability) marshal(wr *buffer) error {
	return marshal(wr, uint32(d))
}

func (d *Durability) unmarshal(r *buffer) error {
	return unmarshal(r, (*uint32)(d))
}

// Expiry Policies
const (
	// The expiry timer starts when terminus is detached.
	ExpiryLinkDetach ExpiryPolicy = "link-detach"

	// The expiry timer starts when the most recently
	// associated session is ended.
	ExpirySessionEnd ExpiryPolicy = "session-end"

	// The expiry timer starts when most recently associated
	// connection is closed.
	ExpiryConnectionClose ExpiryPolicy = "connection-close"

	// The terminus never expires.
	ExpiryNever ExpiryPolicy = "never"
)

// ExpiryPolicy specifies when the expiry timer of a terminus
// starts counting down from the timeout value.
//
// If the link is subsequently re-attached before the terminus is expired,
// then the count down is aborted. If the conditions for the
// terminus-expiry-policy are subsequently re-met, the expiry timer restarts
// from its originally configured timeout value.
type ExpiryPolicy symbol

func (e ExpiryPolicy) validate() error {
	switch e {
	case ExpiryLinkDetach,
		ExpirySessionEnd,
		ExpiryConnectionClose,
		ExpiryNever:
		return nil
	default:
		return errorErrorf("unknown expiry-policy %q", e)
	}
}

func (e ExpiryPolicy) marshal(wr *buffer) error {
	return symbol(e).marshal(wr)
}

func (e *ExpiryPolicy) unmarshal(r *buffer) error {
	err := unmarshal(r, (*symbol)(e))
	if err != nil {
		return err
	}
	return e.validate()
}

func (e *ExpiryPolicy) String() string {
	if e == nil {
		return "<nil>"
	}
	return string(*e)
}

type describedType struct {
	descriptor interface{}
	value      interface{}
}

func (t describedType) marshal(wr *buffer) error {
	wr.writeByte(0x0) // descriptor constructor
	err := marshal(wr, t.descriptor)
	if err != nil {
		return err
	}
	return marshal(wr, t.value)
}

func (t *describedType) unmarshal(r *buffer) error {
	b, err := r.readByte()
	if err != nil {
		return err
	}

	if b != 0x0 {
		return errorErrorf("invalid described type header %02x", b)
	}

	err = unmarshal(r, &t.descriptor)
	if err != nil {
		return err
	}
	return unmarshal(r, &t.value)
}

func (t describedType) String() string {
	return fmt.Sprintf("describedType{descriptor: %v, value: %v}",
		t.descriptor,
		t.value,
	)
}

// SLICES

// ArrayUByte allows encoding []uint8/[]byte as an array
// rather than binary data.
type ArrayUByte []uint8

func (a ArrayUByte) marshal(wr *buffer) error {
	const typeSize = 1

	writeArrayHeader(wr, len(a), typeSize, typeCodeUbyte)
	wr.write(a)

	return nil
}

func (a *ArrayUByte) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	if type_ != typeCodeUbyte {
		return errorErrorf("invalid type for []uint16 %02x", type_)
	}

	buf, ok := r.next(length)
	if !ok {
		return errorErrorf("invalid length %d", length)
	}
	*a = append([]byte(nil), buf...)

	return nil
}

type arrayInt8 []int8

func (a arrayInt8) marshal(wr *buffer) error {
	const typeSize = 1

	writeArrayHeader(wr, len(a), typeSize, typeCodeByte)

	for _, value := range a {
		wr.writeByte(uint8(value))
	}

	return nil
}

func (a *arrayInt8) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	if type_ != typeCodeByte {
		return errorErrorf("invalid type for []uint16 %02x", type_)
	}

	buf, ok := r.next(length)
	if !ok {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]int8, length)
	} else {
		aa = aa[:length]
	}

	for i, value := range buf {
		aa[i] = int8(value)
	}

	*a = aa
	return nil
}

type arrayUint16 []uint16

func (a arrayUint16) marshal(wr *buffer) error {
	const typeSize = 2

	writeArrayHeader(wr, len(a), typeSize, typeCodeUshort)

	for _, element := range a {
		wr.writeUint16(element)
	}

	return nil
}

func (a *arrayUint16) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	if type_ != typeCodeUshort {
		return errorErrorf("invalid type for []uint16 %02x", type_)
	}

	const typeSize = 2
	buf, ok := r.next(length * typeSize)
	if !ok {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]uint16, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		aa[i] = binary.BigEndian.Uint16(buf[bufIdx:])
		bufIdx += 2
	}

	*a = aa
	return nil
}

type arrayInt16 []int16

func (a arrayInt16) marshal(wr *buffer) error {
	const typeSize = 2

	writeArrayHeader(wr, len(a), typeSize, typeCodeShort)

	for _, element := range a {
		wr.writeUint16(uint16(element))
	}

	return nil
}

func (a *arrayInt16) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	if type_ != typeCodeShort {
		return errorErrorf("invalid type for []uint16 %02x", type_)
	}

	const typeSize = 2
	buf, ok := r.next(length * typeSize)
	if !ok {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]int16, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		aa[i] = int16(binary.BigEndian.Uint16(buf[bufIdx : bufIdx+2]))
		bufIdx += 2
	}

	*a = aa
	return nil
}

type arrayUint32 []uint32

func (a arrayUint32) marshal(wr *buffer) error {
	var (
		typeSize = 1
		typeCode = typeCodeSmallUint
	)
	for _, n := range a {
		if n > math.MaxUint8 {
			typeSize = 4
			typeCode = typeCodeUint
			break
		}
	}

	writeArrayHeader(wr, len(a), typeSize, typeCode)

	if typeCode == typeCodeUint {
		for _, element := range a {
			wr.writeUint32(element)
		}
	} else {
		for _, element := range a {
			wr.writeByte(byte(element))
		}
	}

	return nil
}

func (a *arrayUint32) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]

	type_, err := r.readType()
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeUint0:
		if int64(cap(aa)) < length {
			aa = make([]uint32, length)
		} else {
			aa = aa[:length]
			for i := range aa {
				aa[i] = 0
			}
		}
	case typeCodeSmallUint:
		buf, ok := r.next(length)
		if !ok {
			return errorNew("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]uint32, length)
		} else {
			aa = aa[:length]
		}

		for i, n := range buf {
			aa[i] = uint32(n)
		}
	case typeCodeUint:
		const typeSize = 4
		buf, ok := r.next(length * typeSize)
		if !ok {
			return errorErrorf("invalid length %d", length)
		}

		if int64(cap(aa)) < length {
			aa = make([]uint32, length)
		} else {
			aa = aa[:length]
		}

		var bufIdx int
		for i := range aa {
			aa[i] = binary.BigEndian.Uint32(buf[bufIdx : bufIdx+4])
			bufIdx += 4
		}
	default:
		return errorErrorf("invalid type for []uint32 %02x", type_)
	}

	*a = aa
	return nil
}

type arrayInt32 []int32

func (a arrayInt32) marshal(wr *buffer) error {
	var (
		typeSize = 1
		typeCode = typeCodeSmallint
	)
	for _, n := range a {
		if n > math.MaxInt8 {
			typeSize = 4
			typeCode = typeCodeInt
			break
		}
	}

	writeArrayHeader(wr, len(a), typeSize, typeCode)

	if typeCode == typeCodeInt {
		for _, element := range a {
			wr.writeUint32(uint32(element))
		}
	} else {
		for _, element := range a {
			wr.writeByte(byte(element))
		}
	}

	return nil
}

func (a *arrayInt32) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]

	type_, err := r.readType()
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeSmallint:
		buf, ok := r.next(length)
		if !ok {
			return errorNew("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]int32, length)
		} else {
			aa = aa[:length]
		}

		for i, n := range buf {
			aa[i] = int32(int8(n))
		}
	case typeCodeInt:
		const typeSize = 4
		buf, ok := r.next(length * typeSize)
		if !ok {
			return errorErrorf("invalid length %d", length)
		}

		if int64(cap(aa)) < length {
			aa = make([]int32, length)
		} else {
			aa = aa[:length]
		}

		var bufIdx int
		for i := range aa {
			aa[i] = int32(binary.BigEndian.Uint32(buf[bufIdx:]))
			bufIdx += 4
		}
	default:
		return errorErrorf("invalid type for []int32 %02x", type_)
	}

	*a = aa
	return nil
}

type arrayUint64 []uint64

func (a arrayUint64) marshal(wr *buffer) error {
	var (
		typeSize = 1
		typeCode = typeCodeSmallUlong
	)
	for _, n := range a {
		if n > math.MaxUint8 {
			typeSize = 8
			typeCode = typeCodeUlong
			break
		}
	}

	writeArrayHeader(wr, len(a), typeSize, typeCode)

	if typeCode == typeCodeUlong {
		for _, element := range a {
			wr.writeUint64(element)
		}
	} else {
		for _, element := range a {
			wr.writeByte(byte(element))
		}
	}

	return nil
}

func (a *arrayUint64) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]

	type_, err := r.readType()
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeUlong0:
		if int64(cap(aa)) < length {
			aa = make([]uint64, length)
		} else {
			aa = aa[:length]
			for i := range aa {
				aa[i] = 0
			}
		}
	case typeCodeSmallUlong:
		buf, ok := r.next(length)
		if !ok {
			return errorNew("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]uint64, length)
		} else {
			aa = aa[:length]
		}

		for i, n := range buf {
			aa[i] = uint64(n)
		}
	case typeCodeUlong:
		const typeSize = 8
		buf, ok := r.next(length * typeSize)
		if !ok {
			return errorNew("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]uint64, length)
		} else {
			aa = aa[:length]
		}

		var bufIdx int
		for i := range aa {
			aa[i] = binary.BigEndian.Uint64(buf[bufIdx : bufIdx+8])
			bufIdx += 8
		}
	default:
		return errorErrorf("invalid type for []uint64 %02x", type_)
	}

	*a = aa
	return nil
}

type arrayInt64 []int64

func (a arrayInt64) marshal(wr *buffer) error {
	var (
		typeSize = 1
		typeCode = typeCodeSmalllong
	)
	for _, n := range a {
		if n > math.MaxUint8 {
			typeSize = 8
			typeCode = typeCodeLong
			break
		}
	}

	writeArrayHeader(wr, len(a), typeSize, typeCode)

	if typeCode == typeCodeLong {
		for _, element := range a {
			wr.writeUint64(uint64(element))
		}
	} else {
		for _, element := range a {
			wr.writeByte(byte(element))
		}
	}

	return nil
}

func (a *arrayInt64) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]

	type_, err := r.readType()
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeSmalllong:
		buf, ok := r.next(length)
		if !ok {
			return errorNew("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]int64, length)
		} else {
			aa = aa[:length]
		}

		for i, n := range buf {
			aa[i] = int64(int8(n))
		}
	case typeCodeLong:
		const typeSize = 8
		buf, ok := r.next(length * typeSize)
		if !ok {
			return errorNew("invalid length")
		}

		if int64(cap(aa)) < length {
			aa = make([]int64, length)
		} else {
			aa = aa[:length]
		}

		var bufIdx int
		for i := range aa {
			aa[i] = int64(binary.BigEndian.Uint64(buf[bufIdx:]))
			bufIdx += 8
		}
	default:
		return errorErrorf("invalid type for []uint64 %02x", type_)
	}

	*a = aa
	return nil
}

type arrayFloat []float32

func (a arrayFloat) marshal(wr *buffer) error {
	const typeSize = 4

	writeArrayHeader(wr, len(a), typeSize, typeCodeFloat)

	for _, element := range a {
		wr.writeUint32(math.Float32bits(element))
	}

	return nil
}

func (a *arrayFloat) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	if type_ != typeCodeFloat {
		return errorErrorf("invalid type for []float32 %02x", type_)
	}

	const typeSize = 4
	buf, ok := r.next(length * typeSize)
	if !ok {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]float32, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		bits := binary.BigEndian.Uint32(buf[bufIdx:])
		aa[i] = math.Float32frombits(bits)
		bufIdx += typeSize
	}

	*a = aa
	return nil
}

type arrayDouble []float64

func (a arrayDouble) marshal(wr *buffer) error {
	const typeSize = 8

	writeArrayHeader(wr, len(a), typeSize, typeCodeDouble)

	for _, element := range a {
		wr.writeUint64(math.Float64bits(element))
	}

	return nil
}

func (a *arrayDouble) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	if type_ != typeCodeDouble {
		return errorErrorf("invalid type for []float64 %02x", type_)
	}

	const typeSize = 8
	buf, ok := r.next(length * typeSize)
	if !ok {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]float64, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		bits := binary.BigEndian.Uint64(buf[bufIdx:])
		aa[i] = math.Float64frombits(bits)
		bufIdx += typeSize
	}

	*a = aa
	return nil
}

type arrayBool []bool

func (a arrayBool) marshal(wr *buffer) error {
	const typeSize = 1

	writeArrayHeader(wr, len(a), typeSize, typeCodeBool)

	for _, element := range a {
		value := byte(0)
		if element {
			value = 1
		}
		wr.writeByte(value)
	}

	return nil
}

func (a *arrayBool) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]bool, length)
	} else {
		aa = aa[:length]
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeBool:
		buf, ok := r.next(length)
		if !ok {
			return errorNew("invalid length")
		}

		for i, value := range buf {
			if value == 0 {
				aa[i] = false
			} else {
				aa[i] = true
			}
		}

	case typeCodeBoolTrue:
		for i := range aa {
			aa[i] = true
		}
	case typeCodeBoolFalse:
		for i := range aa {
			aa[i] = false
		}
	default:
		return errorErrorf("invalid type for []bool %02x", type_)
	}

	*a = aa
	return nil
}

type arrayString []string

func (a arrayString) marshal(wr *buffer) error {
	var (
		elementType       = typeCodeStr8
		elementsSizeTotal int
	)
	for _, element := range a {
		if !utf8.ValidString(element) {
			return errorNew("not a valid UTF-8 string")
		}

		elementsSizeTotal += len(element)

		if len(element) > math.MaxUint8 {
			elementType = typeCodeStr32
		}
	}

	writeVariableArrayHeader(wr, len(a), elementsSizeTotal, elementType)

	if elementType == typeCodeStr32 {
		for _, element := range a {
			wr.writeUint32(uint32(len(element)))
			wr.writeString(element)
		}
	} else {
		for _, element := range a {
			wr.writeByte(byte(len(element)))
			wr.writeString(element)
		}
	}

	return nil
}

func (a *arrayString) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	const typeSize = 2 // assume all strings are at least 2 bytes
	if length*typeSize > int64(r.len()) {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]string, length)
	} else {
		aa = aa[:length]
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeStr8:
		for i := range aa {
			size, err := r.readByte()
			if err != nil {
				return err
			}

			buf, ok := r.next(int64(size))
			if !ok {
				return errorNew("invalid length")
			}

			aa[i] = string(buf)
		}
	case typeCodeStr32:
		for i := range aa {
			buf, ok := r.next(4)
			if !ok {
				return errorNew("invalid length")
			}
			size := int64(binary.BigEndian.Uint32(buf))

			buf, ok = r.next(size)
			if !ok {
				return errorNew("invalid length")
			}
			aa[i] = string(buf)
		}
	default:
		return errorErrorf("invalid type for []string %02x", type_)
	}

	*a = aa
	return nil
}

type arraySymbol []symbol

func (a arraySymbol) marshal(wr *buffer) error {
	var (
		elementType       = typeCodeSym8
		elementsSizeTotal int
	)
	for _, element := range a {
		elementsSizeTotal += len(element)

		if len(element) > math.MaxUint8 {
			elementType = typeCodeSym32
		}
	}

	writeVariableArrayHeader(wr, len(a), elementsSizeTotal, elementType)

	if elementType == typeCodeSym32 {
		for _, element := range a {
			wr.writeUint32(uint32(len(element)))
			wr.writeString(string(element))
		}
	} else {
		for _, element := range a {
			wr.writeByte(byte(len(element)))
			wr.writeString(string(element))
		}
	}

	return nil
}

func (a *arraySymbol) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	const typeSize = 2 // assume all symbols are at least 2 bytes
	if length*typeSize > int64(r.len()) {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]symbol, length)
	} else {
		aa = aa[:length]
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeSym8:
		for i := range aa {
			size, err := r.readByte()
			if err != nil {
				return err
			}

			buf, ok := r.next(int64(size))
			if !ok {
				return errorNew("invalid length")
			}
			aa[i] = symbol(buf)
		}
	case typeCodeSym32:
		for i := range aa {
			buf, ok := r.next(4)
			if !ok {
				return errorNew("invalid length")
			}
			size := int64(binary.BigEndian.Uint32(buf))

			buf, ok = r.next(size)
			if !ok {
				return errorNew("invalid length")
			}
			aa[i] = symbol(buf)
		}
	default:
		return errorErrorf("invalid type for []symbol %02x", type_)
	}

	*a = aa
	return nil
}

type arrayBinary [][]byte

func (a arrayBinary) marshal(wr *buffer) error {
	var (
		elementType       = typeCodeVbin8
		elementsSizeTotal int
	)
	for _, element := range a {
		elementsSizeTotal += len(element)

		if len(element) > math.MaxUint8 {
			elementType = typeCodeVbin32
		}
	}

	writeVariableArrayHeader(wr, len(a), elementsSizeTotal, elementType)

	if elementType == typeCodeVbin32 {
		for _, element := range a {
			wr.writeUint32(uint32(len(element)))
			wr.write(element)
		}
	} else {
		for _, element := range a {
			wr.writeByte(byte(len(element)))
			wr.write(element)
		}
	}

	return nil
}

func (a *arrayBinary) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	const typeSize = 2 // assume all binary is at least 2 bytes
	if length*typeSize > int64(r.len()) {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([][]byte, length)
	} else {
		aa = aa[:length]
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	switch type_ {
	case typeCodeVbin8:
		for i := range aa {
			size, err := r.readByte()
			if err != nil {
				return err
			}

			buf, ok := r.next(int64(size))
			if !ok {
				return errorErrorf("invalid length %d", length)
			}
			aa[i] = append([]byte(nil), buf...)
		}
	case typeCodeVbin32:
		for i := range aa {
			buf, ok := r.next(4)
			if !ok {
				return errorNew("invalid length")
			}
			size := binary.BigEndian.Uint32(buf)

			buf, ok = r.next(int64(size))
			if !ok {
				return errorNew("invalid length")
			}
			aa[i] = append([]byte(nil), buf...)
		}
	default:
		return errorErrorf("invalid type for [][]byte %02x", type_)
	}

	*a = aa
	return nil
}

type arrayTimestamp []time.Time

func (a arrayTimestamp) marshal(wr *buffer) error {
	const typeSize = 8

	writeArrayHeader(wr, len(a), typeSize, typeCodeTimestamp)

	for _, element := range a {
		ms := element.UnixNano() / int64(time.Millisecond)
		wr.writeUint64(uint64(ms))
	}

	return nil
}

func (a *arrayTimestamp) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	if type_ != typeCodeTimestamp {
		return errorErrorf("invalid type for []time.Time %02x", type_)
	}

	const typeSize = 8
	buf, ok := r.next(length * typeSize)
	if !ok {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]time.Time, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		ms := int64(binary.BigEndian.Uint64(buf[bufIdx:]))
		bufIdx += typeSize
		aa[i] = time.Unix(ms/1000, (ms%1000)*1000000).UTC()
	}

	*a = aa
	return nil
}

type arrayUUID []UUID

func (a arrayUUID) marshal(wr *buffer) error {
	const typeSize = 16

	writeArrayHeader(wr, len(a), typeSize, typeCodeUUID)

	for _, element := range a {
		wr.write(element[:])
	}

	return nil
}

func (a *arrayUUID) unmarshal(r *buffer) error {
	length, err := readArrayHeader(r)
	if err != nil {
		return err
	}

	type_, err := r.readType()
	if err != nil {
		return err
	}
	if type_ != typeCodeUUID {
		return errorErrorf("invalid type for []UUID %#02x", type_)
	}

	const typeSize = 16
	buf, ok := r.next(length * typeSize)
	if !ok {
		return errorErrorf("invalid length %d", length)
	}

	aa := (*a)[:0]
	if int64(cap(aa)) < length {
		aa = make([]UUID, length)
	} else {
		aa = aa[:length]
	}

	var bufIdx int
	for i := range aa {
		copy(aa[i][:], buf[bufIdx:bufIdx+16])
		bufIdx += 16
	}

	*a = aa
	return nil
}

// LIST

type list []interface{}

func (l list) marshal(wr *buffer) error {
	length := len(l)

	// type
	if length == 0 {
		wr.writeByte(byte(typeCodeList0))
		return nil
	}
	wr.writeByte(byte(typeCodeList32))

	// size
	sizeIdx := wr.len()
	wr.write([]byte{0, 0, 0, 0})

	// length
	wr.writeUint32(uint32(length))

	for _, element := range l {
		err := marshal(wr, element)
		if err != nil {
			return err
		}
	}

	// overwrite size
	binary.BigEndian.PutUint32(wr.bytes()[sizeIdx:], uint32(wr.len()-(sizeIdx+4)))

	return nil
}

func (l *list) unmarshal(r *buffer) error {
	length, err := readListHeader(r)
	if err != nil {
		return err
	}

	// assume that all types are at least 1 byte
	if length > int64(r.len()) {
		return errorErrorf("invalid length %d", length)
	}

	ll := *l
	if int64(cap(ll)) < length {
		ll = make([]interface{}, length)
	} else {
		ll = ll[:length]
	}

	for i := range ll {
		ll[i], err = readAny(r)
		if err != nil {
			return err
		}
	}

	*l = ll
	return nil
}
