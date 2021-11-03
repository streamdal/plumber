// Code generated by protoc-gen-go. DO NOT EDIT.
// source: ps_args_rabbit.proto

package args

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type RabbitConn struct {
	// @gotags: kong:"help='Destination host address (full DSN)',env='PLUMBER_RELAY_RABBIT_ADDRESS',default='amqp://localhost',required"
	Address string `protobuf:"bytes,1,opt,name=address,proto3" json:"address,omitempty" kong:"help='Destination host address (full DSN)',env='PLUMBER_RELAY_RABBIT_ADDRESS',default='amqp://localhost',required"`
	// @gotags: kong:"help='Force TLS usage (regardless of DSN)',env='PLUMBER_RELAY_RABBIT_USE_TLS'"
	UseTls bool `protobuf:"varint,2,opt,name=use_tls,json=useTls,proto3" json:"use_tls,omitempty" kong:"help='Force TLS usage (regardless of DSN)',env='PLUMBER_RELAY_RABBIT_USE_TLS'"`
	// @gotags: kong:"help='Whether to verify server TLS certificate',env='PLUMBER_RELAY_RABBIT_SKIP_VERIFY_TLS'"
	InsecureTls          bool     `protobuf:"varint,3,opt,name=insecure_tls,json=insecureTls,proto3" json:"insecure_tls,omitempty" kong:"help='Whether to verify server TLS certificate',env='PLUMBER_RELAY_RABBIT_SKIP_VERIFY_TLS'"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RabbitConn) Reset()         { *m = RabbitConn{} }
func (m *RabbitConn) String() string { return proto.CompactTextString(m) }
func (*RabbitConn) ProtoMessage()    {}
func (*RabbitConn) Descriptor() ([]byte, []int) {
	return fileDescriptor_01d1eee3dc8ebf97, []int{0}
}

func (m *RabbitConn) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RabbitConn.Unmarshal(m, b)
}
func (m *RabbitConn) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RabbitConn.Marshal(b, m, deterministic)
}
func (m *RabbitConn) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RabbitConn.Merge(m, src)
}
func (m *RabbitConn) XXX_Size() int {
	return xxx_messageInfo_RabbitConn.Size(m)
}
func (m *RabbitConn) XXX_DiscardUnknown() {
	xxx_messageInfo_RabbitConn.DiscardUnknown(m)
}

var xxx_messageInfo_RabbitConn proto.InternalMessageInfo

func (m *RabbitConn) GetAddress() string {
	if m != nil {
		return m.Address
	}
	return ""
}

func (m *RabbitConn) GetUseTls() bool {
	if m != nil {
		return m.UseTls
	}
	return false
}

func (m *RabbitConn) GetInsecureTls() bool {
	if m != nil {
		return m.InsecureTls
	}
	return false
}

type RabbitReadArgs struct {
	// @gotags: kong:"help='Name of the exchange',env='PLUMBER_RELAY_RABBIT_EXCHANGE',required"
	ExchangeName string `protobuf:"bytes,1,opt,name=exchange_name,json=exchangeName,proto3" json:"exchange_name,omitempty" kong:"help='Name of the exchange',env='PLUMBER_RELAY_RABBIT_EXCHANGE',required"`
	// @gotags: kong:"help='Name of the queue where messages will be routed to',env='PLUMBER_RELAY_RABBIT_QUEUE',required"
	QueueName string `protobuf:"bytes,2,opt,name=queue_name,json=queueName,proto3" json:"queue_name,omitempty" kong:"help='Name of the queue where messages will be routed to',env='PLUMBER_RELAY_RABBIT_QUEUE',required"`
	// @gotags: kong:"help='Binding key for topic based exchanges',env='PLUMBER_RELAY_RABBIT_ROUTING_KEY',required"
	BindingKey string `protobuf:"bytes,3,opt,name=binding_key,json=bindingKey,proto3" json:"binding_key,omitempty" kong:"help='Binding key for topic based exchanges',env='PLUMBER_RELAY_RABBIT_ROUTING_KEY',required"`
	// @gotags: kong:"help='Whether plumber should be the only one using the queue',env='PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE'"
	QueueExclusive bool `protobuf:"varint,4,opt,name=queue_exclusive,json=queueExclusive,proto3" json:"queue_exclusive,omitempty" kong:"help='Whether plumber should be the only one using the queue',env='PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE'"`
	// @gotags: kong:"help='Whether to create/declare the queue (if it does not exist)',env='PLUMBER_RELAY_RABBIT_QUEUE_DECLARE',default=true"
	QueueDeclare bool `protobuf:"varint,5,opt,name=queue_declare,json=queueDeclare,proto3" json:"queue_declare,omitempty" kong:"help='Whether to create/declare the queue (if it does not exist)',env='PLUMBER_RELAY_RABBIT_QUEUE_DECLARE',default=true"`
	// @gotags: kong:"help='Whether the queue should survive after disconnect',env='PLUMBER_RELAY_RABBIT_QUEUE_DURABLE'"
	QueueDurable bool `protobuf:"varint,6,opt,name=queue_durable,json=queueDurable,proto3" json:"queue_durable,omitempty" kong:"help='Whether the queue should survive after disconnect',env='PLUMBER_RELAY_RABBIT_QUEUE_DURABLE'"`
	// @gotags: kong:"help='Automatically acknowledge receipt of read/received messages',env='PLUMBER_RELAY_RABBIT_AUTOACK',default=true"
	AutoAck bool `protobuf:"varint,7,opt,name=auto_ack,json=autoAck,proto3" json:"auto_ack,omitempty" kong:"help='Automatically acknowledge receipt of read/received messages',env='PLUMBER_RELAY_RABBIT_AUTOACK',default=true"`
	// @gotags: kong:"help='How to identify the consumer to RabbitMQ',env='PLUMBER_RELAY_CONSUMER_TAG',default=plumber"
	ConsumerTag string `protobuf:"bytes,8,opt,name=consumer_tag,json=consumerTag,proto3" json:"consumer_tag,omitempty" kong:"help='How to identify the consumer to RabbitMQ',env='PLUMBER_RELAY_CONSUMER_TAG',default=plumber"`
	// @gotags: kong:"help='Whether to auto-delete the queue after plumber has disconnected',env='PLUMBER_RELAY_RABBIT_QUEUE_AUTO_DELETE',default=true"
	QueueDelete          bool     `protobuf:"varint,9,opt,name=queue_delete,json=queueDelete,proto3" json:"queue_delete,omitempty" kong:"help='Whether to auto-delete the queue after plumber has disconnected',env='PLUMBER_RELAY_RABBIT_QUEUE_AUTO_DELETE',default=true"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RabbitReadArgs) Reset()         { *m = RabbitReadArgs{} }
func (m *RabbitReadArgs) String() string { return proto.CompactTextString(m) }
func (*RabbitReadArgs) ProtoMessage()    {}
func (*RabbitReadArgs) Descriptor() ([]byte, []int) {
	return fileDescriptor_01d1eee3dc8ebf97, []int{1}
}

func (m *RabbitReadArgs) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RabbitReadArgs.Unmarshal(m, b)
}
func (m *RabbitReadArgs) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RabbitReadArgs.Marshal(b, m, deterministic)
}
func (m *RabbitReadArgs) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RabbitReadArgs.Merge(m, src)
}
func (m *RabbitReadArgs) XXX_Size() int {
	return xxx_messageInfo_RabbitReadArgs.Size(m)
}
func (m *RabbitReadArgs) XXX_DiscardUnknown() {
	xxx_messageInfo_RabbitReadArgs.DiscardUnknown(m)
}

var xxx_messageInfo_RabbitReadArgs proto.InternalMessageInfo

func (m *RabbitReadArgs) GetExchangeName() string {
	if m != nil {
		return m.ExchangeName
	}
	return ""
}

func (m *RabbitReadArgs) GetQueueName() string {
	if m != nil {
		return m.QueueName
	}
	return ""
}

func (m *RabbitReadArgs) GetBindingKey() string {
	if m != nil {
		return m.BindingKey
	}
	return ""
}

func (m *RabbitReadArgs) GetQueueExclusive() bool {
	if m != nil {
		return m.QueueExclusive
	}
	return false
}

func (m *RabbitReadArgs) GetQueueDeclare() bool {
	if m != nil {
		return m.QueueDeclare
	}
	return false
}

func (m *RabbitReadArgs) GetQueueDurable() bool {
	if m != nil {
		return m.QueueDurable
	}
	return false
}

func (m *RabbitReadArgs) GetAutoAck() bool {
	if m != nil {
		return m.AutoAck
	}
	return false
}

func (m *RabbitReadArgs) GetConsumerTag() string {
	if m != nil {
		return m.ConsumerTag
	}
	return ""
}

func (m *RabbitReadArgs) GetQueueDelete() bool {
	if m != nil {
		return m.QueueDelete
	}
	return false
}

type RabbitWriteArgs struct {
	// @gotags: kong:"help='Exchange to write message(s) to',required"
	ExchangeName string `protobuf:"bytes,1,opt,name=exchange_name,json=exchangeName,proto3" json:"exchange_name,omitempty" kong:"help='Exchange to write message(s) to',required"`
	// @gotags: kong:"help='Routing key to write message(s) to',required"
	RoutingKey string `protobuf:"bytes,2,opt,name=routing_key,json=routingKey,proto3" json:"routing_key,omitempty" kong:"help='Routing key to write message(s) to',required"`
	// @gotags: kong:"help='Fills message properties $app_id with this value',default=plumber"
	AppId string `protobuf:"bytes,3,opt,name=app_id,json=appId,proto3" json:"app_id,omitempty" kong:"help='Fills message properties  with this value',default=plumber"`
	// @gotags: kong:"help='The type of exchange we are working with',enum='direct,topic,headers,fanout',default=topic,group=exchange"
	ExchangeType string `protobuf:"bytes,4,opt,name=exchange_type,json=exchangeType,proto3" json:"exchange_type,omitempty" kong:"help='The type of exchange we are working with',enum='direct,topic,headers,fanout',default=topic,group=exchange"`
	// @gotags: kong:"help='Whether to declare an exchange (if it does not exist)',group=exchange"
	ExchangeDeclare bool `protobuf:"varint,5,opt,name=exchange_declare,json=exchangeDeclare,proto3" json:"exchange_declare,omitempty" kong:"help='Whether to declare an exchange (if it does not exist)',group=exchange"`
	// @gotags: kong:"help='Whether to make a declared exchange durable',group=exchange"
	ExchangeDurable bool `protobuf:"varint,6,opt,name=exchange_durable,json=exchangeDurable,proto3" json:"exchange_durable,omitempty" kong:"help='Whether to make a declared exchange durable',group=exchange"`
	// @gotags: kong:"help='Whether to auto-delete the exchange (after writes)',group=exchange"
	ExchangeAutoDelete   bool     `protobuf:"varint,7,opt,name=exchange_auto_delete,json=exchangeAutoDelete,proto3" json:"exchange_auto_delete,omitempty" kong:"help='Whether to auto-delete the exchange (after writes)',group=exchange"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RabbitWriteArgs) Reset()         { *m = RabbitWriteArgs{} }
func (m *RabbitWriteArgs) String() string { return proto.CompactTextString(m) }
func (*RabbitWriteArgs) ProtoMessage()    {}
func (*RabbitWriteArgs) Descriptor() ([]byte, []int) {
	return fileDescriptor_01d1eee3dc8ebf97, []int{2}
}

func (m *RabbitWriteArgs) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RabbitWriteArgs.Unmarshal(m, b)
}
func (m *RabbitWriteArgs) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RabbitWriteArgs.Marshal(b, m, deterministic)
}
func (m *RabbitWriteArgs) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RabbitWriteArgs.Merge(m, src)
}
func (m *RabbitWriteArgs) XXX_Size() int {
	return xxx_messageInfo_RabbitWriteArgs.Size(m)
}
func (m *RabbitWriteArgs) XXX_DiscardUnknown() {
	xxx_messageInfo_RabbitWriteArgs.DiscardUnknown(m)
}

var xxx_messageInfo_RabbitWriteArgs proto.InternalMessageInfo

func (m *RabbitWriteArgs) GetExchangeName() string {
	if m != nil {
		return m.ExchangeName
	}
	return ""
}

func (m *RabbitWriteArgs) GetRoutingKey() string {
	if m != nil {
		return m.RoutingKey
	}
	return ""
}

func (m *RabbitWriteArgs) GetAppId() string {
	if m != nil {
		return m.AppId
	}
	return ""
}

func (m *RabbitWriteArgs) GetExchangeType() string {
	if m != nil {
		return m.ExchangeType
	}
	return ""
}

func (m *RabbitWriteArgs) GetExchangeDeclare() bool {
	if m != nil {
		return m.ExchangeDeclare
	}
	return false
}

func (m *RabbitWriteArgs) GetExchangeDurable() bool {
	if m != nil {
		return m.ExchangeDurable
	}
	return false
}

func (m *RabbitWriteArgs) GetExchangeAutoDelete() bool {
	if m != nil {
		return m.ExchangeAutoDelete
	}
	return false
}

func init() {
	proto.RegisterType((*RabbitConn)(nil), "protos.args.RabbitConn")
	proto.RegisterType((*RabbitReadArgs)(nil), "protos.args.RabbitReadArgs")
	proto.RegisterType((*RabbitWriteArgs)(nil), "protos.args.RabbitWriteArgs")
}

func init() { proto.RegisterFile("ps_args_rabbit.proto", fileDescriptor_01d1eee3dc8ebf97) }

var fileDescriptor_01d1eee3dc8ebf97 = []byte{
	// 450 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x93, 0xcb, 0x8e, 0xd3, 0x30,
	0x14, 0x86, 0xd5, 0xc2, 0xf4, 0x72, 0x52, 0xa6, 0xc8, 0x1a, 0x44, 0x58, 0xa0, 0x19, 0xca, 0x82,
	0x61, 0x41, 0x83, 0xc4, 0x0a, 0xb1, 0x2a, 0x97, 0x05, 0x42, 0x62, 0x11, 0x55, 0x42, 0x62, 0x13,
	0xd9, 0xce, 0x51, 0x6a, 0xd5, 0x89, 0x8d, 0x2f, 0x68, 0xfa, 0x2c, 0x3c, 0x11, 0x6f, 0x85, 0x62,
	0xc7, 0xa5, 0xdd, 0xcd, 0x2a, 0xf2, 0xf7, 0xff, 0xf1, 0x39, 0xfa, 0x24, 0xc3, 0x95, 0xb6, 0x15,
	0x35, 0x8d, 0xad, 0x0c, 0x65, 0x4c, 0xb8, 0xb5, 0x36, 0xca, 0x29, 0x92, 0x85, 0x8f, 0x5d, 0xf7,
	0xc9, 0x8a, 0x01, 0x94, 0x21, 0xfc, 0xa4, 0xba, 0x8e, 0xe4, 0x30, 0xa5, 0x75, 0x6d, 0xd0, 0xda,
	0x7c, 0x74, 0x33, 0xba, 0x9d, 0x97, 0xe9, 0x48, 0x9e, 0xc2, 0xd4, 0x5b, 0xac, 0x9c, 0xb4, 0xf9,
	0xf8, 0x66, 0x74, 0x3b, 0x2b, 0x27, 0xde, 0xe2, 0x56, 0x5a, 0xf2, 0x02, 0x16, 0xa2, 0xb3, 0xc8,
	0xbd, 0x89, 0xe9, 0x83, 0x90, 0x66, 0x89, 0x6d, 0xa5, 0x5d, 0xfd, 0x1d, 0xc3, 0x65, 0x1c, 0x52,
	0x22, 0xad, 0x37, 0xa6, 0xb1, 0xe4, 0x25, 0x3c, 0xc2, 0x3b, 0xbe, 0xa3, 0x5d, 0x83, 0x55, 0x47,
	0x5b, 0x1c, 0xc6, 0x2d, 0x12, 0xfc, 0x4e, 0x5b, 0x24, 0xcf, 0x01, 0x7e, 0x79, 0xf4, 0x43, 0x63,
	0x1c, 0x1a, 0xf3, 0x40, 0x42, 0x7c, 0x0d, 0x19, 0x13, 0x5d, 0x2d, 0xba, 0xa6, 0xda, 0xe3, 0x21,
	0x0c, 0x9e, 0x97, 0x30, 0xa0, 0x6f, 0x78, 0x20, 0xaf, 0x60, 0x19, 0xff, 0xc7, 0x3b, 0x2e, 0xbd,
	0x15, 0xbf, 0x31, 0x7f, 0x18, 0xb6, 0xbb, 0x0c, 0xf8, 0x4b, 0xa2, 0xfd, 0x36, 0xb1, 0x58, 0x23,
	0x97, 0xd4, 0x60, 0x7e, 0x11, 0x6a, 0x8b, 0x00, 0x3f, 0x47, 0x76, 0x52, 0xf2, 0x86, 0x32, 0x89,
	0xf9, 0xe4, 0xb4, 0x14, 0x19, 0x79, 0x06, 0x33, 0xea, 0x9d, 0xaa, 0x28, 0xdf, 0xe7, 0xd3, 0x90,
	0x4f, 0xfb, 0xf3, 0x86, 0xef, 0x7b, 0x51, 0x5c, 0x75, 0xd6, 0xb7, 0x68, 0x2a, 0x47, 0x9b, 0x7c,
	0x16, 0xf6, 0xcd, 0x12, 0xdb, 0xd2, 0xa6, 0xaf, 0xa4, 0x3d, 0x24, 0x3a, 0xcc, 0xe7, 0xd1, 0xe5,
	0xb0, 0x46, 0x8f, 0x56, 0x7f, 0xc6, 0xb0, 0x8c, 0x2e, 0x7f, 0x18, 0xe1, 0xf0, 0xfe, 0x32, 0xaf,
	0x21, 0x33, 0xca, 0xbb, 0x64, 0x2b, 0xda, 0x84, 0x01, 0xf5, 0xb6, 0x9e, 0xc0, 0x84, 0x6a, 0x5d,
	0x89, 0x7a, 0x30, 0x79, 0x41, 0xb5, 0xfe, 0x5a, 0x9f, 0x5d, 0xee, 0x0e, 0x3a, 0x2a, 0x3c, 0xb9,
	0x7c, 0x7b, 0xd0, 0x48, 0x5e, 0xc3, 0xe3, 0x63, 0xe9, 0xdc, 0xe1, 0x32, 0xf1, 0xa4, 0xf1, 0xac,
	0x7a, 0x66, 0xf2, 0x7f, 0x75, 0x90, 0xf9, 0x16, 0xae, 0x8e, 0xd5, 0x60, 0x75, 0xd0, 0x12, 0xc5,
	0x92, 0x94, 0x6d, 0xbc, 0x53, 0xd1, 0xce, 0xc7, 0x0f, 0x3f, 0xdf, 0x37, 0xc2, 0xed, 0x3c, 0x5b,
	0x73, 0xd5, 0x16, 0x8c, 0x3a, 0xbe, 0xe3, 0xca, 0xe8, 0x42, 0x4b, 0xdf, 0x32, 0x34, 0x6f, 0x2c,
	0xdf, 0x61, 0x4b, 0x6d, 0xc1, 0xbc, 0x90, 0x75, 0xd1, 0xa8, 0x22, 0x3e, 0x85, 0xa2, 0x7f, 0x0a,
	0x6c, 0x12, 0x0e, 0xef, 0xfe, 0x05, 0x00, 0x00, 0xff, 0xff, 0xf8, 0xe7, 0x52, 0xcf, 0x36, 0x03,
	0x00, 0x00,
}