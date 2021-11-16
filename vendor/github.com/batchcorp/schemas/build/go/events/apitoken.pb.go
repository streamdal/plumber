// Code generated by protoc-gen-go. DO NOT EDIT.
// source: apitoken.proto

package events

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

// ApiToken message contains info for an API token used to authenticate plumber dynamic destinations
type APIToken struct {
	// Internal database ID of the token
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// The actual token used for authentication
	Token                string   `protobuf:"bytes,2,opt,name=token,proto3" json:"token,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *APIToken) Reset()         { *m = APIToken{} }
func (m *APIToken) String() string { return proto.CompactTextString(m) }
func (*APIToken) ProtoMessage()    {}
func (*APIToken) Descriptor() ([]byte, []int) {
	return fileDescriptor_2671613489b1fb3b, []int{0}
}

func (m *APIToken) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_APIToken.Unmarshal(m, b)
}
func (m *APIToken) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_APIToken.Marshal(b, m, deterministic)
}
func (m *APIToken) XXX_Merge(src proto.Message) {
	xxx_messageInfo_APIToken.Merge(m, src)
}
func (m *APIToken) XXX_Size() int {
	return xxx_messageInfo_APIToken.Size(m)
}
func (m *APIToken) XXX_DiscardUnknown() {
	xxx_messageInfo_APIToken.DiscardUnknown(m)
}

var xxx_messageInfo_APIToken proto.InternalMessageInfo

func (m *APIToken) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *APIToken) GetToken() string {
	if m != nil {
		return m.Token
	}
	return ""
}

func init() {
	proto.RegisterType((*APIToken)(nil), "events.APIToken")
}

func init() { proto.RegisterFile("apitoken.proto", fileDescriptor_2671613489b1fb3b) }

var fileDescriptor_2671613489b1fb3b = []byte{
	// 135 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4b, 0x2c, 0xc8, 0x2c,
	0xc9, 0xcf, 0x4e, 0xcd, 0xd3, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x4b, 0x2d, 0x4b, 0xcd,
	0x2b, 0x29, 0x56, 0x32, 0xe0, 0xe2, 0x70, 0x0c, 0xf0, 0x0c, 0x01, 0xc9, 0x08, 0xf1, 0x71, 0x31,
	0x65, 0xa6, 0x48, 0x30, 0x2a, 0x30, 0x6a, 0x70, 0x06, 0x31, 0x65, 0xa6, 0x08, 0x89, 0x70, 0xb1,
	0x82, 0xb5, 0x48, 0x30, 0x81, 0x85, 0x20, 0x1c, 0x27, 0xbd, 0x28, 0x9d, 0xf4, 0xcc, 0x92, 0x8c,
	0xd2, 0x24, 0xbd, 0xe4, 0xfc, 0x5c, 0xfd, 0xa4, 0xc4, 0x92, 0xe4, 0x8c, 0xe4, 0xfc, 0xa2, 0x02,
	0xfd, 0xe2, 0xe4, 0x8c, 0xd4, 0xdc, 0xc4, 0x62, 0xfd, 0xa4, 0xd2, 0xcc, 0x9c, 0x14, 0xfd, 0xf4,
	0x7c, 0x7d, 0x88, 0x0d, 0x49, 0x6c, 0x60, 0x0b, 0x8d, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff, 0x13,
	0x5f, 0x1a, 0x91, 0x82, 0x00, 0x00, 0x00,
}