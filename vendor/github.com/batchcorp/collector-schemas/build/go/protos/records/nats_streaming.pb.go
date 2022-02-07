// Code generated by protoc-gen-go. DO NOT EDIT.
// source: records/nats_streaming.proto

package records

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

type NATSStreamingRecord struct {
	Channel              string   `protobuf:"bytes,1,opt,name=channel,proto3" json:"channel,omitempty"`
	Value                []byte   `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Sequence             uint64   `protobuf:"varint,3,opt,name=sequence,proto3" json:"sequence,omitempty"`
	Subject              string   `protobuf:"bytes,4,opt,name=subject,proto3" json:"subject,omitempty"`
	Redelivered          bool     `protobuf:"varint,5,opt,name=redelivered,proto3" json:"redelivered,omitempty"`
	RedeliveryCount      uint32   `protobuf:"varint,6,opt,name=redelivery_count,json=redeliveryCount,proto3" json:"redelivery_count,omitempty"`
	Crc32                uint32   `protobuf:"varint,7,opt,name=crc32,proto3" json:"crc32,omitempty"`
	Timestamp            int64    `protobuf:"varint,8,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *NATSStreamingRecord) Reset()         { *m = NATSStreamingRecord{} }
func (m *NATSStreamingRecord) String() string { return proto.CompactTextString(m) }
func (*NATSStreamingRecord) ProtoMessage()    {}
func (*NATSStreamingRecord) Descriptor() ([]byte, []int) {
	return fileDescriptor_15ce22ab495ce0bb, []int{0}
}

func (m *NATSStreamingRecord) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_NATSStreamingRecord.Unmarshal(m, b)
}
func (m *NATSStreamingRecord) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_NATSStreamingRecord.Marshal(b, m, deterministic)
}
func (m *NATSStreamingRecord) XXX_Merge(src proto.Message) {
	xxx_messageInfo_NATSStreamingRecord.Merge(m, src)
}
func (m *NATSStreamingRecord) XXX_Size() int {
	return xxx_messageInfo_NATSStreamingRecord.Size(m)
}
func (m *NATSStreamingRecord) XXX_DiscardUnknown() {
	xxx_messageInfo_NATSStreamingRecord.DiscardUnknown(m)
}

var xxx_messageInfo_NATSStreamingRecord proto.InternalMessageInfo

func (m *NATSStreamingRecord) GetChannel() string {
	if m != nil {
		return m.Channel
	}
	return ""
}

func (m *NATSStreamingRecord) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *NATSStreamingRecord) GetSequence() uint64 {
	if m != nil {
		return m.Sequence
	}
	return 0
}

func (m *NATSStreamingRecord) GetSubject() string {
	if m != nil {
		return m.Subject
	}
	return ""
}

func (m *NATSStreamingRecord) GetRedelivered() bool {
	if m != nil {
		return m.Redelivered
	}
	return false
}

func (m *NATSStreamingRecord) GetRedeliveryCount() uint32 {
	if m != nil {
		return m.RedeliveryCount
	}
	return 0
}

func (m *NATSStreamingRecord) GetCrc32() uint32 {
	if m != nil {
		return m.Crc32
	}
	return 0
}

func (m *NATSStreamingRecord) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func init() {
	proto.RegisterType((*NATSStreamingRecord)(nil), "records.NATSStreamingRecord")
}

func init() { proto.RegisterFile("records/nats_streaming.proto", fileDescriptor_15ce22ab495ce0bb) }

var fileDescriptor_15ce22ab495ce0bb = []byte{
	// 282 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x4c, 0x90, 0x4b, 0x4b, 0x03, 0x31,
	0x14, 0x85, 0x89, 0x7d, 0x47, 0x45, 0x89, 0x82, 0x41, 0xba, 0x08, 0xae, 0xc6, 0x85, 0x13, 0xb0,
	0x7b, 0x41, 0xdd, 0xbb, 0x48, 0xdd, 0xe8, 0xa6, 0x64, 0xee, 0x5c, 0x3a, 0x23, 0x99, 0xa4, 0xe6,
	0x51, 0xf0, 0xbf, 0xbb, 0x90, 0xce, 0x4c, 0x5b, 0x97, 0xdf, 0x77, 0x6e, 0x0e, 0xe1, 0xd0, 0xb9,
	0x47, 0x70, 0xbe, 0x0c, 0xd2, 0xea, 0x18, 0x56, 0x21, 0x7a, 0xd4, 0x4d, 0x6d, 0xd7, 0xf9, 0xc6,
	0xbb, 0xe8, 0xd8, 0xa4, 0x4f, 0xef, 0x7e, 0x09, 0xbd, 0x7a, 0x7b, 0x7e, 0x5f, 0x2e, 0xf7, 0x07,
	0xaa, 0x0d, 0x18, 0xa7, 0x13, 0xa8, 0xb4, 0xb5, 0x68, 0x38, 0x11, 0x24, 0x9b, 0xa9, 0x3d, 0xb2,
	0x6b, 0x3a, 0xda, 0x6a, 0x93, 0x90, 0x9f, 0x08, 0x92, 0x9d, 0xa9, 0x0e, 0xd8, 0x2d, 0x9d, 0x06,
	0xfc, 0x4e, 0x68, 0x01, 0xf9, 0x40, 0x90, 0x6c, 0xa8, 0x0e, 0xbc, 0xeb, 0x0a, 0xa9, 0xf8, 0x42,
	0x88, 0x7c, 0xd8, 0x75, 0xf5, 0xc8, 0x04, 0x3d, 0xf5, 0x58, 0xa2, 0xa9, 0xb7, 0xe8, 0xb1, 0xe4,
	0x23, 0x41, 0xb2, 0xa9, 0xfa, 0xaf, 0xd8, 0x3d, 0xbd, 0x3c, 0xe0, 0xcf, 0x0a, 0x5c, 0xb2, 0x91,
	0x8f, 0x05, 0xc9, 0xce, 0xd5, 0xc5, 0xd1, 0xbf, 0xee, 0xf4, 0xee, 0x63, 0xe0, 0x61, 0xf1, 0xc8,
	0x27, 0x6d, 0xde, 0x01, 0x9b, 0xd3, 0x59, 0xac, 0x1b, 0x0c, 0x51, 0x37, 0x1b, 0x3e, 0x15, 0x24,
	0x1b, 0xa8, 0xa3, 0x78, 0xf9, 0xa0, 0x37, 0xa1, 0xca, 0x0b, 0x1d, 0xa1, 0xca, 0x71, 0x8b, 0x36,
	0x86, 0xbc, 0x5f, 0xe6, 0xf3, 0x69, 0x5d, 0xc7, 0x2a, 0x15, 0x39, 0xb8, 0x46, 0xb6, 0x07, 0xe0,
	0xfc, 0x46, 0x82, 0x33, 0x06, 0x21, 0x3a, 0xff, 0x10, 0xa0, 0xc2, 0x46, 0x07, 0x59, 0xa4, 0xda,
	0x94, 0x72, 0xed, 0x64, 0xbb, 0x6c, 0x90, 0xfd, 0xfb, 0x62, 0xdc, 0xf2, 0xe2, 0x2f, 0x00, 0x00,
	0xff, 0xff, 0xad, 0x58, 0x04, 0x41, 0x89, 0x01, 0x00, 0x00,
}