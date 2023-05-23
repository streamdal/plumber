// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.6
// source: records/ps_records_nats_jetstream.proto

package records

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type NatsJetstream struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Stream       string `protobuf:"bytes,1,opt,name=stream,proto3" json:"stream,omitempty"`
	Value        []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	ConsumerName string `protobuf:"bytes,3,opt,name=consumer_name,json=consumerName,proto3" json:"consumer_name,omitempty"`
	Sequence     int64  `protobuf:"varint,4,opt,name=sequence,proto3" json:"sequence,omitempty"`
}

func (x *NatsJetstream) Reset() {
	*x = NatsJetstream{}
	if protoimpl.UnsafeEnabled {
		mi := &file_records_ps_records_nats_jetstream_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NatsJetstream) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NatsJetstream) ProtoMessage() {}

func (x *NatsJetstream) ProtoReflect() protoreflect.Message {
	mi := &file_records_ps_records_nats_jetstream_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NatsJetstream.ProtoReflect.Descriptor instead.
func (*NatsJetstream) Descriptor() ([]byte, []int) {
	return file_records_ps_records_nats_jetstream_proto_rawDescGZIP(), []int{0}
}

func (x *NatsJetstream) GetStream() string {
	if x != nil {
		return x.Stream
	}
	return ""
}

func (x *NatsJetstream) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *NatsJetstream) GetConsumerName() string {
	if x != nil {
		return x.ConsumerName
	}
	return ""
}

func (x *NatsJetstream) GetSequence() int64 {
	if x != nil {
		return x.Sequence
	}
	return 0
}

var File_records_ps_records_nats_jetstream_proto protoreflect.FileDescriptor

var file_records_ps_records_nats_jetstream_proto_rawDesc = []byte{
	0x0a, 0x27, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x2f, 0x70, 0x73, 0x5f, 0x72, 0x65, 0x63,
	0x6f, 0x72, 0x64, 0x73, 0x5f, 0x6e, 0x61, 0x74, 0x73, 0x5f, 0x6a, 0x65, 0x74, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x73, 0x2e, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x22, 0x7e, 0x0a, 0x0d, 0x4e, 0x61, 0x74,
	0x73, 0x4a, 0x65, 0x74, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x72, 0x65,
	0x61, 0x6d, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x23, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x73,
	0x75, 0x6d, 0x65, 0x72, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0c, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a,
	0x08, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x08, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x42, 0x3e, 0x5a, 0x3c, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x61, 0x74, 0x63, 0x68, 0x63, 0x6f, 0x72,
	0x70, 0x2f, 0x70, 0x6c, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x2d, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61,
	0x73, 0x2f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2f, 0x67, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x73, 0x2f, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_records_ps_records_nats_jetstream_proto_rawDescOnce sync.Once
	file_records_ps_records_nats_jetstream_proto_rawDescData = file_records_ps_records_nats_jetstream_proto_rawDesc
)

func file_records_ps_records_nats_jetstream_proto_rawDescGZIP() []byte {
	file_records_ps_records_nats_jetstream_proto_rawDescOnce.Do(func() {
		file_records_ps_records_nats_jetstream_proto_rawDescData = protoimpl.X.CompressGZIP(file_records_ps_records_nats_jetstream_proto_rawDescData)
	})
	return file_records_ps_records_nats_jetstream_proto_rawDescData
}

var file_records_ps_records_nats_jetstream_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_records_ps_records_nats_jetstream_proto_goTypes = []interface{}{
	(*NatsJetstream)(nil), // 0: protos.records.NatsJetstream
}
var file_records_ps_records_nats_jetstream_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_records_ps_records_nats_jetstream_proto_init() }
func file_records_ps_records_nats_jetstream_proto_init() {
	if File_records_ps_records_nats_jetstream_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_records_ps_records_nats_jetstream_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NatsJetstream); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_records_ps_records_nats_jetstream_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_records_ps_records_nats_jetstream_proto_goTypes,
		DependencyIndexes: file_records_ps_records_nats_jetstream_proto_depIdxs,
		MessageInfos:      file_records_ps_records_nats_jetstream_proto_msgTypes,
	}.Build()
	File_records_ps_records_nats_jetstream_proto = out.File
	file_records_ps_records_nats_jetstream_proto_rawDesc = nil
	file_records_ps_records_nats_jetstream_proto_goTypes = nil
	file_records_ps_records_nats_jetstream_proto_depIdxs = nil
}
