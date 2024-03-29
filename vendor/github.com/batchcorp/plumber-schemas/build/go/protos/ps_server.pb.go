// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.6
// source: ps_server.proto

package protos

import (
	common "github.com/batchcorp/plumber-schemas/build/go/protos/common"
	opts "github.com/batchcorp/plumber-schemas/build/go/protos/opts"
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

type GetServerOptionsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Every gRPC request must have a valid auth config
	Auth *common.Auth `protobuf:"bytes,9999,opt,name=auth,proto3" json:"auth,omitempty"`
}

func (x *GetServerOptionsRequest) Reset() {
	*x = GetServerOptionsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ps_server_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetServerOptionsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetServerOptionsRequest) ProtoMessage() {}

func (x *GetServerOptionsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_ps_server_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetServerOptionsRequest.ProtoReflect.Descriptor instead.
func (*GetServerOptionsRequest) Descriptor() ([]byte, []int) {
	return file_ps_server_proto_rawDescGZIP(), []int{0}
}

func (x *GetServerOptionsRequest) GetAuth() *common.Auth {
	if x != nil {
		return x.Auth
	}
	return nil
}

type GetServerOptionsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ServerOptions *opts.ServerOptions `protobuf:"bytes,1,opt,name=server_options,json=serverOptions,proto3" json:"server_options,omitempty"`
}

func (x *GetServerOptionsResponse) Reset() {
	*x = GetServerOptionsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ps_server_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetServerOptionsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetServerOptionsResponse) ProtoMessage() {}

func (x *GetServerOptionsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_ps_server_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetServerOptionsResponse.ProtoReflect.Descriptor instead.
func (*GetServerOptionsResponse) Descriptor() ([]byte, []int) {
	return file_ps_server_proto_rawDescGZIP(), []int{1}
}

func (x *GetServerOptionsResponse) GetServerOptions() *opts.ServerOptions {
	if x != nil {
		return x.ServerOptions
	}
	return nil
}

var File_ps_server_proto protoreflect.FileDescriptor

var file_ps_server_proto_rawDesc = []byte{
	0x0a, 0x0f, 0x70, 0x73, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x1a, 0x1b, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x2f, 0x70, 0x73, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x5f, 0x61, 0x75, 0x74, 0x68,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x19, 0x6f, 0x70, 0x74, 0x73, 0x2f, 0x70, 0x73, 0x5f,
	0x6f, 0x70, 0x74, 0x73, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x43, 0x0a, 0x17, 0x47, 0x65, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x4f, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x28, 0x0a, 0x04,
	0x61, 0x75, 0x74, 0x68, 0x18, 0x8f, 0x4e, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x41, 0x75, 0x74, 0x68,
	0x52, 0x04, 0x61, 0x75, 0x74, 0x68, 0x22, 0x5d, 0x0a, 0x18, 0x47, 0x65, 0x74, 0x53, 0x65, 0x72,
	0x76, 0x65, 0x72, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x41, 0x0a, 0x0e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x5f, 0x6f, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x73, 0x2e, 0x6f, 0x70, 0x74, 0x73, 0x2e, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x4f,
	0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x0d, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x4f, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e,
	0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x61, 0x74, 0x63, 0x68, 0x63, 0x6f, 0x72, 0x70, 0x2f, 0x70, 0x6c,
	0x75, 0x6d, 0x62, 0x65, 0x72, 0x2d, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x73, 0x2f, 0x62, 0x75,
	0x69, 0x6c, 0x64, 0x2f, 0x67, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_ps_server_proto_rawDescOnce sync.Once
	file_ps_server_proto_rawDescData = file_ps_server_proto_rawDesc
)

func file_ps_server_proto_rawDescGZIP() []byte {
	file_ps_server_proto_rawDescOnce.Do(func() {
		file_ps_server_proto_rawDescData = protoimpl.X.CompressGZIP(file_ps_server_proto_rawDescData)
	})
	return file_ps_server_proto_rawDescData
}

var file_ps_server_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_ps_server_proto_goTypes = []interface{}{
	(*GetServerOptionsRequest)(nil),  // 0: protos.GetServerOptionsRequest
	(*GetServerOptionsResponse)(nil), // 1: protos.GetServerOptionsResponse
	(*common.Auth)(nil),              // 2: protos.common.Auth
	(*opts.ServerOptions)(nil),       // 3: protos.opts.ServerOptions
}
var file_ps_server_proto_depIdxs = []int32{
	2, // 0: protos.GetServerOptionsRequest.auth:type_name -> protos.common.Auth
	3, // 1: protos.GetServerOptionsResponse.server_options:type_name -> protos.opts.ServerOptions
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_ps_server_proto_init() }
func file_ps_server_proto_init() {
	if File_ps_server_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_ps_server_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetServerOptionsRequest); i {
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
		file_ps_server_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetServerOptionsResponse); i {
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
			RawDescriptor: file_ps_server_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_ps_server_proto_goTypes,
		DependencyIndexes: file_ps_server_proto_depIdxs,
		MessageInfos:      file_ps_server_proto_msgTypes,
	}.Build()
	File_ps_server_proto = out.File
	file_ps_server_proto_rawDesc = nil
	file_ps_server_proto_goTypes = nil
	file_ps_server_proto_depIdxs = nil
}
