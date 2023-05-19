// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.6
// source: ps_args_mongo.proto

package args

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

type MongoConn struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// @gotags: kong:"help='Dial string for mongo server (Ex: mongodb://localhost:27017)',env='PLUMBER_RELAY_CDCMONGO_DSN',default='mongodb://localhost:27017'"
	Dsn string `protobuf:"bytes,1,opt,name=dsn,proto3" json:"dsn,omitempty" kong:"help='Dial string for mongo server (Ex: mongodb://localhost:27017)',env='PLUMBER_RELAY_CDCMONGO_DSN',default='mongodb://localhost:27017'"`
}

func (x *MongoConn) Reset() {
	*x = MongoConn{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ps_args_mongo_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MongoConn) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MongoConn) ProtoMessage() {}

func (x *MongoConn) ProtoReflect() protoreflect.Message {
	mi := &file_ps_args_mongo_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MongoConn.ProtoReflect.Descriptor instead.
func (*MongoConn) Descriptor() ([]byte, []int) {
	return file_ps_args_mongo_proto_rawDescGZIP(), []int{0}
}

func (x *MongoConn) GetDsn() string {
	if x != nil {
		return x.Dsn
	}
	return ""
}

type MongoReadArgs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// @gotags: kong:"help='Database name',env='PLUMBER_RELAY_CDCMONGO_DATABASE'"
	Database string `protobuf:"bytes,1,opt,name=database,proto3" json:"database,omitempty" kong:"help='Database name',env='PLUMBER_RELAY_CDCMONGO_DATABASE'"`
	// @gotags: kong:"help='Collection name',env='PLUMBER_RELAY_CDCMONGO_COLLECTION'"
	Collection string `protobuf:"bytes,2,opt,name=collection,proto3" json:"collection,omitempty" kong:"help='Collection name',env='PLUMBER_RELAY_CDCMONGO_COLLECTION'"`
	// @gotags: kong:"help='Include full document in update in update changes (default - return deltas only)',env='PLUMBER_RELAY_CDCMONGO_INCLUDE_FULL_DOC'"
	IncludeFullDocument bool `protobuf:"varint,3,opt,name=include_full_document,json=includeFullDocument,proto3" json:"include_full_document,omitempty" kong:"help='Include full document in update in update changes (default - return deltas only)',env='PLUMBER_RELAY_CDCMONGO_INCLUDE_FULL_DOC'"`
}

func (x *MongoReadArgs) Reset() {
	*x = MongoReadArgs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ps_args_mongo_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MongoReadArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MongoReadArgs) ProtoMessage() {}

func (x *MongoReadArgs) ProtoReflect() protoreflect.Message {
	mi := &file_ps_args_mongo_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MongoReadArgs.ProtoReflect.Descriptor instead.
func (*MongoReadArgs) Descriptor() ([]byte, []int) {
	return file_ps_args_mongo_proto_rawDescGZIP(), []int{1}
}

func (x *MongoReadArgs) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *MongoReadArgs) GetCollection() string {
	if x != nil {
		return x.Collection
	}
	return ""
}

func (x *MongoReadArgs) GetIncludeFullDocument() bool {
	if x != nil {
		return x.IncludeFullDocument
	}
	return false
}

var File_ps_args_mongo_proto protoreflect.FileDescriptor

var file_ps_args_mongo_proto_rawDesc = []byte{
	0x0a, 0x13, 0x70, 0x73, 0x5f, 0x61, 0x72, 0x67, 0x73, 0x5f, 0x6d, 0x6f, 0x6e, 0x67, 0x6f, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2e, 0x61, 0x72,
	0x67, 0x73, 0x22, 0x1d, 0x0a, 0x09, 0x4d, 0x6f, 0x6e, 0x67, 0x6f, 0x43, 0x6f, 0x6e, 0x6e, 0x12,
	0x10, 0x0a, 0x03, 0x64, 0x73, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x64, 0x73,
	0x6e, 0x22, 0x7f, 0x0a, 0x0d, 0x4d, 0x6f, 0x6e, 0x67, 0x6f, 0x52, 0x65, 0x61, 0x64, 0x41, 0x72,
	0x67, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x12, 0x1e,
	0x0a, 0x0a, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0a, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x32,
	0x0a, 0x15, 0x69, 0x6e, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x5f, 0x66, 0x75, 0x6c, 0x6c, 0x5f, 0x64,
	0x6f, 0x63, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x13, 0x69,
	0x6e, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x46, 0x75, 0x6c, 0x6c, 0x44, 0x6f, 0x63, 0x75, 0x6d, 0x65,
	0x6e, 0x74, 0x42, 0x3b, 0x5a, 0x39, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x62, 0x61, 0x74, 0x63, 0x68, 0x63, 0x6f, 0x72, 0x70, 0x2f, 0x70, 0x6c, 0x75, 0x6d, 0x62,
	0x65, 0x72, 0x2d, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x73, 0x2f, 0x62, 0x75, 0x69, 0x6c, 0x64,
	0x2f, 0x67, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x61, 0x72, 0x67, 0x73, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_ps_args_mongo_proto_rawDescOnce sync.Once
	file_ps_args_mongo_proto_rawDescData = file_ps_args_mongo_proto_rawDesc
)

func file_ps_args_mongo_proto_rawDescGZIP() []byte {
	file_ps_args_mongo_proto_rawDescOnce.Do(func() {
		file_ps_args_mongo_proto_rawDescData = protoimpl.X.CompressGZIP(file_ps_args_mongo_proto_rawDescData)
	})
	return file_ps_args_mongo_proto_rawDescData
}

var file_ps_args_mongo_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_ps_args_mongo_proto_goTypes = []interface{}{
	(*MongoConn)(nil),     // 0: protos.args.MongoConn
	(*MongoReadArgs)(nil), // 1: protos.args.MongoReadArgs
}
var file_ps_args_mongo_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_ps_args_mongo_proto_init() }
func file_ps_args_mongo_proto_init() {
	if File_ps_args_mongo_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_ps_args_mongo_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MongoConn); i {
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
		file_ps_args_mongo_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MongoReadArgs); i {
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
			RawDescriptor: file_ps_args_mongo_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_ps_args_mongo_proto_goTypes,
		DependencyIndexes: file_ps_args_mongo_proto_depIdxs,
		MessageInfos:      file_ps_args_mongo_proto_msgTypes,
	}.Build()
	File_ps_args_mongo_proto = out.File
	file_ps_args_mongo_proto_rawDesc = nil
	file_ps_args_mongo_proto_goTypes = nil
	file_ps_args_mongo_proto_depIdxs = nil
}
