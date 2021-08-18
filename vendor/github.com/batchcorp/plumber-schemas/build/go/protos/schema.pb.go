// Code generated by protoc-gen-go. DO NOT EDIT.
// source: schema.proto

package protos

import (
	fmt "fmt"
	common "github.com/batchcorp/plumber-schemas/build/go/protos/common"
	encoding "github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
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

type Schema struct {
	Id    string            `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name  string            `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Type  encoding.Type     `protobuf:"varint,3,opt,name=type,proto3,enum=protos.encoding.Type" json:"type,omitempty"`
	Files map[string]string `protobuf:"bytes,4,rep,name=files,proto3" json:"files,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// Protobuf only, root message name
	RootType string `protobuf:"bytes,5,opt,name=root_type,json=rootType,proto3" json:"root_type,omitempty"`
	// Protobuf only, root message descriptor
	MessageDescriptor    []byte   `protobuf:"bytes,6,opt,name=message_descriptor,json=messageDescriptor,proto3" json:"message_descriptor,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Schema) Reset()         { *m = Schema{} }
func (m *Schema) String() string { return proto.CompactTextString(m) }
func (*Schema) ProtoMessage()    {}
func (*Schema) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{0}
}

func (m *Schema) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Schema.Unmarshal(m, b)
}
func (m *Schema) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Schema.Marshal(b, m, deterministic)
}
func (m *Schema) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Schema.Merge(m, src)
}
func (m *Schema) XXX_Size() int {
	return xxx_messageInfo_Schema.Size(m)
}
func (m *Schema) XXX_DiscardUnknown() {
	xxx_messageInfo_Schema.DiscardUnknown(m)
}

var xxx_messageInfo_Schema proto.InternalMessageInfo

func (m *Schema) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *Schema) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Schema) GetType() encoding.Type {
	if m != nil {
		return m.Type
	}
	return encoding.Type_NONE
}

func (m *Schema) GetFiles() map[string]string {
	if m != nil {
		return m.Files
	}
	return nil
}

func (m *Schema) GetRootType() string {
	if m != nil {
		return m.RootType
	}
	return ""
}

func (m *Schema) GetMessageDescriptor() []byte {
	if m != nil {
		return m.MessageDescriptor
	}
	return nil
}

type GetSchemaRequest struct {
	// Every gRPC request must have a valid auth config
	Auth                 *common.Auth `protobuf:"bytes,9999,opt,name=auth,proto3" json:"auth,omitempty"`
	Id                   string       `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *GetSchemaRequest) Reset()         { *m = GetSchemaRequest{} }
func (m *GetSchemaRequest) String() string { return proto.CompactTextString(m) }
func (*GetSchemaRequest) ProtoMessage()    {}
func (*GetSchemaRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{1}
}

func (m *GetSchemaRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetSchemaRequest.Unmarshal(m, b)
}
func (m *GetSchemaRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetSchemaRequest.Marshal(b, m, deterministic)
}
func (m *GetSchemaRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetSchemaRequest.Merge(m, src)
}
func (m *GetSchemaRequest) XXX_Size() int {
	return xxx_messageInfo_GetSchemaRequest.Size(m)
}
func (m *GetSchemaRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetSchemaRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetSchemaRequest proto.InternalMessageInfo

func (m *GetSchemaRequest) GetAuth() *common.Auth {
	if m != nil {
		return m.Auth
	}
	return nil
}

func (m *GetSchemaRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

type GetSchemaResponse struct {
	Schema               *Schema  `protobuf:"bytes,1,opt,name=schema,proto3" json:"schema,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetSchemaResponse) Reset()         { *m = GetSchemaResponse{} }
func (m *GetSchemaResponse) String() string { return proto.CompactTextString(m) }
func (*GetSchemaResponse) ProtoMessage()    {}
func (*GetSchemaResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{2}
}

func (m *GetSchemaResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetSchemaResponse.Unmarshal(m, b)
}
func (m *GetSchemaResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetSchemaResponse.Marshal(b, m, deterministic)
}
func (m *GetSchemaResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetSchemaResponse.Merge(m, src)
}
func (m *GetSchemaResponse) XXX_Size() int {
	return xxx_messageInfo_GetSchemaResponse.Size(m)
}
func (m *GetSchemaResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetSchemaResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetSchemaResponse proto.InternalMessageInfo

func (m *GetSchemaResponse) GetSchema() *Schema {
	if m != nil {
		return m.Schema
	}
	return nil
}

type GetAllSchemasRequest struct {
	// Every gRPC request must have a valid auth config
	Auth                 *common.Auth `protobuf:"bytes,9999,opt,name=auth,proto3" json:"auth,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *GetAllSchemasRequest) Reset()         { *m = GetAllSchemasRequest{} }
func (m *GetAllSchemasRequest) String() string { return proto.CompactTextString(m) }
func (*GetAllSchemasRequest) ProtoMessage()    {}
func (*GetAllSchemasRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{3}
}

func (m *GetAllSchemasRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetAllSchemasRequest.Unmarshal(m, b)
}
func (m *GetAllSchemasRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetAllSchemasRequest.Marshal(b, m, deterministic)
}
func (m *GetAllSchemasRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetAllSchemasRequest.Merge(m, src)
}
func (m *GetAllSchemasRequest) XXX_Size() int {
	return xxx_messageInfo_GetAllSchemasRequest.Size(m)
}
func (m *GetAllSchemasRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetAllSchemasRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetAllSchemasRequest proto.InternalMessageInfo

func (m *GetAllSchemasRequest) GetAuth() *common.Auth {
	if m != nil {
		return m.Auth
	}
	return nil
}

type GetAllSchemasResponse struct {
	Schema               []*Schema `protobuf:"bytes,1,rep,name=schema,proto3" json:"schema,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *GetAllSchemasResponse) Reset()         { *m = GetAllSchemasResponse{} }
func (m *GetAllSchemasResponse) String() string { return proto.CompactTextString(m) }
func (*GetAllSchemasResponse) ProtoMessage()    {}
func (*GetAllSchemasResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{4}
}

func (m *GetAllSchemasResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetAllSchemasResponse.Unmarshal(m, b)
}
func (m *GetAllSchemasResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetAllSchemasResponse.Marshal(b, m, deterministic)
}
func (m *GetAllSchemasResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetAllSchemasResponse.Merge(m, src)
}
func (m *GetAllSchemasResponse) XXX_Size() int {
	return xxx_messageInfo_GetAllSchemasResponse.Size(m)
}
func (m *GetAllSchemasResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetAllSchemasResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetAllSchemasResponse proto.InternalMessageInfo

func (m *GetAllSchemasResponse) GetSchema() []*Schema {
	if m != nil {
		return m.Schema
	}
	return nil
}

type ImportGithubRequest struct {
	// Every gRPC request must have a valid auth config
	Auth      *common.Auth  `protobuf:"bytes,9999,opt,name=auth,proto3" json:"auth,omitempty"`
	Name      string        `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Type      encoding.Type `protobuf:"varint,2,opt,name=type,proto3,enum=protos.encoding.Type" json:"type,omitempty"`
	GithubUrl string        `protobuf:"bytes,3,opt,name=github_url,json=githubUrl,proto3" json:"github_url,omitempty"`
	// Protobuf only, root message name
	RootType string `protobuf:"bytes,4,opt,name=root_type,json=rootType,proto3" json:"root_type,omitempty"`
	// Directory inside github repo where protos are stored
	RootDir              string   `protobuf:"bytes,5,opt,name=root_dir,json=rootDir,proto3" json:"root_dir,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ImportGithubRequest) Reset()         { *m = ImportGithubRequest{} }
func (m *ImportGithubRequest) String() string { return proto.CompactTextString(m) }
func (*ImportGithubRequest) ProtoMessage()    {}
func (*ImportGithubRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{5}
}

func (m *ImportGithubRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ImportGithubRequest.Unmarshal(m, b)
}
func (m *ImportGithubRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ImportGithubRequest.Marshal(b, m, deterministic)
}
func (m *ImportGithubRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ImportGithubRequest.Merge(m, src)
}
func (m *ImportGithubRequest) XXX_Size() int {
	return xxx_messageInfo_ImportGithubRequest.Size(m)
}
func (m *ImportGithubRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ImportGithubRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ImportGithubRequest proto.InternalMessageInfo

func (m *ImportGithubRequest) GetAuth() *common.Auth {
	if m != nil {
		return m.Auth
	}
	return nil
}

func (m *ImportGithubRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *ImportGithubRequest) GetType() encoding.Type {
	if m != nil {
		return m.Type
	}
	return encoding.Type_NONE
}

func (m *ImportGithubRequest) GetGithubUrl() string {
	if m != nil {
		return m.GithubUrl
	}
	return ""
}

func (m *ImportGithubRequest) GetRootType() string {
	if m != nil {
		return m.RootType
	}
	return ""
}

func (m *ImportGithubRequest) GetRootDir() string {
	if m != nil {
		return m.RootDir
	}
	return ""
}

type ImportGithubResponse struct {
	Status               *common.Status `protobuf:"bytes,1000,opt,name=status,proto3" json:"status,omitempty"`
	Id                   string         `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *ImportGithubResponse) Reset()         { *m = ImportGithubResponse{} }
func (m *ImportGithubResponse) String() string { return proto.CompactTextString(m) }
func (*ImportGithubResponse) ProtoMessage()    {}
func (*ImportGithubResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{6}
}

func (m *ImportGithubResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ImportGithubResponse.Unmarshal(m, b)
}
func (m *ImportGithubResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ImportGithubResponse.Marshal(b, m, deterministic)
}
func (m *ImportGithubResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ImportGithubResponse.Merge(m, src)
}
func (m *ImportGithubResponse) XXX_Size() int {
	return xxx_messageInfo_ImportGithubResponse.Size(m)
}
func (m *ImportGithubResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ImportGithubResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ImportGithubResponse proto.InternalMessageInfo

func (m *ImportGithubResponse) GetStatus() *common.Status {
	if m != nil {
		return m.Status
	}
	return nil
}

func (m *ImportGithubResponse) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

type ImportLocalRequest struct {
	// Every gRPC request must have a valid auth config
	Auth       *common.Auth  `protobuf:"bytes,9999,opt,name=auth,proto3" json:"auth,omitempty"`
	Name       string        `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Type       encoding.Type `protobuf:"varint,2,opt,name=type,proto3,enum=protos.encoding.Type" json:"type,omitempty"`
	ZipArchive []byte        `protobuf:"bytes,3,opt,name=zip_archive,json=zipArchive,proto3" json:"zip_archive,omitempty"`
	// Protobuf only, root message name
	RootType             string   `protobuf:"bytes,4,opt,name=root_type,json=rootType,proto3" json:"root_type,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ImportLocalRequest) Reset()         { *m = ImportLocalRequest{} }
func (m *ImportLocalRequest) String() string { return proto.CompactTextString(m) }
func (*ImportLocalRequest) ProtoMessage()    {}
func (*ImportLocalRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{7}
}

func (m *ImportLocalRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ImportLocalRequest.Unmarshal(m, b)
}
func (m *ImportLocalRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ImportLocalRequest.Marshal(b, m, deterministic)
}
func (m *ImportLocalRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ImportLocalRequest.Merge(m, src)
}
func (m *ImportLocalRequest) XXX_Size() int {
	return xxx_messageInfo_ImportLocalRequest.Size(m)
}
func (m *ImportLocalRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ImportLocalRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ImportLocalRequest proto.InternalMessageInfo

func (m *ImportLocalRequest) GetAuth() *common.Auth {
	if m != nil {
		return m.Auth
	}
	return nil
}

func (m *ImportLocalRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *ImportLocalRequest) GetType() encoding.Type {
	if m != nil {
		return m.Type
	}
	return encoding.Type_NONE
}

func (m *ImportLocalRequest) GetZipArchive() []byte {
	if m != nil {
		return m.ZipArchive
	}
	return nil
}

func (m *ImportLocalRequest) GetRootType() string {
	if m != nil {
		return m.RootType
	}
	return ""
}

type ImportLocalResponse struct {
	Status               *common.Status `protobuf:"bytes,1000,opt,name=status,proto3" json:"status,omitempty"`
	Id                   string         `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *ImportLocalResponse) Reset()         { *m = ImportLocalResponse{} }
func (m *ImportLocalResponse) String() string { return proto.CompactTextString(m) }
func (*ImportLocalResponse) ProtoMessage()    {}
func (*ImportLocalResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{8}
}

func (m *ImportLocalResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ImportLocalResponse.Unmarshal(m, b)
}
func (m *ImportLocalResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ImportLocalResponse.Marshal(b, m, deterministic)
}
func (m *ImportLocalResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ImportLocalResponse.Merge(m, src)
}
func (m *ImportLocalResponse) XXX_Size() int {
	return xxx_messageInfo_ImportLocalResponse.Size(m)
}
func (m *ImportLocalResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ImportLocalResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ImportLocalResponse proto.InternalMessageInfo

func (m *ImportLocalResponse) GetStatus() *common.Status {
	if m != nil {
		return m.Status
	}
	return nil
}

func (m *ImportLocalResponse) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

type DeleteSchemaRequest struct {
	// Every gRPC request must have a valid auth config
	Auth                 *common.Auth `protobuf:"bytes,9999,opt,name=auth,proto3" json:"auth,omitempty"`
	Id                   string       `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *DeleteSchemaRequest) Reset()         { *m = DeleteSchemaRequest{} }
func (m *DeleteSchemaRequest) String() string { return proto.CompactTextString(m) }
func (*DeleteSchemaRequest) ProtoMessage()    {}
func (*DeleteSchemaRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{9}
}

func (m *DeleteSchemaRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DeleteSchemaRequest.Unmarshal(m, b)
}
func (m *DeleteSchemaRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DeleteSchemaRequest.Marshal(b, m, deterministic)
}
func (m *DeleteSchemaRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DeleteSchemaRequest.Merge(m, src)
}
func (m *DeleteSchemaRequest) XXX_Size() int {
	return xxx_messageInfo_DeleteSchemaRequest.Size(m)
}
func (m *DeleteSchemaRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_DeleteSchemaRequest.DiscardUnknown(m)
}

var xxx_messageInfo_DeleteSchemaRequest proto.InternalMessageInfo

func (m *DeleteSchemaRequest) GetAuth() *common.Auth {
	if m != nil {
		return m.Auth
	}
	return nil
}

func (m *DeleteSchemaRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

type DeleteSchemaResponse struct {
	Status               *common.Status `protobuf:"bytes,1000,opt,name=status,proto3" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *DeleteSchemaResponse) Reset()         { *m = DeleteSchemaResponse{} }
func (m *DeleteSchemaResponse) String() string { return proto.CompactTextString(m) }
func (*DeleteSchemaResponse) ProtoMessage()    {}
func (*DeleteSchemaResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c5fb4d8cc22d66a, []int{10}
}

func (m *DeleteSchemaResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DeleteSchemaResponse.Unmarshal(m, b)
}
func (m *DeleteSchemaResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DeleteSchemaResponse.Marshal(b, m, deterministic)
}
func (m *DeleteSchemaResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DeleteSchemaResponse.Merge(m, src)
}
func (m *DeleteSchemaResponse) XXX_Size() int {
	return xxx_messageInfo_DeleteSchemaResponse.Size(m)
}
func (m *DeleteSchemaResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_DeleteSchemaResponse.DiscardUnknown(m)
}

var xxx_messageInfo_DeleteSchemaResponse proto.InternalMessageInfo

func (m *DeleteSchemaResponse) GetStatus() *common.Status {
	if m != nil {
		return m.Status
	}
	return nil
}

func init() {
	proto.RegisterType((*Schema)(nil), "protos.Schema")
	proto.RegisterMapType((map[string]string)(nil), "protos.Schema.FilesEntry")
	proto.RegisterType((*GetSchemaRequest)(nil), "protos.GetSchemaRequest")
	proto.RegisterType((*GetSchemaResponse)(nil), "protos.GetSchemaResponse")
	proto.RegisterType((*GetAllSchemasRequest)(nil), "protos.GetAllSchemasRequest")
	proto.RegisterType((*GetAllSchemasResponse)(nil), "protos.GetAllSchemasResponse")
	proto.RegisterType((*ImportGithubRequest)(nil), "protos.ImportGithubRequest")
	proto.RegisterType((*ImportGithubResponse)(nil), "protos.ImportGithubResponse")
	proto.RegisterType((*ImportLocalRequest)(nil), "protos.ImportLocalRequest")
	proto.RegisterType((*ImportLocalResponse)(nil), "protos.ImportLocalResponse")
	proto.RegisterType((*DeleteSchemaRequest)(nil), "protos.DeleteSchemaRequest")
	proto.RegisterType((*DeleteSchemaResponse)(nil), "protos.DeleteSchemaResponse")
}

func init() { proto.RegisterFile("schema.proto", fileDescriptor_1c5fb4d8cc22d66a) }

var fileDescriptor_1c5fb4d8cc22d66a = []byte{
	// 560 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xbc, 0x54, 0xdd, 0x8a, 0xd3, 0x40,
	0x14, 0x26, 0xe9, 0xcf, 0xda, 0xd3, 0x52, 0xb6, 0xd3, 0x56, 0xb2, 0x15, 0xb1, 0xe4, 0x42, 0xe2,
	0xc5, 0x26, 0x50, 0x45, 0x16, 0xbd, 0xd0, 0x4a, 0xdd, 0x22, 0x2c, 0x0a, 0x59, 0xd7, 0x0b, 0x6f,
	0x4a, 0x7e, 0xc6, 0x66, 0x30, 0xc9, 0x8c, 0x33, 0x93, 0x85, 0xee, 0x43, 0xe8, 0x03, 0xf9, 0x1c,
	0xbe, 0x83, 0x8f, 0x21, 0x99, 0x49, 0xd4, 0xd6, 0x65, 0x65, 0x65, 0xf1, 0x2a, 0x33, 0xe7, 0x1c,
	0xbe, 0xf3, 0x9d, 0x6f, 0xbe, 0x13, 0xe8, 0x89, 0x28, 0xc1, 0x59, 0xe0, 0x32, 0x4e, 0x25, 0x45,
	0x6d, 0xf5, 0x11, 0x93, 0xdb, 0x38, 0x8f, 0x68, 0x4c, 0xf2, 0xb5, 0x47, 0x99, 0x24, 0x34, 0x17,
	0x3a, 0x3f, 0x19, 0x44, 0x34, 0xcb, 0x68, 0xee, 0x05, 0x85, 0x4c, 0xaa, 0xd0, 0xb0, 0x0a, 0x09,
	0x19, 0xc8, 0xa2, 0xaa, 0xb3, 0x3f, 0x9b, 0xd0, 0x3e, 0x55, 0xc0, 0xa8, 0x0f, 0x26, 0x89, 0x2d,
	0x63, 0x6a, 0x38, 0x1d, 0xdf, 0x24, 0x31, 0x42, 0xd0, 0xcc, 0x83, 0x0c, 0x5b, 0xa6, 0x8a, 0xa8,
	0x33, 0x7a, 0x00, 0x4d, 0xb9, 0x61, 0xd8, 0x6a, 0x4c, 0x0d, 0xa7, 0x3f, 0x1b, 0x6b, 0x10, 0xe1,
	0xd6, 0x24, 0xdc, 0xb7, 0x1b, 0x86, 0x7d, 0x55, 0x82, 0x3c, 0x68, 0x7d, 0x20, 0x29, 0x16, 0x56,
	0x73, 0xda, 0x70, 0xba, 0xb3, 0x83, 0xba, 0x56, 0x77, 0x73, 0x8f, 0xcb, 0xdc, 0xcb, 0x5c, 0xf2,
	0x8d, 0xaf, 0xeb, 0xd0, 0x1d, 0xe8, 0x70, 0x4a, 0xe5, 0x4a, 0x35, 0x68, 0xa9, 0xa6, 0xb7, 0xca,
	0x40, 0x89, 0x89, 0x0e, 0x01, 0x65, 0x58, 0x88, 0x60, 0x8d, 0x57, 0x31, 0x16, 0x11, 0x27, 0x4c,
	0x52, 0x6e, 0xb5, 0xa7, 0x86, 0xd3, 0xf3, 0x07, 0x55, 0x66, 0xf1, 0x33, 0x31, 0x39, 0x02, 0xf8,
	0xd5, 0x00, 0xed, 0x43, 0xe3, 0x23, 0xde, 0x54, 0xa3, 0x95, 0x47, 0x34, 0x82, 0xd6, 0x79, 0x90,
	0x16, 0xf5, 0x70, 0xfa, 0xf2, 0xc4, 0x3c, 0x32, 0xec, 0x13, 0xd8, 0x5f, 0x62, 0xa9, 0x49, 0xfa,
	0xf8, 0x53, 0x81, 0x85, 0x44, 0x0e, 0x34, 0x4b, 0x1d, 0xad, 0x2f, 0xaf, 0xa7, 0x86, 0xd3, 0x9d,
	0x0d, 0xeb, 0x51, 0xb4, 0xa0, 0xee, 0xbc, 0x90, 0x89, 0xaf, 0x2a, 0x76, 0x35, 0xb4, 0x9f, 0xc2,
	0xe0, 0x37, 0x34, 0xc1, 0x68, 0x2e, 0x30, 0xba, 0x0f, 0x6d, 0xfd, 0x96, 0xaa, 0xb0, 0x3b, 0xeb,
	0x6f, 0x4b, 0xe3, 0x57, 0x59, 0xfb, 0x39, 0x8c, 0x96, 0x58, 0xce, 0xd3, 0x54, 0xc7, 0xc5, 0xb5,
	0xe9, 0xd8, 0xcf, 0x60, 0xbc, 0x83, 0x70, 0x09, 0x85, 0xc6, 0x15, 0x14, 0xbe, 0x19, 0x30, 0x7c,
	0x95, 0x31, 0xca, 0xe5, 0x92, 0xc8, 0xa4, 0x08, 0xaf, 0xaf, 0x48, 0xed, 0x22, 0xe3, 0x12, 0x17,
	0x99, 0x7f, 0x77, 0xd1, 0x5d, 0x80, 0xb5, 0xea, 0xbc, 0x2a, 0x78, 0xaa, 0x6c, 0xd7, 0xf1, 0x3b,
	0x3a, 0x72, 0xc6, 0xd3, 0x6d, 0xcf, 0x34, 0x77, 0x3c, 0x73, 0x00, 0xea, 0xbc, 0x8a, 0x09, 0xaf,
	0xfc, 0xb4, 0x57, 0xde, 0x17, 0x84, 0xdb, 0xef, 0x60, 0xb4, 0x3d, 0x56, 0xa5, 0x8b, 0x0b, 0x6d,
	0xbd, 0x1e, 0xd6, 0xf7, 0x3d, 0x35, 0xd9, 0x78, 0x67, 0xb2, 0x53, 0x95, 0xf5, 0xab, 0xaa, 0x3f,
	0xde, 0xfb, 0xab, 0x01, 0x48, 0x03, 0x9f, 0xd0, 0x28, 0x48, 0xff, 0xbb, 0x5c, 0xf7, 0xa0, 0x7b,
	0x41, 0xd8, 0x2a, 0xe0, 0x51, 0x42, 0xce, 0xf5, 0x9a, 0xf6, 0x7c, 0xb8, 0x20, 0x6c, 0xae, 0x23,
	0x57, 0x0a, 0x66, 0x9f, 0xd5, 0x8f, 0x5d, 0x91, 0xbf, 0x21, 0x51, 0xde, 0xc0, 0x70, 0x81, 0x53,
	0x2c, 0xf1, 0x4d, 0x6d, 0xd5, 0x31, 0x8c, 0xb6, 0x01, 0xff, 0x8d, 0xe8, 0x8b, 0xc7, 0xef, 0x1f,
	0x69, 0x2b, 0x95, 0x79, 0x2f, 0x0c, 0x64, 0x94, 0x44, 0x94, 0x33, 0x8f, 0xa5, 0x45, 0x16, 0x62,
	0x7e, 0xa8, 0x97, 0x40, 0x78, 0x61, 0x41, 0xd2, 0xd8, 0x5b, 0x53, 0x4f, 0xa3, 0x85, 0xfa, 0xe7,
	0xfb, 0xf0, 0x47, 0x00, 0x00, 0x00, 0xff, 0xff, 0x33, 0x43, 0x4c, 0x46, 0x93, 0x05, 0x00, 0x00,
}