// Code generated by protoc-gen-go. DO NOT EDIT.
// source: ps_vc_server.proto

package protos

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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

type VCEvent_Type int32

const (
	VCEvent_UNSET         VCEvent_Type = 0
	VCEvent_AUTH_RESPONSE VCEvent_Type = 1
	VCEvent_NEW_JWT_TOKEN VCEvent_Type = 2
	VCEvent_GITHUB        VCEvent_Type = 3
	VCEvent_GITLAB        VCEvent_Type = 4
	VCEvent_BITBUCKET     VCEvent_Type = 5
)

var VCEvent_Type_name = map[int32]string{
	0: "UNSET",
	1: "AUTH_RESPONSE",
	2: "NEW_JWT_TOKEN",
	3: "GITHUB",
	4: "GITLAB",
	5: "BITBUCKET",
}

var VCEvent_Type_value = map[string]int32{
	"UNSET":         0,
	"AUTH_RESPONSE": 1,
	"NEW_JWT_TOKEN": 2,
	"GITHUB":        3,
	"GITLAB":        4,
	"BITBUCKET":     5,
}

func (x VCEvent_Type) String() string {
	return proto.EnumName(VCEvent_Type_name, int32(x))
}

func (VCEvent_Type) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{0, 0}
}

type GithubEvent_Type int32

const (
	GithubEvent_UNSET               GithubEvent_Type = 0
	GithubEvent_INSTALL_CREATED     GithubEvent_Type = 1
	GithubEvent_INSTALL_UPDATED     GithubEvent_Type = 2
	GithubEvent_INSTALL_DELETED     GithubEvent_Type = 3
	GithubEvent_INSTALL_SUSPENDED   GithubEvent_Type = 5
	GithubEvent_INSTALL_UNSUSPENDED GithubEvent_Type = 6
	GithubEvent_PULL_CREATED        GithubEvent_Type = 7
	GithubEvent_PULL_MERGED         GithubEvent_Type = 8
	GithubEvent_PULL_CLOSED         GithubEvent_Type = 9
	GithubEvent_PULL_REOPENED       GithubEvent_Type = 10
	GithubEvent_ISSUE_CREATED       GithubEvent_Type = 11
	GithubEvent_ISSUE_REOPENED      GithubEvent_Type = 12
	GithubEvent_ISSUE_CLOSED        GithubEvent_Type = 13
)

var GithubEvent_Type_name = map[int32]string{
	0:  "UNSET",
	1:  "INSTALL_CREATED",
	2:  "INSTALL_UPDATED",
	3:  "INSTALL_DELETED",
	5:  "INSTALL_SUSPENDED",
	6:  "INSTALL_UNSUSPENDED",
	7:  "PULL_CREATED",
	8:  "PULL_MERGED",
	9:  "PULL_CLOSED",
	10: "PULL_REOPENED",
	11: "ISSUE_CREATED",
	12: "ISSUE_REOPENED",
	13: "ISSUE_CLOSED",
}

var GithubEvent_Type_value = map[string]int32{
	"UNSET":               0,
	"INSTALL_CREATED":     1,
	"INSTALL_UPDATED":     2,
	"INSTALL_DELETED":     3,
	"INSTALL_SUSPENDED":   5,
	"INSTALL_UNSUSPENDED": 6,
	"PULL_CREATED":        7,
	"PULL_MERGED":         8,
	"PULL_CLOSED":         9,
	"PULL_REOPENED":       10,
	"ISSUE_CREATED":       11,
	"ISSUE_REOPENED":      12,
	"ISSUE_CLOSED":        13,
}

func (x GithubEvent_Type) String() string {
	return proto.EnumName(GithubEvent_Type_name, int32(x))
}

func (GithubEvent_Type) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{3, 0}
}

// VCEvent is sent by batchcorp/vc-service and received by Plumber instances to be acted upon
// It is also sent to the frontend to be acted upon
type VCEvent struct {
	Type VCEvent_Type `protobuf:"varint,1,opt,name=type,proto3,enum=protos.VCEvent_Type" json:"type,omitempty"`
	// Types that are valid to be assigned to VcEvent:
	//	*VCEvent_AuthResponse
	//	*VCEvent_GithubEvent
	//	*VCEvent_GitlabEvent
	//	*VCEvent_BitbucketEvent
	//	*VCEvent_NewJwtToken
	VcEvent              isVCEvent_VcEvent `protobuf_oneof:"vc_event"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *VCEvent) Reset()         { *m = VCEvent{} }
func (m *VCEvent) String() string { return proto.CompactTextString(m) }
func (*VCEvent) ProtoMessage()    {}
func (*VCEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{0}
}

func (m *VCEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_VCEvent.Unmarshal(m, b)
}
func (m *VCEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_VCEvent.Marshal(b, m, deterministic)
}
func (m *VCEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VCEvent.Merge(m, src)
}
func (m *VCEvent) XXX_Size() int {
	return xxx_messageInfo_VCEvent.Size(m)
}
func (m *VCEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_VCEvent.DiscardUnknown(m)
}

var xxx_messageInfo_VCEvent proto.InternalMessageInfo

func (m *VCEvent) GetType() VCEvent_Type {
	if m != nil {
		return m.Type
	}
	return VCEvent_UNSET
}

type isVCEvent_VcEvent interface {
	isVCEvent_VcEvent()
}

type VCEvent_AuthResponse struct {
	AuthResponse *AuthResponse `protobuf:"bytes,100,opt,name=auth_response,json=authResponse,proto3,oneof"`
}

type VCEvent_GithubEvent struct {
	GithubEvent *GithubEvent `protobuf:"bytes,101,opt,name=github_event,json=githubEvent,proto3,oneof"`
}

type VCEvent_GitlabEvent struct {
	GitlabEvent *GitlabEvent `protobuf:"bytes,102,opt,name=gitlab_event,json=gitlabEvent,proto3,oneof"`
}

type VCEvent_BitbucketEvent struct {
	BitbucketEvent *BitbucketEvent `protobuf:"bytes,103,opt,name=bitbucket_event,json=bitbucketEvent,proto3,oneof"`
}

type VCEvent_NewJwtToken struct {
	NewJwtToken *NewJwtToken `protobuf:"bytes,104,opt,name=new_jwt_token,json=newJwtToken,proto3,oneof"`
}

func (*VCEvent_AuthResponse) isVCEvent_VcEvent() {}

func (*VCEvent_GithubEvent) isVCEvent_VcEvent() {}

func (*VCEvent_GitlabEvent) isVCEvent_VcEvent() {}

func (*VCEvent_BitbucketEvent) isVCEvent_VcEvent() {}

func (*VCEvent_NewJwtToken) isVCEvent_VcEvent() {}

func (m *VCEvent) GetVcEvent() isVCEvent_VcEvent {
	if m != nil {
		return m.VcEvent
	}
	return nil
}

func (m *VCEvent) GetAuthResponse() *AuthResponse {
	if x, ok := m.GetVcEvent().(*VCEvent_AuthResponse); ok {
		return x.AuthResponse
	}
	return nil
}

func (m *VCEvent) GetGithubEvent() *GithubEvent {
	if x, ok := m.GetVcEvent().(*VCEvent_GithubEvent); ok {
		return x.GithubEvent
	}
	return nil
}

func (m *VCEvent) GetGitlabEvent() *GitlabEvent {
	if x, ok := m.GetVcEvent().(*VCEvent_GitlabEvent); ok {
		return x.GitlabEvent
	}
	return nil
}

func (m *VCEvent) GetBitbucketEvent() *BitbucketEvent {
	if x, ok := m.GetVcEvent().(*VCEvent_BitbucketEvent); ok {
		return x.BitbucketEvent
	}
	return nil
}

func (m *VCEvent) GetNewJwtToken() *NewJwtToken {
	if x, ok := m.GetVcEvent().(*VCEvent_NewJwtToken); ok {
		return x.NewJwtToken
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*VCEvent) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*VCEvent_AuthResponse)(nil),
		(*VCEvent_GithubEvent)(nil),
		(*VCEvent_GitlabEvent)(nil),
		(*VCEvent_BitbucketEvent)(nil),
		(*VCEvent_NewJwtToken)(nil),
	}
}

type GitlabEvent struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GitlabEvent) Reset()         { *m = GitlabEvent{} }
func (m *GitlabEvent) String() string { return proto.CompactTextString(m) }
func (*GitlabEvent) ProtoMessage()    {}
func (*GitlabEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{1}
}

func (m *GitlabEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GitlabEvent.Unmarshal(m, b)
}
func (m *GitlabEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GitlabEvent.Marshal(b, m, deterministic)
}
func (m *GitlabEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GitlabEvent.Merge(m, src)
}
func (m *GitlabEvent) XXX_Size() int {
	return xxx_messageInfo_GitlabEvent.Size(m)
}
func (m *GitlabEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_GitlabEvent.DiscardUnknown(m)
}

var xxx_messageInfo_GitlabEvent proto.InternalMessageInfo

type BitbucketEvent struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *BitbucketEvent) Reset()         { *m = BitbucketEvent{} }
func (m *BitbucketEvent) String() string { return proto.CompactTextString(m) }
func (*BitbucketEvent) ProtoMessage()    {}
func (*BitbucketEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{2}
}

func (m *BitbucketEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BitbucketEvent.Unmarshal(m, b)
}
func (m *BitbucketEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BitbucketEvent.Marshal(b, m, deterministic)
}
func (m *BitbucketEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BitbucketEvent.Merge(m, src)
}
func (m *BitbucketEvent) XXX_Size() int {
	return xxx_messageInfo_BitbucketEvent.Size(m)
}
func (m *BitbucketEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_BitbucketEvent.DiscardUnknown(m)
}

var xxx_messageInfo_BitbucketEvent proto.InternalMessageInfo

// See the following URL for reference to events we are receiving from github
// https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads#
type GithubEvent struct {
	Type GithubEvent_Type `protobuf:"varint,1,opt,name=type,proto3,enum=protos.GithubEvent_Type" json:"type,omitempty"`
	// Types that are valid to be assigned to Payload:
	//	*GithubEvent_Install
	//	*GithubEvent_PullRequest
	//	*GithubEvent_Issue
	Payload              isGithubEvent_Payload `protobuf_oneof:"payload"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *GithubEvent) Reset()         { *m = GithubEvent{} }
func (m *GithubEvent) String() string { return proto.CompactTextString(m) }
func (*GithubEvent) ProtoMessage()    {}
func (*GithubEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{3}
}

func (m *GithubEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GithubEvent.Unmarshal(m, b)
}
func (m *GithubEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GithubEvent.Marshal(b, m, deterministic)
}
func (m *GithubEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GithubEvent.Merge(m, src)
}
func (m *GithubEvent) XXX_Size() int {
	return xxx_messageInfo_GithubEvent.Size(m)
}
func (m *GithubEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_GithubEvent.DiscardUnknown(m)
}

var xxx_messageInfo_GithubEvent proto.InternalMessageInfo

func (m *GithubEvent) GetType() GithubEvent_Type {
	if m != nil {
		return m.Type
	}
	return GithubEvent_UNSET
}

type isGithubEvent_Payload interface {
	isGithubEvent_Payload()
}

type GithubEvent_Install struct {
	Install *Install `protobuf:"bytes,100,opt,name=install,proto3,oneof"`
}

type GithubEvent_PullRequest struct {
	PullRequest *PullRequest `protobuf:"bytes,101,opt,name=pull_request,json=pullRequest,proto3,oneof"`
}

type GithubEvent_Issue struct {
	Issue *Issue `protobuf:"bytes,102,opt,name=issue,proto3,oneof"`
}

func (*GithubEvent_Install) isGithubEvent_Payload() {}

func (*GithubEvent_PullRequest) isGithubEvent_Payload() {}

func (*GithubEvent_Issue) isGithubEvent_Payload() {}

func (m *GithubEvent) GetPayload() isGithubEvent_Payload {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (m *GithubEvent) GetInstall() *Install {
	if x, ok := m.GetPayload().(*GithubEvent_Install); ok {
		return x.Install
	}
	return nil
}

func (m *GithubEvent) GetPullRequest() *PullRequest {
	if x, ok := m.GetPayload().(*GithubEvent_PullRequest); ok {
		return x.PullRequest
	}
	return nil
}

func (m *GithubEvent) GetIssue() *Issue {
	if x, ok := m.GetPayload().(*GithubEvent_Issue); ok {
		return x.Issue
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*GithubEvent) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*GithubEvent_Install)(nil),
		(*GithubEvent_PullRequest)(nil),
		(*GithubEvent_Issue)(nil),
	}
}

// Sent by plumber, received by github-service
type ConnectAuthRequest struct {
	// JWT token returned during install process
	ApiToken string `protobuf:"bytes,1,opt,name=api_token,json=apiToken,proto3" json:"api_token,omitempty"`
	// unix timestamp. VC-service will pull all events from the database starting at this timestamp
	StartAt              int64    `protobuf:"varint,2,opt,name=start_at,json=startAt,proto3" json:"start_at,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ConnectAuthRequest) Reset()         { *m = ConnectAuthRequest{} }
func (m *ConnectAuthRequest) String() string { return proto.CompactTextString(m) }
func (*ConnectAuthRequest) ProtoMessage()    {}
func (*ConnectAuthRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{4}
}

func (m *ConnectAuthRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ConnectAuthRequest.Unmarshal(m, b)
}
func (m *ConnectAuthRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ConnectAuthRequest.Marshal(b, m, deterministic)
}
func (m *ConnectAuthRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ConnectAuthRequest.Merge(m, src)
}
func (m *ConnectAuthRequest) XXX_Size() int {
	return xxx_messageInfo_ConnectAuthRequest.Size(m)
}
func (m *ConnectAuthRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ConnectAuthRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ConnectAuthRequest proto.InternalMessageInfo

func (m *ConnectAuthRequest) GetApiToken() string {
	if m != nil {
		return m.ApiToken
	}
	return ""
}

func (m *ConnectAuthRequest) GetStartAt() int64 {
	if m != nil {
		return m.StartAt
	}
	return 0
}

// AuthResponse is sent by github-service and received by plumber VCServiceServer.Connect()
type AuthResponse struct {
	Authorized           bool     `protobuf:"varint,1,opt,name=authorized,proto3" json:"authorized,omitempty"`
	Message              string   `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AuthResponse) Reset()         { *m = AuthResponse{} }
func (m *AuthResponse) String() string { return proto.CompactTextString(m) }
func (*AuthResponse) ProtoMessage()    {}
func (*AuthResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{5}
}

func (m *AuthResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AuthResponse.Unmarshal(m, b)
}
func (m *AuthResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AuthResponse.Marshal(b, m, deterministic)
}
func (m *AuthResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AuthResponse.Merge(m, src)
}
func (m *AuthResponse) XXX_Size() int {
	return xxx_messageInfo_AuthResponse.Size(m)
}
func (m *AuthResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_AuthResponse.DiscardUnknown(m)
}

var xxx_messageInfo_AuthResponse proto.InternalMessageInfo

func (m *AuthResponse) GetAuthorized() bool {
	if m != nil {
		return m.Authorized
	}
	return false
}

func (m *AuthResponse) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

// Sent by github-service and received by plumber
// Sent by plumber, received by UI
type PullRequest struct {
	Owner                string   `protobuf:"bytes,1,opt,name=owner,proto3" json:"owner,omitempty"`
	Repo                 string   `protobuf:"bytes,2,opt,name=repo,proto3" json:"repo,omitempty"`
	Number               int32    `protobuf:"varint,3,opt,name=number,proto3" json:"number,omitempty"`
	Url                  string   `protobuf:"bytes,4,opt,name=url,proto3" json:"url,omitempty"`
	Description          string   `protobuf:"bytes,5,opt,name=description,proto3" json:"description,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PullRequest) Reset()         { *m = PullRequest{} }
func (m *PullRequest) String() string { return proto.CompactTextString(m) }
func (*PullRequest) ProtoMessage()    {}
func (*PullRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{6}
}

func (m *PullRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PullRequest.Unmarshal(m, b)
}
func (m *PullRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PullRequest.Marshal(b, m, deterministic)
}
func (m *PullRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PullRequest.Merge(m, src)
}
func (m *PullRequest) XXX_Size() int {
	return xxx_messageInfo_PullRequest.Size(m)
}
func (m *PullRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PullRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PullRequest proto.InternalMessageInfo

func (m *PullRequest) GetOwner() string {
	if m != nil {
		return m.Owner
	}
	return ""
}

func (m *PullRequest) GetRepo() string {
	if m != nil {
		return m.Repo
	}
	return ""
}

func (m *PullRequest) GetNumber() int32 {
	if m != nil {
		return m.Number
	}
	return 0
}

func (m *PullRequest) GetUrl() string {
	if m != nil {
		return m.Url
	}
	return ""
}

func (m *PullRequest) GetDescription() string {
	if m != nil {
		return m.Description
	}
	return ""
}

// Sent by github-service and received by plumber
// Sent by plumber, received by UI
type Install struct {
	InstallId            int64    `protobuf:"varint,1,opt,name=install_id,json=installId,proto3" json:"install_id,omitempty"`
	AccountId            int64    `protobuf:"varint,2,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Install) Reset()         { *m = Install{} }
func (m *Install) String() string { return proto.CompactTextString(m) }
func (*Install) ProtoMessage()    {}
func (*Install) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{7}
}

func (m *Install) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Install.Unmarshal(m, b)
}
func (m *Install) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Install.Marshal(b, m, deterministic)
}
func (m *Install) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Install.Merge(m, src)
}
func (m *Install) XXX_Size() int {
	return xxx_messageInfo_Install.Size(m)
}
func (m *Install) XXX_DiscardUnknown() {
	xxx_messageInfo_Install.DiscardUnknown(m)
}

var xxx_messageInfo_Install proto.InternalMessageInfo

func (m *Install) GetInstallId() int64 {
	if m != nil {
		return m.InstallId
	}
	return 0
}

func (m *Install) GetAccountId() int64 {
	if m != nil {
		return m.AccountId
	}
	return 0
}

type NewJwtToken struct {
	Token                string   `protobuf:"bytes,1,opt,name=token,proto3" json:"token,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *NewJwtToken) Reset()         { *m = NewJwtToken{} }
func (m *NewJwtToken) String() string { return proto.CompactTextString(m) }
func (*NewJwtToken) ProtoMessage()    {}
func (*NewJwtToken) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{8}
}

func (m *NewJwtToken) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_NewJwtToken.Unmarshal(m, b)
}
func (m *NewJwtToken) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_NewJwtToken.Marshal(b, m, deterministic)
}
func (m *NewJwtToken) XXX_Merge(src proto.Message) {
	xxx_messageInfo_NewJwtToken.Merge(m, src)
}
func (m *NewJwtToken) XXX_Size() int {
	return xxx_messageInfo_NewJwtToken.Size(m)
}
func (m *NewJwtToken) XXX_DiscardUnknown() {
	xxx_messageInfo_NewJwtToken.DiscardUnknown(m)
}

var xxx_messageInfo_NewJwtToken proto.InternalMessageInfo

func (m *NewJwtToken) GetToken() string {
	if m != nil {
		return m.Token
	}
	return ""
}

type Issue struct {
	Owner                string   `protobuf:"bytes,1,opt,name=owner,proto3" json:"owner,omitempty"`
	Repo                 string   `protobuf:"bytes,2,opt,name=repo,proto3" json:"repo,omitempty"`
	Number               int32    `protobuf:"varint,3,opt,name=number,proto3" json:"number,omitempty"`
	Url                  string   `protobuf:"bytes,4,opt,name=url,proto3" json:"url,omitempty"`
	Description          string   `protobuf:"bytes,5,opt,name=description,proto3" json:"description,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Issue) Reset()         { *m = Issue{} }
func (m *Issue) String() string { return proto.CompactTextString(m) }
func (*Issue) ProtoMessage()    {}
func (*Issue) Descriptor() ([]byte, []int) {
	return fileDescriptor_592e880440735ae1, []int{9}
}

func (m *Issue) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Issue.Unmarshal(m, b)
}
func (m *Issue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Issue.Marshal(b, m, deterministic)
}
func (m *Issue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Issue.Merge(m, src)
}
func (m *Issue) XXX_Size() int {
	return xxx_messageInfo_Issue.Size(m)
}
func (m *Issue) XXX_DiscardUnknown() {
	xxx_messageInfo_Issue.DiscardUnknown(m)
}

var xxx_messageInfo_Issue proto.InternalMessageInfo

func (m *Issue) GetOwner() string {
	if m != nil {
		return m.Owner
	}
	return ""
}

func (m *Issue) GetRepo() string {
	if m != nil {
		return m.Repo
	}
	return ""
}

func (m *Issue) GetNumber() int32 {
	if m != nil {
		return m.Number
	}
	return 0
}

func (m *Issue) GetUrl() string {
	if m != nil {
		return m.Url
	}
	return ""
}

func (m *Issue) GetDescription() string {
	if m != nil {
		return m.Description
	}
	return ""
}

func init() {
	proto.RegisterEnum("protos.VCEvent_Type", VCEvent_Type_name, VCEvent_Type_value)
	proto.RegisterEnum("protos.GithubEvent_Type", GithubEvent_Type_name, GithubEvent_Type_value)
	proto.RegisterType((*VCEvent)(nil), "protos.VCEvent")
	proto.RegisterType((*GitlabEvent)(nil), "protos.GitlabEvent")
	proto.RegisterType((*BitbucketEvent)(nil), "protos.BitbucketEvent")
	proto.RegisterType((*GithubEvent)(nil), "protos.GithubEvent")
	proto.RegisterType((*ConnectAuthRequest)(nil), "protos.ConnectAuthRequest")
	proto.RegisterType((*AuthResponse)(nil), "protos.AuthResponse")
	proto.RegisterType((*PullRequest)(nil), "protos.PullRequest")
	proto.RegisterType((*Install)(nil), "protos.Install")
	proto.RegisterType((*NewJwtToken)(nil), "protos.NewJwtToken")
	proto.RegisterType((*Issue)(nil), "protos.Issue")
}

func init() { proto.RegisterFile("ps_vc_server.proto", fileDescriptor_592e880440735ae1) }

var fileDescriptor_592e880440735ae1 = []byte{
	// 830 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xc4, 0x55, 0xdf, 0x8f, 0xea, 0x44,
	0x14, 0xa6, 0x0b, 0x2c, 0xf4, 0x00, 0x4b, 0xef, 0xec, 0xf5, 0x5a, 0xaf, 0xd1, 0x90, 0x1a, 0x13,
	0x12, 0x75, 0x31, 0x57, 0x63, 0x34, 0x3e, 0xf1, 0x63, 0xb2, 0x70, 0x2f, 0x76, 0xc9, 0xb4, 0xdc,
	0x9b, 0xf8, 0x60, 0xd3, 0x96, 0x11, 0xea, 0x76, 0xdb, 0xda, 0x4e, 0x21, 0x6b, 0x62, 0x62, 0xe2,
	0xff, 0xe7, 0xab, 0xff, 0x8e, 0x99, 0xe9, 0x14, 0x8a, 0xeb, 0xbb, 0x4f, 0xcc, 0xf9, 0xbe, 0xf3,
	0x9d, 0xd3, 0x39, 0x3f, 0x06, 0x40, 0x49, 0xe6, 0xec, 0x7d, 0x27, 0xa3, 0xe9, 0x9e, 0xa6, 0x37,
	0x49, 0x1a, 0xb3, 0x18, 0x5d, 0x8a, 0x9f, 0xcc, 0xf8, 0xab, 0x0e, 0xad, 0xb7, 0x53, 0xbc, 0xa7,
	0x11, 0x43, 0x43, 0x68, 0xb0, 0xc7, 0x84, 0xea, 0xca, 0x40, 0x19, 0x5e, 0xbd, 0x7a, 0x5e, 0x78,
	0x66, 0x37, 0x92, 0xbe, 0xb1, 0x1f, 0x13, 0x4a, 0x84, 0x07, 0xfa, 0x1e, 0x7a, 0x6e, 0xce, 0x76,
	0x4e, 0x4a, 0xb3, 0x24, 0x8e, 0x32, 0xaa, 0x6f, 0x06, 0xca, 0xb0, 0x73, 0x92, 0x8c, 0x73, 0xb6,
	0x23, 0x92, 0x9b, 0xd7, 0x48, 0xd7, 0xad, 0xd8, 0xe8, 0x5b, 0xe8, 0x6e, 0x03, 0xb6, 0xcb, 0x3d,
	0x87, 0xf2, 0xb8, 0x3a, 0x15, 0xda, 0xeb, 0x52, 0x7b, 0x2b, 0x38, 0x91, 0x72, 0x5e, 0x23, 0x9d,
	0xed, 0xc9, 0x94, 0xca, 0xd0, 0x2d, 0x95, 0x3f, 0x3f, 0x51, 0x86, 0xee, 0x99, 0xb2, 0x34, 0xd1,
	0x18, 0xfa, 0x5e, 0xc0, 0xbc, 0xdc, 0xbf, 0xa7, 0x4c, 0x8a, 0xb7, 0x42, 0xfc, 0xa2, 0x14, 0x4f,
	0x4a, 0xba, 0xd4, 0x5f, 0x79, 0x67, 0x08, 0xfa, 0x0e, 0x7a, 0x11, 0x3d, 0x38, 0xbf, 0x1c, 0x98,
	0xc3, 0xe2, 0x7b, 0x1a, 0xe9, 0xbb, 0xf3, 0xec, 0x26, 0x3d, 0xbc, 0x3e, 0x30, 0x9b, 0x53, 0x3c,
	0x7b, 0x74, 0x32, 0x8d, 0x9f, 0xa0, 0xc1, 0x8b, 0x87, 0x54, 0x68, 0xae, 0x4d, 0x0b, 0xdb, 0x5a,
	0x0d, 0x3d, 0x83, 0xde, 0x78, 0x6d, 0xcf, 0x1d, 0x82, 0xad, 0xd5, 0x9d, 0x69, 0x61, 0x4d, 0xe1,
	0x90, 0x89, 0xdf, 0x39, 0xaf, 0xdf, 0xd9, 0x8e, 0x7d, 0xf7, 0x06, 0x9b, 0xda, 0x05, 0x02, 0xb8,
	0xbc, 0x5d, 0xd8, 0xf3, 0xf5, 0x44, 0xab, 0xcb, 0xf3, 0x72, 0x3c, 0xd1, 0x1a, 0xa8, 0x07, 0xea,
	0x64, 0x61, 0x4f, 0xd6, 0xd3, 0x37, 0xd8, 0xd6, 0x9a, 0x13, 0x80, 0xf6, 0xde, 0x2f, 0xae, 0x65,
	0xf4, 0xa0, 0x53, 0xa9, 0x83, 0xa1, 0xc1, 0xd5, 0xf9, 0xcd, 0x8c, 0xbf, 0xeb, 0xc2, 0xe3, 0x58,
	0xd4, 0xcf, 0xcf, 0xba, 0xae, 0xff, 0x47, 0x1b, 0xaa, 0x9d, 0xff, 0x0c, 0x5a, 0x41, 0x94, 0x31,
	0x37, 0x0c, 0x65, 0xcf, 0xfb, 0xa5, 0x60, 0x51, 0xc0, 0xf3, 0x1a, 0x29, 0x3d, 0x78, 0xbf, 0x92,
	0x3c, 0x0c, 0x9d, 0x94, 0xfe, 0x9a, 0xd3, 0xec, 0x49, 0xa7, 0x57, 0x79, 0x18, 0x92, 0x82, 0xe2,
	0x15, 0x4b, 0x4e, 0x26, 0xfa, 0x14, 0x9a, 0x41, 0x96, 0xe5, 0x54, 0xb6, 0xb8, 0x77, 0x4c, 0xc2,
	0xc1, 0x79, 0x8d, 0x14, 0xac, 0xf1, 0xc7, 0xc5, 0xd3, 0xca, 0x5e, 0x43, 0x7f, 0x61, 0x5a, 0xf6,
	0x78, 0xb9, 0x74, 0xa6, 0x04, 0x8f, 0x6d, 0x3c, 0xd3, 0x94, 0x2a, 0xb8, 0x5e, 0xcd, 0x04, 0x78,
	0x51, 0x05, 0x67, 0x78, 0x89, 0x39, 0x58, 0x47, 0xef, 0xc1, 0xb3, 0x12, 0xb4, 0xd6, 0xd6, 0x0a,
	0x9b, 0x33, 0x3c, 0xd3, 0x9a, 0xe8, 0x7d, 0xb8, 0x3e, 0x06, 0x30, 0x4f, 0xc4, 0x25, 0xd2, 0xa0,
	0xbb, 0x5a, 0x57, 0x72, 0xb5, 0x50, 0x1f, 0x3a, 0x02, 0xf9, 0x01, 0x93, 0x5b, 0x3c, 0xd3, 0xda,
	0x47, 0x60, 0xba, 0xbc, 0xb3, 0xf0, 0x4c, 0x53, 0x79, 0xa7, 0x05, 0x40, 0xf0, 0xdd, 0x0a, 0x9b,
	0x78, 0xa6, 0x01, 0x87, 0x16, 0x96, 0xb5, 0xc6, 0xc7, 0x38, 0x1d, 0x84, 0xe0, 0xaa, 0x80, 0x8e,
	0x6e, 0x5d, 0x9e, 0x4d, 0xba, 0x15, 0xb1, 0x7a, 0x13, 0x15, 0x5a, 0x89, 0xfb, 0x18, 0xc6, 0xee,
	0xc6, 0x58, 0x02, 0x9a, 0xc6, 0x51, 0x44, 0x7d, 0x56, 0xec, 0x5f, 0x51, 0xca, 0x0f, 0x41, 0x75,
	0x93, 0x40, 0xce, 0x2c, 0x6f, 0xb2, 0x4a, 0xda, 0x6e, 0x12, 0x88, 0xc9, 0x44, 0x1f, 0x40, 0x3b,
	0x63, 0x6e, 0xca, 0x1c, 0x97, 0xe9, 0x17, 0x03, 0x65, 0x58, 0x27, 0x2d, 0x61, 0x8f, 0x99, 0x31,
	0x87, 0x6e, 0x75, 0x8d, 0xd1, 0xc7, 0x00, 0x7c, 0x8d, 0xe3, 0x34, 0xf8, 0x8d, 0x6e, 0x44, 0xa0,
	0x36, 0xa9, 0x20, 0x48, 0x87, 0xd6, 0x03, 0xcd, 0x32, 0x77, 0x4b, 0x45, 0x24, 0x95, 0x94, 0xa6,
	0xf1, 0xa7, 0x02, 0x9d, 0x4a, 0xaf, 0xd1, 0x73, 0x68, 0xc6, 0x87, 0x88, 0xa6, 0xf2, 0x6b, 0x0a,
	0x03, 0x21, 0x68, 0xa4, 0x34, 0x89, 0xa5, 0x58, 0x9c, 0xd1, 0x0b, 0xb8, 0x8c, 0xf2, 0x07, 0x8f,
	0xa6, 0x7a, 0x7d, 0xa0, 0x0c, 0x9b, 0x44, 0x5a, 0x48, 0x83, 0x7a, 0x9e, 0x86, 0x7a, 0x43, 0xb8,
	0xf2, 0x23, 0x1a, 0x40, 0x67, 0x43, 0x33, 0x3f, 0x0d, 0x12, 0x16, 0xc4, 0x91, 0xde, 0x14, 0x4c,
	0x15, 0x32, 0x6e, 0xa1, 0x25, 0x47, 0x14, 0x7d, 0x04, 0x20, 0x47, 0xd4, 0x09, 0x8a, 0xab, 0xd4,
	0x89, 0x2a, 0x91, 0xc5, 0x86, 0xd3, 0xae, 0xef, 0xc7, 0x79, 0xc4, 0x38, 0x5d, 0x94, 0x45, 0x95,
	0xc8, 0x62, 0x63, 0x7c, 0x02, 0x9d, 0xca, 0xae, 0xf3, 0xdb, 0x54, 0x6b, 0x5b, 0x18, 0xc6, 0xef,
	0xd0, 0x14, 0xb3, 0xfa, 0xff, 0x5c, 0xf6, 0x15, 0x06, 0xf5, 0xed, 0xd4, 0xa2, 0xe9, 0x3e, 0xf0,
	0xf9, 0x83, 0xdb, 0x92, 0x73, 0x81, 0x5e, 0x96, 0x8b, 0xf4, 0x74, 0x50, 0x5e, 0xf6, 0xff, 0xf5,
	0xe0, 0x7f, 0xa9, 0x4c, 0xbe, 0xf9, 0xf1, 0xeb, 0xe2, 0xfd, 0xbd, 0xf1, 0xe3, 0x87, 0x91, 0xe7,
	0x32, 0x7f, 0xe7, 0xc7, 0x69, 0x32, 0x4a, 0x42, 0xf1, 0x69, 0x5f, 0x64, 0xfe, 0x8e, 0x3e, 0xb8,
	0xd9, 0xc8, 0xcb, 0x83, 0x70, 0x33, 0xda, 0xc6, 0xa3, 0x22, 0x82, 0x57, 0xfc, 0xbb, 0x7c, 0xf5,
	0x4f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xb7, 0x00, 0xfb, 0xed, 0x7a, 0x06, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// VCServiceClient is the client API for VCService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type VCServiceClient interface {
	Connect(ctx context.Context, in *ConnectAuthRequest, opts ...grpc.CallOption) (VCService_ConnectClient, error)
}

type vCServiceClient struct {
	cc *grpc.ClientConn
}

func NewVCServiceClient(cc *grpc.ClientConn) VCServiceClient {
	return &vCServiceClient{cc}
}

func (c *vCServiceClient) Connect(ctx context.Context, in *ConnectAuthRequest, opts ...grpc.CallOption) (VCService_ConnectClient, error) {
	stream, err := c.cc.NewStream(ctx, &_VCService_serviceDesc.Streams[0], "/protos.VCService/Connect", opts...)
	if err != nil {
		return nil, err
	}
	x := &vCServiceConnectClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type VCService_ConnectClient interface {
	Recv() (*VCEvent, error)
	grpc.ClientStream
}

type vCServiceConnectClient struct {
	grpc.ClientStream
}

func (x *vCServiceConnectClient) Recv() (*VCEvent, error) {
	m := new(VCEvent)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// VCServiceServer is the server API for VCService service.
type VCServiceServer interface {
	Connect(*ConnectAuthRequest, VCService_ConnectServer) error
}

// UnimplementedVCServiceServer can be embedded to have forward compatible implementations.
type UnimplementedVCServiceServer struct {
}

func (*UnimplementedVCServiceServer) Connect(req *ConnectAuthRequest, srv VCService_ConnectServer) error {
	return status.Errorf(codes.Unimplemented, "method Connect not implemented")
}

func RegisterVCServiceServer(s *grpc.Server, srv VCServiceServer) {
	s.RegisterService(&_VCService_serviceDesc, srv)
}

func _VCService_Connect_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ConnectAuthRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(VCServiceServer).Connect(m, &vCServiceConnectServer{stream})
}

type VCService_ConnectServer interface {
	Send(*VCEvent) error
	grpc.ServerStream
}

type vCServiceConnectServer struct {
	grpc.ServerStream
}

func (x *vCServiceConnectServer) Send(m *VCEvent) error {
	return x.ServerStream.SendMsg(m)
}

var _VCService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "protos.VCService",
	HandlerType: (*VCServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Connect",
			Handler:       _VCService_Connect_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "ps_vc_server.proto",
}