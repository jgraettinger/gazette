// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: table/protocol/protocol.proto

package protocol

import (
	context "context"
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	types "github.com/gogo/protobuf/types"
	go_gazette_dev_core_broker_protocol "go.gazette.dev/core/broker/protocol"
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
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type InsertRequest struct {
	Rows                 []types.Any `protobuf:"bytes,1,rep,name=rows,proto3" json:"rows"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
}

func (m *InsertRequest) Reset()         { *m = InsertRequest{} }
func (m *InsertRequest) String() string { return proto.CompactTextString(m) }
func (*InsertRequest) ProtoMessage()    {}
func (*InsertRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_5c049facc0b9aaf8, []int{0}
}
func (m *InsertRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_InsertRequest.Unmarshal(m, b)
}
func (m *InsertRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_InsertRequest.Marshal(b, m, deterministic)
}
func (m *InsertRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_InsertRequest.Merge(m, src)
}
func (m *InsertRequest) XXX_Size() int {
	return xxx_messageInfo_InsertRequest.Size(m)
}
func (m *InsertRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_InsertRequest.DiscardUnknown(m)
}

var xxx_messageInfo_InsertRequest proto.InternalMessageInfo

type InsertResponse struct {
	// Journals and offsets this insert transaction published through, including
	// acknowledgements.
	PublishAt            map[go_gazette_dev_core_broker_protocol.Journal]go_gazette_dev_core_broker_protocol.Offset `protobuf:"bytes,4,rep,name=publish_at,json=publishAt,proto3,castkey=go.gazette.dev/core/broker/protocol.Journal,castvalue=go.gazette.dev/core/broker/protocol.Offset" json:"publish_at,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}                                                                                   `json:"-"`
	XXX_unrecognized     []byte                                                                                     `json:"-"`
}

func (m *InsertResponse) Reset()         { *m = InsertResponse{} }
func (m *InsertResponse) String() string { return proto.CompactTextString(m) }
func (*InsertResponse) ProtoMessage()    {}
func (*InsertResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_5c049facc0b9aaf8, []int{1}
}
func (m *InsertResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_InsertResponse.Unmarshal(m, b)
}
func (m *InsertResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_InsertResponse.Marshal(b, m, deterministic)
}
func (m *InsertResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_InsertResponse.Merge(m, src)
}
func (m *InsertResponse) XXX_Size() int {
	return xxx_messageInfo_InsertResponse.Size(m)
}
func (m *InsertResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_InsertResponse.DiscardUnknown(m)
}

var xxx_messageInfo_InsertResponse proto.InternalMessageInfo

func init() {
	proto.RegisterType((*InsertRequest)(nil), "table.InsertRequest")
	proto.RegisterType((*InsertResponse)(nil), "table.InsertResponse")
	proto.RegisterMapType((map[go_gazette_dev_core_broker_protocol.Journal]go_gazette_dev_core_broker_protocol.Offset)(nil), "table.InsertResponse.PublishAtEntry")
}

func init() { proto.RegisterFile("table/protocol/protocol.proto", fileDescriptor_5c049facc0b9aaf8) }

var fileDescriptor_5c049facc0b9aaf8 = []byte{
	// 339 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x91, 0xcd, 0x4a, 0x33, 0x31,
	0x14, 0x86, 0x3b, 0xfd, 0xf9, 0xa0, 0x29, 0x5f, 0x91, 0x30, 0xc2, 0x38, 0x60, 0x5b, 0x8a, 0x8b,
	0xa2, 0x92, 0x81, 0xba, 0x91, 0x22, 0x48, 0x2b, 0x2e, 0x74, 0xa3, 0x0c, 0x5d, 0x09, 0x22, 0x99,
	0x7a, 0x3a, 0x96, 0x0e, 0x39, 0x63, 0x92, 0xa9, 0x8c, 0x4b, 0x97, 0x5e, 0x86, 0x57, 0xd3, 0xa5,
	0x57, 0x60, 0x11, 0x2f, 0xc1, 0x1b, 0x90, 0x66, 0x18, 0xa5, 0xd2, 0x85, 0xbb, 0x27, 0xc9, 0xc3,
	0x7b, 0xf2, 0x26, 0x64, 0x5b, 0xf3, 0x20, 0x02, 0x2f, 0x96, 0xa8, 0x71, 0x84, 0xd1, 0x37, 0x30,
	0x03, 0xb4, 0x62, 0x8e, 0xdd, 0xad, 0x10, 0x31, 0xcc, 0xb5, 0x20, 0x19, 0x7b, 0x5c, 0xa4, 0x99,
	0xe1, 0xda, 0x21, 0x86, 0x68, 0xd0, 0x5b, 0x52, 0xb6, 0xdb, 0x3e, 0x26, 0xff, 0xcf, 0x84, 0x02,
	0xa9, 0x7d, 0xb8, 0x4f, 0x40, 0x69, 0xca, 0x48, 0x59, 0xe2, 0x83, 0x72, 0xac, 0x56, 0xa9, 0x53,
	0xeb, 0xda, 0x2c, 0x0b, 0x64, 0x79, 0x20, 0xeb, 0x8b, 0x74, 0x50, 0x9e, 0xbf, 0x35, 0x0b, 0xbe,
	0xf1, 0xda, 0x9f, 0x16, 0xa9, 0xe7, 0x09, 0x2a, 0x46, 0xa1, 0x80, 0xbe, 0x58, 0x84, 0xc4, 0x49,
	0x10, 0x4d, 0xd4, 0xdd, 0x0d, 0xd7, 0x4e, 0xd9, 0x24, 0xed, 0x30, 0x73, 0x43, 0xb6, 0xea, 0xb2,
	0xcb, 0xcc, 0xeb, 0xeb, 0x53, 0xa1, 0x65, 0x3a, 0xb8, 0x7e, 0x5a, 0x34, 0xf7, 0x42, 0x64, 0x21,
	0x7f, 0x04, 0xad, 0x81, 0xdd, 0xc2, 0xcc, 0x1b, 0xa1, 0x04, 0x2f, 0x90, 0x38, 0x05, 0xf9, 0xd3,
	0xfa, 0x1c, 0x13, 0x29, 0x78, 0xf4, 0xbc, 0x68, 0xee, 0xfe, 0x45, 0xbf, 0x18, 0x8f, 0x15, 0x68,
	0xbf, 0x1a, 0xe7, 0xe3, 0xdc, 0x23, 0x52, 0x5f, 0x9d, 0x4d, 0x37, 0x48, 0x69, 0x0a, 0xa9, 0x63,
	0xb5, 0xac, 0x4e, 0xd5, 0x5f, 0x22, 0xb5, 0x49, 0x65, 0xc6, 0xa3, 0x04, 0x9c, 0x62, 0xcb, 0xea,
	0x94, 0xfc, 0x6c, 0xd1, 0x2b, 0x1e, 0x5a, 0xdd, 0x13, 0x52, 0x19, 0x2e, 0xeb, 0xd0, 0x1e, 0xa9,
	0x0d, 0x25, 0x17, 0x8a, 0x8f, 0xf4, 0x04, 0x05, 0xb5, 0x7f, 0xb5, 0x34, 0x6f, 0xea, 0x6e, 0xae,
	0xed, 0x3e, 0xd8, 0x9f, 0xbf, 0x37, 0x0a, 0xaf, 0x1f, 0x8d, 0xc2, 0x55, 0x7b, 0x5d, 0x87, 0xd5,
	0x0f, 0x0f, 0xfe, 0x19, 0x3a, 0xf8, 0x0a, 0x00, 0x00, 0xff, 0xff, 0x73, 0x1e, 0xb1, 0x96, 0x09,
	0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// TableClient is the client API for Table service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type TableClient interface {
	Transaction(ctx context.Context, in *InsertRequest, opts ...grpc.CallOption) (*InsertResponse, error)
}

type tableClient struct {
	cc *grpc.ClientConn
}

func NewTableClient(cc *grpc.ClientConn) TableClient {
	return &tableClient{cc}
}

func (c *tableClient) Transaction(ctx context.Context, in *InsertRequest, opts ...grpc.CallOption) (*InsertResponse, error) {
	out := new(InsertResponse)
	err := c.cc.Invoke(ctx, "/table.Table/Transaction", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TableServer is the server API for Table service.
type TableServer interface {
	Transaction(context.Context, *InsertRequest) (*InsertResponse, error)
}

// UnimplementedTableServer can be embedded to have forward compatible implementations.
type UnimplementedTableServer struct {
}

func (*UnimplementedTableServer) Transaction(ctx context.Context, req *InsertRequest) (*InsertResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Transaction not implemented")
}

func RegisterTableServer(s *grpc.Server, srv TableServer) {
	s.RegisterService(&_Table_serviceDesc, srv)
}

func _Table_Transaction_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InsertRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TableServer).Transaction(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/table.Table/Transaction",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TableServer).Transaction(ctx, req.(*InsertRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Table_serviceDesc = grpc.ServiceDesc{
	ServiceName: "table.Table",
	HandlerType: (*TableServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Transaction",
			Handler:    _Table_Transaction_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "table/protocol/protocol.proto",
}
