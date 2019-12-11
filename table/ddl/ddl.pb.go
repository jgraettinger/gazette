// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: table/ddl/ddl.proto

package ddl

import (
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	descriptor "github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	protocol "go.gazette.dev/core/broker/protocol"
	io "io"
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
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type Record struct {
	// Assigned publisher UUID of the Record.
	Uuid []byte `protobuf:"bytes,1,opt,name=uuid,proto3" json:"uuid,omitempty"`
	// Effective journal offset of the Record.
	// If this Record was produced as the result of a journal compaction,
	// then effective_offset is the maximum offset of the compacted
	// journal fragments. Otherwise, effective_offset is zero and its
	// implied value is the literal offset at which this Record was read.
	//
	// Effective offsets are used to:
	//   1) Determine whether a stream processor should ignore a Record
	//      (because it started reading the journal before the Record's
	//      effective_offset, and therefore has already seen all of its
	//      constituents.
	//   2) Determine the relative order in which Records should be merged.
	EffectiveOffset      int64    `protobuf:"varint,2,opt,name=effective_offset,json=effectiveOffset,proto3" json:"effective_offset,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Record) Reset()         { *m = Record{} }
func (m *Record) String() string { return proto.CompactTextString(m) }
func (*Record) ProtoMessage()    {}
func (*Record) Descriptor() ([]byte, []int) {
	return fileDescriptor_26e0ccd7fc972eb1, []int{0}
}
func (m *Record) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Record) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Record.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Record) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Record.Merge(m, src)
}
func (m *Record) XXX_Size() int {
	return m.ProtoSize()
}
func (m *Record) XXX_DiscardUnknown() {
	xxx_messageInfo_Record.DiscardUnknown(m)
}

var xxx_messageInfo_Record proto.InternalMessageInfo

func (m *Record) GetUuid() []byte {
	if m != nil {
		return m.Uuid
	}
	return nil
}

func (m *Record) GetEffectiveOffset() int64 {
	if m != nil {
		return m.EffectiveOffset
	}
	return 0
}

type MergeOptions struct {
	Lww                  bool                      `protobuf:"varint,1,opt,name=lww,proto3" json:"lww,omitempty"`
	Sum                  bool                      `protobuf:"varint,2,opt,name=sum,proto3" json:"sum,omitempty"`
	Max                  bool                      `protobuf:"varint,3,opt,name=max,proto3" json:"max,omitempty"`
	Min                  bool                      `protobuf:"varint,4,opt,name=min,proto3" json:"min,omitempty"`
	Append               *MergeOptions_Append      `protobuf:"bytes,5,opt,name=append,proto3" json:"append,omitempty"`
	Hll                  *MergeOptions_HyperLogLog `protobuf:"bytes,6,opt,name=hll,proto3" json:"hll,omitempty"`
	Key                  bool                      `protobuf:"varint,7,opt,name=key,proto3" json:"key,omitempty"`
	PruneOrder           bool                      `protobuf:"varint,8,opt,name=prune_order,json=pruneOrder,proto3" json:"prune_order,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                  `json:"-"`
	XXX_unrecognized     []byte                    `json:"-"`
	XXX_sizecache        int32                     `json:"-"`
}

func (m *MergeOptions) Reset()         { *m = MergeOptions{} }
func (m *MergeOptions) String() string { return proto.CompactTextString(m) }
func (*MergeOptions) ProtoMessage()    {}
func (*MergeOptions) Descriptor() ([]byte, []int) {
	return fileDescriptor_26e0ccd7fc972eb1, []int{1}
}
func (m *MergeOptions) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MergeOptions.Unmarshal(m, b)
}
func (m *MergeOptions) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MergeOptions.Marshal(b, m, deterministic)
}
func (m *MergeOptions) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MergeOptions.Merge(m, src)
}
func (m *MergeOptions) XXX_Size() int {
	return xxx_messageInfo_MergeOptions.Size(m)
}
func (m *MergeOptions) XXX_DiscardUnknown() {
	xxx_messageInfo_MergeOptions.DiscardUnknown(m)
}

var xxx_messageInfo_MergeOptions proto.InternalMessageInfo

func (m *MergeOptions) GetLww() bool {
	if m != nil {
		return m.Lww
	}
	return false
}

func (m *MergeOptions) GetSum() bool {
	if m != nil {
		return m.Sum
	}
	return false
}

func (m *MergeOptions) GetMax() bool {
	if m != nil {
		return m.Max
	}
	return false
}

func (m *MergeOptions) GetMin() bool {
	if m != nil {
		return m.Min
	}
	return false
}

func (m *MergeOptions) GetAppend() *MergeOptions_Append {
	if m != nil {
		return m.Append
	}
	return nil
}

func (m *MergeOptions) GetHll() *MergeOptions_HyperLogLog {
	if m != nil {
		return m.Hll
	}
	return nil
}

func (m *MergeOptions) GetKey() bool {
	if m != nil {
		return m.Key
	}
	return false
}

func (m *MergeOptions) GetPruneOrder() bool {
	if m != nil {
		return m.PruneOrder
	}
	return false
}

type MergeOptions_Append struct {
	Limit                int32    `protobuf:"varint,1,opt,name=limit,proto3" json:"limit,omitempty"`
	Fifo                 bool     `protobuf:"varint,2,opt,name=fifo,proto3" json:"fifo,omitempty"`
	Lifo                 bool     `protobuf:"varint,3,opt,name=lifo,proto3" json:"lifo,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MergeOptions_Append) Reset()         { *m = MergeOptions_Append{} }
func (m *MergeOptions_Append) String() string { return proto.CompactTextString(m) }
func (*MergeOptions_Append) ProtoMessage()    {}
func (*MergeOptions_Append) Descriptor() ([]byte, []int) {
	return fileDescriptor_26e0ccd7fc972eb1, []int{1, 0}
}
func (m *MergeOptions_Append) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MergeOptions_Append.Unmarshal(m, b)
}
func (m *MergeOptions_Append) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MergeOptions_Append.Marshal(b, m, deterministic)
}
func (m *MergeOptions_Append) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MergeOptions_Append.Merge(m, src)
}
func (m *MergeOptions_Append) XXX_Size() int {
	return xxx_messageInfo_MergeOptions_Append.Size(m)
}
func (m *MergeOptions_Append) XXX_DiscardUnknown() {
	xxx_messageInfo_MergeOptions_Append.DiscardUnknown(m)
}

var xxx_messageInfo_MergeOptions_Append proto.InternalMessageInfo

func (m *MergeOptions_Append) GetLimit() int32 {
	if m != nil {
		return m.Limit
	}
	return 0
}

func (m *MergeOptions_Append) GetFifo() bool {
	if m != nil {
		return m.Fifo
	}
	return false
}

func (m *MergeOptions_Append) GetLifo() bool {
	if m != nil {
		return m.Lifo
	}
	return false
}

type MergeOptions_HyperLogLog struct {
	P                    int32    `protobuf:"varint,1,opt,name=p,proto3" json:"p,omitempty"`
	Encoding             string   `protobuf:"bytes,2,opt,name=encoding,proto3" json:"encoding,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MergeOptions_HyperLogLog) Reset()         { *m = MergeOptions_HyperLogLog{} }
func (m *MergeOptions_HyperLogLog) String() string { return proto.CompactTextString(m) }
func (*MergeOptions_HyperLogLog) ProtoMessage()    {}
func (*MergeOptions_HyperLogLog) Descriptor() ([]byte, []int) {
	return fileDescriptor_26e0ccd7fc972eb1, []int{1, 1}
}
func (m *MergeOptions_HyperLogLog) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MergeOptions_HyperLogLog.Unmarshal(m, b)
}
func (m *MergeOptions_HyperLogLog) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MergeOptions_HyperLogLog.Marshal(b, m, deterministic)
}
func (m *MergeOptions_HyperLogLog) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MergeOptions_HyperLogLog.Merge(m, src)
}
func (m *MergeOptions_HyperLogLog) XXX_Size() int {
	return xxx_messageInfo_MergeOptions_HyperLogLog.Size(m)
}
func (m *MergeOptions_HyperLogLog) XXX_DiscardUnknown() {
	xxx_messageInfo_MergeOptions_HyperLogLog.DiscardUnknown(m)
}

var xxx_messageInfo_MergeOptions_HyperLogLog proto.InternalMessageInfo

func (m *MergeOptions_HyperLogLog) GetP() int32 {
	if m != nil {
		return m.P
	}
	return 0
}

func (m *MergeOptions_HyperLogLog) GetEncoding() string {
	if m != nil {
		return m.Encoding
	}
	return ""
}

type ParquetOptions struct {
	BloomFilter          bool     `protobuf:"varint,1,opt,name=bloom_filter,json=bloomFilter,proto3" json:"bloom_filter,omitempty"`
	Zorder               bool     `protobuf:"varint,2,opt,name=zorder,proto3" json:"zorder,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ParquetOptions) Reset()         { *m = ParquetOptions{} }
func (m *ParquetOptions) String() string { return proto.CompactTextString(m) }
func (*ParquetOptions) ProtoMessage()    {}
func (*ParquetOptions) Descriptor() ([]byte, []int) {
	return fileDescriptor_26e0ccd7fc972eb1, []int{2}
}
func (m *ParquetOptions) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ParquetOptions.Unmarshal(m, b)
}
func (m *ParquetOptions) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ParquetOptions.Marshal(b, m, deterministic)
}
func (m *ParquetOptions) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ParquetOptions.Merge(m, src)
}
func (m *ParquetOptions) XXX_Size() int {
	return xxx_messageInfo_ParquetOptions.Size(m)
}
func (m *ParquetOptions) XXX_DiscardUnknown() {
	xxx_messageInfo_ParquetOptions.DiscardUnknown(m)
}

var xxx_messageInfo_ParquetOptions proto.InternalMessageInfo

func (m *ParquetOptions) GetBloomFilter() bool {
	if m != nil {
		return m.BloomFilter
	}
	return false
}

func (m *ParquetOptions) GetZorder() bool {
	if m != nil {
		return m.Zorder
	}
	return false
}

type TableSpec struct {
	Namespace            string                `protobuf:"bytes,1,opt,name=namespace,proto3" json:"namespace,omitempty"`
	Name                 string                `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Template             *protocol.JournalSpec `protobuf:"bytes,3,opt,name=template,proto3" json:"template,omitempty"`
	PartitionLimit       int32                 `protobuf:"varint,4,opt,name=partition_limit,json=partitionLimit,proto3" json:"partition_limit,omitempty"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *TableSpec) Reset()         { *m = TableSpec{} }
func (m *TableSpec) String() string { return proto.CompactTextString(m) }
func (*TableSpec) ProtoMessage()    {}
func (*TableSpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_26e0ccd7fc972eb1, []int{3}
}
func (m *TableSpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_TableSpec.Unmarshal(m, b)
}
func (m *TableSpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_TableSpec.Marshal(b, m, deterministic)
}
func (m *TableSpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TableSpec.Merge(m, src)
}
func (m *TableSpec) XXX_Size() int {
	return xxx_messageInfo_TableSpec.Size(m)
}
func (m *TableSpec) XXX_DiscardUnknown() {
	xxx_messageInfo_TableSpec.DiscardUnknown(m)
}

var xxx_messageInfo_TableSpec proto.InternalMessageInfo

func (m *TableSpec) GetNamespace() string {
	if m != nil {
		return m.Namespace
	}
	return ""
}

func (m *TableSpec) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *TableSpec) GetTemplate() *protocol.JournalSpec {
	if m != nil {
		return m.Template
	}
	return nil
}

func (m *TableSpec) GetPartitionLimit() int32 {
	if m != nil {
		return m.PartitionLimit
	}
	return 0
}

type TableFieldSpec struct {
	Name                 string          `protobuf:"bytes,8,opt,name=name,proto3" json:"name,omitempty"`
	Partitioned          bool            `protobuf:"varint,1,opt,name=partitioned,proto3" json:"partitioned,omitempty"`
	Group                bool            `protobuf:"varint,2,opt,name=group,proto3" json:"group,omitempty"`
	Merge                *MergeOptions   `protobuf:"bytes,3,opt,name=merge,proto3" json:"merge,omitempty"`
	FilterAfter          string          `protobuf:"bytes,4,opt,name=filter_after,json=filterAfter,proto3" json:"filter_after,omitempty"`
	Parquet              *ParquetOptions `protobuf:"bytes,6,opt,name=parquet,proto3" json:"parquet,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *TableFieldSpec) Reset()         { *m = TableFieldSpec{} }
func (m *TableFieldSpec) String() string { return proto.CompactTextString(m) }
func (*TableFieldSpec) ProtoMessage()    {}
func (*TableFieldSpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_26e0ccd7fc972eb1, []int{4}
}
func (m *TableFieldSpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_TableFieldSpec.Unmarshal(m, b)
}
func (m *TableFieldSpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_TableFieldSpec.Marshal(b, m, deterministic)
}
func (m *TableFieldSpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TableFieldSpec.Merge(m, src)
}
func (m *TableFieldSpec) XXX_Size() int {
	return xxx_messageInfo_TableFieldSpec.Size(m)
}
func (m *TableFieldSpec) XXX_DiscardUnknown() {
	xxx_messageInfo_TableFieldSpec.DiscardUnknown(m)
}

var xxx_messageInfo_TableFieldSpec proto.InternalMessageInfo

func (m *TableFieldSpec) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *TableFieldSpec) GetPartitioned() bool {
	if m != nil {
		return m.Partitioned
	}
	return false
}

func (m *TableFieldSpec) GetGroup() bool {
	if m != nil {
		return m.Group
	}
	return false
}

func (m *TableFieldSpec) GetMerge() *MergeOptions {
	if m != nil {
		return m.Merge
	}
	return nil
}

func (m *TableFieldSpec) GetFilterAfter() string {
	if m != nil {
		return m.FilterAfter
	}
	return ""
}

func (m *TableFieldSpec) GetParquet() *ParquetOptions {
	if m != nil {
		return m.Parquet
	}
	return nil
}

var E_Spec = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.MessageOptions)(nil),
	ExtensionType: (*TableSpec)(nil),
	Field:         86753,
	Name:          "gazette.table.spec",
	Tag:           "bytes,86753,opt,name=spec",
	Filename:      "table/ddl/ddl.proto",
}

var E_Field = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.FieldOptions)(nil),
	ExtensionType: (*TableFieldSpec)(nil),
	Field:         86753,
	Name:          "gazette.table.field",
	Tag:           "bytes,86753,opt,name=field",
	Filename:      "table/ddl/ddl.proto",
}

func init() {
	proto.RegisterType((*Record)(nil), "gazette.table.Record")
	proto.RegisterType((*MergeOptions)(nil), "gazette.table.MergeOptions")
	proto.RegisterType((*MergeOptions_Append)(nil), "gazette.table.MergeOptions.Append")
	proto.RegisterType((*MergeOptions_HyperLogLog)(nil), "gazette.table.MergeOptions.HyperLogLog")
	proto.RegisterType((*ParquetOptions)(nil), "gazette.table.ParquetOptions")
	proto.RegisterType((*TableSpec)(nil), "gazette.table.TableSpec")
	proto.RegisterType((*TableFieldSpec)(nil), "gazette.table.TableFieldSpec")
	proto.RegisterExtension(E_Spec)
	proto.RegisterExtension(E_Field)
}

func init() { proto.RegisterFile("table/ddl/ddl.proto", fileDescriptor_26e0ccd7fc972eb1) }

var fileDescriptor_26e0ccd7fc972eb1 = []byte{
	// 665 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x54, 0x4d, 0x6f, 0xd3, 0x40,
	0x10, 0x95, 0xc9, 0x47, 0x93, 0x71, 0x68, 0xab, 0xa5, 0x20, 0x2b, 0x10, 0x12, 0x72, 0x69, 0xb9,
	0x38, 0x6a, 0x39, 0x54, 0xe4, 0x56, 0x0e, 0x15, 0x82, 0x56, 0x41, 0x4b, 0xb9, 0x70, 0x89, 0x1c,
	0x7b, 0x6c, 0xac, 0xda, 0xde, 0x65, 0x6d, 0xf7, 0xeb, 0xc6, 0x8d, 0x23, 0x57, 0x8e, 0x3d, 0xf0,
	0xbf, 0xe0, 0xcc, 0x9f, 0x40, 0x3b, 0xeb, 0x98, 0xb4, 0xa0, 0x1e, 0x22, 0xcd, 0x3c, 0xcf, 0xbc,
	0x7d, 0xf3, 0x66, 0x37, 0xf0, 0xa0, 0xf0, 0x16, 0x09, 0x4e, 0x82, 0x20, 0xd1, 0x3f, 0x57, 0x2a,
	0x51, 0x08, 0x76, 0x3f, 0xf2, 0xae, 0xb0, 0x28, 0xd0, 0xa5, 0x8f, 0xfd, 0xa7, 0x0b, 0x25, 0x4e,
	0x51, 0x4d, 0xe8, 0xa3, 0x2f, 0x92, 0x3a, 0x30, 0xe5, 0xfd, 0x51, 0x24, 0x44, 0x94, 0xa0, 0x81,
	0x17, 0x65, 0x38, 0x09, 0x30, 0xf7, 0x55, 0x2c, 0x0b, 0xa1, 0xaa, 0x8a, 0xad, 0x48, 0x44, 0x82,
	0xc2, 0x89, 0x8e, 0x0c, 0x3a, 0xfe, 0x00, 0x6d, 0x8e, 0xbe, 0x50, 0x01, 0x63, 0xd0, 0x2c, 0xcb,
	0x38, 0x70, 0xac, 0x91, 0xb5, 0xd3, 0xe3, 0x14, 0xb3, 0xe7, 0xb0, 0x89, 0x61, 0x88, 0x7e, 0x11,
	0x9f, 0xe1, 0x5c, 0x84, 0x61, 0x8e, 0x85, 0x73, 0x6f, 0x64, 0xed, 0x34, 0xf8, 0x46, 0x8d, 0xcf,
	0x08, 0x9e, 0xf6, 0xbe, 0x5e, 0x0f, 0xad, 0x6f, 0xd7, 0x43, 0xeb, 0xe7, 0xf5, 0xd0, 0x1a, 0x7f,
	0x69, 0x40, 0xef, 0x18, 0x55, 0x84, 0x33, 0x59, 0xc4, 0x22, 0xcb, 0xd9, 0x26, 0x34, 0x92, 0xf3,
	0x73, 0x22, 0xef, 0x70, 0x1d, 0x6a, 0x24, 0x2f, 0x53, 0xa2, 0xeb, 0x70, 0x1d, 0x6a, 0x24, 0xf5,
	0x2e, 0x9c, 0x86, 0x41, 0x52, 0xef, 0x82, 0x90, 0x38, 0x73, 0x9a, 0x15, 0x12, 0x67, 0x6c, 0x0a,
	0x6d, 0x4f, 0x4a, 0xcc, 0x02, 0xa7, 0x35, 0xb2, 0x76, 0xec, 0xbd, 0xb1, 0x7b, 0xc3, 0x27, 0x77,
	0xf5, 0x50, 0xf7, 0x80, 0x2a, 0x79, 0xd5, 0xc1, 0x5e, 0x42, 0xe3, 0x53, 0x92, 0x38, 0x6d, 0x6a,
	0xdc, 0xbe, 0xab, 0xf1, 0xf5, 0xa5, 0x44, 0x75, 0x24, 0xa2, 0x23, 0x11, 0x71, 0xdd, 0xa3, 0x85,
	0x9c, 0xe2, 0xa5, 0xb3, 0x66, 0x84, 0x9c, 0xe2, 0x25, 0x1b, 0x82, 0x2d, 0x55, 0x99, 0xe1, 0x5c,
	0xa8, 0x00, 0x95, 0xd3, 0xa1, 0x2f, 0x40, 0xd0, 0x4c, 0x23, 0xfd, 0x43, 0x68, 0x9b, 0xf3, 0xd9,
	0x16, 0xb4, 0x92, 0x38, 0x8d, 0x0b, 0x9a, 0xbe, 0xc5, 0x4d, 0xa2, 0xfd, 0x0e, 0xe3, 0x50, 0x54,
	0x06, 0x50, 0xac, 0xb1, 0x44, 0x63, 0xc6, 0x02, 0x8a, 0xfb, 0xfb, 0x60, 0xaf, 0xc8, 0x61, 0x3d,
	0xb0, 0x64, 0x45, 0x64, 0x49, 0xd6, 0x87, 0x0e, 0x66, 0xbe, 0x08, 0xe2, 0x2c, 0x22, 0xa2, 0x2e,
	0xaf, 0xf3, 0xf1, 0x5b, 0x58, 0x7f, 0xe7, 0xa9, 0xcf, 0x25, 0x16, 0xcb, 0x25, 0x3c, 0x83, 0xde,
	0x22, 0x11, 0x22, 0x9d, 0x87, 0x71, 0x52, 0xa0, 0xaa, 0xb6, 0x61, 0x13, 0x76, 0x48, 0x10, 0x7b,
	0x04, 0xed, 0x2b, 0x33, 0x91, 0xd1, 0x55, 0x65, 0xe3, 0xef, 0x16, 0x74, 0x4f, 0xb4, 0x51, 0xef,
	0x25, 0xfa, 0xec, 0x09, 0x74, 0x33, 0x2f, 0xc5, 0x5c, 0x7a, 0x3e, 0x12, 0x4b, 0x97, 0xff, 0x05,
	0xf4, 0x14, 0x3a, 0xa9, 0x04, 0x51, 0xcc, 0x76, 0xa1, 0x53, 0x60, 0x2a, 0x13, 0xaf, 0x40, 0x9a,
	0xce, 0xde, 0x7b, 0xe8, 0xd6, 0x57, 0xf8, 0x8d, 0x28, 0x55, 0xe6, 0x25, 0x9a, 0x9a, 0xd7, 0x65,
	0x6c, 0x1b, 0x36, 0xa4, 0xa7, 0x8a, 0x58, 0x6b, 0x9f, 0x1b, 0x03, 0x9b, 0x34, 0xf7, 0x7a, 0x0d,
	0x1f, 0x69, 0x74, 0xfc, 0xdb, 0x82, 0x75, 0xd2, 0x76, 0x18, 0x63, 0x12, 0x90, 0xc0, 0xa5, 0x84,
	0xce, 0x8a, 0x84, 0x11, 0xd8, 0x75, 0x23, 0x06, 0xcb, 0xe1, 0x57, 0x20, 0xbd, 0xa8, 0x48, 0x89,
	0x52, 0x56, 0xb3, 0x9b, 0x84, 0xed, 0x42, 0x2b, 0xd5, 0x97, 0xa3, 0xd2, 0xfd, 0xf8, 0x8e, 0x8b,
	0xc3, 0x4d, 0xa5, 0x36, 0xda, 0x58, 0x3c, 0xf7, 0x42, 0x6d, 0x74, 0x93, 0x64, 0xd8, 0x06, 0x3b,
	0xd0, 0x10, 0xdb, 0x87, 0x35, 0x69, 0xb6, 0x53, 0x5d, 0xc8, 0xc1, 0x2d, 0xde, 0x9b, 0xbb, 0xe3,
	0xcb, 0xea, 0xe9, 0x0c, 0x9a, 0xb9, 0x1e, 0x71, 0xe8, 0x9a, 0x27, 0xef, 0x2e, 0x9f, 0xbc, 0x7b,
	0x8c, 0x79, 0xee, 0xd5, 0x5a, 0x9c, 0x5f, 0x3f, 0xcc, 0x13, 0x71, 0x6e, 0x11, 0xd7, 0x6b, 0xe4,
	0x44, 0x34, 0x3d, 0x81, 0x56, 0xa8, 0x8d, 0x63, 0x83, 0x7f, 0x18, 0xc9, 0xd0, 0xdb, 0x7c, 0x83,
	0xff, 0xf1, 0xd5, 0xd6, 0x73, 0x43, 0xf6, 0x6a, 0xf8, 0x71, 0x10, 0x89, 0xba, 0x32, 0xc0, 0xb3,
	0x89, 0x2f, 0x14, 0x4e, 0xea, 0xbf, 0xba, 0x45, 0x9b, 0x4e, 0x79, 0xf1, 0x27, 0x00, 0x00, 0xff,
	0xff, 0xd9, 0x53, 0x8f, 0xda, 0xfe, 0x04, 0x00, 0x00,
}

func (m *Record) Marshal() (dAtA []byte, err error) {
	size := m.ProtoSize()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Record) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Uuid) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintDdl(dAtA, i, uint64(len(m.Uuid)))
		i += copy(dAtA[i:], m.Uuid)
	}
	if m.EffectiveOffset != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintDdl(dAtA, i, uint64(m.EffectiveOffset))
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeVarintDdl(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Record) ProtoSize() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Uuid)
	if l > 0 {
		n += 1 + l + sovDdl(uint64(l))
	}
	if m.EffectiveOffset != 0 {
		n += 1 + sovDdl(uint64(m.EffectiveOffset))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovDdl(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozDdl(x uint64) (n int) {
	return sovDdl(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Record) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDdl
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Record: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Record: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Uuid", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDdl
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthDdl
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthDdl
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Uuid = append(m.Uuid[:0], dAtA[iNdEx:postIndex]...)
			if m.Uuid == nil {
				m.Uuid = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field EffectiveOffset", wireType)
			}
			m.EffectiveOffset = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDdl
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.EffectiveOffset |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipDdl(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthDdl
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthDdl
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipDdl(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowDdl
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDdl
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDdl
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthDdl
			}
			iNdEx += length
			if iNdEx < 0 {
				return 0, ErrInvalidLengthDdl
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowDdl
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipDdl(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
				if iNdEx < 0 {
					return 0, ErrInvalidLengthDdl
				}
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthDdl = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowDdl   = fmt.Errorf("proto: integer overflow")
)
