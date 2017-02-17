// Code generated by protoc-gen-go.
// source: traceinfo.proto
// DO NOT EDIT!

/*
Package recorders is a generated protocol buffer package.

It is generated from these files:
	traceinfo.proto

It has these top-level messages:
	TraceInfo
	Event
*/
package recorders

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type TraceInfo struct {
	ReqType          string   `protobuf:"bytes,1,opt,name=req_type,json=reqType" json:"req_type,omitempty"`
	CoordinatorAddr  []byte   `protobuf:"bytes,2,opt,name=coordinator_addr,json=coordinatorAddr,proto3" json:"coordinator_addr,omitempty"`
	DurationMicros   int32    `protobuf:"varint,3,opt,name=duration_micros,json=durationMicros" json:"duration_micros,omitempty"`
	Events           []*Event `protobuf:"bytes,4,rep,name=events" json:"events,omitempty"`
	ReqEndTimeMillis int64    `protobuf:"varint,5,opt,name=req_end_time_millis,json=reqEndTimeMillis" json:"req_end_time_millis,omitempty"`
}

func (m *TraceInfo) Reset()                    { *m = TraceInfo{} }
func (m *TraceInfo) String() string            { return proto.CompactTextString(m) }
func (*TraceInfo) ProtoMessage()               {}
func (*TraceInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *TraceInfo) GetEvents() []*Event {
	if m != nil {
		return m.Events
	}
	return nil
}

type Event struct {
	Source         []byte `protobuf:"bytes,1,opt,name=source,proto3" json:"source,omitempty"`
	Desc           string `protobuf:"bytes,2,opt,name=desc" json:"desc,omitempty"`
	DurationMicros int32  `protobuf:"varint,3,opt,name=duration_micros,json=durationMicros" json:"duration_micros,omitempty"`
}

func (m *Event) Reset()                    { *m = Event{} }
func (m *Event) String() string            { return proto.CompactTextString(m) }
func (*Event) ProtoMessage()               {}
func (*Event) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func init() {
	proto.RegisterType((*TraceInfo)(nil), "TraceInfo")
	proto.RegisterType((*Event)(nil), "Event")
}

func init() { proto.RegisterFile("traceinfo.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 269 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x8c, 0xd0, 0xc1, 0x4a, 0xf3, 0x40,
	0x10, 0x07, 0x70, 0xf6, 0x4b, 0x93, 0xcf, 0xac, 0xc5, 0xd4, 0x15, 0x24, 0x5e, 0x4a, 0xe8, 0xc5,
	0x78, 0x30, 0x07, 0x7d, 0x02, 0x85, 0x1e, 0x3c, 0xf4, 0xb2, 0xe4, 0x24, 0x42, 0x48, 0x76, 0xa7,
	0xb8, 0xd0, 0xec, 0x24, 0xb3, 0x5b, 0x21, 0x8f, 0xe8, 0x5b, 0x49, 0xd6, 0x0a, 0x1e, 0xbd, 0xcd,
	0xff, 0x07, 0x03, 0xff, 0x19, 0x9e, 0x79, 0x6a, 0x15, 0x18, 0xbb, 0xc7, 0x6a, 0x20, 0xf4, 0xb8,
	0xf9, 0x64, 0x3c, 0xad, 0x67, 0x7b, 0xb1, 0x7b, 0x14, 0x37, 0xfc, 0x8c, 0x60, 0x6c, 0xfc, 0x34,
	0x40, 0xce, 0x0a, 0x56, 0xa6, 0xf2, 0x3f, 0xc1, 0x58, 0x4f, 0x03, 0x88, 0x3b, 0xbe, 0x52, 0x88,
	0xa4, 0x8d, 0x6d, 0x3d, 0x52, 0xd3, 0x6a, 0x4d, 0xf9, 0xbf, 0x82, 0x95, 0x4b, 0x99, 0xfd, 0xf2,
	0x27, 0xad, 0x49, 0xdc, 0xf2, 0x4c, 0x1f, 0xa9, 0xf5, 0x06, 0x6d, 0xd3, 0x1b, 0x45, 0xe8, 0xf2,
	0xa8, 0x60, 0x65, 0x2c, 0x2f, 0x7e, 0x78, 0x17, 0x54, 0xac, 0x79, 0x02, 0x1f, 0x60, 0xbd, 0xcb,
	0x17, 0x45, 0x54, 0x9e, 0x3f, 0x24, 0xd5, 0x76, 0x8e, 0xf2, 0xa4, 0xe2, 0x9e, 0x5f, 0xcd, 0x75,
	0xc0, 0xea, 0xc6, 0x9b, 0x1e, 0x9a, 0xde, 0x1c, 0x0e, 0xc6, 0xe5, 0x71, 0xc1, 0xca, 0x48, 0xae,
	0x08, 0xc6, 0xad, 0xd5, 0xb5, 0xe9, 0x61, 0x17, 0x7c, 0xf3, 0xc6, 0xe3, 0xb0, 0x2f, 0xae, 0x79,
	0xe2, 0xf0, 0x48, 0xea, 0xfb, 0x88, 0xa5, 0x3c, 0x25, 0x21, 0xf8, 0x42, 0x83, 0x53, 0xa1, 0x77,
	0x2a, 0xc3, 0xfc, 0xe7, 0xb2, 0xcf, 0x6b, 0x7e, 0xa9, 0xb0, 0xaf, 0xa6, 0xf6, 0x1d, 0xb1, 0x9a,
	0x94, 0xeb, 0x2a, 0xdd, 0xbd, 0xa6, 0x04, 0x0a, 0x49, 0x03, 0xb9, 0x2e, 0x09, 0x0f, 0x7d, 0xfc,
	0x0a, 0x00, 0x00, 0xff, 0xff, 0x63, 0x54, 0x13, 0x7b, 0x63, 0x01, 0x00, 0x00,
}
