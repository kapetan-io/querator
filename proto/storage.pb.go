//
//Copyright 2024 Derrick J Wippler
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        (unknown)
// source: proto/storage.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type StorageInspectRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *StorageInspectRequest) Reset() {
	*x = StorageInspectRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_storage_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StorageInspectRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StorageInspectRequest) ProtoMessage() {}

func (x *StorageInspectRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_storage_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StorageInspectRequest.ProtoReflect.Descriptor instead.
func (*StorageInspectRequest) Descriptor() ([]byte, []int) {
	return file_proto_storage_proto_rawDescGZIP(), []int{0}
}

func (x *StorageInspectRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type StorageListRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	QueueName string `protobuf:"bytes,1,opt,name=queueName,json=queue_name,proto3" json:"queueName,omitempty"`
	Pivot     string `protobuf:"bytes,2,opt,name=pivot,proto3" json:"pivot,omitempty"`
	Limit     int32  `protobuf:"varint,3,opt,name=limit,proto3" json:"limit,omitempty"`
}

func (x *StorageListRequest) Reset() {
	*x = StorageListRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_storage_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StorageListRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StorageListRequest) ProtoMessage() {}

func (x *StorageListRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_storage_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StorageListRequest.ProtoReflect.Descriptor instead.
func (*StorageListRequest) Descriptor() ([]byte, []int) {
	return file_proto_storage_proto_rawDescGZIP(), []int{1}
}

func (x *StorageListRequest) GetQueueName() string {
	if x != nil {
		return x.QueueName
	}
	return ""
}

func (x *StorageListRequest) GetPivot() string {
	if x != nil {
		return x.Pivot
	}
	return ""
}

func (x *StorageListRequest) GetLimit() int32 {
	if x != nil {
		return x.Limit
	}
	return 0
}

type StorageListResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Items []*StorageItem `protobuf:"bytes,1,rep,name=items,proto3" json:"items,omitempty"`
}

func (x *StorageListResponse) Reset() {
	*x = StorageListResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_storage_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StorageListResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StorageListResponse) ProtoMessage() {}

func (x *StorageListResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_storage_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StorageListResponse.ProtoReflect.Descriptor instead.
func (*StorageListResponse) Descriptor() ([]byte, []int) {
	return file_proto_storage_proto_rawDescGZIP(), []int{2}
}

func (x *StorageListResponse) GetItems() []*StorageItem {
	if x != nil {
		return x.Items
	}
	return nil
}

type StorageItem struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id              string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	IsReserved      bool                   `protobuf:"varint,2,opt,name=isReserved,json=is_reserved,proto3" json:"isReserved,omitempty"`
	ReserveDeadline *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=reserveDeadline,json=reserve_deadline,proto3" json:"reserveDeadline,omitempty"`
	DeadDeadline    *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=deadDeadline,json=dead_deadline,proto3" json:"deadDeadline,omitempty"`
	Attempts        int32                  `protobuf:"varint,5,opt,name=attempts,proto3" json:"attempts,omitempty"`
	MaxAttempts     int32                  `protobuf:"varint,6,opt,name=maxAttempts,json=max_attempts,proto3" json:"maxAttempts,omitempty"`
	Reference       string                 `protobuf:"bytes,7,opt,name=reference,proto3" json:"reference,omitempty"`
	Encoding        string                 `protobuf:"bytes,8,opt,name=encoding,proto3" json:"encoding,omitempty"`
	Kind            string                 `protobuf:"bytes,9,opt,name=kind,proto3" json:"kind,omitempty"`
	Body            []byte                 `protobuf:"bytes,10,opt,name=body,proto3" json:"body,omitempty"`
}

func (x *StorageItem) Reset() {
	*x = StorageItem{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_storage_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StorageItem) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StorageItem) ProtoMessage() {}

func (x *StorageItem) ProtoReflect() protoreflect.Message {
	mi := &file_proto_storage_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StorageItem.ProtoReflect.Descriptor instead.
func (*StorageItem) Descriptor() ([]byte, []int) {
	return file_proto_storage_proto_rawDescGZIP(), []int{3}
}

func (x *StorageItem) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *StorageItem) GetIsReserved() bool {
	if x != nil {
		return x.IsReserved
	}
	return false
}

func (x *StorageItem) GetReserveDeadline() *timestamppb.Timestamp {
	if x != nil {
		return x.ReserveDeadline
	}
	return nil
}

func (x *StorageItem) GetDeadDeadline() *timestamppb.Timestamp {
	if x != nil {
		return x.DeadDeadline
	}
	return nil
}

func (x *StorageItem) GetAttempts() int32 {
	if x != nil {
		return x.Attempts
	}
	return 0
}

func (x *StorageItem) GetMaxAttempts() int32 {
	if x != nil {
		return x.MaxAttempts
	}
	return 0
}

func (x *StorageItem) GetReference() string {
	if x != nil {
		return x.Reference
	}
	return ""
}

func (x *StorageItem) GetEncoding() string {
	if x != nil {
		return x.Encoding
	}
	return ""
}

func (x *StorageItem) GetKind() string {
	if x != nil {
		return x.Kind
	}
	return ""
}

func (x *StorageItem) GetBody() []byte {
	if x != nil {
		return x.Body
	}
	return nil
}

var File_proto_storage_proto protoreflect.FileDescriptor

var file_proto_storage_proto_rawDesc = []byte{
	0x0a, 0x13, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x71, 0x75, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x1a,
	0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x27, 0x0a, 0x15, 0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x49, 0x6e, 0x73, 0x70, 0x65,
	0x63, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x22, 0x5f, 0x0a, 0x12, 0x53, 0x74, 0x6f,
	0x72, 0x61, 0x67, 0x65, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x1d, 0x0a, 0x09, 0x71, 0x75, 0x65, 0x75, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0a, 0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x14,
	0x0a, 0x05, 0x70, 0x69, 0x76, 0x6f, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x70,
	0x69, 0x76, 0x6f, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x05, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x22, 0x42, 0x0a, 0x13, 0x53, 0x74,
	0x6f, 0x72, 0x61, 0x67, 0x65, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x2b, 0x0a, 0x05, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x15, 0x2e, 0x71, 0x75, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x53, 0x74, 0x6f, 0x72,
	0x61, 0x67, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x05, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x22, 0xe7,
	0x02, 0x0a, 0x0b, 0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x1f,
	0x0a, 0x0a, 0x69, 0x73, 0x52, 0x65, 0x73, 0x65, 0x72, 0x76, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x08, 0x52, 0x0b, 0x69, 0x73, 0x5f, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76, 0x65, 0x64, 0x12,
	0x45, 0x0a, 0x0f, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76, 0x65, 0x44, 0x65, 0x61, 0x64, 0x6c, 0x69,
	0x6e, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x52, 0x10, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76, 0x65, 0x5f, 0x64, 0x65,
	0x61, 0x64, 0x6c, 0x69, 0x6e, 0x65, 0x12, 0x3f, 0x0a, 0x0c, 0x64, 0x65, 0x61, 0x64, 0x44, 0x65,
	0x61, 0x64, 0x6c, 0x69, 0x6e, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0d, 0x64, 0x65, 0x61, 0x64, 0x5f, 0x64,
	0x65, 0x61, 0x64, 0x6c, 0x69, 0x6e, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x61, 0x74, 0x74, 0x65, 0x6d,
	0x70, 0x74, 0x73, 0x18, 0x05, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x61, 0x74, 0x74, 0x65, 0x6d,
	0x70, 0x74, 0x73, 0x12, 0x21, 0x0a, 0x0b, 0x6d, 0x61, 0x78, 0x41, 0x74, 0x74, 0x65, 0x6d, 0x70,
	0x74, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x6d, 0x61, 0x78, 0x5f, 0x61, 0x74,
	0x74, 0x65, 0x6d, 0x70, 0x74, 0x73, 0x12, 0x1c, 0x0a, 0x09, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65,
	0x6e, 0x63, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x72, 0x65, 0x66, 0x65, 0x72,
	0x65, 0x6e, 0x63, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67,
	0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67,
	0x12, 0x12, 0x0a, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x18, 0x09, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6b, 0x69, 0x6e, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x18, 0x0a, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x42, 0x26, 0x5a, 0x24, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6b, 0x61, 0x70, 0x65, 0x74, 0x61, 0x6e, 0x2d, 0x69,
	0x6f, 0x2f, 0x71, 0x75, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_storage_proto_rawDescOnce sync.Once
	file_proto_storage_proto_rawDescData = file_proto_storage_proto_rawDesc
)

func file_proto_storage_proto_rawDescGZIP() []byte {
	file_proto_storage_proto_rawDescOnce.Do(func() {
		file_proto_storage_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_storage_proto_rawDescData)
	})
	return file_proto_storage_proto_rawDescData
}

var file_proto_storage_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_proto_storage_proto_goTypes = []interface{}{
	(*StorageInspectRequest)(nil), // 0: querator.StorageInspectRequest
	(*StorageListRequest)(nil),    // 1: querator.StorageListRequest
	(*StorageListResponse)(nil),   // 2: querator.StorageListResponse
	(*StorageItem)(nil),           // 3: querator.StorageItem
	(*timestamppb.Timestamp)(nil), // 4: google.protobuf.Timestamp
}
var file_proto_storage_proto_depIdxs = []int32{
	3, // 0: querator.StorageListResponse.items:type_name -> querator.StorageItem
	4, // 1: querator.StorageItem.reserveDeadline:type_name -> google.protobuf.Timestamp
	4, // 2: querator.StorageItem.deadDeadline:type_name -> google.protobuf.Timestamp
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_proto_storage_proto_init() }
func file_proto_storage_proto_init() {
	if File_proto_storage_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_storage_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StorageInspectRequest); i {
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
		file_proto_storage_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StorageListRequest); i {
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
		file_proto_storage_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StorageListResponse); i {
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
		file_proto_storage_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StorageItem); i {
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
			RawDescriptor: file_proto_storage_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_storage_proto_goTypes,
		DependencyIndexes: file_proto_storage_proto_depIdxs,
		MessageInfos:      file_proto_storage_proto_msgTypes,
	}.Build()
	File_proto_storage_proto = out.File
	file_proto_storage_proto_rawDesc = nil
	file_proto_storage_proto_goTypes = nil
	file_proto_storage_proto_depIdxs = nil
}
