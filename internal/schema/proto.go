package schema

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/polygate/polygate/internal/model"
)

// polyTypeToProtoType maps portable column types to protobuf field types.
func polyTypeToProtoType(t ColumnType) descriptorpb.FieldDescriptorProto_Type {
	switch t {
	case TypeString, TypeText, TypeJSON:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	case TypeInt32:
		return descriptorpb.FieldDescriptorProto_TYPE_INT32
	case TypeInt64, TypeTimestamp: // timestamp as unix nanos
		return descriptorpb.FieldDescriptorProto_TYPE_INT64
	case TypeFloat32:
		return descriptorpb.FieldDescriptorProto_TYPE_FLOAT
	case TypeFloat64:
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	case TypeBool:
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL
	case TypeBytes:
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES
	default:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	}
}

// ProtoDefinition generates a .proto file string from the table schema.
func (s *TableSchema) ProtoDefinition() string {
	cols := sortedCols(s)

	var sb strings.Builder
	sb.WriteString("syntax = \"proto3\";\n\n")

	msgName := protoMessageName(s.Table)

	// Single record message.
	sb.WriteString(fmt.Sprintf("message %s {\n", msgName))
	for i, col := range cols {
		protoType := protoTypeString(s.Columns[col])
		sb.WriteString(fmt.Sprintf("  %s %s = %d;\n", protoType, col, i+1))
	}
	sb.WriteString("}\n\n")

	// Batch wrapper.
	sb.WriteString(fmt.Sprintf("message %sBatch {\n", msgName))
	sb.WriteString(fmt.Sprintf("  repeated %s records = 1;\n", msgName))
	sb.WriteString("}\n")

	return sb.String()
}

// ProtoMessageDescriptor builds a dynamic protobuf message descriptor at runtime.
func (s *TableSchema) ProtoMessageDescriptor() (protoreflect.MessageDescriptor, error) {
	cols := sortedCols(s)
	msgName := protoMessageName(s.Table)

	// Build the record message descriptor.
	fields := make([]*descriptorpb.FieldDescriptorProto, len(cols))
	for i, col := range cols {
		t := polyTypeToProtoType(s.Columns[col])
		fields[i] = &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(col),
			Number: proto.Int32(int32(i + 1)),
			Type:   &t,
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}
	}

	// Build the batch message with a repeated field referencing the record message.
	batchMsgName := msgName + "Batch"
	repeatedLabel := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	msgType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	batchFields := []*descriptorpb.FieldDescriptorProto{
		{
			Name:     proto.String("records"),
			Number:   proto.Int32(1),
			Type:     &msgType,
			TypeName: proto.String("." + msgName),
			Label:    &repeatedLabel,
		},
	}

	fdp := &descriptorpb.FileDescriptorProto{
		Name:   proto.String(s.Table + ".proto"),
		Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String(msgName), Field: fields},
			{Name: proto.String(batchMsgName), Field: batchFields},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		return nil, fmt.Errorf("building proto descriptor: %w", err)
	}

	// Return the batch message descriptor.
	return fd.Messages().ByName(protoreflect.Name(batchMsgName)), nil
}

// DecodeProtobuf deserializes a protobuf-encoded batch into records.
func (s *TableSchema) DecodeProtobuf(data []byte) ([]model.Record, error) {
	batchDesc, err := s.ProtoMessageDescriptor()
	if err != nil {
		return nil, err
	}

	batchMsg := dynamicpb.NewMessage(batchDesc)
	if err := proto.Unmarshal(data, batchMsg); err != nil {
		return nil, fmt.Errorf("unmarshaling protobuf: %w", err)
	}

	recordsField := batchDesc.Fields().ByName("records")
	if recordsField == nil {
		return nil, fmt.Errorf("batch message missing 'records' field")
	}

	list := batchMsg.Get(recordsField).List()
	records := make([]model.Record, list.Len())

	for i := 0; i < list.Len(); i++ {
		msg := list.Get(i).Message()
		rec := make(model.Record)

		fields := msg.Descriptor().Fields()
		for j := 0; j < fields.Len(); j++ {
			fd := fields.Get(j)
			val := msg.Get(fd)
			rec[string(fd.Name())] = protoValueToGo(fd, val)
		}
		records[i] = rec
	}

	return records, nil
}

func protoValueToGo(fd protoreflect.FieldDescriptor, val protoreflect.Value) any {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return val.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int64(val.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return val.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return int64(val.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return int64(val.Uint())
	case protoreflect.FloatKind:
		return float64(val.Float())
	case protoreflect.DoubleKind:
		return val.Float()
	case protoreflect.StringKind:
		return val.String()
	case protoreflect.BytesKind:
		return val.Bytes()
	default:
		return val.Interface()
	}
}

func protoTypeString(t ColumnType) string {
	switch t {
	case TypeString, TypeText, TypeJSON:
		return "string"
	case TypeInt32:
		return "int32"
	case TypeInt64, TypeTimestamp:
		return "int64"
	case TypeFloat32:
		return "float"
	case TypeFloat64:
		return "double"
	case TypeBool:
		return "bool"
	case TypeBytes:
		return "bytes"
	default:
		return "string"
	}
}

func protoMessageName(table string) string {
	// Capitalize first letter for proto convention.
	if len(table) == 0 {
		return "Record"
	}
	return strings.ToUpper(table[:1]) + table[1:]
}

func sortedCols(ts *TableSchema) []string {
	cols := make([]string, 0, len(ts.Columns))
	for c := range ts.Columns {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}
