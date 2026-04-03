package schema

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestProtoDefinition(t *testing.T) {
	ts := &TableSchema{
		Table: "events",
		Columns: map[string]ColumnType{
			"id":   TypeInt64,
			"name": TypeString,
			"ts":   TypeTimestamp,
		},
	}

	def := ts.ProtoDefinition()

	if !strings.Contains(def, `syntax = "proto3"`) {
		t.Error("missing proto3 syntax")
	}
	if !strings.Contains(def, "message Events {") {
		t.Error("missing Events message")
	}
	if !strings.Contains(def, "message EventsBatch {") {
		t.Error("missing EventsBatch message")
	}
	if !strings.Contains(def, "int64 id = 1") {
		t.Error("missing id field")
	}
	if !strings.Contains(def, "string name = 2") {
		t.Error("missing name field")
	}
	if !strings.Contains(def, "int64 ts = 3") {
		t.Error("missing ts field")
	}
	if !strings.Contains(def, "repeated Events records = 1") {
		t.Error("missing repeated records field")
	}
}

func TestProtoMessageDescriptor(t *testing.T) {
	ts := &TableSchema{
		Table: "metrics",
		Columns: map[string]ColumnType{
			"host": TypeString,
			"cpu":  TypeFloat64,
			"mem":  TypeFloat64,
		},
	}

	desc, err := ts.ProtoMessageDescriptor()
	if err != nil {
		t.Fatalf("ProtoMessageDescriptor: %v", err)
	}

	if string(desc.Name()) != "MetricsBatch" {
		t.Errorf("expected MetricsBatch, got %s", desc.Name())
	}

	recordsField := desc.Fields().ByName("records")
	if recordsField == nil {
		t.Fatal("missing 'records' field in batch message")
	}

	recordDesc := recordsField.Message()
	if recordDesc == nil {
		t.Fatal("records field has no message type")
	}

	// Check fields on inner message.
	if recordDesc.Fields().ByName("host") == nil {
		t.Error("missing 'host' field")
	}
	if recordDesc.Fields().ByName("cpu") == nil {
		t.Error("missing 'cpu' field")
	}
	if recordDesc.Fields().ByName("mem") == nil {
		t.Error("missing 'mem' field")
	}
}

func TestEncodeDecodeProtobuf(t *testing.T) {
	ts := &TableSchema{
		Table: "events",
		Columns: map[string]ColumnType{
			"id":   TypeInt64,
			"name": TypeString,
		},
	}

	// Build a batch message manually using dynamic protobuf.
	batchDesc, err := ts.ProtoMessageDescriptor()
	if err != nil {
		t.Fatalf("descriptor: %v", err)
	}

	batchMsg := dynamicpb.NewMessage(batchDesc)
	recordsField := batchDesc.Fields().ByName("records")
	recordDesc := recordsField.Message()

	// Add two records.
	list := batchMsg.NewField(recordsField).List()

	rec1 := dynamicpb.NewMessage(recordDesc)
	rec1.Set(recordDesc.Fields().ByName("id"), protoreflectInt64(1))
	rec1.Set(recordDesc.Fields().ByName("name"), protoreflectString("alice"))
	list.Append(protoreflectMessage(rec1))

	rec2 := dynamicpb.NewMessage(recordDesc)
	rec2.Set(recordDesc.Fields().ByName("id"), protoreflectInt64(2))
	rec2.Set(recordDesc.Fields().ByName("name"), protoreflectString("bob"))
	list.Append(protoreflectMessage(rec2))

	batchMsg.Set(recordsField, protoreflectList(list))

	// Serialize.
	data, err := proto.Marshal(batchMsg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Decode using our DecodeProtobuf.
	records, err := ts.DecodeProtobuf(data)
	if err != nil {
		t.Fatalf("DecodeProtobuf: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if records[0]["id"] != int64(1) {
		t.Errorf("record[0].id = %v, want 1", records[0]["id"])
	}
	if records[0]["name"] != "alice" {
		t.Errorf("record[0].name = %v, want alice", records[0]["name"])
	}
	if records[1]["id"] != int64(2) {
		t.Errorf("record[1].id = %v, want 2", records[1]["id"])
	}
	if records[1]["name"] != "bob" {
		t.Errorf("record[1].name = %v, want bob", records[1]["name"])
	}
}

func protoreflectInt64(v int64) protoreflect.Value {
	return protoreflect.ValueOfInt64(v)
}

func protoreflectString(v string) protoreflect.Value {
	return protoreflect.ValueOfString(v)
}

func protoreflectMessage(m *dynamicpb.Message) protoreflect.Value {
	return protoreflect.ValueOfMessage(m)
}

func protoreflectList(l protoreflect.List) protoreflect.Value {
	return protoreflect.ValueOfList(l)
}
