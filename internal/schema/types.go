package schema

import "fmt"

// ColumnType represents a portable column type.
type ColumnType string

const (
	TypeString    ColumnType = "string"
	TypeText      ColumnType = "text"
	TypeInt32     ColumnType = "int32"
	TypeInt64     ColumnType = "int64"
	TypeFloat32   ColumnType = "float32"
	TypeFloat64   ColumnType = "float64"
	TypeBool      ColumnType = "bool"
	TypeTimestamp ColumnType = "timestamp"
	TypeJSON      ColumnType = "json"
	TypeBytes     ColumnType = "bytes"
)

var validTypes = map[ColumnType]bool{
	TypeString: true, TypeText: true,
	TypeInt32: true, TypeInt64: true,
	TypeFloat32: true, TypeFloat64: true,
	TypeBool: true, TypeTimestamp: true,
	TypeJSON: true, TypeBytes: true,
}

func ValidateColumnType(t ColumnType) error {
	if !validTypes[t] {
		return fmt.Errorf("unknown column type %q", t)
	}
	return nil
}

// NativeType returns the DB-specific type for a portable column type.
// If the schema has an override for this sink+column, use that instead.
func NativeType(t ColumnType, sinkName string) string {
	m, ok := typeMap[sinkName]
	if !ok {
		return string(t)
	}
	if native, ok := m[t]; ok {
		return native
	}
	return string(t)
}

var typeMap = map[string]map[ColumnType]string{
	"postgres": {
		TypeString:    "TEXT",
		TypeText:      "TEXT",
		TypeInt32:     "INTEGER",
		TypeInt64:     "BIGINT",
		TypeFloat32:   "REAL",
		TypeFloat64:   "DOUBLE PRECISION",
		TypeBool:      "BOOLEAN",
		TypeTimestamp: "TIMESTAMPTZ",
		TypeJSON:      "JSONB",
		TypeBytes:     "BYTEA",
	},
	"clickhouse": {
		TypeString:    "String",
		TypeText:      "String",
		TypeInt32:     "Int32",
		TypeInt64:     "Int64",
		TypeFloat32:   "Float32",
		TypeFloat64:   "Float64",
		TypeBool:      "Bool",
		TypeTimestamp: "DateTime64(3)",
		TypeJSON:      "String",
		TypeBytes:     "String",
	},
	"questdb": {
		TypeString:    "STRING",
		TypeText:      "STRING",
		TypeInt32:     "INT",
		TypeInt64:     "LONG",
		TypeFloat32:   "FLOAT",
		TypeFloat64:   "DOUBLE",
		TypeBool:      "BOOLEAN",
		TypeTimestamp: "TIMESTAMP",
		TypeJSON:      "STRING",
		TypeBytes:     "STRING",
	},
	"elasticsearch": {
		TypeString:    "keyword",
		TypeText:      "text",
		TypeInt32:     "integer",
		TypeInt64:     "long",
		TypeFloat32:   "float",
		TypeFloat64:   "double",
		TypeBool:      "boolean",
		TypeTimestamp: "date",
		TypeJSON:      "object",
		TypeBytes:     "binary",
	},
}
