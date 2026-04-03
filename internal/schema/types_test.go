package schema

import "testing"

func TestNativeType(t *testing.T) {
	tests := []struct {
		colType  ColumnType
		sink     string
		expected string
	}{
		{TypeString, "postgres", "TEXT"},
		{TypeInt64, "postgres", "BIGINT"},
		{TypeTimestamp, "postgres", "TIMESTAMPTZ"},
		{TypeJSON, "postgres", "JSONB"},
		{TypeBytes, "postgres", "BYTEA"},

		{TypeString, "clickhouse", "String"},
		{TypeInt64, "clickhouse", "Int64"},
		{TypeTimestamp, "clickhouse", "DateTime64(3)"},
		{TypeBool, "clickhouse", "Bool"},

		{TypeString, "questdb", "STRING"},
		{TypeInt64, "questdb", "LONG"},
		{TypeFloat64, "questdb", "DOUBLE"},
		{TypeTimestamp, "questdb", "TIMESTAMP"},

		{TypeString, "elasticsearch", "keyword"},
		{TypeText, "elasticsearch", "text"},
		{TypeTimestamp, "elasticsearch", "date"},
		{TypeJSON, "elasticsearch", "object"},
	}

	for _, tt := range tests {
		t.Run(string(tt.colType)+"_"+tt.sink, func(t *testing.T) {
			got := NativeType(tt.colType, tt.sink)
			if got != tt.expected {
				t.Errorf("NativeType(%s, %s) = %s, want %s", tt.colType, tt.sink, got, tt.expected)
			}
		})
	}
}

func TestValidateColumnType(t *testing.T) {
	for _, valid := range []ColumnType{TypeString, TypeText, TypeInt32, TypeInt64, TypeFloat32, TypeFloat64, TypeBool, TypeTimestamp, TypeJSON, TypeBytes} {
		if err := ValidateColumnType(valid); err != nil {
			t.Errorf("expected %s to be valid", valid)
		}
	}

	if err := ValidateColumnType("invalid"); err == nil {
		t.Error("expected 'invalid' to fail validation")
	}
}
