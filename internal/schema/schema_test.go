package schema

import (
	"testing"
)

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry([]string{"postgres", "clickhouse"})

	ts := &TableSchema{
		Table:   "events",
		Columns: map[string]ColumnType{"id": TypeInt64, "name": TypeString},
		Sinks:   []string{"postgres"},
	}

	if err := r.Register(ts); err != nil {
		t.Fatalf("register: %v", err)
	}

	got, ok := r.Get("events")
	if !ok {
		t.Fatal("expected to find schema")
	}
	if got.Table != "events" {
		t.Errorf("expected table=events, got %s", got.Table)
	}
}

func TestRegistryDefaultSinks(t *testing.T) {
	r := NewRegistry([]string{"postgres", "clickhouse", "elasticsearch"})

	ts := &TableSchema{
		Table:   "logs",
		Columns: map[string]ColumnType{"msg": TypeString},
		// Sinks omitted — should default to all enabled.
	}

	if err := r.Register(ts); err != nil {
		t.Fatalf("register: %v", err)
	}

	got, _ := r.Get("logs")
	if len(got.Sinks) != 3 {
		t.Errorf("expected 3 default sinks, got %d", len(got.Sinks))
	}
}

func TestRegistrySinksFor(t *testing.T) {
	r := NewRegistry([]string{"postgres", "clickhouse"})
	r.Register(&TableSchema{
		Table:   "t1",
		Columns: map[string]ColumnType{"id": TypeInt64},
		Sinks:   []string{"postgres"},
	})

	sinks := r.SinksFor("t1")
	if len(sinks) != 1 || sinks[0] != "postgres" {
		t.Errorf("expected [postgres], got %v", sinks)
	}

	sinks = r.SinksFor("nonexistent")
	if sinks != nil {
		t.Errorf("expected nil for nonexistent table, got %v", sinks)
	}
}

func TestRegistryList(t *testing.T) {
	r := NewRegistry([]string{"postgres"})
	r.Register(&TableSchema{Table: "a", Columns: map[string]ColumnType{"x": TypeString}})
	r.Register(&TableSchema{Table: "b", Columns: map[string]ColumnType{"y": TypeInt64}})

	list := r.List()
	if len(list) != 2 {
		t.Errorf("expected 2 schemas, got %d", len(list))
	}
}

func TestSchemaValidation(t *testing.T) {
	enabled := []string{"postgres", "clickhouse"}

	tests := []struct {
		name    string
		schema  TableSchema
		wantErr bool
	}{
		{
			name:    "empty table name",
			schema:  TableSchema{Columns: map[string]ColumnType{"id": TypeInt64}},
			wantErr: true,
		},
		{
			name:    "no columns",
			schema:  TableSchema{Table: "t"},
			wantErr: true,
		},
		{
			name: "invalid column type",
			schema: TableSchema{
				Table:   "t",
				Columns: map[string]ColumnType{"id": "invalid_type"},
			},
			wantErr: true,
		},
		{
			name: "primary key references missing column",
			schema: TableSchema{
				Table:      "t",
				Columns:    map[string]ColumnType{"id": TypeInt64},
				PrimaryKey: []string{"missing"},
			},
			wantErr: true,
		},
		{
			name: "index references missing column",
			schema: TableSchema{
				Table:   "t",
				Columns: map[string]ColumnType{"id": TypeInt64},
				Indexes: []Index{{Columns: []string{"missing"}}},
			},
			wantErr: true,
		},
		{
			name: "sink not enabled",
			schema: TableSchema{
				Table:   "t",
				Columns: map[string]ColumnType{"id": TypeInt64},
				Sinks:   []string{"mongodb"},
			},
			wantErr: true,
		},
		{
			name: "valid schema",
			schema: TableSchema{
				Table:      "t",
				Columns:    map[string]ColumnType{"id": TypeInt64, "name": TypeString},
				Sinks:      []string{"postgres"},
				PrimaryKey: []string{"id"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.schema.Validate(enabled)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestColumnTypeFor(t *testing.T) {
	ts := &TableSchema{
		Table:   "t",
		Columns: map[string]ColumnType{"status": TypeString, "id": TypeInt64},
		Overrides: map[string]map[string]string{
			"clickhouse": {"status": "LowCardinality(String)"},
			"questdb":    {"status": "symbol"},
		},
	}

	// Override takes priority.
	if got := ts.ColumnTypeFor("status", "clickhouse"); got != "LowCardinality(String)" {
		t.Errorf("expected LowCardinality(String), got %s", got)
	}
	if got := ts.ColumnTypeFor("status", "questdb"); got != "symbol" {
		t.Errorf("expected symbol, got %s", got)
	}

	// No override — falls back to portable type mapping.
	if got := ts.ColumnTypeFor("status", "postgres"); got != "TEXT" {
		t.Errorf("expected TEXT, got %s", got)
	}
	if got := ts.ColumnTypeFor("id", "postgres"); got != "BIGINT" {
		t.Errorf("expected BIGINT, got %s", got)
	}
	if got := ts.ColumnTypeFor("id", "clickhouse"); got != "Int64" {
		t.Errorf("expected Int64, got %s", got)
	}
}
