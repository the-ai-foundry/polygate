package schema

import (
	"testing"
	"time"

	"github.com/polygate/polygate/internal/model"
)

func TestCoerceTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"RFC3339", "2026-04-03T12:00:00Z", false},
		{"RFC3339Nano", "2026-04-03T12:00:00.123456789Z", false},
		{"datetime", "2026-04-03T12:00:00", false},
		{"datetime space", "2026-04-03 12:00:00", false},
		{"date only", "2026-04-03", false},
		{"unix epoch float64", float64(1712150400), false},
		{"unix epoch int64", int64(1712150400), false},
		{"unix epoch string", "1712150400", false},
		{"invalid string", "not-a-date", true},
		{"invalid type", []int{1, 2, 3}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := coerceTimestamp(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("coerceTimestamp(%v) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
			if err == nil {
				if result.IsZero() {
					t.Error("expected non-zero time")
				}
			}
		})
	}
}

func TestCoerceInt(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    int64
		wantErr bool
	}{
		{"int64", int64(42), 42, false},
		{"float64 whole", float64(42), 42, false},
		{"float64 fractional", float64(42.5), 0, true},
		{"string", "42", 42, false},
		{"invalid string", "abc", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := coerceInt(tt.input, 64)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCoerceFloat(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  float64
	}{
		{"float64", float64(3.14), 3.14},
		{"int64", int64(42), 42.0},
		{"string", "3.14", 3.14},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := coerceFloat(tt.input, 64)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestCoerceBool(t *testing.T) {
	tests := []struct {
		input any
		want  bool
	}{
		{true, true},
		{false, false},
		{"true", true},
		{"false", false},
		{"1", true},
		{"0", false},
		{"yes", true},
		{"no", false},
		{float64(1), true},
		{float64(0), false},
	}

	for _, tt := range tests {
		got, err := coerceBool(tt.input)
		if err != nil {
			t.Errorf("coerceBool(%v) error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("coerceBool(%v) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestCoerceBytes(t *testing.T) {
	got, err := coerceBytes("0xdeadbeef")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(got))
	}

	got, err = coerceBytes("deadbeef")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(got))
	}
}

func TestCoerceBatch(t *testing.T) {
	ts := &TableSchema{
		Table: "test",
		Columns: map[string]ColumnType{
			"id":   TypeInt64,
			"name": TypeString,
			"ts":   TypeTimestamp,
			"ok":   TypeBool,
		},
	}

	records := []model.Record{
		{"id": "42", "name": "alice", "ts": "2026-04-03T12:00:00Z", "ok": "true"},
		{"id": float64(43), "name": "bob", "ts": float64(1712150400), "ok": false},
	}

	coerced, err := ts.CoerceBatch(records)
	if err != nil {
		t.Fatalf("CoerceBatch error: %v", err)
	}

	// Check first record.
	if id, ok := coerced[0]["id"].(int64); !ok || id != 42 {
		t.Errorf("expected id=42, got %v", coerced[0]["id"])
	}
	if _, ok := coerced[0]["ts"].(time.Time); !ok {
		t.Errorf("expected ts to be time.Time, got %T", coerced[0]["ts"])
	}
	if b, ok := coerced[0]["ok"].(bool); !ok || !b {
		t.Errorf("expected ok=true, got %v", coerced[0]["ok"])
	}

	// Check second record.
	if id, ok := coerced[1]["id"].(int64); !ok || id != 43 {
		t.Errorf("expected id=43, got %v", coerced[1]["id"])
	}
}

func TestCoerceBatchError(t *testing.T) {
	ts := &TableSchema{
		Table:   "test",
		Columns: map[string]ColumnType{"id": TypeInt64},
	}

	records := []model.Record{
		{"id": "not_a_number"},
	}

	_, err := ts.CoerceBatch(records)
	if err == nil {
		t.Fatal("expected error for invalid int")
	}
}
