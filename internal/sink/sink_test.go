package sink

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
)

// mockSink records calls for testing.
type mockSink struct {
	name     string
	writes   []writeCall
	mu       sync.Mutex
	failWith error
}

type writeCall struct {
	table   string
	records []model.Record
}

func (m *mockSink) Name() string { return m.name }

func (m *mockSink) CreateTable(ctx context.Context, s *schema.TableSchema) error { return nil }

func (m *mockSink) WriteBatch(ctx context.Context, table string, records []model.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, writeCall{table: table, records: records})
	return m.failWith
}

func (m *mockSink) Ping(ctx context.Context) error { return nil }
func (m *mockSink) Close() error                   { return nil }

func TestSinkSetFanOut(t *testing.T) {
	reg := schema.NewRegistry([]string{"a", "b"})
	reg.Register(&schema.TableSchema{
		Table:   "t1",
		Columns: map[string]schema.ColumnType{"id": schema.TypeInt64},
		Sinks:   []string{"a", "b"},
	})

	sinkA := &mockSink{name: "a"}
	sinkB := &mockSink{name: "b"}

	ss := NewSinkSet(map[string]Sink{"a": sinkA, "b": sinkB}, reg)

	records := []model.Record{{"id": int64(1)}, {"id": int64(2)}}
	err := ss.WriteBatch(context.Background(), "t1", records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Both sinks should have received the write.
	if len(sinkA.writes) != 1 {
		t.Errorf("sinkA: expected 1 write, got %d", len(sinkA.writes))
	}
	if len(sinkB.writes) != 1 {
		t.Errorf("sinkB: expected 1 write, got %d", len(sinkB.writes))
	}
	if len(sinkA.writes[0].records) != 2 {
		t.Errorf("sinkA: expected 2 records, got %d", len(sinkA.writes[0].records))
	}
}

func TestSinkSetRoutesToConfiguredSinks(t *testing.T) {
	reg := schema.NewRegistry([]string{"a", "b", "c"})
	reg.Register(&schema.TableSchema{
		Table:   "t1",
		Columns: map[string]schema.ColumnType{"id": schema.TypeInt64},
		Sinks:   []string{"a"}, // only sink "a"
	})

	sinkA := &mockSink{name: "a"}
	sinkB := &mockSink{name: "b"}
	sinkC := &mockSink{name: "c"}

	ss := NewSinkSet(map[string]Sink{"a": sinkA, "b": sinkB, "c": sinkC}, reg)

	err := ss.WriteBatch(context.Background(), "t1", []model.Record{{"id": int64(1)}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sinkA.writes) != 1 {
		t.Error("sinkA should have received the write")
	}
	if len(sinkB.writes) != 0 {
		t.Error("sinkB should NOT have received the write")
	}
	if len(sinkC.writes) != 0 {
		t.Error("sinkC should NOT have received the write")
	}
}

func TestSinkSetFailFast(t *testing.T) {
	reg := schema.NewRegistry([]string{"a", "b"})
	reg.Register(&schema.TableSchema{
		Table:   "t1",
		Columns: map[string]schema.ColumnType{"id": schema.TypeInt64},
		Sinks:   []string{"a", "b"},
	})

	sinkA := &mockSink{name: "a", failWith: errors.New("sink a failed")}
	sinkB := &mockSink{name: "b"}

	ss := NewSinkSet(map[string]Sink{"a": sinkA, "b": sinkB}, reg)

	err := ss.WriteBatch(context.Background(), "t1", []model.Record{{"id": int64(1)}})
	if err == nil {
		t.Fatal("expected error from failing sink")
	}
}

func TestSinkSetUnknownTable(t *testing.T) {
	reg := schema.NewRegistry([]string{"a"})
	sinkA := &mockSink{name: "a"}
	ss := NewSinkSet(map[string]Sink{"a": sinkA}, reg)

	// Writing to an unregistered table should be a no-op.
	err := ss.WriteBatch(context.Background(), "nonexistent", []model.Record{{"id": int64(1)}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sinkA.writes) != 0 {
		t.Error("no writes should have happened for unknown table")
	}
}

func TestPingAll(t *testing.T) {
	reg := schema.NewRegistry([]string{"a", "b"})
	sinkA := &mockSink{name: "a"}
	sinkB := &mockSink{name: "b"}
	ss := NewSinkSet(map[string]Sink{"a": sinkA, "b": sinkB}, reg)

	results := ss.PingAll(context.Background())
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
	for name, err := range results {
		if err != nil {
			t.Errorf("ping %s failed: %v", name, err)
		}
	}
}
