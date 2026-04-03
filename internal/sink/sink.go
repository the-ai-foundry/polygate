package sink

import (
	"context"
	"log/slog"

	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
	"golang.org/x/sync/errgroup"
)

// Sink is a database backend that can accept bulk writes.
type Sink interface {
	// Name returns a human-readable identifier ("postgres", "clickhouse", etc.)
	Name() string

	// CreateTable executes DDL to create a table based on the schema.
	CreateTable(ctx context.Context, s *schema.TableSchema) error

	// WriteBatch inserts a batch of records using the optimal bulk method.
	WriteBatch(ctx context.Context, table string, records []model.Record) error

	// Ping checks connectivity.
	Ping(ctx context.Context) error

	// Close releases resources.
	Close() error
}

// SinkSet fans out writes to the sinks configured for each table.
type SinkSet struct {
	sinks    map[string]Sink
	registry *schema.Registry
}

func NewSinkSet(sinks map[string]Sink, registry *schema.Registry) *SinkSet {
	return &SinkSet{sinks: sinks, registry: registry}
}

// WriteBatch writes records to all sinks configured for the given table.
func (s *SinkSet) WriteBatch(ctx context.Context, table string, records []model.Record) error {
	targetSinks := s.registry.SinksFor(table)
	if len(targetSinks) == 0 {
		return nil
	}

	g, ctx := errgroup.WithContext(ctx)
	for _, name := range targetSinks {
		sk, ok := s.sinks[name]
		if !ok {
			slog.Warn("sink not found, skipping", "sink", name, "table", table)
			continue
		}
		g.Go(func() error {
			return sk.WriteBatch(ctx, table, records)
		})
	}
	return g.Wait()
}

// CreateTable creates the table in all sinks configured for this schema.
func (s *SinkSet) CreateTable(ctx context.Context, ts *schema.TableSchema) error {
	for _, name := range ts.Sinks {
		sk, ok := s.sinks[name]
		if !ok {
			continue
		}
		if err := sk.CreateTable(ctx, ts); err != nil {
			return err
		}
	}
	return nil
}

// PingAll pings all sinks and returns the first error encountered.
func (s *SinkSet) PingAll(ctx context.Context) map[string]error {
	results := make(map[string]error, len(s.sinks))
	for name, sk := range s.sinks {
		results[name] = sk.Ping(ctx)
	}
	return results
}

// CloseAll closes all sinks.
func (s *SinkSet) CloseAll() {
	for name, sk := range s.sinks {
		if err := sk.Close(); err != nil {
			slog.Error("error closing sink", "sink", name, "error", err)
		}
	}
}

// Sinks returns the underlying sink map.
func (s *SinkSet) Sinks() map[string]Sink {
	return s.sinks
}
