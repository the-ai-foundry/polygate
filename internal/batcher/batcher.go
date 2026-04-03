package batcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
	"github.com/polygate/polygate/internal/sink"
)

// IngestRequest represents a batch of records to be written to a table.
type IngestRequest struct {
	Table   string
	Records []model.Record
}

// Batcher accumulates records and flushes them to sinks in batches.
type Batcher struct {
	input    chan IngestRequest
	sinkSet  *sink.SinkSet
	registry *schema.Registry
	maxSize  int
	period   time.Duration
}

func New(sinkSet *sink.SinkSet, registry *schema.Registry, maxSize, bufferSize int, period time.Duration) *Batcher {
	return &Batcher{
		input:    make(chan IngestRequest, bufferSize),
		sinkSet:  sinkSet,
		registry: registry,
		maxSize:  maxSize,
		period:   period,
	}
}

// Submit adds records to the batcher. Returns false if the buffer is full.
func (b *Batcher) Submit(req IngestRequest) bool {
	select {
	case b.input <- req:
		metrics.IngestRecordsTotal.Add(float64(len(req.Records)))
		return true
	default:
		return false
	}
}

// Run starts the batcher loop. It blocks until ctx is cancelled.
func (b *Batcher) Run(ctx context.Context) {
	ticker := time.NewTicker(b.period)
	defer ticker.Stop()

	// Per-table accumulation.
	batches := make(map[string][]model.Record)
	var mu sync.Mutex

	flush := func() {
		mu.Lock()
		if len(batches) == 0 {
			mu.Unlock()
			return
		}
		toFlush := batches
		batches = make(map[string][]model.Record)
		mu.Unlock()

		for table, records := range toFlush {
			if len(records) == 0 {
				continue
			}

			// Coerce types using schema.
			ts, ok := b.registry.Get(table)
			if !ok {
				slog.Error("schema not found during flush, dropping batch", "table", table, "records", len(records))
				continue
			}
			coerced, err := ts.CoerceBatch(records)
			if err != nil {
				slog.Error("coercion error, dropping batch", "table", table, "error", err)
				continue
			}

			metrics.IngestBatchesTotal.Inc()
			if err := b.sinkSet.WriteBatch(ctx, table, coerced); err != nil {
				slog.Error("sink write error", "table", table, "error", err, "records", len(coerced))
			}
		}
	}

	for {
		select {
		case req := <-b.input:
			mu.Lock()
			batches[req.Table] = append(batches[req.Table], req.Records...)
			size := len(batches[req.Table])
			metrics.BatcherQueueSize.Set(float64(size))
			mu.Unlock()

			if size >= b.maxSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-ctx.Done():
			// Drain remaining items.
			close(b.input)
			for req := range b.input {
				mu.Lock()
				batches[req.Table] = append(batches[req.Table], req.Records...)
				mu.Unlock()
			}
			flush()
			return
		}
	}
}
