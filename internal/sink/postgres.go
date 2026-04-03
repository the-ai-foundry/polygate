package sink

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
)

type PostgresSink struct {
	pool       *pgxpool.Pool
	bulkSize   int
	maxRetries int
	retryBase  int
}

func NewPostgresSink(ctx context.Context, dsn string, maxConns int, bulkSize, maxRetries, retryBase int) (*PostgresSink, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres dsn: %w", err)
	}
	cfg.MaxConns = int32(maxConns)

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	return &PostgresSink{
		pool:       pool,
		bulkSize:   bulkSize,
		maxRetries: maxRetries,
		retryBase:  retryBase,
	}, nil
}

func (s *PostgresSink) Name() string { return "postgres" }

func (s *PostgresSink) CreateTable(ctx context.Context, ts *schema.TableSchema) error {
	cols := sortedColumns(ts)
	var parts []string
	for _, col := range cols {
		nativeType := ts.ColumnTypeFor(col, "postgres")
		parts = append(parts, fmt.Sprintf("%s %s", pgQuote(col), nativeType))
	}
	if len(ts.PrimaryKey) > 0 {
		quoted := make([]string, len(ts.PrimaryKey))
		for i, pk := range ts.PrimaryKey {
			quoted[i] = pgQuote(pk)
		}
		parts = append(parts, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(quoted, ", ")))
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)", pgQuote(ts.Table), strings.Join(parts, ",\n  "))
	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("creating table %q: %w", ts.Table, err)
	}

	// Create indexes.
	for _, idx := range ts.Indexes {
		quotedCols := make([]string, len(idx.Columns))
		for i, c := range idx.Columns {
			quotedCols[i] = pgQuote(c)
		}
		idxName := fmt.Sprintf("idx_%s_%s", ts.Table, strings.Join(idx.Columns, "_"))
		idxDDL := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
			pgQuote(idxName), pgQuote(ts.Table), strings.Join(quotedCols, ", "))
		if _, err := s.pool.Exec(ctx, idxDDL); err != nil {
			return fmt.Errorf("creating index %q: %w", idxName, err)
		}
	}

	return nil
}

func (s *PostgresSink) WriteBatch(ctx context.Context, table string, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	err := WithRetry(ctx, s.maxRetries, s.retryBase, func() error {
		return s.writeCopy(ctx, table, records)
	})
	dur := time.Since(start)

	metrics.SinkWriteDuration.WithLabelValues("postgres", table).Observe(dur.Seconds())
	if err != nil {
		metrics.SinkErrorsTotal.WithLabelValues("postgres").Inc()
		return fmt.Errorf("postgres write to %q: %w", table, err)
	}
	metrics.SinkWriteRowsTotal.WithLabelValues("postgres", table).Add(float64(len(records)))
	return nil
}

func (s *PostgresSink) writeCopy(ctx context.Context, table string, records []model.Record) error {
	cols := sortedKeys(records[0])
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = pgQuote(c)
	}

	// Build rows for CopyFrom.
	rows := make([][]any, len(records))
	for i, rec := range records {
		row := make([]any, len(cols))
		for j, col := range cols {
			row[j] = rec[col]
		}
		rows[i] = row
	}

	copyCount, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{table},
		cols,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return err
	}

	slog.Debug("postgres COPY complete", "table", table, "rows", copyCount)
	return nil
}

func (s *PostgresSink) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *PostgresSink) Close() error {
	s.pool.Close()
	return nil
}

func pgQuote(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedColumns(ts *schema.TableSchema) []string {
	cols := make([]string, 0, len(ts.Columns))
	for c := range ts.Columns {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}
