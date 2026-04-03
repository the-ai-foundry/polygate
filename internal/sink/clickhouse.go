package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
)

type ClickHouseSink struct {
	url        string
	client     *http.Client
	bulkSize   int
	maxRetries int
	retryBase  int
}

func NewClickHouseSink(url string, timeoutSecs, bulkSize, maxRetries, retryBase int) *ClickHouseSink {
	return &ClickHouseSink{
		url: strings.TrimRight(url, "/"),
		client: &http.Client{
			Timeout: time.Duration(timeoutSecs) * time.Second,
		},
		bulkSize:   bulkSize,
		maxRetries: maxRetries,
		retryBase:  retryBase,
	}
}

func (s *ClickHouseSink) Name() string { return "clickhouse" }

func (s *ClickHouseSink) CreateTable(ctx context.Context, ts *schema.TableSchema) error {
	cols := sortedColumns(ts)
	var parts []string
	for _, col := range cols {
		nativeType := ts.ColumnTypeFor(col, "clickhouse")
		parts = append(parts, fmt.Sprintf("  %s %s", col, nativeType))
	}

	orderBy := "tuple()"
	if len(ts.PrimaryKey) > 0 {
		orderBy = strings.Join(ts.PrimaryKey, ", ")
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n) ENGINE = ReplacingMergeTree()\nORDER BY (%s)",
		ts.Table, strings.Join(parts, ",\n"), orderBy)

	if ts.PartitionBy != "" {
		ddl += fmt.Sprintf("\nPARTITION BY toYYYYMM(%s)", ts.PartitionBy)
	}

	return s.execDDL(ctx, ddl)
}

func (s *ClickHouseSink) WriteBatch(ctx context.Context, table string, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	// Chunk records by bulkSize.
	for i := 0; i < len(records); i += s.bulkSize {
		end := i + s.bulkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]
		err := WithRetry(ctx, s.maxRetries, s.retryBase, func() error {
			return s.insertJSONEachRow(ctx, table, chunk)
		})
		if err != nil {
			metrics.SinkErrorsTotal.WithLabelValues("clickhouse").Inc()
			return fmt.Errorf("clickhouse write to %q: %w", table, err)
		}
	}

	dur := time.Since(start)
	metrics.SinkWriteDuration.WithLabelValues("clickhouse", table).Observe(dur.Seconds())
	metrics.SinkWriteRowsTotal.WithLabelValues("clickhouse", table).Add(float64(len(records)))
	return nil
}

func (s *ClickHouseSink) insertJSONEachRow(ctx context.Context, table string, records []model.Record) error {
	var buf bytes.Buffer
	for _, rec := range records {
		if err := json.NewEncoder(&buf).Encode(rec); err != nil {
			return fmt.Errorf("encoding record: %w", err)
		}
	}

	query := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", table)
	url := fmt.Sprintf("%s/?query=%s", s.url, query)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (s *ClickHouseSink) execDDL(ctx context.Context, ddl string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"/", strings.NewReader(ddl))
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse DDL error %d: %s", resp.StatusCode, string(body))
	}

	slog.Debug("clickhouse DDL executed", "ddl", ddl[:min(len(ddl), 100)])
	return nil
}

func (s *ClickHouseSink) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url+"/ping", nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("clickhouse ping returned %d", resp.StatusCode)
	}
	return nil
}

func (s *ClickHouseSink) Close() error { return nil }

func chSortedColumns(ts *schema.TableSchema) []string {
	cols := make([]string, 0, len(ts.Columns))
	for c := range ts.Columns {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}
