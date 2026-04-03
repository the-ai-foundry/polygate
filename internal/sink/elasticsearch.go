package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
)

type ElasticsearchSink struct {
	url        string
	client     *http.Client
	bulkSize   int
	maxRetries int
	retryBase  int
}

func NewElasticsearchSink(url string, timeoutSecs, bulkSize, maxRetries, retryBase int) *ElasticsearchSink {
	return &ElasticsearchSink{
		url: strings.TrimRight(url, "/"),
		client: &http.Client{
			Timeout: time.Duration(timeoutSecs) * time.Second,
		},
		bulkSize:   bulkSize,
		maxRetries: maxRetries,
		retryBase:  retryBase,
	}
}

func (s *ElasticsearchSink) Name() string { return "elasticsearch" }

func (s *ElasticsearchSink) CreateTable(ctx context.Context, ts *schema.TableSchema) error {
	// Create index with explicit mapping.
	properties := make(map[string]any, len(ts.Columns))
	for col := range ts.Columns {
		nativeType := ts.ColumnTypeFor(col, "elasticsearch")
		properties[col] = map[string]string{"type": nativeType}
	}
	mapping := map[string]any{
		"mappings": map[string]any{
			"properties": properties,
		},
	}

	body, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s", s.url, ts.Table)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 200 = created, 400 with "already_exists" = OK.
	if resp.StatusCode == http.StatusOK {
		slog.Debug("elasticsearch index created", "index", ts.Table)
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusBadRequest && strings.Contains(string(respBody), "already_exists") {
		return nil
	}

	return fmt.Errorf("elasticsearch create index %q returned %d: %s", ts.Table, resp.StatusCode, string(respBody))
}

func (s *ElasticsearchSink) WriteBatch(ctx context.Context, table string, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	for i := 0; i < len(records); i += s.bulkSize {
		end := i + s.bulkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]
		err := WithRetry(ctx, s.maxRetries, s.retryBase, func() error {
			return s.bulkInsert(ctx, table, chunk)
		})
		if err != nil {
			metrics.SinkErrorsTotal.WithLabelValues("elasticsearch").Inc()
			return fmt.Errorf("elasticsearch write to %q: %w", table, err)
		}
	}

	dur := time.Since(start)
	metrics.SinkWriteDuration.WithLabelValues("elasticsearch", table).Observe(dur.Seconds())
	metrics.SinkWriteRowsTotal.WithLabelValues("elasticsearch", table).Add(float64(len(records)))
	return nil
}

func (s *ElasticsearchSink) bulkInsert(ctx context.Context, index string, records []model.Record) error {
	var buf bytes.Buffer
	for _, rec := range records {
		// Action line.
		action := map[string]any{"index": map[string]string{"_index": index}}
		if err := json.NewEncoder(&buf).Encode(action); err != nil {
			return err
		}
		// Document line.
		if err := json.NewEncoder(&buf).Encode(rec); err != nil {
			return err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"/_bulk", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch bulk returned %d: %s", resp.StatusCode, string(body))
	}

	// Check for per-item errors.
	var result struct {
		Errors bool `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil // If we can't parse, assume success since status was 200.
	}
	if result.Errors {
		return fmt.Errorf("elasticsearch bulk had item-level errors")
	}

	return nil
}

func (s *ElasticsearchSink) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("elasticsearch ping returned %d", resp.StatusCode)
	}
	return nil
}

func (s *ElasticsearchSink) Close() error { return nil }
