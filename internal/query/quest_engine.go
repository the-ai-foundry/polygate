package query

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/polygate/polygate/internal/schema"
)

type QuestEngine struct {
	url      string
	client   *http.Client
	registry *schema.Registry
}

// NewQuestEngine creates a QuestDB query engine.
// questURL should be the QuestDB REST API endpoint (e.g., http://localhost:9000).
func NewQuestEngine(questURL string, registry *schema.Registry) *QuestEngine {
	return &QuestEngine{
		url: strings.TrimRight(questURL, "/"),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		registry: registry,
	}
}

func (e *QuestEngine) Name() string { return "questdb" }

func (e *QuestEngine) Execute(ctx context.Context, query string, page *PageOptions) (*Result, error) {
	q := query
	if page != nil {
		if page.Limit > 0 {
			q += fmt.Sprintf(" LIMIT %d", page.Limit)
		}
		if page.Offset > 0 {
			q += fmt.Sprintf(" OFFSET %d", page.Offset)
		}
	}
	reqURL := fmt.Sprintf("%s/exec?query=%s", e.url, url.QueryEscape(q))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("questdb returned %d: %s", resp.StatusCode, string(body))
	}

	var qResp struct {
		Columns []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"columns"`
		Dataset [][]any `json:"dataset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&qResp); err != nil {
		return nil, fmt.Errorf("decoding questdb response: %w", err)
	}

	cols := make([]string, len(qResp.Columns))
	for i, c := range qResp.Columns {
		cols[i] = c.Name
	}

	var rows []map[string]any
	for _, dataRow := range qResp.Dataset {
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			if i < len(dataRow) {
				row[col] = dataRow[i]
			}
		}
		rows = append(rows, row)
	}

	result := &Result{Columns: cols, Rows: rows, Engine: "questdb"}
	if page != nil && page.Limit > 0 && len(rows) == page.Limit {
		result.NextPage = &NextPage{
			Offset: page.Offset + page.Limit,
			Limit:  page.Limit,
		}
	}
	return result, nil
}

// ExecuteStream streams results using QuestDB's /exp CSV endpoint.
// The table parameter (passed via query context) is used to look up the schema for type coercion.
func (e *QuestEngine) ExecuteStream(ctx context.Context, query string, pageSize int, out chan<- StreamResult) error {
	defer close(out)

	streamClient := &http.Client{Timeout: 0}

	// Use /exp endpoint which streams CSV.
	reqURL := fmt.Sprintf("%s/exp?query=%s", e.url, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("questdb /exp returned %d: %s", resp.StatusCode, string(body))
	}

	// Try to extract table name from context for schema lookup.
	tableName, _ := ctx.Value(streamTableKey{}).(string)
	var ts *schema.TableSchema
	if tableName != "" {
		ts, _ = e.registry.Get(tableName)
	}

	reader := csv.NewReader(bufio.NewReader(resp.Body))

	// Read header row.
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading CSV header: %w", err)
	}
	cols := make([]string, len(header))
	copy(cols, header)

	var batch []map[string]any
	page := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading CSV row: %w", err)
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			if i < len(record) {
				row[col] = coerceCSVValue(record[i], col, ts)
			}
		}
		batch = append(batch, row)

		if len(batch) >= pageSize {
			page++
			out <- StreamResult{Columns: cols, Rows: batch, Page: page, Done: false, Engine: "questdb"}
			batch = nil
		}
	}

	// Flush remaining.
	page++
	out <- StreamResult{Columns: cols, Rows: batch, Page: page, Done: true, Engine: "questdb"}
	return nil
}

// coerceCSVValue converts a CSV string value to a typed Go value using the schema.
func coerceCSVValue(val string, col string, ts *schema.TableSchema) any {
	if ts == nil {
		return val
	}
	colType, ok := ts.Columns[col]
	if !ok {
		return val
	}
	switch colType {
	case schema.TypeInt32, schema.TypeInt64:
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return n
		}
	case schema.TypeFloat32, schema.TypeFloat64:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	case schema.TypeBool:
		return val == "true"
	}
	return val
}

func (e *QuestEngine) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.url+"/exec?query=SELECT+1", nil)
	if err != nil {
		return err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// streamTableKey is used to pass the table name through context for CSV schema lookup.
type streamTableKey struct{}

// WithStreamTable adds the table name to the context for QuestDB CSV parsing.
func WithStreamTable(ctx context.Context, table string) context.Context {
	return context.WithValue(ctx, streamTableKey{}, table)
}
