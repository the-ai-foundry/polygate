package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type TrinoEngine struct {
	url    string
	client *http.Client
}

// NewTrinoEngine creates a Trino query engine.
// trinoURL should be the Trino coordinator HTTP endpoint (e.g., http://localhost:8085).
func NewTrinoEngine(trinoURL string) *TrinoEngine {
	return &TrinoEngine{
		url: strings.TrimRight(trinoURL, "/"),
		client: &http.Client{
			Timeout: 120 * time.Second, // Trino queries can be slow
		},
	}
}

func (e *TrinoEngine) Name() string { return "trino" }

func (e *TrinoEngine) Execute(ctx context.Context, query string) (*Result, error) {
	// Submit query.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/v1/statement", strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Trino-User", "polygate")
	req.Header.Set("X-Trino-Source", "polygate")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("trino returned %d: %s", resp.StatusCode, string(body))
	}

	// Trino returns results in pages. Collect all pages.
	var cols []string
	var rows []map[string]any

	var trinoResp trinoResponse
	if err := json.NewDecoder(resp.Body).Decode(&trinoResp); err != nil {
		return nil, fmt.Errorf("decoding trino response: %w", err)
	}

	// Extract columns from first response.
	if len(trinoResp.Columns) > 0 {
		cols = make([]string, len(trinoResp.Columns))
		for i, c := range trinoResp.Columns {
			cols[i] = c.Name
		}
	}

	// Collect data from first page.
	rows = append(rows, convertTrinoRows(cols, trinoResp.Data)...)

	// Follow nextUri to get remaining pages.
	nextURI := trinoResp.NextURI
	for nextURI != "" {
		pageReq, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURI, nil)
		if err != nil {
			return nil, err
		}
		pageReq.Header.Set("X-Trino-User", "polygate")

		pageResp, err := e.client.Do(pageReq)
		if err != nil {
			return nil, err
		}

		var page trinoResponse
		if err := json.NewDecoder(pageResp.Body).Decode(&page); err != nil {
			pageResp.Body.Close()
			return nil, err
		}
		pageResp.Body.Close()

		// Extract columns if not yet set.
		if len(cols) == 0 && len(page.Columns) > 0 {
			cols = make([]string, len(page.Columns))
			for i, c := range page.Columns {
				cols[i] = c.Name
			}
		}

		if page.Error != nil {
			return nil, fmt.Errorf("trino query error: %s (type: %s)", page.Error.Message, page.Error.ErrorType)
		}

		rows = append(rows, convertTrinoRows(cols, page.Data)...)
		nextURI = page.NextURI
	}

	return &Result{Columns: cols, Rows: rows, Engine: "trino"}, nil
}

func (e *TrinoEngine) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.url+"/v1/info", nil)
	if err != nil {
		return err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("trino ping returned %d", resp.StatusCode)
	}
	return nil
}

type trinoResponse struct {
	Columns []trinoColumn `json:"columns"`
	Data    [][]any       `json:"data"`
	NextURI string        `json:"nextUri"`
	Error   *trinoError   `json:"error"`
}

type trinoColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type trinoError struct {
	Message   string `json:"message"`
	ErrorType string `json:"errorType"`
}

func convertTrinoRows(cols []string, data [][]any) []map[string]any {
	if len(data) == 0 || len(cols) == 0 {
		return nil
	}
	rows := make([]map[string]any, len(data))
	for i, dataRow := range data {
		row := make(map[string]any, len(cols))
		for j, col := range cols {
			if j < len(dataRow) {
				row[col] = dataRow[j]
			}
		}
		rows[i] = row
	}
	return rows
}
