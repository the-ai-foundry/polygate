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

func NewTrinoEngine(trinoURL string) *TrinoEngine {
	return &TrinoEngine{
		url: strings.TrimRight(trinoURL, "/"),
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

func (e *TrinoEngine) Name() string { return "trino" }

func (e *TrinoEngine) Execute(ctx context.Context, query string) (*Result, error) {
	cols, rows, err := e.fetchAllPages(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Result{Columns: cols, Rows: rows, Engine: "trino"}, nil
}

func (e *TrinoEngine) ExecuteStream(ctx context.Context, query string, pageSize int, out chan<- StreamResult) error {
	defer close(out)

	streamClient := &http.Client{Timeout: 0}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/v1/statement", strings.NewReader(query))
	if err != nil {
		return err
	}
	req.Header.Set("X-Trino-User", "polygate")
	req.Header.Set("X-Trino-Source", "polygate")

	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("trino returned %d: %s", resp.StatusCode, string(body))
	}

	var trinoResp trinoResponse
	if err := json.NewDecoder(resp.Body).Decode(&trinoResp); err != nil {
		return err
	}

	var cols []string
	if len(trinoResp.Columns) > 0 {
		cols = make([]string, len(trinoResp.Columns))
		for i, c := range trinoResp.Columns {
			cols[i] = c.Name
		}
	}

	page := 0
	nextURI := trinoResp.NextURI

	// Send first page data if any.
	rows := convertTrinoRows(cols, trinoResp.Data)
	if len(rows) > 0 {
		page++
		out <- StreamResult{Columns: cols, Rows: rows, Page: page, Done: nextURI == "", Engine: "trino"}
	}

	// Follow nextUri for remaining pages.
	for nextURI != "" {
		pageReq, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURI, nil)
		if err != nil {
			return err
		}
		pageReq.Header.Set("X-Trino-User", "polygate")

		pageResp, err := streamClient.Do(pageReq)
		if err != nil {
			return err
		}

		var pageData trinoResponse
		if err := json.NewDecoder(pageResp.Body).Decode(&pageData); err != nil {
			pageResp.Body.Close()
			return err
		}
		pageResp.Body.Close()

		if len(cols) == 0 && len(pageData.Columns) > 0 {
			cols = make([]string, len(pageData.Columns))
			for i, c := range pageData.Columns {
				cols[i] = c.Name
			}
		}

		if pageData.Error != nil {
			return fmt.Errorf("trino query error: %s (type: %s)", pageData.Error.Message, pageData.Error.ErrorType)
		}

		nextURI = pageData.NextURI
		rows := convertTrinoRows(cols, pageData.Data)
		if len(rows) > 0 {
			page++
			out <- StreamResult{Columns: cols, Rows: rows, Page: page, Done: nextURI == "", Engine: "trino"}
		} else if nextURI == "" {
			out <- StreamResult{Columns: cols, Page: page, Done: true, Engine: "trino"}
		}
	}

	return nil
}

func (e *TrinoEngine) fetchAllPages(ctx context.Context, query string) ([]string, []map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/v1/statement", strings.NewReader(query))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("X-Trino-User", "polygate")
	req.Header.Set("X-Trino-Source", "polygate")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("trino returned %d: %s", resp.StatusCode, string(body))
	}

	var trinoResp trinoResponse
	if err := json.NewDecoder(resp.Body).Decode(&trinoResp); err != nil {
		return nil, nil, err
	}

	var cols []string
	if len(trinoResp.Columns) > 0 {
		cols = make([]string, len(trinoResp.Columns))
		for i, c := range trinoResp.Columns {
			cols[i] = c.Name
		}
	}

	rows := convertTrinoRows(cols, trinoResp.Data)
	nextURI := trinoResp.NextURI

	for nextURI != "" {
		pageReq, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURI, nil)
		if err != nil {
			return nil, nil, err
		}
		pageReq.Header.Set("X-Trino-User", "polygate")

		pageResp, err := e.client.Do(pageReq)
		if err != nil {
			return nil, nil, err
		}

		var page trinoResponse
		if err := json.NewDecoder(pageResp.Body).Decode(&page); err != nil {
			pageResp.Body.Close()
			return nil, nil, err
		}
		pageResp.Body.Close()

		if len(cols) == 0 && len(page.Columns) > 0 {
			cols = make([]string, len(page.Columns))
			for i, c := range page.Columns {
				cols[i] = c.Name
			}
		}

		if page.Error != nil {
			return nil, nil, fmt.Errorf("trino query error: %s (type: %s)", page.Error.Message, page.Error.ErrorType)
		}

		rows = append(rows, convertTrinoRows(cols, page.Data)...)
		nextURI = page.NextURI
	}

	return cols, rows, nil
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
