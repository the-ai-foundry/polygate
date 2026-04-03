package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type ESEngine struct {
	url    string
	client *http.Client
}

func NewESEngine(url string) *ESEngine {
	return &ESEngine{
		url: strings.TrimRight(url, "/"),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (e *ESEngine) Name() string { return "elasticsearch" }

func (e *ESEngine) Execute(ctx context.Context, query string, page *PageOptions) (*Result, error) {
	body := e.buildPagedQueryBody(query, page)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/_search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("elasticsearch returned %d: %s", resp.StatusCode, string(respBody))
	}

	var esResp esSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&esResp); err != nil {
		return nil, fmt.Errorf("decoding elasticsearch response: %w", err)
	}

	rows, cols := extractHits(esResp.Hits.Hits)

	result := &Result{Columns: cols, Rows: rows, Engine: "elasticsearch"}
	if page != nil && page.Limit > 0 && len(rows) == page.Limit {
		result.NextPage = &NextPage{
			Offset: page.Offset + page.Limit,
			Limit:  page.Limit,
		}
	}
	return result, nil
}

// ExecuteStream uses PIT (Point in Time) + search_after for paginated streaming.
// This replaces the deprecated Scroll API which was removed in ES 9.
func (e *ESEngine) ExecuteStream(ctx context.Context, query string, pageSize int, out chan<- StreamResult) error {
	defer close(out)

	streamClient := &http.Client{Timeout: 0}

	// Step 1: Open a Point in Time.
	pitReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/_pit?keep_alive=5m", nil)
	if err != nil {
		return err
	}
	pitResp, err := streamClient.Do(pitReq)
	if err != nil {
		return err
	}
	defer pitResp.Body.Close()

	if pitResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(pitResp.Body)
		return fmt.Errorf("elasticsearch PIT creation returned %d: %s", pitResp.StatusCode, string(body))
	}

	var pitResult struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(pitResp.Body).Decode(&pitResult); err != nil {
		return err
	}
	pitID := pitResult.ID

	defer func() {
		// Clean up PIT.
		if pitID != "" {
			clearBody, _ := json.Marshal(map[string]string{"id": pitID})
			clearReq, _ := http.NewRequestWithContext(context.Background(), http.MethodDelete, e.url+"/_pit", bytes.NewReader(clearBody))
			clearReq.Header.Set("Content-Type", "application/json")
			streamClient.Do(clearReq)
		}
	}()

	// Step 2: Build the base query.
	baseQuery := e.buildQueryMap(query)

	var cols []string
	var searchAfter []any
	page := 0

	for {
		// Build search request with PIT + search_after.
		searchBody := make(map[string]any)
		for k, v := range baseQuery {
			searchBody[k] = v
		}
		searchBody["size"] = pageSize
		searchBody["pit"] = map[string]any{"id": pitID, "keep_alive": "5m"}
		searchBody["sort"] = []map[string]string{{"_shard_doc": "asc"}}

		if searchAfter != nil {
			searchBody["search_after"] = searchAfter
		}

		bodyBytes, _ := json.Marshal(searchBody)

		// Search with PIT uses /_search without an index (index is in the PIT).
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/_search", bytes.NewReader(bodyBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := streamClient.Do(req)
		if err != nil {
			return err
		}

		var esResp esPITResponse
		if err := json.NewDecoder(resp.Body).Decode(&esResp); err != nil {
			resp.Body.Close()
			return err
		}
		resp.Body.Close()

		// Update PIT ID (may change between requests).
		if esResp.PitID != "" {
			pitID = esResp.PitID
		}

		rows, pageCols := extractPITHits(esResp.Hits.Hits)
		if cols == nil {
			cols = pageCols
		}

		if len(rows) == 0 {
			out <- StreamResult{Columns: cols, Page: page, Done: true, Engine: "elasticsearch"}
			break
		}

		// Get search_after from the last hit's sort values.
		lastHit := esResp.Hits.Hits[len(esResp.Hits.Hits)-1]
		searchAfter = lastHit.Sort

		page++
		done := len(rows) < pageSize
		out <- StreamResult{Columns: cols, Rows: rows, Page: page, Done: done, Engine: "elasticsearch"}
		if done {
			break
		}
	}

	return nil
}

func (e *ESEngine) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.url, nil)
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

func (e *ESEngine) buildQueryMap(query string) map[string]any {
	if strings.HasPrefix(strings.TrimSpace(query), "{") {
		var q map[string]any
		json.Unmarshal([]byte(query), &q)
		if q != nil {
			return q
		}
	}
	return map[string]any{
		"query": map[string]any{
			"query_string": map[string]any{"query": query},
		},
	}
}

func (e *ESEngine) buildPagedQueryBody(query string, page *PageOptions) []byte {
	q := e.buildQueryMap(query)
	if page != nil {
		if page.Limit > 0 {
			q["size"] = page.Limit
		}
		if page.Offset > 0 {
			q["from"] = page.Offset
		}
	}
	body, _ := json.Marshal(q)
	return body
}

type esSearchResponse struct {
	Hits struct {
		Hits []struct {
			Source map[string]any `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type esPITResponse struct {
	PitID string `json:"pit_id"`
	Hits  struct {
		Hits []struct {
			Source map[string]any `json:"_source"`
			Sort   []any          `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
}

func extractHits(hits []struct {
	Source map[string]any `json:"_source"`
}) ([]map[string]any, []string) {
	var rows []map[string]any
	var cols []string
	for i, hit := range hits {
		rows = append(rows, hit.Source)
		if i == 0 {
			for k := range hit.Source {
				cols = append(cols, k)
			}
		}
	}
	return rows, cols
}

func extractPITHits(hits []struct {
	Source map[string]any `json:"_source"`
	Sort   []any          `json:"sort"`
}) ([]map[string]any, []string) {
	var rows []map[string]any
	var cols []string
	for i, hit := range hits {
		rows = append(rows, hit.Source)
		if i == 0 {
			for k := range hit.Source {
				cols = append(cols, k)
			}
		}
	}
	return rows, cols
}
