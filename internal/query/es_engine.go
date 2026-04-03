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

func (e *ESEngine) Execute(ctx context.Context, query string) (*Result, error) {
	body := e.buildQueryBody(query)

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

	var rows []map[string]any
	var cols []string
	for i, hit := range esResp.Hits.Hits {
		rows = append(rows, hit.Source)
		if i == 0 {
			for k := range hit.Source {
				cols = append(cols, k)
			}
		}
	}

	return &Result{Columns: cols, Rows: rows, Engine: "elasticsearch"}, nil
}

func (e *ESEngine) ExecuteStream(ctx context.Context, query string, pageSize int, out chan<- StreamResult) error {
	defer close(out)

	streamClient := &http.Client{Timeout: 0}

	// Initial scroll request.
	body := e.buildScrollQueryBody(query, pageSize)
	scrollURL := fmt.Sprintf("%s/_search?scroll=5m", e.url)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, scrollURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch returned %d: %s", resp.StatusCode, string(respBody))
	}

	var esResp esScrollResponse
	if err := json.NewDecoder(resp.Body).Decode(&esResp); err != nil {
		return err
	}

	page := 0
	var cols []string
	scrollID := esResp.ScrollID

	defer func() {
		// Clean up scroll context.
		if scrollID != "" {
			clearBody, _ := json.Marshal(map[string]string{"scroll_id": scrollID})
			clearReq, _ := http.NewRequestWithContext(context.Background(), http.MethodDelete, e.url+"/_search/scroll", bytes.NewReader(clearBody))
			clearReq.Header.Set("Content-Type", "application/json")
			streamClient.Do(clearReq)
		}
	}()

	for {
		rows, pageCols := extractHits(esResp.Hits.Hits)
		if cols == nil {
			cols = pageCols
		}

		if len(rows) == 0 {
			out <- StreamResult{Columns: cols, Page: page, Done: true, Engine: "elasticsearch"}
			break
		}

		page++
		done := len(rows) < pageSize
		out <- StreamResult{Columns: cols, Rows: rows, Page: page, Done: done, Engine: "elasticsearch"}
		if done {
			break
		}

		// Fetch next scroll page.
		scrollBody, _ := json.Marshal(map[string]string{"scroll": "5m", "scroll_id": scrollID})
		scrollReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/_search/scroll", bytes.NewReader(scrollBody))
		if err != nil {
			return err
		}
		scrollReq.Header.Set("Content-Type", "application/json")

		scrollResp, err := streamClient.Do(scrollReq)
		if err != nil {
			return err
		}

		esResp = esScrollResponse{}
		if err := json.NewDecoder(scrollResp.Body).Decode(&esResp); err != nil {
			scrollResp.Body.Close()
			return err
		}
		scrollResp.Body.Close()
		scrollID = esResp.ScrollID
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

func (e *ESEngine) buildQueryBody(query string) []byte {
	if strings.HasPrefix(strings.TrimSpace(query), "{") {
		return []byte(query)
	}
	q := map[string]any{
		"query": map[string]any{
			"query_string": map[string]any{"query": query},
		},
	}
	body, _ := json.Marshal(q)
	return body
}

func (e *ESEngine) buildScrollQueryBody(query string, size int) []byte {
	if strings.HasPrefix(strings.TrimSpace(query), "{") {
		var q map[string]any
		json.Unmarshal([]byte(query), &q)
		q["size"] = size
		body, _ := json.Marshal(q)
		return body
	}
	q := map[string]any{
		"size": size,
		"query": map[string]any{
			"query_string": map[string]any{"query": query},
		},
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

type esScrollResponse struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []struct {
			Source map[string]any `json:"_source"`
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
