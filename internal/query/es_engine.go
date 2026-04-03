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
	// Elasticsearch uses JSON query DSL. Accept raw JSON or a simple search string.
	var body []byte
	if strings.HasPrefix(strings.TrimSpace(query), "{") {
		body = []byte(query)
	} else {
		// Wrap as a simple query_string query.
		q := map[string]any{
			"query": map[string]any{
				"query_string": map[string]any{
					"query": query,
				},
			},
		}
		var err error
		body, err = json.Marshal(q)
		if err != nil {
			return nil, err
		}
	}

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

	var esResp struct {
		Hits struct {
			Hits []struct {
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
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
