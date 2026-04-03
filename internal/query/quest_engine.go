package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type QuestEngine struct {
	url    string
	client *http.Client
}

// NewQuestEngine creates a QuestDB query engine.
// questURL should be the QuestDB REST API endpoint (e.g., http://localhost:9000).
func NewQuestEngine(questURL string) *QuestEngine {
	return &QuestEngine{
		url: strings.TrimRight(questURL, "/"),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (e *QuestEngine) Name() string { return "questdb" }

func (e *QuestEngine) Execute(ctx context.Context, query string) (*Result, error) {
	reqURL := fmt.Sprintf("%s/exec?query=%s", e.url, url.QueryEscape(query))
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

	return &Result{Columns: cols, Rows: rows, Engine: "questdb"}, nil
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
