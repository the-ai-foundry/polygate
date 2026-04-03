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

type CHEngine struct {
	url    string
	client *http.Client
}

func NewCHEngine(url string) *CHEngine {
	return &CHEngine{
		url: strings.TrimRight(url, "/"),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (e *CHEngine) Name() string { return "clickhouse" }

func (e *CHEngine) Execute(ctx context.Context, query string) (*Result, error) {
	fullQuery := query + " FORMAT JSON"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/", strings.NewReader(fullQuery))
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
		return nil, fmt.Errorf("clickhouse returned %d: %s", resp.StatusCode, string(body))
	}

	var chResp struct {
		Meta []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"meta"`
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&chResp); err != nil {
		return nil, fmt.Errorf("decoding clickhouse response: %w", err)
	}

	cols := make([]string, len(chResp.Meta))
	for i, m := range chResp.Meta {
		cols[i] = m.Name
	}

	return &Result{Columns: cols, Rows: chResp.Data, Engine: "clickhouse"}, nil
}

func (e *CHEngine) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.url+"/ping", nil)
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
