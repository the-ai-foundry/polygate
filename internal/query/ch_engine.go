package query

import (
	"bufio"
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

func (e *CHEngine) ExecuteStream(ctx context.Context, query string, pageSize int, out chan<- StreamResult) error {
	defer close(out)

	// Use JSONEachRow format — one JSON object per line, streamed.
	fullQuery := query + " FORMAT JSONEachRow"
	streamClient := &http.Client{Timeout: 0} // no timeout for streaming
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/", strings.NewReader(fullQuery))
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
		return fmt.Errorf("clickhouse returned %d: %s", resp.StatusCode, string(body))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	var cols []string
	var batch []map[string]any
	page := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			continue
		}

		if cols == nil {
			cols = make([]string, 0, len(row))
			for k := range row {
				cols = append(cols, k)
			}
		}

		batch = append(batch, row)

		if len(batch) >= pageSize {
			page++
			out <- StreamResult{Columns: cols, Rows: batch, Page: page, Done: false, Engine: "clickhouse"}
			batch = nil
		}
	}

	// Flush remaining rows.
	page++
	out <- StreamResult{Columns: cols, Rows: batch, Page: page, Done: true, Engine: "clickhouse"}

	return scanner.Err()
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
