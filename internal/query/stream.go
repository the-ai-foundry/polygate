package query

import "context"

// StreamResult represents a single page/chunk of streaming query results.
type StreamResult struct {
	Columns []string         `json:"columns,omitempty"`
	Rows    []map[string]any `json:"rows"`
	Page    int              `json:"page"`
	Done    bool             `json:"done"`
	Engine  string           `json:"engine"`
}

// StreamEngine can execute queries and return results in pages via a channel.
type StreamEngine interface {
	Name() string
	ExecuteStream(ctx context.Context, query string, pageSize int, out chan<- StreamResult) error
}
