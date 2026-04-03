package query

import "context"

// Result holds the output of a query execution.
type Result struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Engine  string           `json:"engine"`
}

// QueryEngine can execute read queries against a database backend.
type QueryEngine interface {
	Name() string
	Execute(ctx context.Context, query string) (*Result, error)
	Ping(ctx context.Context) error
}
