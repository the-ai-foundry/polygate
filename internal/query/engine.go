package query

import "context"

// PageOptions controls pagination for query results.
type PageOptions struct {
	Limit  int // max rows to return (0 = no limit)
	Offset int // rows to skip
}

// Result holds the output of a query execution.
type Result struct {
	Columns  []string         `json:"columns"`
	Rows     []map[string]any `json:"rows"`
	Engine   string           `json:"engine"`
	Total    int              `json:"total,omitempty"`     // total rows (if available)
	NextPage *NextPage        `json:"next_page,omitempty"` // pagination info
}

// NextPage tells the client how to fetch the next page.
type NextPage struct {
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

// QueryEngine can execute read queries against a database backend.
type QueryEngine interface {
	Name() string
	Execute(ctx context.Context, query string, page *PageOptions) (*Result, error)
	Ping(ctx context.Context) error
}
