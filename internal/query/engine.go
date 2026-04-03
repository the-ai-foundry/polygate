package query

import (
	"context"
	"fmt"
	"strings"
)

// SortField represents a single sort column and direction.
type SortField struct {
	Column string // column name
	Desc   bool   // true = descending, false = ascending
}

// QueryOptions controls pagination and sorting for query results.
type QueryOptions struct {
	Limit  int         // max rows to return (0 = no limit)
	Offset int         // rows to skip
	Sort   []SortField // sort fields
}

// ParseSort parses a sort string like "ts:desc,name:asc" into SortFields.
func ParseSort(s string) []SortField {
	if s == "" {
		return nil
	}
	var fields []SortField
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		col, dir, _ := strings.Cut(part, ":")
		col = strings.TrimSpace(col)
		dir = strings.TrimSpace(strings.ToLower(dir))
		fields = append(fields, SortField{
			Column: col,
			Desc:   dir == "desc",
		})
	}
	return fields
}

// SQLOrderBy generates an ORDER BY clause from sort fields.
func SQLOrderBy(sort []SortField) string {
	if len(sort) == 0 {
		return ""
	}
	var parts []string
	for _, f := range sort {
		dir := "ASC"
		if f.Desc {
			dir = "DESC"
		}
		parts = append(parts, fmt.Sprintf("%s %s", f.Column, dir))
	}
	return " ORDER BY " + strings.Join(parts, ", ")
}

// Result holds the output of a query execution.
type Result struct {
	Columns  []string         `json:"columns"`
	Rows     []map[string]any `json:"rows"`
	Engine   string           `json:"engine"`
	Total    int              `json:"total,omitempty"`
	NextPage *NextPage        `json:"next_page,omitempty"`
}

// NextPage tells the client how to fetch the next page.
type NextPage struct {
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

// QueryEngine can execute read queries against a database backend.
type QueryEngine interface {
	Name() string
	Execute(ctx context.Context, query string, opts *QueryOptions) (*Result, error)
	Ping(ctx context.Context) error
}
