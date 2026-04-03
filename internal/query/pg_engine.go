package query

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PGEngine struct {
	pool *pgxpool.Pool
}

func NewPGEngine(pool *pgxpool.Pool) *PGEngine {
	return &PGEngine{pool: pool}
}

func (e *PGEngine) Name() string { return "postgres" }

func (e *PGEngine) Execute(ctx context.Context, query string) (*Result, error) {
	rows, err := e.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("postgres query: %w", err)
	}
	defer rows.Close()

	cols := make([]string, len(rows.FieldDescriptions()))
	for i, fd := range rows.FieldDescriptions() {
		cols[i] = fd.Name
	}

	var resultRows []map[string]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &Result{Columns: cols, Rows: resultRows, Engine: "postgres"}, nil
}

func (e *PGEngine) Ping(ctx context.Context) error {
	return e.pool.Ping(ctx)
}
