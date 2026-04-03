package query

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
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

func (e *PGEngine) ExecuteStream(ctx context.Context, query string, pageSize int, out chan<- StreamResult) error {
	defer close(out)

	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	cursorName := "polygate_stream_cursor"
	if _, err := tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorName, query)); err != nil {
		return fmt.Errorf("declare cursor: %w", err)
	}

	fetchSQL := fmt.Sprintf("FETCH %d FROM %s", pageSize, cursorName)
	page := 0
	var cols []string

	for {
		rows, err := tx.Query(ctx, fetchSQL)
		if err != nil {
			return fmt.Errorf("fetch: %w", err)
		}

		if cols == nil {
			cols = make([]string, len(rows.FieldDescriptions()))
			for i, fd := range rows.FieldDescriptions() {
				cols[i] = fd.Name
			}
		}

		var batch []map[string]any
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				rows.Close()
				return err
			}
			row := make(map[string]any, len(cols))
			for i, col := range cols {
				row[col] = vals[i]
			}
			batch = append(batch, row)
		}
		rows.Close()

		if len(batch) == 0 {
			out <- StreamResult{Columns: cols, Rows: nil, Page: page, Done: true, Engine: "postgres"}
			break
		}

		page++
		done := len(batch) < pageSize
		out <- StreamResult{Columns: cols, Rows: batch, Page: page, Done: done, Engine: "postgres"}
		if done {
			break
		}
	}

	return tx.Commit(ctx)
}

func (e *PGEngine) Ping(ctx context.Context) error {
	return e.pool.Ping(ctx)
}
