package query

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoEngine struct {
	db *mongo.Database
}

func NewMongoEngine(db *mongo.Database) *MongoEngine {
	return &MongoEngine{db: db}
}

func (e *MongoEngine) Name() string { return "mongodb" }

func (e *MongoEngine) Execute(ctx context.Context, query string) (*Result, error) {
	req, err := parseMongoQuery(query)
	if err != nil {
		return nil, err
	}

	cursor, err := e.db.Collection(req.Collection).Find(ctx, req.Filter)
	if err != nil {
		return nil, fmt.Errorf("mongodb find: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []map[string]any
	var cols []string
	for cursor.Next(ctx) {
		row := decodeBsonDoc(cursor)
		if len(cols) == 0 {
			for k := range row {
				cols = append(cols, k)
			}
		}
		rows = append(rows, row)
	}

	return &Result{Columns: cols, Rows: rows, Engine: "mongodb"}, nil
}

func (e *MongoEngine) ExecuteStream(ctx context.Context, query string, pageSize int, out chan<- StreamResult) error {
	defer close(out)

	req, err := parseMongoQuery(query)
	if err != nil {
		return err
	}

	opts := options.Find().SetBatchSize(int32(pageSize))
	cursor, err := e.db.Collection(req.Collection).Find(ctx, req.Filter, opts)
	if err != nil {
		return fmt.Errorf("mongodb find: %w", err)
	}
	defer cursor.Close(ctx)

	var cols []string
	var batch []map[string]any
	page := 0

	for cursor.Next(ctx) {
		row := decodeBsonDoc(cursor)
		if len(cols) == 0 {
			for k := range row {
				cols = append(cols, k)
			}
		}
		batch = append(batch, row)

		if len(batch) >= pageSize {
			page++
			out <- StreamResult{Columns: cols, Rows: batch, Page: page, Done: false, Engine: "mongodb"}
			batch = nil
		}
	}

	// Flush remaining.
	page++
	out <- StreamResult{Columns: cols, Rows: batch, Page: page, Done: true, Engine: "mongodb"}

	return cursor.Err()
}

func (e *MongoEngine) Ping(ctx context.Context) error {
	return e.db.Client().Ping(ctx, nil)
}

type mongoQueryRequest struct {
	Collection string `json:"collection"`
	Filter     bson.M `json:"filter"`
	Limit      int64  `json:"limit"`
}

func parseMongoQuery(query string) (*mongoQueryRequest, error) {
	var req mongoQueryRequest
	if err := json.Unmarshal([]byte(query), &req); err != nil {
		return nil, fmt.Errorf("mongodb query must be JSON: %w", err)
	}
	if req.Collection == "" {
		return nil, fmt.Errorf("mongodb query requires 'collection' field")
	}
	return &req, nil
}

func decodeBsonDoc(cursor *mongo.Cursor) map[string]any {
	var doc bson.M
	cursor.Decode(&doc)
	row := make(map[string]any, len(doc))
	for k, v := range doc {
		row[k] = v
	}
	return row
}
