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

func (e *MongoEngine) Execute(ctx context.Context, query string, qopts *QueryOptions) (*Result, error) {
	req, err := parseMongoQuery(query)
	if err != nil {
		return nil, err
	}

	opts := options.Find()
	if qopts != nil {
		if qopts.Limit > 0 {
			opts.SetLimit(int64(qopts.Limit))
		}
		if qopts.Offset > 0 {
			opts.SetSkip(int64(qopts.Offset))
		}
		if len(qopts.Sort) > 0 {
			sortDoc := bson.D{}
			for _, f := range qopts.Sort {
				order := 1
				if f.Desc {
					order = -1
				}
				sortDoc = append(sortDoc, bson.E{Key: f.Column, Value: order})
			}
			opts.SetSort(sortDoc)
		}
	} else if req.Limit > 0 {
		opts.SetLimit(req.Limit)
	}

	cursor, err := e.db.Collection(req.Collection).Find(ctx, req.Filter, opts)
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

	result := &Result{Columns: cols, Rows: rows, Engine: "mongodb"}
	if qopts != nil && qopts.Limit > 0 && len(rows) == qopts.Limit {
		result.NextPage = &NextPage{
			Offset: qopts.Offset + qopts.Limit,
			Limit:  qopts.Limit,
		}
	}
	return result, nil
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
