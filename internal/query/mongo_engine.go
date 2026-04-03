package query

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type MongoEngine struct {
	db *mongo.Database
}

func NewMongoEngine(db *mongo.Database) *MongoEngine {
	return &MongoEngine{db: db}
}

func (e *MongoEngine) Name() string { return "mongodb" }

func (e *MongoEngine) Execute(ctx context.Context, query string) (*Result, error) {
	// Expect JSON: {"collection": "name", "filter": {...}, "limit": 100}
	var req struct {
		Collection string   `json:"collection"`
		Filter     bson.M   `json:"filter"`
		Limit      int64    `json:"limit"`
		Sort       bson.D   `json:"sort"`
	}
	if err := json.Unmarshal([]byte(query), &req); err != nil {
		return nil, fmt.Errorf("mongodb query must be JSON: %w", err)
	}

	if req.Collection == "" {
		return nil, fmt.Errorf("mongodb query requires 'collection' field")
	}
	if req.Limit == 0 {
		req.Limit = 1000
	}

	coll := e.db.Collection(req.Collection)
	cursor, err := coll.Find(ctx, req.Filter)
	if err != nil {
		return nil, fmt.Errorf("mongodb find: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []map[string]any
	var cols []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(doc))
		for k, v := range doc {
			row[k] = v
		}
		if len(cols) == 0 {
			for k := range row {
				cols = append(cols, k)
			}
		}
		rows = append(rows, row)
	}

	return &Result{Columns: cols, Rows: rows, Engine: "mongodb"}, nil
}

func (e *MongoEngine) Ping(ctx context.Context) error {
	return e.db.Client().Ping(ctx, nil)
}
