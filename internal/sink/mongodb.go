package sink

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
)

type MongoDBSink struct {
	client     *mongo.Client
	db         *mongo.Database
	bulkSize   int
	maxRetries int
	retryBase  int
}

func NewMongoDBSink(ctx context.Context, dsn string, bulkSize, maxRetries, retryBase int) (*MongoDBSink, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(dsn))
	if err != nil {
		return nil, fmt.Errorf("connecting to mongodb: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("pinging mongodb: %w", err)
	}

	// Extract database name from URI path (e.g. mongodb://host:27017/mydb).
	dbName := "polygate"
	if parts := strings.SplitN(dsn, "?", 2); len(parts) > 0 {
		if idx := strings.LastIndex(parts[0], "/"); idx > 0 {
			if name := parts[0][idx+1:]; name != "" {
				dbName = name
			}
		}
	}

	return &MongoDBSink{
		client:     client,
		db:         client.Database(dbName),
		bulkSize:   bulkSize,
		maxRetries: maxRetries,
		retryBase:  retryBase,
	}, nil
}

func (s *MongoDBSink) Name() string { return "mongodb" }

func (s *MongoDBSink) CreateTable(ctx context.Context, ts *schema.TableSchema) error {
	// MongoDB auto-creates collections on first write.
	// Create indexes if specified.
	coll := s.db.Collection(ts.Table)
	for _, idx := range ts.Indexes {
		keys := bson.D{}
		for _, col := range idx.Columns {
			keys = append(keys, bson.E{Key: col, Value: 1})
		}
		_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: keys})
		if err != nil {
			return fmt.Errorf("creating mongodb index on %q: %w", ts.Table, err)
		}
	}
	slog.Debug("mongodb collection ready", "collection", ts.Table)
	return nil
}

func (s *MongoDBSink) WriteBatch(ctx context.Context, table string, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	coll := s.db.Collection(table)

	for i := 0; i < len(records); i += s.bulkSize {
		end := i + s.bulkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]

		docs := make([]any, len(chunk))
		for j, rec := range chunk {
			docs[j] = rec
		}

		err := WithRetry(ctx, s.maxRetries, s.retryBase, func() error {
			opts := options.InsertMany().SetOrdered(false)
			_, err := coll.InsertMany(ctx, docs, opts)
			return err
		})
		if err != nil {
			metrics.SinkErrorsTotal.WithLabelValues("mongodb").Inc()
			return fmt.Errorf("mongodb write to %q: %w", table, err)
		}
	}

	dur := time.Since(start)
	metrics.SinkWriteDuration.WithLabelValues("mongodb", table).Observe(dur.Seconds())
	metrics.SinkWriteRowsTotal.WithLabelValues("mongodb", table).Add(float64(len(records)))
	return nil
}

func (s *MongoDBSink) Ping(ctx context.Context) error {
	return s.client.Ping(ctx, nil)
}

func (s *MongoDBSink) Close() error {
	return s.client.Disconnect(context.Background())
}
